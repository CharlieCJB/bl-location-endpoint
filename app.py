import os, json, traceback, requests
from flask import Flask, request, jsonify, make_response

BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN = os.environ.get("BL_TOKEN")
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")
WAREHOUSE_ID = "77617"
TIMEOUT = 30

app = Flask(__name__)

# ------------------ Helper functions ------------------

def http_error(status, msg, detail=""):
    payload = {"error": msg}
    if detail:
        payload["detail"] = detail
    return make_response(jsonify(payload), status)

def bl_call(method, params):
    headers = {"X-BLToken": BL_TOKEN}
    data = {"method": method, "parameters": json.dumps(params)}
    r = requests.post(BL_API_URL, headers=headers, data=data, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    if isinstance(j, dict) and j.get("error"):
        raise RuntimeError(f"BL API error {method}: {j['error']}")
    return j

def require_catalog_id():
    inv_id = os.environ.get("INVENTORY_ID")
    if not inv_id:
        raise RuntimeError("INVENTORY_ID not set")
    return int(inv_id)

# ------------------ Inventory helpers ------------------

def get_erp_units_for_product(pid):
    inv_id = require_catalog_id()
    resp = bl_call("getInventoryProductsData", {
        "inventory_id": inv_id,
        "products": [int(pid)],
        "include_erp_units": True
    })
    pdata = (resp.get("products") or {}).get(str(pid)) or {}
    units = pdata.get("erp_units") or []
    result = []
    for u in units:
        result.append({
            "price": u.get("price"),
            "expiry_date": u.get("expiry_date"),
            "batch": u.get("batch"),
            "qty": int(u.get("quantity") or u.get("qty") or 0)
        })
    result.sort(key=lambda u: (u["expiry_date"] or "9999-12-31"))
    return result

def create_document(doc_type, warehouse_id):
    resp = bl_call("addInventoryDocument", {
        "inventory_id": require_catalog_id(),
        "warehouse_id": int(warehouse_id),
        "document_type": int(doc_type)
    })
    return int(resp["document_id"])

def add_items_verbose(doc_id, lines):
    resp = bl_call("addInventoryDocumentItems", {"document_id": int(doc_id), "items": lines})
    return resp.get("items", []), resp

def confirm_document(doc_id):
    bl_call("setInventoryDocumentStatusConfirmed", {"document_id": int(doc_id)})

def find_catalog_product(sku, include=None):
    inv_id = require_catalog_id()
    resp = bl_call("getInventoryProductsList", {"inventory_id": inv_id, "filter_sku": sku})
    prods = resp.get("products", {}) or {}
    if not prods:
        return None
    first = list(prods.values())[0]
    first["product_id"] = int(list(prods.keys())[0])
    return first

# ------------------ ROUTES ------------------

@app.get("/bl/inspect_sku")
def inspect_sku():
    sku = (request.args.get("sku") or "").strip()
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")
    if not sku:
        return http_error(400, "Provide sku")

    try:
        rec = find_catalog_product(sku=sku)
        if not rec:
            return http_error(404, "Product not found")
        pid = rec["product_id"]
        units = get_erp_units_for_product(pid)
        return jsonify({"sku": sku, "product_id": pid, "erp_units": units})
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

@app.get("/bl/seed_erp_unit")
def seed_erp_unit():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")

    sku = (request.args.get("sku") or "").strip()
    qty = int(request.args.get("qty") or 0)
    expiry = (request.args.get("expiry_date") or "").strip() or None
    price_raw = (request.args.get("price") or "").strip()
    price = float(price_raw) if price_raw else None
    bin_name = (request.args.get("bin") or "").strip() or None

    if not sku or qty <= 0:
        return http_error(400, "Provide sku and qty>0")

    try:
        rec = find_catalog_product(sku=sku)
        if not rec:
            return http_error(404, f"SKU {sku} not found")
        pid = rec["product_id"]

        igr_id = create_document(1, WAREHOUSE_ID)
        line = {"product_id": pid, "quantity": qty}
        if bin_name:
            line["location_name"] = bin_name
        if expiry:
            line["expiry_date"] = expiry
        if price is not None:
            line["price"] = price

        created, raw = add_items_verbose(igr_id, [line])
        confirm_document(igr_id)
        return jsonify({"ok": True, "igr_document_id": igr_id, "sku": sku, "qty": qty,
                        "expiry_date": expiry, "price": price, "bin": bin_name})
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

@app.get("/bl/probe_issue")
def probe_issue():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")

    sku = (request.args.get("sku") or "").strip()
    src_names = (request.args.get("src_names") or "").strip()
    src_list = [s.strip() for s in src_names.split(",") if s.strip()]

    if not sku:
        return http_error(400, "Provide sku")

    try:
        rec = find_catalog_product(sku=sku)
        if not rec:
            return http_error(404, f"SKU {sku} not found")
        pid = rec["product_id"]
        erp_units = get_erp_units_for_product(pid)

        igi_id = create_document(3, WAREHOUSE_ID)
        attempts = []

        def make_line(unit=None, bin_name=None):
            line = {"product_id": pid, "quantity": 1}
            if bin_name:
                line["location_name"] = bin_name
            if unit:
                if unit.get("expiry_date"):
                    line["expiry_date"] = unit["expiry_date"]
                if unit.get("price") is not None:
                    line["price"] = unit["price"]
                if unit.get("batch"):
                    line["batch"] = unit["batch"]
            return line

        # Unallocated first
        if erp_units:
            created, raw = add_items_verbose(igi_id, [make_line(erp_units[0])])
            attempts.append({"mode": "unallocated_with_erp", "created": bool(created), "response": raw})
        else:
            created, raw = add_items_verbose(igi_id, [make_line()])
            attempts.append({"mode": "unallocated_plain", "created": bool(created), "response": raw})

        # Then each bin
        for src in src_list:
            if erp_units:
                created, raw = add_items_verbose(igi_id, [make_line(erp_units[0], src)])
                attempts.append({"mode": f"bin_with_erp:{src}", "created": bool(created), "response": raw})
            else:
                created, raw = add_items_verbose(igi_id, [make_line(None, src)])
                attempts.append({"mode": f"bin_plain:{src}", "created": bool(created), "response": raw})

        return jsonify({"sku": sku, "product_id": pid, "erp_units_seen": erp_units,
                        "draft_igi_id": igi_id, "attempts": attempts})
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

@app.get("/health")
def health():
    return jsonify({"ok": True, "version": "ERP-aware build v1.0"})
