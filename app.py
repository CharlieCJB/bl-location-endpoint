# app.py — BaseLinker catalog stock + storage documents (IGI + IGR for intra-warehouse relocation)
import os, json, time, traceback
from typing import Optional, List, Dict, Any
from flask import Flask, request, jsonify, make_response
import requests

BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN   = os.environ.get("BL_TOKEN")
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")

# ---- YOUR SETUP ----
WAREHOUSE_ID = "77617"                               # your warehouse id
CATALOG_ID   = os.environ.get("INVENTORY_ID")        # BaseLinker catalog (inventory_id) — REQUIRED, numeric
TIMEOUT      = 30

app = Flask(__name__)

# -------------------- helpers --------------------
def http_error(status: int, msg: str, detail: str = ""):
    payload = {"error": msg}
    if detail:
        payload["detail"] = detail
    return make_response(jsonify(payload), status)

def bl_call(method: str, params: dict) -> dict:
    if not BL_TOKEN:
        raise RuntimeError("BL_TOKEN not set")
    headers = {"X-BLToken": BL_TOKEN}
    data = {"method": method, "parameters": json.dumps(params)}
    r = requests.post(BL_API_URL, headers=headers, data=data, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    if isinstance(j, dict) and j.get("error"):
        raise RuntimeError(f"BaseLinker API error in {method}: {j['error']}")
    return j

def to_int(x) -> int:
    try: return int(x or 0)
    except: return 0

def require_catalog_id() -> int:
    cid = os.environ.get("INVENTORY_ID")
    if not cid:
        raise RuntimeError("INVENTORY_ID env var (BaseLinker catalog id) is required.")
    try:
        return int(cid)
    except:
        raise RuntimeError("INVENTORY_ID must be numeric (your BaseLinker catalog id).")

# -------------------- orders ---------------------
def resolve_order_id(order_id: Optional[str], order_number: Optional[str]) -> str:
    if order_id:
        return str(order_id).strip()
    if not order_number:
        raise ValueError("Provide order_id or order_number")
    needle = str(order_number).strip()
    date_from = int(time.time()) - 60 * 24 * 60 * 60
    matches = []
    page = 1
    while page <= 300:
        resp = bl_call("getOrders", {"date_from": date_from, "get_unconfirmed_orders": True, "page": page})
        rows = resp.get("orders", []) or []
        if not rows: break
        for o in rows:
            o_num = str(o.get("order_number", "")).strip()
            o_id  = str(o.get("order_id", "")).strip()
            if (o_num and o_num == needle) or (not o_num and o_id == needle):
                matches.append(o)
        page += 1
    if not matches:
        raise LookupError(f"Order with order_number/id '{order_number}' not found")
    matches.sort(key=lambda o: (to_int(o.get("date_add")), to_int(o.get("date_confirmed")), to_int(o.get("order_id"))), reverse=True)
    return str(matches[0].get("order_id"))

def get_order_by_id_strict(order_id: str) -> dict:
    oid = str(order_id).strip()
    try:
        resp = bl_call("getOrders", {"order_id": oid, "get_unconfirmed_orders": True})
        orders = resp.get("orders", []) or []
        if orders: return orders[0]
    except: pass
    date_from = int(time.time()) - 60 * 24 * 60 * 60
    page = 1
    while page <= 300:
        resp = bl_call("getOrders", {"date_from": date_from, "get_unconfirmed_orders": True, "page": page})
        rows = resp.get("orders", []) or []
        if not rows: break
        for o in rows:
            if str(o.get("order_id", "")).strip() == oid:
                return o
        page += 1
    raise LookupError(f"Order not found by order_id {oid}")

# ----- catalog product lookup (by SKU/EAN) -----
def find_catalog_product_id_by_sku(sku: str) -> Optional[int]:
    inv_id = require_catalog_id()
    if not sku: return None
    resp = bl_call("getInventoryProductsList", {"inventory_id": inv_id, "filter_sku": sku})
    prods = resp.get("products", {}) or {}
    for pid_str, pdata in prods.items():
        if (pdata.get("sku") or "").strip() == sku.strip():
            try: return int(pid_str)
            except: return None
    return None

def find_catalog_product_id_by_ean(ean: str) -> Optional[int]:
    inv_id = require_catalog_id()
    if not ean: return None
    resp = bl_call("getInventoryProductsList", {"inventory_id": inv_id, "filter_ean": ean})
    prods = resp.get("products", {}) or {}
    for pid_str, pdata in prods.items():
        if (pdata.get("ean") or "").strip() == ean.strip():
            try: return int(pid_str)
            except: return None
    return None

def resolve_catalog_product_id_from_order_item(it: Dict[str, Any]) -> Optional[int]:
    sku = (it.get("sku") or it.get("product_sku") or "").strip()
    ean = (it.get("ean") or it.get("product_ean") or "").strip()
    pid = find_catalog_product_id_by_sku(sku) if sku else None
    if pid: return pid
    pid = find_catalog_product_id_by_ean(ean) if ean else None
    return pid

# ------------- warehouse / bin helpers -------------
def get_location_name_by_id(location_id: str) -> Optional[str]:
    """Map numeric location_id -> location_name. Try locations endpoint first, then warehouses."""
    loc_id = str(location_id).strip()
    try:
        resp = bl_call("getInventoryLocations", {"warehouse_id": int(WAREHOUSE_ID)})
        for loc in (resp.get("locations") or []):
            if str(loc.get("location_id")) == loc_id:
                return (loc.get("name") or "").strip()
    except Exception:
        pass
    try:
        resp = bl_call("getInventoryWarehouses", {})
        for wh in (resp.get("warehouses") or []):
            if str(wh.get("warehouse_id")) != str(WAREHOUSE_ID):
                continue
            for loc in (wh.get("locations") or []):
                if str(loc.get("location_id")) == loc_id:
                    return (loc.get("name") or "").strip()
    except Exception:
        pass
    return None

# ---------------- documents (IGI + IGR) ----------------
def create_document(document_type: int, warehouse_id: int) -> int:
    """
    Create an inventory document (draft) in catalog stock system.
    document_type:
      1 = IGR (Internal Goods Receipt)
      3 = IGI (Internal Goods Issue)
      2 = GI  (Goods Issue)        # not used here
      4 = IT  (Internal Transfer)  # requires different source & target warehouses
    """
    inv_id = require_catalog_id()
    payload = {
        "inventory_id": inv_id,
        "warehouse_id": int(warehouse_id),
        "document_type": int(document_type)
    }
    resp = bl_call("addInventoryDocument", payload)
    doc_id = resp.get("document_id")
    if not doc_id:
        raise RuntimeError(f"addInventoryDocument returned no document_id. Response: {resp}")
    return int(doc_id)

def add_items_to_document(document_id: int, lines: List[Dict[str, Any]]) -> List[int]:
    payload = {"document_id": int(document_id), "items": lines}
    resp = bl_call("addInventoryDocumentItems", payload)
    created = []
    for item in (resp.get("items") or []):
        if "item_id" in item:
            try: created.append(int(item["item_id"]))
            except: pass
    return created

def confirm_document(document_id: int) -> None:
    bl_call("setInventoryDocumentStatusConfirmed", {"document_id": int(document_id)})

# -------------------- core endpoint --------------------
@app.get("/bl/transfer_order_qty_catalog")
def transfer_order_qty_catalog():
    """
    Relocate ONLY the ordered qty inside the SAME warehouse via two documents:
      1) IGI (3) — issue from warehouse (decrease)
      2) IGR (1) — receipt into destination location (increase)
    Destination can be provided as:
      - dst (numeric location_id), or
      - dst_name (string location name)
    """
    try:
        supplied = request.args.get("key") or request.headers.get("X-App-Key")
        if SHARED_KEY and supplied != SHARED_KEY:
            return http_error(401, "Unauthorized: key mismatch")

        order_id_param = (request.args.get("order_id") or "").strip() or None
        order_number   = (request.args.get("order_number") or "").strip() or None
        dst_loc_id     = (request.args.get("dst") or "").strip()
        dst_name       = (request.args.get("dst_name") or "").strip()

        # Resolve order & items
        order_id = resolve_order_id(order_id_param, order_number)
        order = get_order_by_id_strict(order_id)
        items = order.get("products", []) or []
        if not items:
            return http_error(400, "Order has no products")

        # Resolve destination location_name
        loc_name = dst_name or (get_location_name_by_id(dst_loc_id) if dst_loc_id else None)
        if not loc_name:
            return http_error(400,
                "Destination not found. Pass a valid numeric 'dst' (location_id) or 'dst_name'. "
                "Tip: use the exact location name as in BaseLinker."
            )

        # Build lines (catalog product_id + ordered qty)
        missing, lines = [], []
        for it in items:
            qty = it.get("quantity") or it.get("qty") or 0
            try: qty = int(qty)
            except: qty = 0
            if qty <= 0:
                continue
            pid = resolve_catalog_product_id_from_order_item(it)
            if not pid:
                missing.append({
                    "sku": (it.get("sku") or it.get("product_sku") or "").strip(),
                    "ean": (it.get("ean") or it.get("product_ean") or "").strip()
                })
                continue
            lines.append({
                "product_id": pid,
                "quantity": qty
                # IGI lines: no location_name (decrease from warehouse pool)
            })

        if not lines:
            return http_error(400, f"No transferrable items (catalog product_id match not found). Missing: {missing}")

        # 1) IGI: decrease from warehouse (no location_name)
        igi_id = create_document(document_type=3, warehouse_id=int(WAREHOUSE_ID))  # IGI
        _igi_items = add_items_to_document(igi_id, lines)
        confirm_document(igi_id)

        # 2) IGR: increase into destination location_name
        igr_lines = []
        for l in lines:
            igr_lines.append({
                "product_id": l["product_id"],
                "quantity": l["quantity"],
                "location_name": loc_name  # put into the target bin
            })
        igr_id = create_document(document_type=1, warehouse_id=int(WAREHOUSE_ID))  # IGR
        _igr_items = add_items_to_document(igr_id, igr_lines)
        confirm_document(igr_id)

        return jsonify({
            "ok": True,
            "message": f"Relocated ordered qty for order {order_id} into '{loc_name}' using IGI+IGR.",
            "igi_document_id": igi_id,
            "igr_document_id": igr_id,
            "moved_units": sum(l["quantity"] for l in lines),
            "missing": missing
        })
    except Exception as e:
        return http_error(500, "Internal error", detail=f"{e.__class__.__name__}: {e}\n{traceback.format_exc()}")

# ---------------- utilities / debug ----------------
@app.get("/bl/recent_orders")
def recent_orders():
    try:
        supplied = request.args.get("key") or request.headers.get("X-App-Key")
        if SHARED_KEY and supplied != SHARED_KEY:
            return http_error(401, "Unauthorized")
        days = int(request.args.get("days", "2"))
        limit = int(request.args.get("limit", "50"))
        date_from = int(time.time()) - max(1, days) * 24 * 60 * 60
        results, page = [], 1
        while len(results) < limit and page <= 200:
            resp = bl_call("getOrders", {"date_from": date_from, "get_unconfirmed_orders": True, "page": page})
            rows = resp.get("orders", []) or []
            if not rows: break
            for o in rows:
                if len(results) >= limit: break
                results.append({
                    "order_id": str(o.get("order_id")),
                    "order_number": str(o.get("order_number")),
                    "date_add": to_int(o.get("date_add")),
                    "date_confirmed": to_int(o.get("date_confirmed")),
                    "status_id": o.get("status_id"),
                    "shop_id": o.get("shop_id"),
                })
            page += 1
        return jsonify({"count": len(results), "orders": results})
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

@app.get("/bl/find_order")
def find_order():
    try:
        supplied = request.args.get("key") or request.headers.get("X-App-Key")
        if SHARED_KEY and supplied != SHARED_KEY:
            return http_error(401, "Unauthorized")
        order_number = (request.args.get("order_number") or "").strip()
        if not order_number:
            return http_error(400, "Provide order_number")
        needle = order_number
        days = int(request.args.get("days", "60"))
        date_from = int(time.time()) - max(1, days) * 24 * 60 * 60
        matches, page = [], 1
        while page <= 300 and len(matches) < 200:
            resp = bl_call("getOrders", {"date_from": date_from, "get_unconfirmed_orders": True, "page": page})
            rows = resp.get("orders", []) or []
            if not rows: break
            for o in rows:
                o_num = str(o.get("order_number", "")).strip()
                o_id  = str(o.get("order_id", "")).strip()
                if (o_num and o_num == needle) or (not o_num and o_id == needle):
                    matches.append(o)
            page += 1
        matches.sort(key=lambda o: (to_int(o.get("date_add")), to_int(o.get("date_confirmed")), to_int(o.get("order_id"))), reverse=True)
        out = [{
            "order_id": str(o.get("order_id")),
            "order_number": str(o.get("order_number")),
            "date_add": to_int(o.get("date_add")),
            "date_confirmed": to_int(o.get("date_confirmed")),
            "status_id": o.get("status_id"),
            "shop_id": o.get("shop_id"),
        } for o in matches]
        return jsonify({"count": len(out), "matches": out})
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

@app.get("/bl/locations")
def list_locations():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")
    try:
        out = {"warehouse_id": WAREHOUSE_ID, "locations": []}
        try:
            resp = bl_call("getInventoryLocations", {"warehouse_id": int(WAREHOUSE_ID)})
            out["locations"] = resp.get("locations") or []
            if out["locations"]:
                return jsonify(out)
        except Exception:
            pass
        resp = bl_call("getInventoryWarehouses", {})
        for wh in (resp.get("warehouses") or []):
            if str(wh.get("warehouse_id")) == str(WAREHOUSE_ID):
                out["locations"] = wh.get("locations") or []
                break
        return jsonify(out)
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

@app.get("/bl/inventories")
def list_inventories():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")
    try:
        resp = bl_call("getInventories", {})
        return jsonify(resp)
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

@app.get("/health")
def health():
    return "OK\n"
