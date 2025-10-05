import os, json, traceback, requests, time
from flask import Flask, request, jsonify, make_response
from typing import List, Dict, Any, Optional, Tuple
from io import StringIO
import csv

# ==== Config ====
BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN = os.environ.get("BL_TOKEN")
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")
WAREHOUSE_ID = "77617"        # your warehouse
TIMEOUT = 30

app = Flask(__name__)

# ==== Helpers ====

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
        raise RuntimeError(f"BL API error in {method}: {j['error']}")
    return j

def require_catalog_id() -> int:
    inv = os.environ.get("INVENTORY_ID")
    if not inv:
        raise RuntimeError("INVENTORY_ID not set")
    return int(inv)

def to_int(x) -> int:
    try: return int(x or 0)
    except: return 0

# ==== Product / ERP helpers ====

def find_catalog_product(sku: Optional[str] = None, ean: Optional[str] = None, include: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    inv_id = require_catalog_id()
    if sku:
        params = {"inventory_id": inv_id, "filter_sku": sku}
    elif ean:
        params = {"inventory_id": inv_id, "filter_ean": ean}
    else:
        return None
    if include:
        params["include"] = include
    resp = bl_call("getInventoryProductsList", params)
    prods = resp.get("products", {}) or {}
    for pid_str, pdata in prods.items():
        pdata = dict(pdata)
        pdata["product_id"] = int(pid_str)
        return pdata
    return None

def get_erp_units_for_product(pid: int) -> List[Dict[str, Any]]:
    """Return ERP (batch) units with price/expiry/batch/qty; earliest expiry first."""
    inv_id = require_catalog_id()
    resp = bl_call("getInventoryProductsData", {
        "inventory_id": inv_id,
        "products": [int(pid)],
        "include_erp_units": True
    })
    pdata = (resp.get("products") or {}).get(str(pid)) or {}
    units = pdata.get("erp_units") or []
    norm = []
    for u in units:
        norm.append({
            "price": u.get("price"),
            "expiry_date": u.get("expiry_date"),
            "batch": u.get("batch"),
            "qty": to_int(u.get("quantity") or u.get("qty")),
        })
    norm.sort(key=lambda u: (u["expiry_date"] or "9999-12-31"))
    return norm

def fetch_last_igr_unit(pid: int, lookback_days: int = 60) -> Optional[Dict[str, Any]]:
    """
    For setups where ERP units are not exposed by getInventoryProductsData,
    find the latest IGR (document_type=1) item for this product in this warehouse,
    and return {expiry_date, price, batch}.
    """
    inv_id = require_catalog_id()
    since = int(time.time()) - lookback_days * 24 * 3600
    page = 1
    latest = None
    while page <= 10:
        docs = bl_call("getInventoryDocumentsList", {
            "inventory_id": inv_id,
            "warehouse_id": int(WAREHOUSE_ID),
            "date_from": since,
            "page": page
        })
        rows = docs.get("documents", []) or []
        if not rows:
            break
        for d in rows:
            try:
                if int(d.get("document_type")) != 1:  # IGR
                    continue
                doc_id = int(d.get("document_id"))
                items = bl_call("getInventoryDocumentItems", {"document_id": doc_id})
                for it in (items.get("items") or []):
                    if int(it.get("product_id", 0)) == int(pid):
                        stamp = to_int(d.get("date_add") or d.get("date"))
                        rec = {
                            "doc_id": doc_id,
                            "ts": stamp,
                            "expiry_date": it.get("expiry_date"),
                            "price": it.get("price"),
                            "batch": it.get("batch") or "",
                        }
                        if (latest is None) or (rec["ts"] > latest["ts"]):
                            latest = rec
            except Exception:
                pass
        page += 1
    if latest:
        return {"expiry_date": latest["expiry_date"], "price": latest["price"], "batch": latest["batch"]}
    return None

# ==== Inventory documents ====

def create_document(document_type: int, warehouse_id: int) -> int:
    resp = bl_call("addInventoryDocument", {
        "inventory_id": require_catalog_id(),
        "warehouse_id": int(warehouse_id),
        "document_type": int(document_type)
    })
    if not resp.get("document_id"):
        raise RuntimeError(f"addInventoryDocument failed: {resp}")
    return int(resp["document_id"])

def add_items_verbose(document_id: int, lines: List[Dict[str, Any]]) -> Tuple[List[int], dict]:
    resp = bl_call("addInventoryDocumentItems", {"document_id": int(document_id), "items": lines})
    created = []
    for item in (resp.get("items") or []):
        if "item_id" in item:
            try: created.append(int(item["item_id"]))
            except: pass
    return created, resp

def confirm_document(document_id: int) -> None:
    bl_call("setInventoryDocumentStatusConfirmed", {"document_id": int(document_id)})

def get_location_name_by_id(location_id: str) -> Optional[str]:
    try:
        resp = bl_call("getInventoryLocations", {"warehouse_id": int(WAREHOUSE_ID)})
        for loc in (resp.get("locations") or []):
            if str(loc.get("location_id")) == str(location_id):
                return loc.get("name")
    except:
        pass
    return None

# ==== Transfer helpers ====

def build_erp_line_base(pid: int, qty: int, unit: Optional[Dict[str, Any]], bin_name: Optional[str] = None) -> Dict[str, Any]:
    line = {"product_id": pid, "quantity": qty}
    if bin_name: line["location_name"] = bin_name
    if unit:
        if unit.get("expiry_date"): line["expiry_date"] = unit["expiry_date"]
        if unit.get("price") is not None: line["price"] = unit["price"]
        if unit.get("batch"): line["batch"] = unit["batch"]
    return line

def issue_unallocated(pid: int, qty: int, igi_id: int) -> Tuple[int, List[Dict[str, Any]], str]:
    """Try: ERP units → last IGR unit → plain. Returns (moved_qty, fail_records, mode_used)."""
    fails = []
    units = get_erp_units_for_product(pid)
    if units:
        remaining, moved = qty, 0
        for u in units:
            if remaining <= 0: break
            take = min(remaining, to_int(u["qty"]))
            if take <= 0: continue
            created, raw = add_items_verbose(igi_id, [build_erp_line_base(pid, take, u)])
            if created:
                moved += take; remaining -= take
            else:
                fails.append({"product_id": pid, "attempt_qty": take, "src": None, "erp_unit": u, "response": raw})
                break
        if moved:
            return moved, fails, "unallocated_with_erp"
    last = fetch_last_igr_unit(pid)
    if last:
        created, raw = add_items_verbose(igi_id, [build_erp_line_base(pid, qty, last)])
        if created:
            return qty, fails, "unallocated_with_last_igr"
        fails.append({"product_id": pid, "attempt_qty": qty, "src": None, "last_igr": last, "response": raw})
    created, raw = add_items_verbose(igi_id, [{"product_id": pid, "quantity": qty}])
    if created:
        return qty, fails, "unallocated_plain"
    fails.append({"product_id": pid, "attempt_qty": qty, "src": None, "response": raw})
    return 0, fails, "unallocated_failed"

def issue_from_bin(pid: int, qty: int, bin_name: str, igi_id: int) -> Tuple[int, List[Dict[str, Any]], str]:
    fails = []
    units = get_erp_units_for_product(pid)
    if units:
        remaining, moved = qty, 0
        for u in units:
            if remaining <= 0: break
            take = min(remaining, to_int(u["qty"]))
            if take <= 0: continue
            created, raw = add_items_verbose(igi_id, [build_erp_line_base(pid, take, u, bin_name)])
            if created:
                moved += take; remaining -= take
            else:
                fails.append({"product_id": pid, "attempt_qty": take, "src": bin_name, "erp_unit": u, "response": raw})
                break
        if moved:
            return moved, fails, "bin_with_erp"
    last = fetch_last_igr_unit(pid)
    if last:
        created, raw = add_items_verbose(igi_id, [build_erp_line_base(pid, qty, last, bin_name)])
        if created:
            return qty, fails, "bin_with_last_igr"
        fails.append({"product_id": pid, "attempt_qty": qty, "src": bin_name, "last_igr": last, "response": raw})
    created, raw = add_items_verbose(igi_id, [{"product_id": pid, "quantity": qty, "location_name": bin_name}])
    if created:
        return qty, fails, "bin_plain"
    fails.append({"product_id": pid, "attempt_qty": qty, "src": bin_name, "response": raw})
    return 0, fails, "bin_failed"

# ==== ROUTES (inspect / seed / inspect_doc / probe / transfer) ====

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
        erp = get_erp_units_for_product(pid)
        return jsonify({"sku": sku, "product_id": pid, "erp_units": erp})
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

@app.get("/bl/seed_erp_unit")
def seed_erp_unit():
    """Create & confirm an IGR to seed an ERP unit for a SKU."""
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")

    sku = (request.args.get("sku") or "").strip()
    qty = to_int(request.args.get("qty"))
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

        igr_id = create_document(1, int(WAREHOUSE_ID))
        line = {"product_id": pid, "quantity": qty}
        if bin_name: line["location_name"] = bin_name
        if expiry:   line["expiry_date"] = expiry
        if price is not None:
            line["price"] = price
            line["purchase_price"] = price  # some accounts expect this
        created, raw = add_items_verbose(igr_id, [line])
        confirm_document(igr_id)

        # read back ERP units immediately
        erp_units_after = get_erp_units_for_product(pid)

        return jsonify({
            "ok": True,
            "igr_document_id": igr_id,
            "sku": sku,
            "qty": qty,
            "expiry_date": expiry,
            "price": price,
            "bin": bin_name,
            "erp_units_after": erp_units_after
        })
    except Exception as e:
        return http_error(500, "Internal error", detail=f"{e}\n{traceback.format_exc()}")

@app.get("/bl/inspect_doc")
def inspect_doc():
    """Inspect an inventory document and its items."""
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")
    doc_id = (request.args.get("doc_id") or "").strip()
    if not doc_id:
        return http_error(400, "Provide doc_id")
    try:
        header = bl_call("getInventoryDocumentsList", {
            "inventory_id": require_catalog_id(),
            "document_id": int(doc_id)
        })
        items = bl_call("getInventoryDocumentItems", {"document_id": int(doc_id)})
        return jsonify({"doc_id": int(doc_id), "document": header, "items": items})
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

@app.get("/bl/probe_issue")
def probe_issue():
    """DRAFT IGI; try adds with ERP units → last-IGR attrs → plain; does not confirm."""
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
        last_igr = fetch_last_igr_unit(pid)

        igi_id = create_document(3, int(WAREHOUSE_ID))
        attempts = []

        def try_line(unit=None, bin_name=None, mode=""):
            line = build_erp_line_base(pid, 1, unit, bin_name)
            created, raw = add_items_verbose(igi_id, [line])
            attempts.append({"mode": mode, "created": bool(created), "response": raw})

        # Unallocated paths
        if erp_units:
            try_line(erp_units[0], None, "unallocated_with_erp")
        else:
            if last_igr:
                try_line(last_igr, None, "unallocated_with_last_igr")
            else:
                try_line(None, None, "unallocated_plain")

        # Each bin paths
        for src in src_list:
            if erp_units:
                try_line(erp_units[0], src, f"bin_with_erp:{src}")
            else:
                if last_igr:
                    try_line(last_igr, src, f"bin_with_last_igr:{src}")
                else:
                    try_line(None, src, f"bin_plain:{src}")

        return jsonify({"sku": sku, "product_id": pid, "erp_units_seen": erp_units,
                        "last_igr_unit": last_igr, "draft_igi_id": igi_id, "attempts": attempts})
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

@app.get("/bl/transfer_order_qty_catalog")
def transfer_order_qty_catalog():
    """
    IGI (3) issue -> bin and/or unallocated (ERP-aware + last-IGR fallback)
    IGR (1) receipt -> dst bin
    Query:
      order_id=... or order_number=...
      dst_name=InternalStock
      src_names=BinA,BinB (optional)
      only_skus=CSV (optional)
      partial=1
      prefer_unallocated=1
      key=...
    """
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")

    order_id_param = (request.args.get("order_id") or "").strip() or None
    order_number   = (request.args.get("order_number") or "").strip() or None
    dst_loc_id     = (request.args.get("dst") or "").strip()
    dst_name       = (request.args.get("dst_name") or "").strip()
    src_names_raw  = (request.args.get("src_names") or "").strip()
    only_skus_raw  = (request.args.get("only_skus") or "").strip()
    partial        = (request.args.get("partial") or "").strip().lower() in ("1","true","yes")
    prefer_unalloc = (request.args.get("prefer_unallocated") or "").strip().lower() in ("1","true","yes")

    src_list = [s.strip() for s in src_names_raw.split(",") if s.strip()] if src_names_raw else []
    only_skus = [s.strip() for s in only_skus_raw.split(",") if s.strip()] if only_skus_raw else []

    if not dst_name:
        dst_name = get_location_name_by_id(dst_loc_id) if dst_loc_id else None
    if not dst_name:
        return http_error(400, "Destination not found. Use dst_name=<bin name>.")
    if not src_list and not prefer_unalloc:
        return http_error(400, "Specify src_names or set prefer_unallocated=1")

    def resolve_order_id(order_id: Optional[str], order_number: Optional[str]) -> str:
        if order_id: return str(order_id).strip()
        if not order_number: raise ValueError("Provide order_id or order_number")
        needle = str(order_number).strip()
        date_from = int(time.time()) - 60 * 24 * 60 * 60
        matches, page = [], 1
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
        if not matches: raise LookupError(f"Order with order_number/id '{order_number}' not found")
        matches.sort(key=lambda o: (to_int(o.get("date_add")), to_int(o.get("order_id"))), reverse=True)
        return str(matches[0].get("order_id"))

    def get_order_by_id_strict(oid: str) -> dict:
        resp = bl_call("getOrders", {"order_id": str(oid), "get_unconfirmed_orders": True})
        orders = resp.get("orders", []) or []
        if orders: return orders[0]
        raise LookupError(f"Order not found by order_id {oid}")

    try:
        order_id = resolve_order_id(order_id_param, order_number)
        order = get_order_by_id_strict(order_id)
        items = order.get("products", []) or []
        if not items: return http_error(400, "Order has no products")

        base_lines, missing, skus_in_scope = [], [], []
        for it in items:
            sku = (it.get("sku") or it.get("product_sku") or "").strip()
            if only_skus and sku not in only_skus:
                continue
            qty = to_int(it.get("quantity") or it.get("qty"))
            if qty <= 0: continue
            rec = find_catalog_product(sku=sku, include=["locations","stock"])
            if not rec:
                missing.append({"sku": sku, "ean": it.get("ean")})
                continue
            base_lines.append({"sku": sku, "product_id": int(rec["product_id"]), "qty": qty})
            skus_in_scope.append(sku)

        if not base_lines:
            return http_error(400, f"No transferrable items. Missing: {missing}, only_skus={only_skus}")

        igi_id = create_document(3, int(WAREHOUSE_ID))
        issued_per_product: Dict[int, int] = {}
        total_issued = 0
        fail_reasons: List[Dict[str, Any]] = []
        modes_used: Dict[int, str] = {}

        for line in base_lines:
            pid, remaining = line["product_id"], line["qty"]

            if prefer_unalloc and remaining > 0:
                moved, fails, mode = issue_unallocated(pid, remaining, igi_id)
                if moved:
                    issued_per_product[pid] = issued_per_product.get(pid, 0) + moved
                    total_issued += moved
                    remaining -= moved
                    modes_used[pid] = mode
                fail_reasons.extend(fails)

            if remaining > 0 and src_list:
                for src in src_list:
                    if remaining <= 0: break
                    moved, fails, mode = issue_from_bin(pid, remaining, src, igi_id)
                    if moved:
                        issued_per_product[pid] = issued_per_product.get(pid, 0) + moved
                        total_issued += moved
                        remaining -= moved
                        modes_used[pid] = mode
                        break
                    fail_reasons.extend(fails)

            if remaining > 0 and not prefer_unalloc:
                moved, fails, mode = issue_unallocated(pid, remaining, igi_id)
                if moved:
                    issued_per_product[pid] = issued_per_product.get(pid, 0) + moved
                    total_issued += moved
                    remaining -= moved
                    modes_used[pid] = mode
                fail_reasons.extend(fails)

            if remaining > 0:
                fail_reasons.append({"product_id": pid, "remaining_unissued": remaining})

        if total_issued == 0:
            ctx = {
                "order_id": order_id,
                "skus_in_scope": skus_in_scope,
                "prefer_unallocated": prefer_unalloc,
                "src_list": src_list,
                "fail_reasons": fail_reasons
            }
            return http_error(400, "IGI could not issue any items", detail=json.dumps(ctx))

        confirm_document(igi_id)

        igr_id = create_document(1, int(WAREHOUSE_ID))
        igr_lines = [{"product_id": pid, "quantity": qty, "location_name": dst_name}
                     for pid, qty in issued_per_product.items() if qty > 0]
        created, raw = add_items_verbose(igr_id, igr_lines)
        if not created:
            fail = {"igr_add_failed": raw, "lines": igr_lines}
            return http_error(400, "IGR failed to add items.", detail=json.dumps(fail))
        confirm_document(igr_id)

        return jsonify({
            "ok": True,
            "igi_document_id": igi_id,
            "igr_document_id": igr_id,
            "moved_units": total_issued,
            "missing": missing,
            "sources_used": src_list,
            "filtered_skus": skus_in_scope,
            "prefer_unallocated": prefer_unalloc,
            "modes_used": modes_used
        })
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

# ==== NEW: Export order to CSV for manual transfer ====

@app.get("/bl/export_order_csv")
def export_order_csv():
    """
    Download a CSV with the order's line items for manual internal transfer.
    Usage:
      /bl/export_order_csv?order_id=12345678&key=YOUR_SHARED_KEY
      or
      /bl/export_order_csv?order_number=21123456&key=YOUR_SHARED_KEY

    Optional flags:
      include_bins=1           -> tries to suggest the first catalog location/bin (if available)
      dst_name=InternalStock   -> destination bin name in CSV (default: InternalStock)
    """
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")

    order_id_param = (request.args.get("order_id") or "").strip() or None
    order_number   = (request.args.get("order_number") or "").strip() or None
    include_bins   = (request.args.get("include_bins") or "").strip().lower() in ("1","true","yes")
    dst_name       = (request.args.get("dst_name") or "").strip() or "InternalStock"

    def to_int_local(x):
        try: return int(x or 0)
        except: return 0

    def resolve_order_id(order_id: Optional[str], order_number: Optional[str]) -> str:
        if order_id:
            return str(order_id).strip()
        if not order_number:
            raise ValueError("Provide order_id or order_number")
        needle = str(order_number).strip()
        date_from = int(time.time()) - 60 * 24 * 60 * 60
        matches, page = [], 1
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
        matches.sort(key=lambda o: (to_int_local(o.get("date_add")), to_int_local(o.get("order_id"))), reverse=True)
        return str(matches[0].get("order_id"))

    def catalog_lookup_for_sku(sku: str):
        try:
            rec = find_catalog_product(sku=sku, include=["locations","stock"])
            return rec
        except:
            return None

    try:
        oid = resolve_order_id(order_id_param, order_number)
        order_resp = bl_call("getOrders", {"order_id": oid, "get_unconfirmed_orders": True})
        orders = order_resp.get("orders", []) or []
        if not orders:
            return http_error(404, f"Order not found: {oid}")
        order = orders[0]

        lines = order.get("products", []) or []
        if not lines:
            return http_error(400, "Order has no products")

        buf = StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "OrderID",
            "SKU",
            "EAN",
            "Product Name",
            "Qty to Move",
            "Catalog Product ID",
            "Suggested Src Bin",
            "Dst Bin",
            "Warehouse ID"
        ])

        for it in lines:
            sku = (it.get("sku") or it.get("product_sku") or "").strip()
            ean = (it.get("ean") or it.get("product_ean") or "").strip()
            name = (it.get("name") or it.get("product_name") or "").strip()
            qty  = to_int_local(it.get("quantity") or it.get("qty"))

            pid = ""
            src_bin = ""
            if sku:
                rec = catalog_lookup_for_sku(sku)
                if rec:
                    pid = str(rec.get("product_id") or "")
                    if include_bins:
                        locs = rec.get("locations")
                        if isinstance(locs, list) and locs:
                            for loc in locs:
                                n = (loc.get("name") or "").strip()
                                if n:
                                    src_bin = n
                                    break

            writer.writerow([
                oid,
                sku,
                ean,
                name,
                qty,
                pid,
                src_bin,
                dst_name,
                WAREHOUSE_ID
            ])

        resp = make_response(buf.getvalue())
        resp.headers["Content-Type"] = "text/csv"
        resp.headers["Content-Disposition"] = f"attachment; filename=order_{oid}_transfer.csv"
        return resp

    except Exception as e:
        return http_error(500, "Internal error", detail=f"{e}\n{traceback.format_exc()}")

@app.get("/health")
def health():
    return jsonify({"ok": True, "version": "ERP-aware build v1.3 + export_order_csv"})
