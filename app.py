# app.py — BaseLinker catalog stock: relocate ordered qty via IGI + IGR (first-fit bins, optional partials)
import os, json, time, traceback
from typing import Optional, List, Dict, Any
from flask import Flask, request, jsonify, make_response
import requests

BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN   = os.environ.get("BL_TOKEN")
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")

# ---- YOUR SETUP ----
WAREHOUSE_ID = "77617"                         # your warehouse id
CATALOG_ID   = os.environ.get("INVENTORY_ID")  # BaseLinker catalog (inventory_id) — REQUIRED, numeric
TIMEOUT      = 30

app = Flask(__name__)

# ---------------- helpers ----------------
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

# ---------------- orders ----------------
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
    except:
        pass
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

# -------- catalog product lookup (by SKU/EAN) --------
def find_catalog_product_id_by_sku(sku: str, include: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    inv_id = require_catalog_id()
    if not sku: return None
    params = {"inventory_id": inv_id, "filter_sku": sku}
    if include: params["include"] = include
    resp = bl_call("getInventoryProductsList", params)
    prods = resp.get("products", {}) or {}
    for pid_str, pdata in prods.items():
        if (pdata.get("sku") or "").strip() == sku.strip():
            pdata = dict(pdata)
            pdata["product_id"] = int(pid_str)
            return pdata
    return None

def find_catalog_product_id_by_ean(ean: str, include: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    inv_id = require_catalog_id()
    if not ean: return None
    params = {"inventory_id": inv_id, "filter_ean": ean}
    if include: params["include"] = include
    resp = bl_call("getInventoryProductsList", params)
    prods = resp.get("products", {}) or {}
    for pid_str, pdata in prods.items():
        if (pdata.get("ean") or "").strip() == ean.strip():
            pdata = dict(pdata)
            pdata["product_id"] = int(pid_str)
            return pdata
    return None

def resolve_catalog_product_from_order_item(it: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # include locations/stock for debugging if BL returns them
    include = ["locations", "stock"]
    sku = (it.get("sku") or it.get("product_sku") or "").strip()
    ean = (it.get("ean") or it.get("product_ean") or "").strip()
    rec = find_catalog_product_id_by_sku(sku, include=include) if sku else None
    if rec: return rec
    rec = find_catalog_product_id_by_ean(ean, include=include) if ean else None
    return rec

# -------- warehouse / bin helpers --------
def get_location_name_by_id(location_id: str) -> Optional[str]:
    """Map numeric location_id -> location_name. Try locations endpoint first, then warehouses."""
    loc_id = str(location_id).strip()
    # Preferred: explicit locations
    try:
        resp = bl_call("getInventoryLocations", {"warehouse_id": int(WAREHOUSE_ID)})
        for loc in (resp.get("locations") or []):
            if str(loc.get("location_id")) == loc_id:
                return (loc.get("name") or "").strip()
    except Exception:
        pass
    # Fallback: nested in warehouses
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
      4 = IT  (Internal Transfer)  # requires different source/target warehouses; not used here
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

# ------------- debug helper: catalog locations for SKU -------------
@app.get("/bl/catalog_locations_for_sku")
def catalog_locations_for_sku():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")
    sku = (request.args.get("sku") or "").strip()
    if not sku:
        return http_error(400, "Provide sku")
    try:
        rec = find_catalog_product_id_by_sku(sku, include=["locations", "stock"])
        if not rec:
            return http_error(404, f"SKU '{sku}' not found in catalog {require_catalog_id()}")
        return jsonify({
            "product_id": rec.get("product_id"),
            "sku": rec.get("sku"),
            "ean": rec.get("ean"),
            "locations": rec.get("locations"),   # if BL returns
            "stock": rec.get("stock")            # if BL returns
        })
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

# ---------------- core endpoint ----------------
@app.get("/bl/transfer_order_qty_catalog")
def transfer_order_qty_catalog():
    """
    Relocate ONLY the ordered qty inside the SAME warehouse via two documents:
      1) IGI (3) — issue from specified source bin(s), first-fit in order
      2) IGR (1) — receipt into destination bin (only qty actually issued)

    Params:
      - order_id or order_number (one required)
      - dst (location_id) OR dst_name (location name)  -> destination bin
      - src_name (single source bin name) OR src_names=CSV (multiple source bin names)
      - partial=true|false (default false): if true, try smaller quantities when full qty can't be issued from any single bin

    Strategy:
      For each product:
        - remaining = ordered qty
        - for each src bin in order:
            attempt to add ONE IGI line with quantity=remaining from that bin
            if accepted -> remaining = 0 and stop
        - if partial and remaining > 0:
            progressively try smaller quantities across bins (binary-ish down to 1)
      Then IGR receives the total quantity that IGI actually issued, into dst.
    """
    try:
        supplied = request.args.get("key") or request.headers.get("X-App-Key")
        if SHARED_KEY and supplied != SHARED_KEY:
            return http_error(401, "Unauthorized: key mismatch")

        order_id_param = (request.args.get("order_id") or "").strip() or None
        order_number   = (request.args.get("order_number") or "").strip() or None

        dst_loc_id     = (request.args.get("dst") or "").strip()
        dst_name       = (request.args.get("dst_name") or "").strip()

        src_name   = (request.args.get("src_name") or "").strip()
        src_names  = (request.args.get("src_names") or "").strip()
        src_list = [s.strip() for s in src_names.split(",") if s.strip()] if src_names else ([src_name] if src_name else [])

        partial = (request.args.get("partial") or "").strip().lower() in ("1","true","yes","y")

        if not src_list:
            return http_error(400, "Please specify source bin(s): use src_name=<bin> or src_names=BinA,BinB.")

        # Resolve order & items
        order_id = resolve_order_id(order_id_param, order_number)
        order = get_order_by_id_strict(order_id)
        items = order.get("products", []) or []
        if not items:
            return http_error(400, "Order has no products")

        # Destination: resolve to location_name
        loc_name = None
        if dst_name:
            loc_name = dst_name
        elif dst_loc_id:
            loc_name = get_location_name_by_id(dst_loc_id)
        if not loc_name:
            return http_error(400,
                "Destination not found. Pass a valid numeric 'dst' (location_id) or 'dst_name' (location name)."
            )

        # Build product list (catalog product_id + ordered qty)
        missing, base_lines = [], []
        for it in items:
            qty = it.get("quantity") or it.get("qty") or 0
            try: qty = int(qty)
            except: qty = 0
            if qty <= 0:
                continue
            rec = resolve_catalog_product_from_order_item(it)
            if not rec:
                missing.append({
                    "sku": (it.get("sku") or it.get("product_sku") or "").strip(),
                    "ean": (it.get("ean") or it.get("product_ean") or "").strip()
                })
                continue
            base_lines.append({"product_id": int(rec["product_id"]), "quantity": qty})

        if not base_lines:
            return http_error(400, f"No transferrable items (catalog product_id match not found). Missing: {missing}")

        # -------- IGI: first-fit over source bins (plus optional partials) --------
        igi_id = create_document(document_type=3, warehouse_id=int(WAREHOUSE_ID))  # IGI

        total_issued = 0
        igi_attempted_items = 0

        def try_issue(pid: int, qty_try: int, src_bin: str) -> int:
            """Attempt to issue qty_try of pid from src_bin. Return qty_success (0 or qty_try)."""
            nonlocal igi_attempted_items
            line = {"product_id": pid, "quantity": qty_try, "location_name": src_bin}
            resp_ids = add_items_to_document(igi_id, [line])
            igi_attempted_items += 1
            return qty_try if resp_ids else 0

        issued_per_product: Dict[int, int] = {}

        for l in base_lines:
            pid = l["product_id"]
            remaining = int(l["quantity"])

            # First-fit: full remaining from each src in order
            for src in src_list:
                if remaining <= 0:
                    break
                got = try_issue(pid, remaining, src)
                if got > 0:
                    issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                    total_issued += got
                    remaining = 0
                    break

            # Partial mode: try smaller amounts if no bin could take full remaining
            if partial and remaining > 0:
                # simple decreasing attempt: try halves until 1
                attempt = max(1, remaining // 2)
                tried_quantities = set()
                while remaining > 0 and attempt >= 1:
                    if attempt in tried_quantities:
                        attempt -= 1
                        continue
                    tried_quantities.add(attempt)
                    placed_any = False
                    for src in src_list:
                        if remaining <= 0:
                            break
                        got = try_issue(pid, min(attempt, remaining), src)
                        if got > 0:
                            issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                            total_issued += got
                            remaining -= got
                            placed_any = True
                    if not placed_any:
                        attempt -= 1

        if total_issued == 0:
            return http_error(400, "IGI could not issue any items. Check source bin names and per-bin stock for these SKUs.")

        confirm_document(igi_id)

        # -------- IGR: receipt exactly what was issued into destination bin --------
        igr_id = create_document(document_type=1, warehouse_id=int(WAREHOUSE_ID))  # IGR
        igr_lines = []
        for pid, qty in issued_per_product.items():
            if qty > 0:
                igr_lines.append({
                    "product_id": pid,
                    "quantity": qty,
                    "location_name": loc_name
                })
        igr_item_ids = add_items_to_document(igr_id, igr_lines)
        if not igr_item_ids:
            return http_error(400, "IGR failed to add items. Check destination bin name.")
        confirm_document(igr_id)

        return jsonify({
            "ok": True,
            "message": f"Relocated issued qty for order {order_id} into '{loc_name}' using IGI+IGR (first-fit{' + partial' if partial else ''}).",
            "igi_document_id": igi_id,
            "igr_document_id": igr_id,
            "moved_units": total_issued,
            "missing": missing,
            "sources_used": src_list
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
    """Try to enumerate locations (may be empty in catalog mode)."""
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

@app.get("/health")
def health():
    return "OK\n"
