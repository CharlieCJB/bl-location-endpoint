# app.py — BaseLinker IGI+IGR relocation with detailed IGI failure reasons

import os, json, time, traceback
from typing import Optional, List, Dict, Any, Tuple
from flask import Flask, request, jsonify, make_response
import requests

BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN   = os.environ.get("BL_TOKEN")
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")

WAREHOUSE_ID = "77617"
TIMEOUT      = 30

app = Flask(__name__)

def http_error(status: int, msg: str, detail: str = ""):
    payload = {"error": msg}
    if detail: payload["detail"] = detail
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
    if not cid: raise RuntimeError("INVENTORY_ID env var is required.")
    return int(cid)

# -------- Orders --------
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
    if not matches:
        raise LookupError(f"Order with order_number/id '{order_number}' not found")
    matches.sort(key=lambda o: (to_int(o.get("date_add")), to_int(o.get("order_id"))), reverse=True)
    return str(matches[0].get("order_id"))

def get_order_by_id_strict(order_id: str) -> dict:
    resp = bl_call("getOrders", {"order_id": str(order_id), "get_unconfirmed_orders": True})
    orders = resp.get("orders", []) or []
    if orders: return orders[0]
    raise LookupError(f"Order not found by order_id {order_id}")

# -------- Products --------
def find_catalog_product(sku=None, ean=None, include=None) -> Optional[Dict[str, Any]]:
    inv_id = require_catalog_id()
    if sku:
        params = {"inventory_id": inv_id, "filter_sku": sku}
    elif ean:
        params = {"inventory_id": inv_id, "filter_ean": ean}
    else:
        return None
    if include: params["include"] = include
    resp = bl_call("getInventoryProductsList", params)
    prods = resp.get("products", {}) or {}
    for pid_str, pdata in prods.items():
        pdata = dict(pdata)
        pdata["product_id"] = int(pid_str)
        return pdata
    return None

def resolve_catalog_product_from_order_item(it: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sku = (it.get("sku") or it.get("product_sku") or "").strip()
    ean = (it.get("ean") or it.get("product_ean") or "").strip()
    return find_catalog_product(sku=sku, include=["locations","stock"]) or \
           find_catalog_product(ean=ean, include=["locations","stock"])

# -------- Documents --------
def create_document(document_type: int, warehouse_id: int) -> int:
    payload = {"inventory_id": require_catalog_id(), "warehouse_id": int(warehouse_id), "document_type": int(document_type)}
    resp = bl_call("addInventoryDocument", payload)
    if not resp.get("document_id"):
        raise RuntimeError(f"addInventoryDocument failed: {resp}")
    return int(resp["document_id"])

def add_items_to_document_verbose(document_id: int, lines: List[Dict[str, Any]]) -> Tuple[List[int], dict]:
    """Return (created_item_ids, raw_response) so we can surface BL's failure reasons."""
    resp = bl_call("addInventoryDocumentItems", {"document_id": int(document_id), "items": lines})
    created = []
    for item in (resp.get("items") or []):
        if "item_id" in item:
            try: created.append(int(item["item_id"]))
            except: pass
    return created, resp

def confirm_document(document_id: int) -> None:
    bl_call("setInventoryDocumentStatusConfirmed", {"document_id": int(document_id)})

# -------- Debug helpers --------
def get_location_name_by_id(location_id: str) -> Optional[str]:
    try:
        resp = bl_call("getInventoryLocations", {"warehouse_id": int(WAREHOUSE_ID)})
        for loc in (resp.get("locations") or []):
            if str(loc.get("location_id")) == str(location_id):
                return loc.get("name")
    except: pass
    return None

@app.get("/bl/catalog_locations_for_sku")
def catalog_locations_for_sku():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")
    sku = (request.args.get("sku") or "").strip()
    if not sku: return http_error(400, "Provide sku")
    rec = find_catalog_product(sku=sku, include=["locations","stock"])
    if not rec: return http_error(404, f"SKU {sku} not found")
    return jsonify({"product_id": rec["product_id"], "sku": rec.get("sku"),
                    "ean": rec.get("ean"), "locations": rec.get("locations"),
                    "stock": rec.get("stock")})

@app.get("/bl/debug_order_lines")
def debug_order_lines():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")
    order_id_param = (request.args.get("order_id") or "").strip() or None
    order_number   = (request.args.get("order_number") or "").strip() or None
    oid = resolve_order_id(order_id_param, order_number)
    order = get_order_by_id_strict(oid)
    items = order.get("products", []) or []
    out = []
    for it in items:
        sku = (it.get("sku") or it.get("product_sku") or "").strip()
        ean = (it.get("ean") or it.get("product_ean") or "").strip()
        qty = int(it.get("quantity") or it.get("qty") or 0)
        rec = resolve_catalog_product_from_order_item(it)
        pid = rec["product_id"] if rec else None
        has_loc = bool(rec.get("locations")) if rec else None
        out.append({"sku": sku, "ean": ean, "qty": qty, "product_id": pid, "has_locations": has_loc})
    return jsonify({"order_id": oid, "lines": out})

# -------- Main endpoint --------
@app.get("/bl/transfer_order_qty_catalog")
def transfer_order_qty_catalog():
    try:
        supplied = request.args.get("key") or request.headers.get("X-App-Key")
        if SHARED_KEY and supplied != SHARED_KEY:
            return http_error(401, "Unauthorized")

        order_id_param = (request.args.get("order_id") or "").strip() or None
        order_number   = (request.args.get("order_number") or "").strip() or None
        dst_loc_id     = (request.args.get("dst") or "").strip()
        dst_name       = (request.args.get("dst_name") or "").strip()

        src_name   = (request.args.get("src_name") or "").strip()
        src_names  = (request.args.get("src_names") or "").strip()
        src_list = [s.strip() for s in src_names.split(",") if s.strip()] if src_names else ([src_name] if src_name else [])

        partial = (request.args.get("partial") or "").strip().lower() in ("1","true","yes")
        only_skus_raw  = (request.args.get("only_skus") or "").strip()
        only_skus = [s.strip() for s in only_skus_raw.split(",") if s.strip()] if only_skus_raw else []
        prefer_unallocated = (request.args.get("prefer_unallocated") or "").strip().lower() in ("1","true","yes")

        # Allow pure unallocated test: src_list can be empty if prefer_unallocated=1
        if not dst_name:
            # allow id-based dst if they prefer
            dst_name = get_location_name_by_id(dst_loc_id) if dst_loc_id else None
        if not dst_name:
            return http_error(400, "Destination not found. Use dst_name=<bin name>.")

        if not src_list and not prefer_unallocated:
            return http_error(400, "Please specify src_name/src_names or set prefer_unallocated=1")

        order_id = resolve_order_id(order_id_param, order_number)
        order = get_order_by_id_strict(order_id)
        items = order.get("products", []) or []
        if not items: return http_error(400, "Order has no products")

        missing, base_lines, has_loc_map, skus_in_scope = [], [], {}, []
        for it in items:
            line_sku = (it.get("sku") or it.get("product_sku") or "").strip()
            if only_skus and line_sku not in only_skus:
                continue
            qty = int(it.get("quantity") or it.get("qty") or 0)
            if qty <= 0: continue
            rec = resolve_catalog_product_from_order_item(it)
            if not rec:
                missing.append({"sku": it.get("sku"), "ean": it.get("ean")})
                continue
            pid = rec["product_id"]
            has_loc_map[pid] = bool(rec.get("locations"))
            base_lines.append({"product_id": pid, "quantity": qty})
            skus_in_scope.append(line_sku)

        if not base_lines:
            return http_error(400, f"No transferrable items. Missing: {missing}, only_skus={only_skus}")

        # IGI
        igi_id = create_document(3, int(WAREHOUSE_ID))
        issued_per_product, total_issued = {}, 0
        fail_reasons: List[Dict[str, Any]] = []

        def try_issue_verbose(pid: int, qty: int, src_bin: Optional[str]) -> int:
            line = {"product_id": pid, "quantity": qty}
            if src_bin: line["location_name"] = src_bin
            created, raw = add_items_to_document_verbose(igi_id, [line])
            if created:
                return qty
            else:
                fail_reasons.append({
                    "product_id": pid,
                    "attempt_qty": qty,
                    "src_bin": src_bin,
                    "response": raw
                })
                return 0

        for l in base_lines:
            pid, remaining = l["product_id"], l["quantity"]

            # Prefer unallocated first if requested
            if prefer_unallocated and remaining > 0:
                got = try_issue_verbose(pid, remaining, None)
                if got:
                    issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                    total_issued += got
                    remaining = 0

            # First-fit bins
            if remaining > 0 and src_list:
                for src in src_list:
                    if remaining <= 0: break
                    got = try_issue_verbose(pid, remaining, src)
                    if got:
                        issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                        total_issued += got
                        remaining = 0
                        break

            # Fallback: unallocated if product has no bin allocations
            if remaining > 0 and not has_loc_map.get(pid, False):
                got = try_issue_verbose(pid, remaining, None)
                if got:
                    issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                    total_issued += got
                    remaining = 0

            # Optional partials
            if remaining > 0 and partial:
                attempt = max(1, remaining // 2)
                while remaining > 0 and attempt >= 1:
                    placed = False
                    if src_list:
                        for src in src_list:
                            if remaining <= 0: break
                            got = try_issue_verbose(pid, min(attempt, remaining), src)
                            if got:
                                issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                                total_issued += got
                                remaining -= got
                                placed = True
                    if not placed and (prefer_unallocated or not has_loc_map.get(pid, False)):
                        got = try_issue_verbose(pid, min(attempt, remaining), None)
                        if got:
                            issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                            total_issued += got
                            remaining -= got
                            placed = True
                    if not placed:
                        attempt -= 1

        if total_issued == 0:
            ctx = {
                "order_id": order_id,
                "skus_in_scope": skus_in_scope,
                "prefer_unallocated": prefer_unallocated,
                "src_list": src_list,
                "fail_reasons": fail_reasons  # <- raw BL responses for each failed add
            }
            return http_error(400, "IGI could not issue any items", detail=json.dumps(ctx))

        confirm_document(igi_id)

        # IGR
        igr_id = create_document(1, int(WAREHOUSE_ID))
        igr_lines = [{"product_id": pid, "quantity": qty, "location_name": dst_name}
                     for pid, qty in issued_per_product.items() if qty > 0]
        created, raw = add_items_to_document_verbose(igr_id, igr_lines)
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
            "prefer_unallocated": prefer_unallocated
        })

    except Exception as e:
        return http_error(500, "Internal error", detail=f"{e}\n{traceback.format_exc()}")

@app.get("/health")
def health(): return "OK\n"
