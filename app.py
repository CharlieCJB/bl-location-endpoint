# app.py — BaseLinker catalog stock relocation via IGI+IGR
# Flexible endpoint: specify src_names (bins) and dst_name (destination bin).
# - First-fit IGI across provided bins (in order)
# - Fallback: if SKU has no bin allocations, issue from unallocated
# - Optional partial mode (&partial=true) to spread across bins
# - IGR receipts only what IGI actually issued

import os, json, time, traceback
from typing import Optional, List, Dict, Any
from flask import Flask, request, jsonify, make_response
import requests

BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN   = os.environ.get("BL_TOKEN")
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")

WAREHOUSE_ID = "77617"  # your warehouse id
TIMEOUT      = 30

app = Flask(__name__)

# ---------- helpers ----------
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
    if not cid:
        raise RuntimeError("INVENTORY_ID env var is required.")
    return int(cid)

# ---------- orders ----------
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

def get_order_by_id_strict(order_id: str) -> dict:
    resp = bl_call("getOrders", {"order_id": str(order_id), "get_unconfirmed_orders": True})
    orders = resp.get("orders", []) or []
    if orders: return orders[0]
    raise LookupError(f"Order not found by order_id {order_id}")

# ---------- product resolution ----------
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
    sku = (it.get("sku") or "").strip()
    ean = (it.get("ean") or "").strip()
    return find_catalog_product(sku=sku, include=["locations","stock"]) or \
           find_catalog_product(ean=ean, include=["locations","stock"])

# ---------- documents ----------
def create_document(document_type: int, warehouse_id: int) -> int:
    payload = {"inventory_id": require_catalog_id(), "warehouse_id": int(warehouse_id), "document_type": int(document_type)}
    resp = bl_call("addInventoryDocument", payload)
    if not resp.get("document_id"):
        raise RuntimeError(f"addInventoryDocument failed: {resp}")
    return int(resp["document_id"])

def add_items_to_document(document_id: int, lines: List[Dict[str, Any]]) -> List[int]:
    resp = bl_call("addInventoryDocumentItems", {"document_id": int(document_id), "items": lines})
    created = []
    for item in (resp.get("items") or []):
        if "item_id" in item:
            try: created.append(int(item["item_id"]))
            except: pass
    return created

def confirm_document(document_id: int) -> None:
    bl_call("setInventoryDocumentStatusConfirmed", {"document_id": int(document_id)})

# ---------- main flexible endpoint ----------
@app.get("/bl/transfer_order_qty_catalog")
def transfer_order_qty_catalog():
    """
    Relocate ONLY the ordered qty inside the SAME warehouse via:
      - IGI (3): issue from source bin(s), first-fit with optional partials
      - IGR (1): receipt into destination bin
    Params (query):
      - order_id=...    (you will manually paste the real ID into the URL)
      - src_name=<bin> or src_names=BinA,BinB
      - dst_name=<destination bin>
      - partial=true|false
      - key=YOUR_SHARED_KEY
    """
    try:
        supplied = request.args.get("key") or request.headers.get("X-App-Key")
        if SHARED_KEY and supplied != SHARED_KEY:
            return http_error(401, "Unauthorized: key mismatch")

        order_id_param = (request.args.get("order_id") or "").strip() or None
        order_number   = (request.args.get("order_number") or "").strip() or None
        dst_name       = (request.args.get("dst_name") or "").strip()
        src_name       = (request.args.get("src_name") or "").strip()
        src_names      = (request.args.get("src_names") or "").strip()
        src_list = [s.strip() for s in src_names.split(",") if s.strip()] if src_names else ([src_name] if src_name else [])
        partial        = (request.args.get("partial") or "").strip().lower() in ("1","true","yes")

        if not src_list: return http_error(400, "Please specify src_name or src_names")
        if not dst_name: return http_error(400, "Please specify dst_name")

        order_id = resolve_order_id(order_id_param, order_number)
        order = get_order_by_id_strict(order_id)
        items = order.get("products", []) or []
        if not items: return http_error(400, "Order has no products")

        missing, base_lines, has_loc_map = [], [], {}
        for it in items:
            qty = int(it.get("quantity") or it.get("qty") or 0)
            if qty <= 0: continue
            rec = resolve_catalog_product_from_order_item(it)
            if not rec:
                missing.append({"sku": it.get("sku"), "ean": it.get("ean")})
                continue
            pid = rec["product_id"]
            has_loc_map[pid] = bool(rec.get("locations"))
            base_lines.append({"product_id": pid, "quantity": qty})

        if not base_lines: return http_error(400, f"No transferrable items. Missing: {missing}")

        # IGI
        igi_id = create_document(3, int(WAREHOUSE_ID))
        issued_per_product, total_issued = {}, 0

        def try_issue(pid: int, qty: int, src_bin: Optional[str]) -> int:
            line = {"product_id": pid, "quantity": qty}
            if src_bin: line["location_name"] = src_bin
            ids = add_items_to_document(igi_id, [line])
            return qty if ids else 0

        for l in base_lines:
            pid, remaining = l["product_id"], l["quantity"]

            # 1) first-fit across bins
            for src in src_list:
                if remaining <= 0: break
                got = try_issue(pid, remaining, src)
                if got:
                    issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                    total_issued += got
                    remaining = 0
                    break

            # 2) fallback: unallocated if product has no bins
            if remaining > 0 and not has_loc_map.get(pid, False):
                got = try_issue(pid, remaining, None)
                if got:
                    issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                    total_issued += got
                    remaining = 0

            # 3) optional partial
            if remaining > 0 and partial:
                attempt = max(1, remaining // 2)
                while remaining > 0 and attempt >= 1:
                    placed = False
                    for src in src_list:
                        got = try_issue(pid, min(attempt, remaining), src)
                        if got:
                            issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                            total_issued += got
                            remaining -= got
                            placed = True
                    if not placed and not has_loc_map.get(pid, False):
                        got = try_issue(pid, min(attempt, remaining), None)
                        if got:
                            issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                            total_issued += got
                            remaining -= got
                            placed = True
                    if not placed: attempt -= 1

        if total_issued == 0: return http_error(400, "IGI could not issue any items")

        confirm_document(igi_id)

        # IGR
        igr_id = create_document(1, int(WAREHOUSE_ID))
        igr_lines = [{"product_id": pid, "quantity": qty, "location_name": dst_name}
                     for pid, qty in issued_per_product.items() if qty > 0]
        igr_item_ids = add_items_to_document(igr_id, igr_lines)
        if not igr_item_ids: return http_error(400, "IGR failed to add items.")
        confirm_document(igr_id)

        return jsonify({"ok": True,
                        "message": f"Relocated issued qty for order {order_id} into '{dst_name}'",
                        "igi_document_id": igi_id,
                        "igr_document_id": igr_id,
                        "moved_units": total_issued,
                        "missing": missing,
                        "sources_used": src_list})
    except Exception as e:
        return http_error(500, "Internal error", detail=f"{e}\n{traceback.format_exc()}")

@app.get("/health")
def health(): return "OK\n"
