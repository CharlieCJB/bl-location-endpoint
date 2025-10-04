# app.py — BaseLinker IGI+IGR relocation (bins + unallocated/ERP) with rich debug + audit exports

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

# ----------------- helpers -----------------
def http_error(status: int, msg: str, detail: str = ""):
    payload = {"error": msg}
    if detail: payload["detail"] = detail
    return make_response(jsonify(payload), status)

def bl_call(method: str, params: dict) -> dict:
    if not BL_TOKEN: raise RuntimeError("BL_TOKEN not set")
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

# ----------------- orders -----------------
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

# -------- products / ERP units --------
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

def get_erp_units_for_product(product_id: int) -> List[Dict[str, Any]]:
    inv_id = require_catalog_id()
    resp = bl_call("getInventoryProductsData", {
        "inventory_id": inv_id,
        "products": [int(product_id)],
        "include_erp_units": True
    })
    pdata = (resp.get("products") or {}).get(str(product_id)) or {}
    units = pdata.get("erp_units") or []
    norm = []
    for u in units:
        norm.append({
            "price": u.get("price"),
            "expiry_date": u.get("expiry_date"),
            "batch": u.get("batch"),
            "qty": to_int(u.get("quantity") or u.get("qty"))
        })
    norm.sort(key=lambda u: (u["expiry_date"] or "9999-12-31"))
    return norm

# ----------------- documents -----------------
def create_document(document_type: int, warehouse_id: int) -> int:
    payload = {"inventory_id": require_catalog_id(), "warehouse_id": int(warehouse_id), "document_type": int(document_type)}
    resp = bl_call("addInventoryDocument", payload)
    if not resp.get("document_id"):
        raise RuntimeError(f"addInventoryDocument failed: {resp}")
    return int(resp["document_id"])

def add_items_to_document_verbose(document_id: int, lines: List[Dict[str, Any]]) -> Tuple[List[int], dict]:
    resp = bl_call("addInventoryDocumentItems", {"document_id": int(document_id), "items": lines})
    created = []
    for item in (resp.get("items") or []):
        if "item_id" in item:
            try: created.append(int(item["item_id"]))
            except: pass
    return created, resp

def confirm_document(document_id: int) -> None:
    bl_call("setInventoryDocumentStatusConfirmed", {"document_id": int(document_id)})

# ----------------- main mover (shortened for clarity) -----------------
@app.get("/bl/transfer_order_qty_catalog")
def transfer_order_qty_catalog():
    # ... same as in last working script ...
    return jsonify({"ok": True, "note": "shortened for brevity here"})

# ----------------- debug endpoints -----------------
@app.get("/bl/catalog_locations_for_sku")
def catalog_locations_for_sku():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY: return http_error(401, "Unauthorized")
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
    if SHARED_KEY and supplied != SHARED_KEY: return http_error(401, "Unauthorized")
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

@app.get("/bl/inspect_sku")
def inspect_sku():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY: return http_error(401, "Unauthorized")
    sku = (request.args.get("sku") or "").strip()
    if not sku: return http_error(400, "Provide sku")
    try:
        inv_id = require_catalog_id()
        rec = find_catalog_product(sku=sku, include=["locations","stock"])
        if not rec: return http_error(404, f"SKU {sku} not found in catalog {inv_id}")
        pid = int(rec["product_id"])
        locations = rec.get("locations")
        stock = rec.get("stock")
        erp = bl_call("getInventoryProductsData", {
            "inventory_id": inv_id, "products": [pid], "include_erp_units": True
        })
        pdata = (erp.get("products") or {}).get(str(pid)) or {}
        erp_units = pdata.get("erp_units") or []
        return jsonify({"sku": sku, "product_id": pid,
                        "warehouse_id_used_by_script": int(WAREHOUSE_ID),
                        "locations": locations, "stock": stock,
                        "erp_units": erp_units})
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

# ----------------- audit endpoints -----------------
def _sku_audit_row(rec: dict, erp_units: list) -> dict:
    pid = int(rec["product_id"])
    locs = rec.get("locations")
    has_locations = bool(locs)
    stock = rec.get("stock") or {}
    erp_list = [
        {"expiry_date": u.get("expiry_date"),
         "price": u.get("price"),
         "batch": u.get("batch"),
         "qty": to_int(u.get("quantity") or u.get("qty") or 0)}
        for u in (erp_units or [])
    ]
    erp_count = sum(max(0, int(u["qty"])) for u in erp_list)
    return {
        "product_id": pid, "sku": rec.get("sku"), "ean": rec.get("ean"),
        "has_locations": has_locations, "locations": locs, "stock": stock,
        "erp_units_count": erp_count, "erp_units": erp_list,
        "can_issue_from_bin": has_locations,
        "can_issue_unallocated": erp_count > 0,
    }

def _fetch_audit_for_skus(skus: list) -> list:
    inv_id = require_catalog_id()
    results = []
    for sku in skus:
        rec = find_catalog_product(sku=sku, include=["locations","stock"])
        if not rec:
            results.append({"sku": sku, "error": f"SKU {sku} not found in catalog {inv_id}"})
            continue
        pid = int(rec["product_id"])
        erp = bl_call("getInventoryProductsData", {
            "inventory_id": inv_id, "products": [pid], "include_erp_units": True
        })
        pdata = (erp.get("products") or {}).get(str(pid)) or {}
        erp_units = pdata.get("erp_units") or []
        results.append(_sku_audit_row(rec, erp_units))
    return results

@app.get("/bl/export_sku_audit")
def export_sku_audit():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY: return http_error(401, "Unauthorized")
    skus_raw = (request.args.get("skus") or "").strip()
    if not skus_raw: return http_error(400, "Provide skus=CSV")
    fmt = (request.args.get("format") or "json").lower()
    skus = [s.strip() for s in skus_raw.split(",") if s.strip()]
    rows = _fetch_audit_for_skus(skus)
    if fmt == "csv":
        import csv; from io import StringIO
        buf = StringIO(); writer = csv.writer(buf)
        writer.writerow(["sku","product_id","has_locations","erp_units_count","stock_bl_77617","can_issue_from_bin","can_issue_unallocated"])
        for r in rows:
            if "error" in r:
                writer.writerow([r.get("sku"), "", "", "", "", "", "ERR:"+r["error"]])
            else:
                stock_77617 = (r.get("stock") or {}).get(f"bl_{WAREHOUSE_ID}", 0)
                writer.writerow([r.get("sku"), r.get("product_id"), r.get("has_locations"),
                                 r.get("erp_units_count"), stock_77617,
                                 r.get("can_issue_from_bin"), r.get("can_issue_unallocated")])
        resp = make_response(buf.getvalue())
        resp.headers["Content-Type"] = "text/csv"
        resp.headers["Content-Disposition"] = "attachment; filename=sku_audit.csv"
        return resp
    return jsonify({"warehouse_id": int(WAREHOUSE_ID), "rows": rows})

@app.get("/bl/export_order_audit")
def export_order_audit():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY: return http_error(401, "Unauthorized")
    order_id_param = (request.args.get("order_id") or "").strip() or None
    order_number   = (request.args.get("order_number") or "").strip() or None
    fmt = (request.args.get("format") or "json").lower()
    oid = resolve_order_id(order_id_param, order_number)
    order = get_order_by_id_strict(oid)
    items = order.get("products", []) or []
    skus = [ (it.get("sku") or it.get("product_sku") or "").strip() for it in items if (it.get("sku") or it.get("product_sku")) ]
    skus = sorted(set(skus))
    rows = _fetch_audit_for_skus(skus)
    if fmt == "csv":
        import csv; from io import StringIO
        buf = StringIO(); writer = csv.writer(buf)
        writer.writerow(["order_id","sku","product_id","has_locations","erp_units_count","stock_bl_77617","can_issue_from_bin","can_issue_unallocated"])
        for r in rows:
            if "error" in r:
                writer.writerow([oid, r.get("sku"), "", "", "", "", "", "ERR:"+r["error"]])
            else:
                stock_77617 = (r.get("stock") or {}).get(f"bl_{WAREHOUSE_ID}", 0)
                writer.writerow([oid, r.get("sku"), r.get("product_id"), r.get("has_locations"),
                                 r.get("erp_units_count"), stock_77617,
                                 r.get("can_issue_from_bin"), r.get("can_issue_unallocated")])
        resp = make_response(buf.getvalue())
        resp.headers["Content-Type"] = "text/csv"
        resp.headers["Content-Disposition"] = f"attachment; filename=order_{oid}_audit.csv"
        return resp
    return jsonify({"order_id": oid, "warehouse_id": int(WAREHOUSE_ID), "rows": rows})

# ----------------- health -----------------
@app.get("/health")
def health(): return "OK\n"
