# app.py – ERP-unit aware mover + probe/debug/audit
import os, json, time, traceback
from flask import Flask, request, jsonify, make_response
import requests
from typing import List, Dict, Any, Tuple
from io import StringIO
import csv

BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN   = os.environ.get("BL_TOKEN")
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")
WAREHOUSE_ID = "77617"
TIMEOUT = 30

app = Flask(__name__)

def http_error(status, msg, detail=""):
    payload = {"error": msg}
    if detail: payload["detail"] = detail
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

def to_int(x): 
    try: return int(x or 0)
    except: return 0

def require_catalog_id():
    cid = os.environ.get("INVENTORY_ID")
    if not cid: raise RuntimeError("INVENTORY_ID not set")
    return int(cid)

# ---------- ERP units ----------
def get_erp_units_for_product(pid: int) -> List[Dict[str, Any]]:
    inv_id = require_catalog_id()
    resp = bl_call("getInventoryProductsData", {
        "inventory_id": inv_id, "products": [int(pid)], "include_erp_units": True
    })
    pdata = (resp.get("products") or {}).get(str(pid)) or {}
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

# ---------- docs ----------
def create_document(doc_type, wh):
    resp = bl_call("addInventoryDocument", {"inventory_id": require_catalog_id(),
                                            "warehouse_id": int(wh),
                                            "document_type": int(doc_type)})
    return int(resp["document_id"])

def add_items_verbose(doc_id, lines):
    resp = bl_call("addInventoryDocumentItems", {"document_id": int(doc_id), "items": lines})
    created = []
    for item in (resp.get("items") or []):
        if "item_id" in item: created.append(int(item["item_id"]))
    return created, resp

def confirm_document(doc_id): bl_call("setInventoryDocumentStatusConfirmed", {"document_id": int(doc_id)})

# ---------- issue helpers ----------
def issue_from_bin(pid, qty, bin_name) -> Tuple[int, dict]:
    units = get_erp_units_for_product(pid)
    if units:
        for u in units:
            if qty <= 0: break
            take = min(qty, u["qty"])
            line = {"product_id": pid, "quantity": take, "location_name": bin_name}
            if u.get("expiry_date"): line["expiry_date"] = u["expiry_date"]
            if u.get("price") is not None: line["price"] = u["price"]
            if u.get("batch"): line["batch"] = u["batch"]
            created, raw = add_items_verbose(global_igi_id, [line])
            if created: return take, raw
    # fallback plain
    line = {"product_id": pid, "quantity": qty, "location_name": bin_name}
    created, raw = add_items_verbose(global_igi_id, [line])
    return (qty if created else 0), raw

def issue_unallocated(pid, qty) -> Tuple[int, dict]:
    units = get_erp_units_for_product(pid)
    if units:
        moved = 0
        for u in units:
            if qty <= 0: break
            take = min(qty, u["qty"])
            line = {"product_id": pid, "quantity": take}
            if u.get("expiry_date"): line["expiry_date"] = u["expiry_date"]
            if u.get("price") is not None: line["price"] = u["price"]
            if u.get("batch"): line["batch"] = u["batch"]
            created, raw = add_items_verbose(global_igi_id, [line])
            if created: 
                moved += take; qty -= take
            else: return moved, raw
        return moved, {}
    # fallback plain
    created, raw = add_items_verbose(global_igi_id, [{"product_id": pid, "quantity": qty}])
    return (qty if created else 0), raw

# ---------- main mover ----------
@app.get("/bl/transfer_order_qty_catalog")
def transfer_order_qty_catalog():
    global global_igi_id
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY: return http_error(401, "Unauthorized")

    order_id = (request.args.get("order_id") or "").strip()
    sku = (request.args.get("only_skus") or "").strip()
    dst = (request.args.get("dst_name") or "").strip()
    srcs = (request.args.get("src_names") or "").split(",") if request.args.get("src_names") else []
    prefer_unallocated = (request.args.get("prefer_unallocated") or "").lower() in ("1","true","yes")

    # For demo: we just test one SKU
    rec = bl_call("getInventoryProductsList", {"inventory_id": require_catalog_id(), "filter_sku": sku})
    prods = rec.get("products", {}) or {}
    if not prods: return http_error(404, f"SKU {sku} not found")
    pid = int(list(prods.keys())[0])

    global_igi_id = create_document(3, WAREHOUSE_ID)
    issued = 0
    fail = []

    if prefer_unallocated:
        got, raw = issue_unallocated(pid, 5)
        issued += got
        if not got: fail.append(raw)
    for s in srcs:
        got, raw = issue_from_bin(pid, 5, s.strip())
        issued += got
        if not got: fail.append(raw)

    if issued <= 0: return http_error(400, "No issue", detail=json.dumps(fail))
    confirm_document(global_igi_id)

    igr = create_document(1, WAREHOUSE_ID)
    created, raw = add_items_verbose(igr, [{"product_id": pid, "quantity": issued, "location_name": dst}])
    if not created: return http_error(400, "IGR add fail", detail=json.dumps(raw))
    confirm_document(igr)
    return jsonify({"ok": True, "igi_document_id": global_igi_id, "igr_document_id": igr, "moved_units": issued})

@app.get("/health")
def health(): return "OK\n"
