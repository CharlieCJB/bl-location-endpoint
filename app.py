# app.py — BaseLinker IGI+IGR relocation (bins + unallocated/ERP) with rich debug, audits & probe

import os, json, time, traceback
from typing import Optional, List, Dict, Any, Tuple
from flask import Flask, request, jsonify, make_response
import requests
from io import StringIO
import csv

BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN   = os.environ.get("BL_TOKEN")
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")

# Adjust to your warehouse
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
    # last 60 days
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
    """Fetch ERP (batch) units with price/expiry/batch/qty."""
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
    # earliest expiry first (None last)
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

def get_location_name_by_id(location_id: str) -> Optional[str]:
    try:
        resp = bl_call("getInventoryLocations", {"warehouse_id": int(WAREHOUSE_ID)})
        for loc in (resp.get("locations") or []):
            if str(loc.get("location_id")) == str(location_id):
                return loc.get("name")
    except: pass
    return None

# ----------------- main mover -----------------
@app.get("/bl/transfer_order_qty_catalog")
def transfer_order_qty_catalog():
    """
    Move ONLY the ordered qty inside the SAME warehouse via:
      IGI (3) issue   -> from src bins, and/or from unallocated (ERP-aware)
      IGR (1) receipt -> into dst bin

    Query params:
      - order_id=... (or order_number=...)
      - dst_name=InternalStock
      - src_names=BinA,BinB   (optional if prefer_unallocated=1)
      - partial=true|false
      - only_skus=SKU1,SKU2
      - prefer_unallocated=1  (try unallocated first, with ERP unit selection)
      - key=YOUR_SHARED_KEY
    """
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

        if not dst_name:
            dst_name = get_location_name_by_id(dst_loc_id) if dst_loc_id else None
        if not dst_name:
            return http_error(400, "Destination not found. Use dst_name=<bin name>.")

        if not src_list and not prefer_unallocated:
            return http_error(400, "Please specify src_name/src_names or set prefer_unallocated=1")

        order_id = resolve_order_id(order_id_param, order_number)
        order = get_order_by_id_strict(order_id)
        items = order.get("products", []) or []
        if not items: return http_error(400, "Order has no products")

        # resolve lines
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

        igi_id = create_document(3, int(WAREHOUSE_ID))
        issued_per_product, total_issued = {}, 0
        fail_reasons: List[Dict[str, Any]] = []

        def try_issue_from_bin(pid: int, qty: int, src_bin: Optional[str]) -> int:
            line = {"product_id": pid, "quantity": qty}
            if src_bin: line["location_name"] = src_bin
            created, raw = add_items_to_document_verbose(igi_id, [line])
            if created: return qty
            fail_reasons.append({"product_id": pid, "attempt_qty": qty, "src_bin": src_bin, "response": raw})
            return 0

        def try_issue_unallocated_with_erp(pid: int, qty: int) -> int:
            units = get_erp_units_for_product(pid)
            if not units:
                # try once without ERP attrs (may pass if ERP not enforced for this SKU)
                created, raw = add_items_to_document_verbose(igi_id, [{"product_id": pid, "quantity": qty}])
                if created: return qty
                fail_reasons.append({"product_id": pid, "attempt_qty": qty, "src_bin": None, "response": raw})
                return 0

            remaining, moved = qty, 0
            for u in units:
                if remaining <= 0: break
                take = min(remaining, to_int(u["qty"]))
                if take <= 0: continue
                line = {"product_id": pid, "quantity": take}
                if u.get("expiry_date"): line["expiry_date"] = u["expiry_date"]
                if u.get("price") is not None: line["price"] = u["price"]
                if u.get("batch"): line["batch"] = u["batch"]
                created, raw = add_items_to_document_verbose(igi_id, [line])
                if created:
                    moved += take
                    remaining -= take
                else:
                    fail_reasons.append({"product_id": pid, "attempt_qty": take, "src_bin": None, "erp_unit": u, "response": raw})
                    break
            return moved

        for l in base_lines:
            pid, remaining = l["product_id"], l["quantity"]

            # optional: unallocated first
            if prefer_unallocated and remaining > 0:
                got = try_issue_unallocated_with_erp(pid, remaining)
                if got:
                    issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                    total_issued += got
                    remaining -= got

            # bins first-fit
            if remaining > 0 and src_list:
                for src in src_list:
                    if remaining <= 0: break
                    got = try_issue_from_bin(pid, remaining, src)
                    if got:
                        issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                        total_issued += got
                        remaining = 0
                        break

            # fallback: unallocated when no bin allocations
            if remaining > 0 and not has_loc_map.get(pid, False):
                got = try_issue_unallocated_with_erp(pid, remaining)
                if got:
                    issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                    total_issued += got
                    remaining -= got

            # optional partial across bins/unallocated
            if remaining > 0 and partial:
                attempt = max(1, remaining // 2)
                while remaining > 0 and attempt >= 1:
                    placed = False
                    if src_list:
                        for src in src_list:
                            if remaining <= 0: break
                            got = try_issue_from_bin(pid, min(attempt, remaining), src)
                            if got:
                                issued_per_product[pid] = issued_per_product.get(pid, 0) + got
                                total_issued += got
                                remaining -= got
                                placed = True
                    if not placed and (prefer_unallocated or not has_loc_map.get(pid, False)):
                        got = try_issue_unallocated_with_erp(pid, min(attempt, remaining))
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
                "fail_reasons": fail_reasons
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

# ----------------- probe (dry-run IGI add tests) -----------------
@app.get("/bl/probe_issue")
def probe_issue():
    """
    Dry-run: create a DRAFT IGI and try to add 1 unit for a given SKU
    - tries: (1) unallocated, (2) each src bin passed (CSV)
    - leaves IGI as DRAFT (not confirmed)
    Query: sku=..., src_names=BinA,BinB (optional), key=...
    """
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        return http_error(401, "Unauthorized")

    sku = (request.args.get("sku") or "").strip()
    src_names = (request.args.get("src_names") or "").strip()
    src_list = [s.strip() for s in src_names.split(",") if s.strip()]
    if not sku:
        return http_error(400, "Provide sku")

    try:
        rec = find_catalog_product(sku=sku, include=["locations","stock"])
        if not rec:
            return http_error(404, f"SKU {sku} not found")
        pid = int(rec["product_id"])

        igi_id = create_document(3, int(WAREHOUSE_ID))
        attempts = []

        # 1) unallocated probe
        created, raw = add_items_to_document_verbose(igi_id, [{"product_id": pid, "quantity": 1}])
        attempts.append({"mode": "unallocated", "created": bool(created), "response": raw})

        # 2) each src bin probe
        for src in src_list:
            created, raw = add_items_to_document_verbose(igi_id, [{"product_id": pid, "quantity": 1, "location_name": src}])
            attempts.append({"mode": f"bin:{src}", "created": bool(created), "response": raw})

        return jsonify({"sku": sku, "product_id": pid, "draft_igi_id": igi_id, "attempts": attempts})
    except Exception as e:
        return http_error(500, "Internal error", detail=str(e))

# ----------------- health -----------------
@app.get("/health")
def health(): return "OK\n"
