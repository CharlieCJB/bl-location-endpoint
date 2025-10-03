# app.py
import os, json, time, traceback
from typing import Optional, List, Dict, Any
from flask import Flask, request, jsonify, make_response
import requests

BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN   = os.environ.get("BL_TOKEN")
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")

WAREHOUSE_ID = "77617"
TIMEOUT      = 30  # seconds
FALLBACK_INVENTORY_ID = os.environ.get("INVENTORY_ID", "")
SRC_BINS_ENV = os.environ.get("SRC_BINS", "")

app = Flask(__name__)

# ---------- utils ----------
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
    try:
        return int(x or 0)
    except Exception:
        return 0

# ---------- order lookup ----------
def resolve_order_id(order_id: Optional[str], order_number: Optional[str]) -> str:
    """
    Prefer order_id (fast).
    If only order_number is provided, do a robust client-side scan of recent orders,
    and consider a match if:
      - order.order_number == provided value (exact), OR
      - order.order_number is empty/None AND order.order_id == provided value (manual orders).
    Return the most recent match by (date_add, date_confirmed, order_id).
    """
    if order_id:
        return str(order_id).strip()
    if not order_number:
        raise ValueError("Provide order_id or order_number")

    needle = str(order_number).strip()

    # Robust scan: last 60 days, up to 300 pages (safety caps)
    date_from = int(time.time()) - 60 * 24 * 60 * 60
    matches: List[dict] = []
    page = 1
    while page <= 300:
        resp = bl_call("getOrders", {"date_from": date_from, "get_unconfirmed_orders": True, "page": page})
        rows = resp.get("orders", []) or []
        if not rows:
            break
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
    """
    Return a single order dict by order_id.
    Tries direct fetch (incl. unconfirmed), then falls back to a client-side scan.
    Raises LookupError if not found.
    """
    oid = str(order_id).strip()

    # Try direct fetch (include unconfirmed)
    try:
        resp = bl_call("getOrders", {"order_id": oid, "get_unconfirmed_orders": True})
        orders = resp.get("orders", []) or []
        if orders:
            return orders[0]
    except Exception:
        pass

    # Fallback: scan last 60 days, client-side match by order_id
    date_from = int(time.time()) - 60 * 24 * 60 * 60
    page = 1
    while page <= 300:
        resp = bl_call("getOrders", {"date_from": date_from, "get_unconfirmed_orders": True, "page": page})
        rows = resp.get("orders", []) or []
        if not rows:
            break
        for o in rows:
            if str(o.get("order_id", "")).strip() == oid:
                return o
        page += 1

    raise LookupError(f"Order not found by order_id {oid}")

# ---------- inventory helpers ----------
def get_inventory_id_for_any_product(product_id: str) -> str:
    inv = bl_call("getInventoryProductsData", {
        "filter_ids": [product_id],
        "include": ["inventory_id"]
    })
    prods = inv.get("products", [])
    if prods and prods[0].get("inventory_id"):
        return str(prods[0]["inventory_id"])
    if FALLBACK_INVENTORY_ID:
        return FALLBACK_INVENTORY_ID
    raise LookupError(f"Inventory product {product_id} missing inventory_id")

def lookup_inventory_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    if not sku:
        return None
    inv = bl_call("getInventoryProductsData", {
        "filter_sku": [sku],
        "include": ["inventory_id"]
    })
    prods = inv.get("products", []) or []
    return prods[0] if prods else None

def lookup_inventory_by_ean(ean: str) -> Optional[Dict[str, Any]]:
    if not ean:
        return None
    inv = bl_call("getInventoryProductsData", {
        "filter_ean": [ean],
        "include": ["inventory_id"]
    })
    prods = inv.get("products", []) or []
    return prods[0] if prods else None

def validate_inventory_product_id(candidate_pid: str) -> Optional[str]:
    if not candidate_pid:
        return None
    inv = bl_call("getInventoryProductsData", {
        "filter_ids": [candidate_pid],
        "include": ["inventory_id"]
    })
    prods = inv.get("products", []) or []
    return str(candidate_pid) if (prods and prods[0].get("inventory_id")) else None

def sku_map_lookup(sku: str) -> Optional[str]:
    raw = os.environ.get("SKU_MAP", "")
    if not raw or not sku:
        return None
    pairs = [p for p in raw.split(";") if "=" in p]
    table = {}
    for p in pairs:
        k, v = p.split("=", 1)
        table[k.strip()] = v.strip()
    return table.get(sku.strip())

def resolve_inventory_pid_from_order_item(it: Dict[str, Any]) -> Optional[str]:
    sku = (it.get("sku") or it.get("product_sku") or "").strip()
    override = sku_map_lookup(sku)
    if override:
        return override

    ean = (it.get("ean") or it.get("product_ean") or "").strip()

    prod = lookup_inventory_by_sku(sku) if sku else None
    if prod and prod.get("product_id"):
        return str(prod["product_id"])

    prod = lookup_inventory_by_ean(ean) if ean else None
    if prod and prod.get("product_id"):
        return str(prod["product_id"])

    for key in ("product_id", "storage_product_id", "inventory_product_id"):
        cand = it.get(key)
        if cand:
            valid = validate_inventory_product_id(str(cand))
            if valid:
                return valid
    return None

# ---------- bin discovery ----------
def list_bins_in_warehouse_dynamic(warehouse_id: str,
                                   exclude_ids: set[str],
                                   dst_id: str) -> List[str]:
    # Try dedicated locations endpoint
    try:
        resp = bl_call("getInventoryLocations", {"warehouse_id": warehouse_id})
        bins: List[str] = []
        for loc in resp.get("locations", []):
            lid = str(loc.get("location_id", "")).strip()
            if not lid:
                continue
            if lid == str(dst_id) or lid in exclude_ids:
                continue
            bins.append(lid)
        if bins:
            return bins
    except Exception:
        pass
    # Try nested locations in warehouses
    try:
        resp = bl_call("getInventoryWarehouses", {})
        bins = []
        for wh in resp.get("warehouses", []):
            if str(wh.get("warehouse_id")) != str(warehouse_id):
                continue
            for loc in wh.get("locations", []):
                lid = str(loc.get("location_id", "")).strip()
                if not lid:
                    continue
                if lid == str(dst_id) or lid in exclude_ids:
                    continue
                bins.append(lid)
        if bins:
            return bins
    except Exception:
        pass
    # Fallback to env var
    csv = SRC_BINS_ENV
    bins = [s.strip() for s in csv.split(",") if s.strip().isdigit()]
    bins = [b for b in bins if b != str(dst_id) and b not in exclude_ids]
    return bins

# ---------- core: transfer ordered qty ----------
@app.get("/bl/transfer_order_qty")
def transfer_order_qty():
    try:
        supplied = request.args.get("key") or request.headers.get("X-App-Key")
        if SHARED_KEY and supplied != SHARED_KEY:
            return http_error(401, "Unauthorized: key mismatch")

        order_id_param = (request.args.get("order_id") or "").strip() or None
        order_number   = (request.args.get("order_number") or "").strip() or None
        src_csv        = (request.args.get("src") or "").strip()
        dst_loc_id     = (request.args.get("dst") or "").strip()
        exclude_csv    = (request.args.get("exclude") or "").strip()

        if not src_csv or not dst_loc_id:
            return http_error(400, "Provide src (comma-separated bin IDs or 'all') and dst (bin ID)")

        exclude_ids = set([s.strip() for s in exclude_csv.split(",") if s.strip().isdigit()])

        if src_csv.lower() == "all":
            src_bins = list_bins_in_warehouse_dynamic(WAREHOUSE_ID, exclude_ids, dst_loc_id)
            if not src_bins:
                return http_error(400, "Could not discover source bins (and no SRC_BINS fallback set).")
        else:
            src_bins = [s.strip() for s in src_csv.split(",") if s.strip().isdigit()]
            if not src_bins:
                return http_error(400, "No valid numeric source bin IDs in 'src'")

        # Resolve to order_id, then fetch strict
        order_id = resolve_order_id(order_id_param, order_number)
        try:
            order = get_order_by_id_strict(order_id)
        except LookupError as e:
            return http_error(404, str(e))

        items = order.get("products", []) or []

        order_lines: List[Dict[str, Any]] = []
        any_pid_for_inv_lookup: Optional[str] = None

        for it in items:
            qty = it.get("quantity") or it.get("qty") or 0
            try:
                qty = int(qty)
            except Exception:
                qty = 0
            if qty <= 0:
                continue

            pid = resolve_inventory_pid_from_order_item(it)
            if pid:
                order_lines.append({"product_id": str(pid), "qty_needed": qty, "raw": it})
                if not any_pid_for_inv_lookup:
                    any_pid_for_inv_lookup = str(pid)

        if not order_lines:
            return http_error(400, "No transferrable items found on this order (could not resolve product_ids/skus)")

        inv_id = get_inventory_id_for_any_product(any_pid_for_inv_lookup)

        total_moved = 0
        per_bin_docs = 0

        for src_loc_id in src_bins:
            products_for_this_bin = []
            for line in order_lines:
                need = line["qty_needed"]
                if need <= 0:
                    continue
                products_for_this_bin.append({
                    "product_id": line["product_id"],
                    "quantity":   need
                })

            if not products_for_this_bin:
                continue

            doc_params = {
                "inventory_id": inv_id,
                "warehouse_id": WAREHOUSE_ID,
                "document_type": "transfer",
                "source_location_id": src_loc_id,
                "target_location_id": dst_loc_id,
                "products": products_for_this_bin,
                "comment": f"Auto sweep for order {order_id}: {src_loc_id} -> {dst_loc_id}"
            }

            try:
                _ = bl_call("addInventoryStockDocument", doc_params)
                for p in products_for_this_bin:
                    for line in order_lines:
                        if line["product_id"] == p["product_id"] and line["qty_needed"] > 0:
                            moved = min(line["qty_needed"], int(p["quantity"]))
                            line["qty_needed"] -= moved
                            total_moved += moved
                per_bin_docs += 1
                if all(l["qty_needed"] <= 0 for l in order_lines):
                    break
            except Exception:
                # skip this bin and continue
                continue

        remaining = sum(l["qty_needed"] for l in order_lines)
        return jsonify({
            "ok": True,
            "message": f"Created {per_bin_docs} transfer document(s) into bin {dst_loc_id} in warehouse {WAREHOUSE_ID}.",
            "moved_units": total_moved,
            "remaining_not_moved": remaining,
            "order_id": order_id
        })
    except Exception as e:
        return http_error(500, "Internal error", detail=f"{e.__class__.__name__}: {e}\n{traceback.format_exc()}")

# ---------- debug endpoints ----------
@app.get("/bl/debug_order")
def debug_order():
    try:
        supplied = request.args.get("key") or request.headers.get("X-App-Key")
        if SHARED_KEY and supplied != SHARED_KEY:
            return http_error(401, "Unauthorized: key mismatch")

        order_id_param = (request.args.get("order_id") or "").strip() or None
        order_number   = (request.args.get("order_number") or "").strip() or None
        order_id = resolve_order_id(order_id_param, order_number)

        try:
            order = get_order_by_id_strict(order_id)
        except LookupError as e:
            return http_error(404, str(e))

        items = order.get("products", []) or []

        out = []
        for it in items:
            resolved = resolve_inventory_pid_from_order_item(it)
            out.append({
                "qty": it.get("quantity") or it.get("qty"),
                "sku": it.get("sku") or it.get("product_sku"),
                "ean": it.get("ean") or it.get("product_ean"),
                "line_product_id": it.get("product_id"),
                "resolved_inventory_product_id": resolved
            })

        return jsonify({
            "order_id": order_id,
            "count_items": len(items),
            "lines": out
        })
    except Exception as e:
        return http_error(500, "Internal error", detail=f"{e.__class__.__name__}: {e}\n{traceback.format_exc()}")

@app.get("/bl/find_order")
def find_order():
    """Client-side scan by provided value; matches order_number OR (when None) order_id."""
    try:
        supplied = request.args.get("key") or request.headers.get("X-App-Key")
        if SHARED_KEY and supplied != SHARED_KEY:
            return http_error(401, "Unauthorized: key mismatch")

        order_number = (request.args.get("order_number") or "").strip()
        if not order_number:
            return http_error(400, "Provide order_number")
        needle = order_number

        days = int(request.args.get("days", "60"))
        date_from = int(time.time()) - max(1, days) * 24 * 60 * 60

        matches: List[dict] = []
        page = 1
        while page <= 300 and len(matches) < 200:
            resp = bl_call("getOrders", {"date_from": date_from, "get_unconfirmed_orders": True, "page": page})
            rows = resp.get("orders", []) or []
            if not rows:
                break
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
        return http_error(500, "Internal error", detail=f"{e.__class__.__name__}: {e}\n{traceback.format_exc()}")

@app.get("/bl/recent_orders")
def recent_orders():
    """List recent orders to verify what BL exposes (order_id, order_number, dates)."""
    try:
        supplied = request.args.get("key") or request.headers.get("X-App-Key")
        if SHARED_KEY and supplied != SHARED_KEY:
            return http_error(401, "Unauthorized: key mismatch")

        days = int(request.args.get("days", "2"))
        limit = int(request.args.get("limit", "50"))
        date_from = int(time.time()) - max(1, days) * 24 * 60 * 60

        results = []
        page = 1
        while len(results) < limit and page <= 200:
            resp = bl_call("getOrders", {"date_from": date_from, "get_unconfirmed_orders": True, "page": page})
            rows = resp.get("orders", []) or []
            if not rows:
                break
            for o in rows:
                if len(results) >= limit:
                    break
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
        return http_error(500, "Internal error", detail=f"{e.__class__.__name__}: {e}\n{traceback.format_exc()}")

@app.get("/health")
def health():
    return "OK\n"
