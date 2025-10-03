# app.py
import os, json, time
from typing import Optional, List, Dict, Any
from flask import Flask, request, abort, jsonify
import requests

BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN   = os.environ.get("BL_TOKEN")
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")

WAREHOUSE_ID = "77617"
TIMEOUT      = 30  # seconds
FALLBACK_INVENTORY_ID = os.environ.get("INVENTORY_ID", "")
SRC_BINS_ENV = os.environ.get("SRC_BINS", "")

app = Flask(__name__)

def bl_call(method: str, params: dict) -> dict:
    if not BL_TOKEN:
        abort(500, "BL_TOKEN not set")
    headers = {"X-BLToken": BL_TOKEN}
    data = {"method": method, "parameters": json.dumps(params)}
    r = requests.post(BL_API_URL, headers=headers, data=data, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    if isinstance(j, dict) and j.get("error"):
        abort(502, f"BaseLinker API error in {method}: {j['error']}")
    return j

# ---------- Order lookup (robust) ----------

def resolve_order_id(order_id: Optional[str], order_number: Optional[str]) -> str:
    """
    Prefer order_id (fast). If only order_number is provided:
      1) Call getOrders(order_number=...), but DO NOT trust the first order —
         explicitly filter for exact order_number and choose the most recent by created/confirmed date.
      2) Fallback: paged scans by creation time (date_from) with exact match check.
    """
    if order_id:
        return str(order_id)
    if not order_number:
        abort(400, "Provide order_id or order_number")

    needle = str(order_number).strip()

    def pick_most_recent(matches: List[dict]) -> Optional[str]:
        if not matches:
            return None
        # Prefer the one with the largest date_add or date_confirmed, then largest order_id
        def keyfn(o):
            return (
                int(o.get("date_add", 0)),
                int(o.get("date_confirmed", 0)),
                int(o.get("order_id", 0)),
            )
        return str(sorted(matches, key=keyfn, reverse=True)[0].get("order_id"))

    # --- 1) Try direct param, but verify matches explicitly
    try:
        resp = bl_call("getOrders", {
            "order_number": needle,
            "get_unconfirmed_orders": True
        })
        orders = resp.get("orders", []) or []
        exact = [o for o in orders if str(o.get("order_number", "")).strip() == needle]
        chosen = pick_most_recent(exact)
        if chosen:
            return chosen
    except Exception:
        pass

    # Helper: paged scan with explicit match check
    def paged_scan(date_from_ts: int, pages_max: int = 200) -> Optional[str]:
        page = 1
        matches: List[dict] = []
        while page <= pages_max:
            params = {
                "date_from": date_from_ts,
                "get_unconfirmed_orders": True,
                "page": page
            }
            resp = bl_call("getOrders", params)
            rows = resp.get("orders", []) or []
            if not rows:
                break
            # Collect only exact matches
            for o in rows:
                if str(o.get("order_number", "")).strip() == needle:
                    matches.append(o)
            page += 1
        return pick_most_recent(matches)

    # --- 2) Broad scan: last 365 days
    date_from = int(time.time()) - 365 * 24 * 60 * 60
    found = paged_scan(date_from_ts=date_from, pages_max=200)
    if found:
        return found

    # --- 3) Very recent scan: last 1 day (edge accounts/window caps)
    one_day_ago = int(time.time()) - 1 * 24 * 60 * 60
    found = paged_scan(date_from_ts=one_day_ago, pages_max=50)
    if found:
        return found

    abort(404, f"Order with order_number '{order_number}' not found in recent history")

# ---------- Inventory helpers ----------

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
    abort(404, f"Inventory product {product_id} not found or missing inventory_id")

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
    # 0) Hard override via env map
    sku = (it.get("sku") or it.get("product_sku") or "").strip()
    override = sku_map_lookup(sku)
    if override:
        return override

    ean = (it.get("ean") or it.get("product_ean") or "").strip()

    # 1) By SKU
    prod = lookup_inventory_by_sku(sku) if sku else None
    if prod and prod.get("product_id"):
        return str(prod["product_id"])

    # 2) By EAN
    prod = lookup_inventory_by_ean(ean) if ean else None
    if prod and prod.get("product_id"):
        return str(prod["product_id"])

    # 3) Validate any id present on the order item
    for key in ("product_id", "storage_product_id", "inventory_product_id"):
        cand = it.get(key)
        if cand:
            valid = validate_inventory_product_id(str(cand))
            if valid:
                return valid

    return None

# ---------- Bin discovery ----------

def list_bins_in_warehouse_dynamic(warehouse_id: str,
                                   exclude_ids: set[str],
                                   dst_id: str) -> List[str]:
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
    csv = SRC_BINS_ENV
    bins = [s.strip() for s in csv.split(",") if s.strip().isdigit()]
    bins = [b for b in bins if b != str(dst_id) and b not in exclude_ids]
    return bins

# ---------- Core: transfer ordered qty ----------

@app.get("/bl/transfer_order_qty")
def transfer_order_qty():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        abort(401, "Unauthorized: key mismatch")

    order_id_param = (request.args.get("order_id") or "").strip() or None
    order_number   = (request.args.get("order_number") or "").strip() or None
    src_csv        = (request.args.get("src") or "").strip()
    dst_loc_id     = (request.args.get("dst") or "").strip()
    exclude_csv    = (request.args.get("exclude") or "").strip()

    if not src_csv or not dst_loc_id:
        abort(400, "Provide src (comma-separated bin IDs or 'all') and dst (bin ID)")

    exclude_ids = set([s.strip() for s in exclude_csv.split(",") if s.strip().isdigit()])

    if src_csv.lower() == "all":
        src_bins = list_bins_in_warehouse_dynamic(WAREHOUSE_ID, exclude_ids, dst_loc_id)
        if not src_bins:
            abort(400, "Could not discover source bins (and no SRC_BINS fallback set).")
    else:
        src_bins = [s.strip() for s in src_csv.split(",") if s.strip().isdigit()]
        if not src_bins:
            abort(400, "No valid numeric source bin IDs in 'src'")

    order_id = resolve_order_id(order_id_param, order_number)

    o = bl_call("getOrders", {"order_id": order_id})
    orders = o.get("orders", [])
    if not orders:
        abort(404, "Order not found by order_id")
    order = orders[0]
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
        abort(400, "No transferrable items found on this order (could not resolve product_ids/skus)")

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
            continue

    remaining = sum(l["qty_needed"] for l in order_lines)
    return (f"OK — created {per_bin_docs} transfer document(s) into bin {dst_loc_id} "
            f"in warehouse {WAREHOUSE_ID}. Moved {total_moved} unit(s). "
            f"Remaining not moved: {remaining}. Order {order_id}\n")

# ---------- Debug ----------

@app.get("/bl/debug_order")
def debug_order():
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        abort(401, "Unauthorized: key mismatch")

    order_id_param = (request.args.get("order_id") or "").strip() or None
    order_number   = (request.args.get("order_number") or "").strip() or None
    order_id = resolve_order_id(order_id_param, order_number)

    o = bl_call("getOrders", {"order_id": order_id})
    orders = o.get("orders", [])
    if not orders:
        abort(404, "Order not found by order_id")
    order = orders[0]
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

@app.get("/health")
def health():
    return "OK\n"
