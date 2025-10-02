# app.py
import os, json, time
from typing import Optional, List, Dict, Any
from flask import Flask, request, abort
import requests

BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN   = os.environ.get("BL_TOKEN")
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")

# Your warehouse
WAREHOUSE_ID = "77617"
TIMEOUT      = 30  # seconds

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

def resolve_order_id(order_id: Optional[str], order_number: Optional[str]) -> str:
    """Prefer order_id. If only order_number is provided, search recent orders."""
    if order_id:
        return str(order_id)
    if not order_number:
        abort(400, "Provide order_id or order_number")
    date_from = int(time.time()) - 365 * 24 * 60 * 60
    resp = bl_call("getOrders", {
        "date_confirmed_from": date_from,
        "get_unconfirmed_orders": True
    })
    for o in resp.get("orders", []):
        if str(o.get("order_number", "")).strip() == str(order_number).strip():
            return str(o.get("order_id"))
    abort(404, f"Order with order_number '{order_number}' not found in last 365 days")

def get_inventory_id_for_any_product(product_id: str) -> str:
    inv = bl_call("getInventoryProductsData", {"filter_ids": [product_id], "include": []})
    prods = inv.get("products", [])
    if not prods or not prods[0].get("inventory_id"):
        abort(404, f"Inventory product {product_id} not found or missing inventory_id")
    return str(prods[0]["inventory_id"])

def list_bins_in_warehouse_dynamic(warehouse_id: str,
                                   exclude_ids: set[str],
                                   dst_id: str) -> List[str]:
    """
    Try to list all bin/location IDs for a warehouse via API.
    If unavailable, fall back to env var SRC_BINS (comma-separated).
    Excludes dst_id and anything in exclude_ids.
    """
    # Attempt: some accounts expose locations nested in getInventoryWarehouses
    try:
        resp = bl_call("getInventoryWarehouses", {})
        bins: List[str] = []
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
        pass  # fall back to env var

    # Fallback: define SRC_BINS in Render env (e.g. "101,102,103")
    csv = os.environ.get("SRC_BINS", "")
    bins = [s.strip() for s in csv.split(",") if s.strip().isdigit()]
    bins = [b for b in bins if b != str(dst_id) and b not in exclude_ids]
    return bins

@app.get("/bl/transfer_order_qty")
def transfer_order_qty():
    """
    Create stock transfer document(s) moving ORDERED QTY ONLY
    from one or more source bins -> destination bin for all items on the order
    (within WAREHOUSE_ID).

    Query:
      - order_id OR order_number
      - src: comma-separated bin IDs or 'all'
      - dst: destination bin ID (Internal Stock bin)
      - exclude (optional): comma-separated bin IDs to skip
      - key: shared secret
    """
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

    # Resolve order
    order_id = resolve_order_id(order_id_param, order_number)

    # Fetch order + items
    o = bl_call("getOrders", {"order_id": order_id})
    orders = o.get("orders", [])
    if not orders:
        abort(404, "Order not found by order_id")
    order = orders[0]
    items = order.get("products", []) or []

    # Prepare lines (resolve product_id by SKU if needed)
    order_lines = []
    any_pid_for_inv_lookup: Optional[str] = None
    for it in items:
        qty = it.get("quantity") or it.get("qty") or 0
        try: qty = int(qty)
        except Exception: qty = 0
        if qty <= 0:
            continue

        pid = it.get("product_id")
        if not pid:
            sku = it.get("sku")
            if sku:
                lookup = bl_call("getInventoryProductsData", {"filter_sku": [sku]})
                prods = lookup.get("products", [])
                if prods and prods[0].get("product_id"):
                    pid = str(prods[0]["product_id"])

        if pid:
            order_lines.append({"product_id": str(pid), "qty_needed": qty})
            if not any_pid_for_inv_lookup:
                any_pid_for_inv_lookup = str(pid)

    if not order_lines:
        abort(400, "No transferrable items found on this order (no product_ids/quantities)")

    inv_id = get_inventory_id_for_any_product(any_pid_for_inv_lookup)

    total_moved = 0
    per_bin_docs = 0

    # Sweep through source bins; create one transfer doc per source bin
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
            "comment": f"Auto multi-bin sweep for order {order_id}: {src_loc_id} -> {dst_loc_id}"
        }

        try:
            _ = bl_call("addInventoryStockDocument", doc_params)
            # Assume requested qty moved; decrement remaining
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
            # If rejected (e.g., not enough in this bin), we skip and continue to next bin.
            continue

    remaining = sum(l["qty_needed"] for l in order_lines)
    return (f"OK — created {per_bin_docs} transfer document(s) into bin {dst_loc_id} "
            f"in warehouse {WAREHOUSE_ID}. Moved {total_moved} unit(s). "
            f"Remaining not moved: {remaining}. Order {order_id}\n")

@app.get("/health")
def health():
    return "OK\n"
