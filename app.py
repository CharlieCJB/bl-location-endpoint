# app.py
import os, json, time
from typing import Optional
from flask import Flask, request, abort
import requests

BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN   = os.environ.get("BL_TOKEN")
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")

# Your one-and-only warehouse id for locations:
WAREHOUSE_ID = "77617"          # <- you said always use this one
TARGET_LOCATION = "Internal Stock"

app = Flask(__name__)

def bl_call(method: str, params: dict):
    """Minimal BaseLinker API wrapper."""
    if not BL_TOKEN:
        abort(500, "BL_TOKEN not set")
    headers = {"X-BLToken": BL_TOKEN}
    data = {"method": method, "parameters": json.dumps(params)}
    r = requests.post(BL_API_URL, headers=headers, data=data, timeout=30)
    r.raise_for_status()
    j = r.json()
    if "error" in j and j["error"]:
        abort(502, f"BaseLinker API error in {method}: {j['error']}")
    return j

def resolve_order_id(order_id: Optional[str], order_number: Optional[str]) -> str:
    """
    Prefer order_id (fast). If only order_number is provided,
    scan recent orders to find the matching order_id.
    """
    if order_id:
        return str(order_id)

    if not order_number:
        abort(400, "Provide order_id or order_number")

    # Search last 365 days (adjust if you need more)
    date_from = int(time.time()) - 365 * 24 * 60 * 60
    resp = bl_call("getOrders", {
        "date_confirmed_from": date_from,
        "get_unconfirmed_orders": True
    })
    for o in resp.get("orders", []):
        if str(o.get("order_number", "")).strip() == str(order_number).strip():
            return str(o.get("order_id"))
    abort(404, f"Order with order_number '{order_number}' not found in last 365 days")

def set_location_for_inventory_product(product_id: str, target_location: str) -> bool:
    """
    Set the Location text for this inventory product, for warehouse 77617 only.
    Uses addInventoryProduct (which updates when product_id exists).
    """
    # 1) Fetch product to learn its inventory_id (required for update)
    inv = bl_call("getInventoryProductsData", {
        "filter_ids": [product_id],
        "include": ["locations"]      # we only need inventory_id + existing locations
    })
    products = inv.get("products", [])
    if not products:
        return False

    p = products[0]
    inventory_id = p.get("inventory_id")
    if not inventory_id:
        return False

    # 2) Build locations dict for ONLY your warehouse 77617
    #    (This will set/override that one key; other warehouses are not included.)
    new_locations = {WAREHOUSE_ID: target_location}

    # 3) Update via addInventoryProduct (acts as update when product_id is present)
    bl_call("addInventoryProduct", {
        "inventory_id": str(inventory_id),
        "product_id": str(product_id),
        "locations": new_locations
    })
    return True

@app.get("/bl/update_location")
def update_location():
    # Simple auth
    supplied = request.args.get("key") or request.headers.get("X-App-Key")
    if SHARED_KEY and supplied != SHARED_KEY:
        abort(401, "Unauthorized: key mismatch")

    order_id_param = (request.args.get("order_id") or "").strip() or None
    order_number   = (request.args.get("order_number") or "").strip() or None

    # 1) Resolve to a real API order_id
    order_id = resolve_order_id(order_id_param, order_number)

    # 2) Get the order and its items
    o = bl_call("getOrders", {"order_id": order_id})
    orders = o.get("orders", [])
    if not orders:
        abort(404, "Order not found by order_id")
    order = orders[0]
    items = order.get("products", [])

    # 3) Update each item's inventory product location (by product_id, or fallback via SKU)
    updated = 0
    for it in items:
        inv_product_id = it.get("product_id")
        sku = it.get("sku")

        if inv_product_id:
            if set_location_for_inventory_product(str(inv_product_id), TARGET_LOCATION):
                updated += 1
            continue

        if sku:
            # Resolve SKU to inventory product_id
            inv = bl_call("getInventoryProductsData", {
                "filter_sku": [sku],
                "include": ["locations"]
            })
            prods = inv.get("products", [])
            if prods and prods[0].get("product_id"):
                pid = str(prods[0]["product_id"])
                if set_location_for_inventory_product(pid, TARGET_LOCATION):
                    updated += 1

    return f"OK — set location to '{TARGET_LOCATION}' for {updated} item(s) in order {order_id}\n"

@app.get("/health")
def health():
    return "OK\n"
