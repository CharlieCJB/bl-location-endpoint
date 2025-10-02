import os, json
from flask import Flask, request, abort
import requests

BL_API_URL = "https://api.baselinker.com/connector.php"
BL_TOKEN   = os.environ.get("BL_TOKEN")            # set on Render
SHARED_KEY = os.environ.get("BL_SHARED_KEY", "")   # set on Render

app = Flask(__name__)

def bl_call(method: str, params: dict):
    headers = {"X-BLToken": BL_TOKEN}
    data = {"method": method, "parameters": json.dumps(params)}
    r = requests.post(BL_API_URL, headers=headers, data=data, timeout=20)
    r.raise_for_status()
    return r.json()

@app.get("/bl/update_location")
def update_location():
    if SHARED_KEY and request.args.get("key") != SHARED_KEY:
        abort(401)

    order_id = request.args.get("order_id", "").strip()
    if not order_id:
        abort(400, "Missing order_id")

    target_location = "Internal Stock"  # your requested location

    o = bl_call("getOrders", {"order_id": order_id})
    orders = o.get("orders", [])
    if not orders:
        abort(404, "Order not found")
    order = orders[0]
    items = order.get("products", [])

    updated = 0
    for it in items:
        product_id = it.get("product_id")
        sku = it.get("sku")

        if product_id:
            bl_call("updateInventoryProduct", {
                "product_id": product_id,
                "location": target_location
            })
            updated += 1
            continue

        if sku:
            inv = bl_call("getInventoryProductsData", {"filter_sku": [sku]})
            products = inv.get("products", [])
            if products and products[0].get("product_id"):
                bl_call("updateInventoryProduct", {
                    "product_id": products[0]["product_id"],
                    "location": target_location
                })
                updated += 1

    return f"OK — set location to '{target_location}' for {updated} item(s) in order {order_id}\n"
