import json
import os
from datetime import datetime
from urllib import error, request

import streamlit as st

API_BASE = os.environ.get("RETAIL_API_URL", "http://127.0.0.1:8001")


def api_get(path: str):
    try:
        with request.urlopen(f"{API_BASE}{path}", timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.URLError as exc:
        st.error(f"API request failed: {exc}")
        return None


def api_post(path: str, payload: dict):
    req = request.Request(
        f"{API_BASE}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        st.error(f"Transaction failed: {detail}")
        return None
    except error.URLError as exc:
        st.error(f"API request failed: {exc}")
        return None


st.set_page_config(page_title="Walmart Live Retail Demo", layout="wide")
st.markdown(
    """
    <style>
    .stApp {background: linear-gradient(180deg, #f5fbff 0%, #eef5ff 100%);} 
    .hero {background: linear-gradient(135deg, #0071ce 0%, #00a3ff 60%, #ffc220 100%); padding: 24px; border-radius: 18px; color: white;}
    .metric-card {background: white; padding: 16px; border-radius: 16px; box-shadow: 0 8px 20px rgba(0,0,0,0.05);} 
    .product-card {background: white; padding: 14px; border-radius: 16px; min-height: 280px; box-shadow: 0 8px 20px rgba(0,0,0,0.06);} 
    </style>
    """,
    unsafe_allow_html=True,
)

if "cart" not in st.session_state:
    st.session_state.cart = {}

products = api_get("/retail/products") or []
overview = api_get("/retail/monitor/overview") or {}

st.markdown("<div class='hero'><h1>Walmart Live Transaction Demo</h1><p>UI -> Kafka -> Spark Streaming -> AWS S3 raw -> Airflow bronze</p></div>", unsafe_allow_html=True)

checkout_tab, monitor_tab = st.tabs(["Live Checkout", "Pipeline Monitor"])

with checkout_tab:
    left, right = st.columns([2.2, 1])
    with left:
        st.subheader("Product Catalog")
        cols = st.columns(4)
        for idx, product in enumerate(products):
            with cols[idx % 4]:
                st.markdown("<div class='product-card'>", unsafe_allow_html=True)
                st.image(product["image"], use_column_width=True)
                st.markdown(f"**{product['name']}**")
                st.caption(f"{product['category']} | SKU {product['sku']}")
                st.write(f"${product['price']:.2f}")
                qty = st.number_input(f"Qty {product['sku']}", min_value=1, max_value=20, value=1, key=f"qty_{product['sku']}")
                if st.button(f"Add {product['sku']}", key=f"add_{product['sku']}"):
                    st.session_state.cart[product["sku"]] = st.session_state.cart.get(product["sku"], 0) + int(qty)
                st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.subheader("Cart & Checkout")
        customer_name = st.text_input("Customer name", value="Ankur Chopra")
        customer_email = st.text_input("Customer email", value="ankur@example.com")
        payment_method = st.selectbox("Payment method", ["card", "wallet", "cash"])
        order_items = []
        subtotal = 0.0
        product_map = {p["sku"]: p for p in products}
        for sku, quantity in st.session_state.cart.items():
            product = product_map.get(sku)
            if not product:
                continue
            line_total = quantity * product["price"]
            subtotal += line_total
            order_items.append({"sku": sku, "quantity": quantity})
            st.write(f"{product['name']} x {quantity} = ${line_total:.2f}")
        tax = subtotal * 0.0825
        total = subtotal + tax
        st.metric("Subtotal", f"${subtotal:.2f}")
        st.metric("Tax", f"${tax:.2f}")
        st.metric("Total", f"${total:.2f}")
        if st.button("Submit live transaction", type="primary", disabled=not order_items):
            payload = {
                "customer_name": customer_name,
                "customer_email": customer_email,
                "payment_method": payment_method,
                "items": order_items,
            }
            result = api_post("/retail/transactions", payload)
            if result:
                st.success(f"Transaction sent. Order ID: {result['order_id']}")
                st.info(f"Kafka topic: {result['topic']} | S3 bucket: {result['bucket']} | raw prefix: {result['raw_prefix']}")
                st.session_state.cart = {}

with monitor_tab:
    kafka = api_get("/retail/monitor/kafka") or {}
    storage = api_get("/retail/monitor/storage") or {}
    stream = api_get("/retail/monitor/streaming") or {}
    overview = api_get("/retail/monitor/overview") or {}

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Kafka est. messages", kafka.get("estimated_messages", 0))
    m2.metric("Kafka partitions", kafka.get("partitions", 0))
    m3.metric("Raw objects", storage.get("raw", {}).get("object_count", 0))
    m4.metric("Bronze objects", storage.get("bronze", {}).get("object_count", 0))

    st.subheader("Spark Streaming")
    st.json(stream)

    st.subheader("S3 Storage")
    st.json(storage)

    st.subheader("Recent Transactions")
    st.dataframe(overview.get("recent_transactions", []), use_container_width=True)

    st.subheader("Tooling Links")
    st.write(f"Redpanda Console: {overview.get('console_url', 'http://localhost:8081')}")
    st.write(f"Raw bucket/prefix: s3://{overview.get('bucket', '')}/{overview.get('raw_prefix', '')}")
    st.write(f"Bronze bucket/prefix: s3://{overview.get('bucket', '')}/{overview.get('bronze_prefix', '')}")
    st.caption(f"Last refresh: {datetime.utcnow().isoformat()}Z")
