import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import requests
import os
from dotenv import load_dotenv

load_dotenv()
RED=os.getenv("RED")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def fetch_stockout_items():
    url = f"{SUPABASE_URL}/rest/v1/Orders_Exceed_Inventory?select=item_id,description,on_hand, on_so,category_id"
    response = requests.get(url, headers=HEADERS)
    if response.ok:
        return response.json()
    else:
        st.error("❌ Out-of-stock data could not be obtained.")
        st.text(f"🔴 Supabase response: {response.status_code} - {response.text}")
        return []

def fetch_categories():
    url = f"{SUPABASE_URL}/rest/v1/Item_Categories?select=id,name"
    response = requests.get(url, headers=HEADERS)
    if response.ok:
        return response.json()
    else:
        return []

def show_demand_exceeds_stock_section():
    st.subheader("Understocked SO Items")

    data = fetch_stockout_items()
    if not data:
        return

    df = pd.DataFrame(data)
    df["on_hand"] = pd.to_numeric(df["on_hand"], errors="coerce")

    categories = fetch_categories()
    category_map = {cat["id"]: cat["name"] for cat in categories}

    df["category_name"] = df["category_id"].map(category_map)

    selected_category = st.selectbox("🔍 Filter by category", ["All"] + sorted(df["category_name"].dropna().unique().tolist()))
    if selected_category != "All":
        df = df[df["category_name"] == selected_category]

    if df.empty:
        st.info("✅ There are no out-of-stock items for this category..")
        return

    df = df.sort_values(by="on_hand")

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["on_hand"],
        y=df["description"],
        orientation='h',
        marker=dict(color="RED"),
        name= "Stock",
        hovertemplate='%{y}<br>Stock: %{x:.0f} units'
    ))

    fig.add_trace(go.Bar(
        x=df["on_so"],
        y=df["description"],
        orientation='h',
        name="On SO",
        marker=dict(color="limegreen"),
        hovertemplate='%{y}<br>On SO: %{x:.0f} units'
    ))

    fig.add_shape(
        type="line",
        x0=0, x1=0,
        y0=-0.5, y1=len(df) - 0.5,
        line=dict(color= "#0DB10D", dash="dash"),
        layer="below"
    )

    # Lines only on the edge
    fig.update_xaxes(showgrid=False, zeroline=True, zerolinecolor="white")
    fig.update_yaxes(showgrid=False)

    fig.update_layout(
        title=f"Stock VS on SO ({len(df)} items)",
        xaxis_title="Quantities",
        yaxis_title="Item",
        yaxis=dict(autorange="reversed"),
        template="plotly_dark"
    )

    st.plotly_chart(fig, use_container_width=True)
