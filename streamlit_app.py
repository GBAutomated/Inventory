import streamlit as st
import os
from dotenv import load_dotenv
from app.views.inventory_dashboard import show_dashboard
from app.views.dashboard import show_dashboard as show_general_dashboard
from app.views.upload_system import show_upload_system
from app.views.upload_physical import (
    show_upload_physical,
    generate_physical_inventory_template,   
)
from app.services.supabase_uploader import (
    get_user_name_by_email,
    get_user_id_by_email,
    fetch_all_categories,
    get_latest_stock_items as fetch_items_by_cat 
)
from app.views.restock_manager import(
    generate_restock_file_by_categories_template,
    show_upload_restock_file,
    show_kpis,
    show_restock_table_and_file_download
)
from app.views.menu import show_sidebar_menu

load_dotenv()
LOGO = os.getenv("LOGO")
GOOGLE = os.getenv("GOOGLE")
BACKEND_URL = os.getenv("BACKEND_URL")

if "user_id" not in st.session_state:
    st.session_state["user_id"] = None

def require_login():
    query_params = st.query_params

    if "logout" in query_params:
        st.session_state.clear()
        st.query_params.clear()
        st.rerun()

    if "user" not in st.session_state:
        if "user" in query_params:

            email = query_params["user"]
            if isinstance(email, list):
                email = email[0]

            st.session_state["user"] = email

            user_name = get_user_name_by_email(email)
            st.session_state["name"] = user_name or email

            st.session_state["user_id"] = get_user_id_by_email(email)
            print("User Id en login:", st.session_state["user_id"])

            st.success(f"Welcome 👋 {st.session_state['name']}")
            st.query_params.clear()
        else:
            st.image(LOGO)
            st.title("🔐 Welcome to SFR GB System ")
            if st.button("Sign in with your Google Account", key="login_btn"):
                login_url = f"{BACKEND_URL}/login"
                st.markdown(
                    f"<meta http-equiv='refresh' content='0;url={login_url}'>",
                    unsafe_allow_html=True
                )
                st.stop()
            st.stop()

# Start of the app
require_login()
st.set_page_config(page_title="Inventory Dashboard", layout="wide")

# Menu
active_menu, active_submenu = show_sidebar_menu()

if active_menu == "Inventory" and active_submenu == "System Inventory":
    show_upload_system()

elif active_menu == "Inventory" and active_submenu == "Physical Count":
    col1, col2 = st.columns([0.6, 0.4])

    with col1:
        show_upload_physical()

    with col2:
        st.subheader("Download Physical Count Sheet")
        st.markdown("Select one or more categories to include in the template.")

        cats = fetch_all_categories() or []
        cat_names = sorted({(c.get("name") or "").strip() for c in cats if c.get("name")})

        if not cat_names:
            st.warning("No categories found.")
        else:
            selected = st.multiselect(
                "Categories",
                options=cat_names,
                key="pc_categories",  # persistimos selección
                placeholder="Choose one or more categories…",
            )

            st.caption(f"{len(selected)} selected")

            if st.button("Generate File", disabled=len(selected) == 0, type="primary", key="gen_file_btn"):
                items = fetch_items_by_cat(categories=selected)  # del servicio
                if not items:
                    st.warning("No items found for the selected categories.")
                else:
                    excel_bytes = generate_physical_inventory_template(
                        items,
                        included_categories=selected,
                    )
                    st.session_state["pc_excel_bytes"] = excel_bytes
                    st.success("Template generated. Use the button below to download it.")

            if "pc_excel_bytes" in st.session_state:
                st.download_button(
                    label="⬇️ Download Physical Count Sheet",
                    data=st.session_state["pc_excel_bytes"],
                    file_name="PhysicalInventorySheet.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_sheet_btn",
                )

elif active_menu == "Inventory" and active_submenu == "Restock Manager":

    show_kpis()

    show_restock_table_and_file_download()

    col1, col2 = st.columns([0.5, 0.5])

    with col1:
        uid = st.session_state.get("user_id")
        print("User ID:", uid)
        if not uid:
            st.warning("No user id in session. Please sign in again.")
        else:
            show_upload_restock_file(uid)

    with col2:
        st.subheader("Download Min Reorder File")
        st.markdown("Select one or more categories to include in the file.")

        cats = fetch_all_categories() or []
        cat_names = sorted({(c.get("name") or "").strip() for c in cats if c.get("name")})

        if not cat_names:
            st.warning("No categories found.")
        else:
            selected = st.multiselect(
                "Categories",
                options=cat_names,
                key="pc_categories", 
                placeholder="Choose one or more categories…",
            )

            st.caption(f"{len(selected)} selected")

            if st.button("Generate File", disabled=len(selected) == 0, type="primary", key="gen_file_btn"):
                items = fetch_items_by_cat(categories=selected) 
                if not items:
                    st.warning("No items found for the selected categories.")
                else:
                    excel_bytes = generate_restock_file_by_categories_template(
                        items
                    )
                    st.session_state["pc_excel_bytes"] = excel_bytes
                    st.success("Template generated. Use the button below to download it.")

            if "pc_excel_bytes" in st.session_state:
                st.download_button(
                    label="⬇️ Download Reordering Quantities Sheet",
                    data=st.session_state["pc_excel_bytes"],
                    file_name="ReorderingQuantities.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_sheet_btn",
                )

elif active_menu == "Dashboard":
    show_general_dashboard()

elif active_menu == "Inventory":
    show_dashboard()

        
#elseif active_menu == "Settings":
#    st.subheader("⚙️ Settings")

else:
    show_general_dashboard()

