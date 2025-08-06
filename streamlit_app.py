import streamlit as st
import os
from dotenv import load_dotenv
from app.views.dashboard import show_dashboard
from app.views.upload_system import show_upload_system
from app.views.upload_physical import show_upload_physical, fetch_latest_stock_items, generate_physical_inventory_template
from app.services.supabase_uploader import get_user_name_by_email
from app.views.menu import show_sidebar_menu

load_dotenv()
LOGO=os.getenv("LOGO")
GOOGLE=os.getenv("GOOGLE")
BACKEND_URL = os.getenv("BACKEND_URL")

def require_login():
    query_params = st.query_params

    if "logout" in query_params:
        st.session_state.clear()
        st.query_params.clear()
        st.rerun()

    if "user" not in st.session_state:
        if "user" in query_params:
            st.session_state["user"] = query_params["user"]
            user_name = get_user_name_by_email(st.session_state["user"])
            if user_name:
                st.session_state["name"] = user_name
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

if active_menu == "Dashboard":
    show_dashboard()

elif active_submenu == "System Inventory":
    show_upload_system()

elif active_submenu == "Physical Count":
    col1, col2= st.columns([0.6, 0.4])

    with col1:
        show_upload_physical()
    with col2:        
        st.subheader("Download Physical Count Sheet")
        st.text("")
        st.text("")
        if st.button("Generate File"):
            items = fetch_latest_stock_items()
            if items:
                excel_bytes = generate_physical_inventory_template(items)
                st.download_button(
                    label="⬇️ Download Physical Count Sheet",
                    data=excel_bytes,
                    file_name="PhysicalInventorySheet.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

#elif active_menu == "Settings":
#    st.subheader("⚙️ Settings")

else:
    show_dashboard()
