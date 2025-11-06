import os
from urllib.parse import urljoin
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

LOGO = os.getenv("LOGO") or ""
STREAMLIT_URL = os.getenv("STREAMLIT_URL") or ""
BACKEND_URL = (os.getenv("BACKEND_URL") or "").rstrip("/")


def show_sidebar_menu():
    if "active_menu" not in st.session_state:
        st.session_state.active_menu = None
    if "active_submenu" not in st.session_state:
        st.session_state.active_submenu = None

    with st.sidebar:
        # Show logo only if present and valid
        if LOGO.strip():
            try:
                st.image(LOGO)
            except Exception as e:
                st.warning(f"Logo could not be loaded: {e}")

        st.subheader(f"Hi, {st.session_state.get('name', 'User')}")

        def uniform_button(label, key):
            # Consistent sidebar buttons
            return st.button(label, key=key, use_container_width=True) 


        # ==== Main Menu ====
        if uniform_button("📈 Dashboard", key="btn_dashboard"):
            st.session_state.active_menu = "Dashboard"
            st.session_state.active_submenu = None

        if uniform_button("📤 Inventory", key="btn_inventory"):
            st.session_state.active_menu = "Inventory"
            st.session_state.active_submenu = None

        if uniform_button("🚩 HubSpot", key="btn_hubspot"):
            st.session_state.active_menu = "HubSpot"
            st.session_state.active_submenu = None

        if uniform_button("🌎 Google Earth", key="btn_google"):
            st.session_state.active_menu = "Google Earth"
            st.session_state.active_submenu = None

        st.markdown("---")

        # ==== Submenus ====
        if st.session_state.active_menu == "Inventory":
            st.markdown("**Inventory Options:**")
            if uniform_button("🖥️ System Inventory", key="btn_sys_inv"):
                st.session_state.active_submenu = "System Inventory"
            if uniform_button("📝 Physical Count", key="btn_phys_count"):
                st.session_state.active_submenu = "Physical Count"
            if uniform_button("🛠️ Restock Manager", key="btn_restock_count"):
                st.session_state.active_submenu = "Restock Manager"

        if st.session_state.active_menu == "HubSpot":
            st.markdown("**HubSpot Options:**")
            if uniform_button("📑 Create New Leads File", key="btn_lds_file"):
                st.session_state.active_submenu = "Create New Leads File"

        if uniform_button("🚪 Log out", key="logout_btn"):
            st.session_state.clear()

            # Clear query params safely across Streamlit versions
            try:
                st.query_params.update({})
            except Exception:
                pass

            # Redirect only if BACKEND_URL is provided
            if BACKEND_URL:
                logout_url = urljoin(BACKEND_URL + "/", "logout")
                st.markdown(
                    f"<meta http-equiv='refresh' content='0;url={logout_url}'>",
                    unsafe_allow_html=True,
                )
            else:
                st.info("Logged out locally (no BACKEND_URL set).")

            st.stop()

    return st.session_state.active_menu, st.session_state.active_submenu
