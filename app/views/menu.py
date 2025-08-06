import streamlit as st
import os
from dotenv import load_dotenv

load_dotenv()
LOGO=os.getenv("LOGO")
STREAMLIT_URL=os.getenv("STREAMLIT_URL")

def show_sidebar_menu():

    if "active_menu" not in st.session_state:
        st.session_state.active_menu = None
    if "active_submenu" not in st.session_state:
        st.session_state.active_submenu = None

    with st.sidebar:
        st.image(LOGO)
        st.subheader(f"Hi, {st.session_state.get('name', 'User')}")

        def uniform_button(label, key):
            return st.button(label, key=key, use_container_width=True)

        # Main Menu
        if uniform_button("📤 Inventory", key="btn_inventory"):
            st.session_state.active_menu = "Inventory"
            st.session_state.active_submenu = None
        if uniform_button("📈 Dashboard", key="btn_dashboard"):
            st.session_state.active_menu = "Dashboard"
            st.session_state.active_submenu = None
        if uniform_button("⚙️ Settings", key="btn_settings"):
            st.session_state.active_menu = "Settings"
            st.session_state.active_submenu = None

        st.markdown("---")

        # Submenu
        if st.session_state.active_menu == "Inventory":
            st.markdown("**Inventory Options:**")
            if uniform_button("📤 System Inventory", key="btn_sys_inv"):
                st.session_state.active_submenu = "System Inventory"
            if uniform_button("📤 Physical Count", key="btn_phys_count"):
                st.session_state.active_submenu = "Physical Count"

        if uniform_button("🚪 Log out", key="logout_btn"):
            st.session_state.clear()
            st.query_params.clear()
            st.markdown("🔄 Closing session...")
            st.markdown(
                "<meta http-equiv='refresh' content='0;url=http://localhost:8000/logout'>",
                unsafe_allow_html=True
            )
            st.stop()

    return st.session_state.active_menu, st.session_state.active_submenu
