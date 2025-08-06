import streamlit as st
import pandas as pd
import tempfile
import os
from dotenv import load_dotenv
from datetime import datetime
from io import BytesIO
from app.services.supabase_uploader import (
    insert_physical_count,
    insert_physical_count_items,
    fetch_latest_stock_items,
    get_item_by_name,
    insert_physical_count_categories,
    get_all_categories
)
from app.services.excel_handler import parse_physical_count


# Download template--
def generate_physical_inventory_template(items: list) -> bytes:
    df = pd.DataFrame(items)

    rename_map = {
        "category_name": "Category",
        "name": "Name",
        "description": "Description"
    }
    df = df.rename(columns=rename_map)

    columns = ["Category", "Name", "Description", "Counted"]
    df = df[columns[:-1]]
    df["Counted"] = ""

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="PhysicalCount", startrow=3)

        workbook = writer.book
        sheet = writer.sheets["PhysicalCount"]

        sheet["A1"] = "Count Date:"
        sheet["B1"] = ""
        sheet["A2"] = "Responsible:"
        sheet["B2"] = ""

        from openpyxl.styles import Font, Border, Side
        thin_border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="thin")
        )
        for cell in sheet[4]:
            cell.font = Font(bold=True)
        for row in sheet.iter_rows(min_row=5, max_row=4 + len(df), min_col=1, max_col=4):
            for cell in row:
                cell.border = thin_border

    output.seek(0)
    return output.getvalue()

# Upload completed file
def process_uploaded_physical_file(path: str):
    st.info("📊 Reading and parsing Excel file...")

    try:
        df = pd.read_excel(path, header=None)
        st.write("📄 File preview:")
        st.dataframe(df.head(6))

        count_date = str(df.iloc[0, 1]).strip()
        responsable = str(df.iloc[1, 1]).strip()

        if not count_date or count_date.lower() == "nan":
            st.error("❌ 'Count Date' is missing in the file.")
            return

        if not responsable or responsable.lower() == "nan":
            st.error("❌ 'Responsible' is missing in the file.")
            return

        st.info(f"📅 Count Date: {count_date} | 👤 Responsible: {responsable}")

        headers = df.iloc[3].tolist()
        headers = [str(h).strip() for h in headers]
        expected_headers = ["Category", "Name", "Description", "Counted", "Notes"]

        if headers != expected_headers:
            st.error(f"❌ Column mismatch.\nExpected: {expected_headers}\nFound: {headers}")
            return

        data_df = df.iloc[4:].copy()
        data_df.columns = expected_headers
        data_df = data_df.dropna(subset=["Name", "Counted"])
        data_df["Counted"] = pd.to_numeric(data_df["Counted"], errors="coerce").fillna(0)

        st.write("📦 Parsed item data:")
        st.dataframe(data_df.head())

        # Insert Stock_Count record
        st.info("📩 Inserting stock count header...")
        count_record = insert_physical_count({
            "count_date": count_date,
            "responsable": responsable
        })

        if not count_record or not count_record.get("id"):
            st.error("❌ Could not create stock count record.")
            return

        count_id = count_record["id"]

        # Prepare item records
        count_items = []
        for _, row in data_df.iterrows():
            item = get_item_by_name(row["Name"])
            if not item:
                st.warning(f"⚠️ Item not found: {row['Name']}")
                continue

            count_items.append({
                "stock_count_id": count_id,
                "item_id": item["id"],
                "counted_qty": row["Counted"]
            })

        # Prepare category summary
        categories = data_df["Category"].dropna().unique().tolist()
        count_categories = [{"stock_count_id": count_id, "category_name": cat} for cat in categories]

        # Insert categories
        st.info(f"🗂️ Uploading {len(count_categories)} categories...")
        cat_success = insert_physical_count_categories(count_categories)
        if not cat_success:
            st.warning("⚠️ Some categories could not be saved.")

        # Insert item rows
        st.info(f"📤 Uploading {len(count_items)} count items...")
        success = insert_physical_count_items(count_items)
        if success:
            st.success("✅ Physical count successfully uploaded.")
        else:
            st.error("❌ Failed to upload physical count items.")

    except Exception as e:
        st.error(f"❌ Error processing file: {str(e)}")

# Main view
def show_upload_physical():
    st.subheader("📥 Upload Physical Count File")

    uploaded_file = st.file_uploader("Select the file with physical count", type=["xlsx"])

    if uploaded_file:

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file.read())
            temp_path = tmp.name

        try:
            df = pd.read_excel(temp_path, header=None)
            st.dataframe(df.head(6))

            # Extract metadata
            count_date = str(df.iloc[0, 1]).strip()
            responsable = str(df.iloc[1, 1]).strip()

            if not count_date or count_date.lower() == "nan":
                st.error("❌ 'Count Date' is missing in the file.")
                return
            if not responsable or responsable.lower() == "nan":
                st.error("❌ 'Responsible' is missing in the file.")
                return

            # Headers
            raw_headers = df.iloc[3].tolist()
            headers = [str(h).strip().lower() for h in raw_headers]
            expected_headers = ["category", "name", "description", "counted", "notes"]

            if headers != expected_headers:
                st.error(f"❌ Header mismatch.\nExpected: {expected_headers}\nFound: {headers}")
                return

            df_data = df.iloc[4:].copy()
            df_data.columns = expected_headers
            st.dataframe(df_data.head())

            df_data = df_data.dropna(subset=["name", "counted"])
            df_data["counted"] = pd.to_numeric(df_data["counted"], errors="coerce").fillna(0)

            count_record = insert_physical_count({
                "count_date": count_date,
                "responsable": responsable
            })

            if not count_record or not count_record.get("id"):
                st.error("❌ Could not create stock count record.")
                return

            count_id = count_record["id"]
            count_items = []
            category_set = set()

            # Initialize progress
            total_steps = len(df_data) + 2  # Items + categories + insert calls
            progress = st.progress(0)
            status = st.empty()
            step = 0

            for i, (_, row) in enumerate(df_data.iterrows(), 1):
                status.info(f"🔍 Matching item {i} of {len(df_data)}: {row['name']}")
                item = get_item_by_name(row["name"])
                if not item:
                    st.warning(f"⚠️ Item not found: {row['name']}")
                    continue
                count_items.append({
                    "stock_count_id": count_id,
                    "item_id": item["id"],
                    "counted_qty": row["counted"]
                })
                if row["category"]:
                    category_set.add(row["category"])
                step += 1
                progress.progress(step / total_steps)

            status.info(f"📤 Uploading {len(count_items)} items to Supabase...")
            items_ok = insert_physical_count_items(count_items)
            step += 1
            progress.progress(step / total_steps)

            status.info("🔎 Matching categories to IDs...")
            all_categories = get_all_categories()
            category_lookup = {
                c["name"].strip().lower(): c["id"]
                for c in all_categories
            }

            matched_categories = []
            for name in category_set:
                category_id = category_lookup.get(name.strip().lower())
                if category_id:
                    matched_categories.append({
                        "stock_count_id": count_id,
                        "category_id": category_id
                    })
                else:
                    st.warning(f"⚠️ Category not found in DB: {name}")
            step += 1
            progress.progress(step / total_steps)

            status.info(f"📁 Uploading {len(matched_categories)} categories...")
            category_ok = insert_physical_count_categories(matched_categories)
            step += 1
            progress.progress(min(step / total_steps, 1.0))  # Ensure 100% max

            if items_ok and category_ok:
                status.success("✅ Physical count and categories successfully uploaded.")
            elif items_ok:
                status.warning("⚠️ Items uploaded, but some categories were not found.")
            else:
                status.error("❌ Failed to upload physical count items.")

            progress.empty()  # Remove progress bar after completion

        except Exception as e:
            st.error(f"❌ Error processing file: {str(e)}")
        finally:
            os.unlink(temp_path)

