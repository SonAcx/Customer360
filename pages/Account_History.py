import streamlit as st
import pandas as pd
from snowflake_connector import get_snowflake_connection

st.title("🔍 Account Lookup")

conn = get_snowflake_connection()
cur = conn.cursor()

# Search inputs
st.subheader("Search by any of the following:")
col1, col2, col3 = st.columns(3)

with col1:
    name_input = st.text_input("Name")
with col2:
    ff_id_input = st.text_input("Firefly ID")
with col3:
    sf_id_input = st.text_input("Salesforce ID")

# Build dynamic WHERE clause
where_clauses = []
if name_input:
    where_clauses.append(f"LOWER(NAME) LIKE '%{name_input.lower()}%'")
if ff_id_input:
    where_clauses.append(f"FF_ID ILIKE '%{ff_id_input}%'")
if sf_id_input:
    where_clauses.append(f"SF_ID ILIKE '%{sf_id_input}%'")

if where_clauses:
    where_sql = "WHERE " + " AND ".join(where_clauses)
    
    search_query = f"""
        SELECT ACCOUNT_ID, NAME, FF_ID, SF_ID
        FROM PROD_DWH.DWH.DIM_ACCOUNT
        {where_sql}
        ORDER BY NAME
        LIMIT 50
    """
    cur.execute(search_query)
    results = cur.fetchall()

    if results:
        options = {
            f"{name} | Firefly: {ff_id or '—'} | Salesforce: {sf_id or '—'}": acc_id
            for acc_id, name, ff_id, sf_id in results
        }
        selected_label = st.selectbox("Select an Account", list(options.keys()))
        selected_account_id = options[selected_label]

        st.subheader(f"Account History for: {selected_label}")

        # Replace with your real history table if different
        history_query = f"""
            SELECT *
            FROM PROD_DWH.DWH.ACCOUNT_HISTORY
            WHERE ACCOUNT_ID = '{selected_account_id}'
            ORDER BY EVENT_DATE DESC
            LIMIT 100
        """
        cur.execute(history_query)
        rows = cur.fetchall()

        if rows:
            cols = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=cols)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No history found for this account.")
    else:
        st.warning("No matching accounts found.")
else:
    st.info("Enter one or more values to search.")

