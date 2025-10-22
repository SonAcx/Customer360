import streamlit as st
import pandas as pd
import sys, os

# --- IMPORTS ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from snowflake_connector import get_snowflake_connection, get_product_activity_by_gamechanger_id, check_activity_exists

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="Customer 360", layout="wide")

# --- INITIALIZE SESSION STATE ---
if 'page' not in st.session_state:
    st.session_state.page = 'search'
if 'selected_account' not in st.session_state:
    st.session_state.selected_account = None
if 'current_page' not in st.session_state:
    st.session_state.current_page = 0

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    /* SIDEBAR */
    section[data-testid="stSidebar"] {
        background-color: #003366 !important;
    }
    section[data-testid="stSidebar"] * {
        color: white !important;
        font-weight: 500 !important;
    }
    
    /* PAGINATION */
    .pagination {
        text-align: center;
        margin-top: 20px;
        font-size: 16px;
        font-weight: bold;
    }
    
    /* DATAFRAME HEADER STYLING - Acxion Colors */
    [data-testid="stDataFrame"] [data-testid="stDataFrameResizableContainer"] > div:first-child {
        background-color: #003366 !important;
    }
    
    [data-testid="stDataFrame"] thead th {
        background-color: #003366 !important;
        color: white !important;
        font-weight: bold !important;
        font-size: 14px !important;
        padding: 12px 8px !important;
        border-right: 1px solid #00a3e0 !important;
    }
    
    /* Column headers */
    [data-testid="stDataFrame"] [data-testid="column-header"] {
        background-color: #003366 !important;
        color: white !important;
        font-weight: bold !important;
    }
    
    /* Zebra striping */
    [data-testid="stDataFrame"] tbody tr:nth-child(even) {
        background-color: #f8f9fa !important;
    }
    
    /* Row hover effect */
    [data-testid="stDataFrame"] tbody tr:hover {
        background-color: #e6f0ff !important;
        cursor: pointer;
    }
    
    /* Info box styling */
    [data-testid="stAlert"] {
        background-color: #e6f0ff !important;
        border-left: 4px solid #003366 !important;
    }
    
    /* SALESFORCE PRODUCT ACTIVITY TABLE STYLING */
    [data-testid="stDataFrame"] {
        border: 2px solid #003366 !important;
    }
    
    /* Darker table lines */
    [data-testid="stDataFrame"] td {
        border: 1px solid #003366 !important;
    }
    
    [data-testid="stDataFrame"] th {
        border: 1px solid #003366 !important;
        text-align: center !important;
        white-space: normal !important;
        word-wrap: break-word !important;
        padding: 8px !important;
        min-width: 100px !important;
    }
    
    /* Center all header text */
    [data-testid="stDataFrame"] [data-testid="column-header"] {
        text-align: center !important;
        justify-content: center !important;
    }
    
    /* Darker row borders */
    [data-testid="stDataFrame"] tbody tr {
        border-bottom: 1px solid #003366 !important;
    }
    </style>
""", unsafe_allow_html=True)

# --- SIDEBAR ---
st.sidebar.image("assets/acxionlogo2.png", use_container_width=True)
st.sidebar.markdown(
    "<h3 style='text-align:center; color:white; font-weight:bold;'>Powered by Acxion</h3>",
    unsafe_allow_html=True
)

# --- FUNCTION TO GET CITY/STATE OPTIONS ---
@st.cache_data(ttl=3600)
def get_filter_options():
    conn = get_snowflake_connection()
    query = """
        SELECT DISTINCT CITY, STATE
        FROM PROD_DWH.DWH.DIM_ACCOUNT
        WHERE (FF_ID IS NOT NULL 
               OR SF_ACCOUNT18_ID__C IS NOT NULL 
               OR AMP_SOURCE_CUSTOMER_ID IS NOT NULL)
          AND CITY IS NOT NULL 
          AND STATE IS NOT NULL
        ORDER BY STATE, CITY
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# --- NAVIGATION LOGIC ---
if st.session_state.page == 'activity':
    # Show Activity Page with account name in title
    account = st.session_state.selected_account
    account_name = account.get('Name', 'N/A') if account else 'N/A'
    
    st.markdown(f"<h1 style='text-align:center; color:#003366;'>PRODUCT ACTIVITY - {account_name}</h1>", unsafe_allow_html=True)
    
    if st.button("← Back to Search"):
        st.session_state.page = 'search'
        st.rerun()
    
    if account:
        st.markdown("---")
        
        # Display all account details in a two-column card layout with blue border box
        st.markdown("#### 📋 Account Details")
        
        # Helper function to display field or empty if None
        def get_display_value(value):
            if pd.notna(value) and value != '' and value != 0:
                return str(value)
            return ''
        
        # Create the account details content
        account_details_html = f"""
        <div style="border: 2px solid #003366; border-radius: 8px; padding: 15px; background-color: white; margin-bottom: 20px;">
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0px 40px;">
                <div>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>Account Name:</strong> {get_display_value(account.get('Name'))}</p>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>Gamechanger ID:</strong> {get_display_value(account.get('Gamechanger ID'))}</p>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>AMP Customer ID:</strong> {get_display_value(account.get('AMP Customer ID'))}</p>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>Firefly ID:</strong> {get_display_value(account.get('Firefly ID'))}</p>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>Address:</strong> {get_display_value(account.get('Address'))}</p>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>City:</strong> {get_display_value(account.get('City'))}</p>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>State:</strong> {get_display_value(account.get('State'))}</p>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>Zip:</strong> {get_display_value(account.get('Zip'))}</p>
                </div>
                <div>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>Type:</strong> {get_display_value(account.get('Account Type'))}</p>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>Primary Employee:</strong> {get_display_value(account.get('Primary Employee'))}</p>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>Primary Distributor:</strong> {get_display_value(account.get('Primary Distributor'))}</p>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>LLO:</strong> {get_display_value(account.get('LLO'))}</p>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>Market:</strong> {get_display_value(account.get('Market'))}</p>
                    <p style="margin: 2px 0; line-height: 1.3;"><strong>Zone:</strong> {get_display_value(account.get('Zone'))}</p>
                </div>
            </div>
        </div>
        """
        
        st.markdown(account_details_html, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Get Salesforce product activity
        gamechanger_id = account.get('Gamechanger ID')
        amp_customer_id = account.get('AMP Customer ID')

        # Salesforce Activity Section
        st.markdown("## 📊 Salesforce Product Activity")
        if gamechanger_id and pd.notna(gamechanger_id) and gamechanger_id != '':
            with st.spinner("Loading Salesforce product activity..."):
                import importlib
                import snowflake_connector
                importlib.reload(snowflake_connector)
                sf_activity_df = snowflake_connector.get_product_activity_by_gamechanger_id(gamechanger_id)
            
            if sf_activity_df.empty:
                st.info("No Salesforce product activity found for this account.")
            else:
                st.success(f"Found {len(sf_activity_df)} Salesforce product activity records")
                
                # Replace None/NaN with empty strings
                sf_activity_df = sf_activity_df.fillna('')
                
                # Replace None/NaN with empty strings
                sf_activity_df = sf_activity_df.fillna('')
                
                # Display the dataframe with horizontal scroll
                st.dataframe(
                    sf_activity_df,
                    use_container_width=False,  # Fit columns to content
                    hide_index=True,
                    column_config={
                        "START_DATE": st.column_config.DatetimeColumn("START_DATE", format="YYYY-MM-DD"),
                        "CLOSED_DATE": st.column_config.DateColumn("CLOSED_DATE", format="YYYY-MM-DD"),
                        "ACTIVITY_STATUS": st.column_config.TextColumn("ACTIVITY_STATUS", width="medium"),
                        "PRODUCT_NAME": st.column_config.TextColumn("PRODUCT_NAME", width="large"),
                        "PRODUCT_SKU": st.column_config.TextColumn("PRODUCT_SKU", width="medium"),
                        "PRODUCT_PACK": st.column_config.TextColumn("PRODUCT_PACK", width="medium"),
                        "CLIENT_NAME": st.column_config.TextColumn("CLIENT_NAME", width="medium"),
                        "PRODUCT_CATEGORY": st.column_config.TextColumn("PRODUCT_CATEGORY", width="medium"),
                        "PIPELINE_ACTIVITY": st.column_config.TextColumn("PIPELINE_ACTIVITY", width="medium"),
                        "PRODUCT_STATUS": st.column_config.TextColumn("PRODUCT_STATUS", width="medium"),
                        "QUANTITY_SOLD": st.column_config.TextColumn("QUANTITY_SOLD", width="medium"),
                        "NEXT_STEPS": st.column_config.TextColumn("NEXT_STEPS", width="large")
                    }
                )
                
        else:
            st.info("No Gamechanger ID available to fetch Salesforce activity.")
        
        st.markdown("---")
        
        # AMP Activity Section
       
        st.markdown("## 🛒 AMP Activity")
        if amp_customer_id and pd.notna(amp_customer_id) and amp_customer_id != '' and amp_customer_id != 0:
            with st.spinner("Loading AMP activity..."):
                from snowflake_connector import get_amp_activity_by_customer_id
                # Convert to proper format for the function
                if isinstance(amp_customer_id, str):
                    amp_id_value = int(float(amp_customer_id)) if amp_customer_id else None
                else:
                    amp_id_value = int(float(amp_customer_id)) if amp_customer_id != 0 else None
                
                if amp_id_value:
                    amp_activity_df = get_amp_activity_by_customer_id(amp_id_value)
                    
                    if amp_activity_df.empty:
                        st.info("No AMP activity found for this account.")
                    else:
                        st.success(f"Found {len(amp_activity_df)} AMP activity records")
                        
                        # Replace None/NaN with empty strings
                        amp_activity_df = amp_activity_df.fillna('')
                        
                        st.dataframe(amp_activity_df, use_container_width=True, height=400, hide_index=True)
                else:
                    st.info("No valid AMP Customer ID available to fetch AMP activity.")
        else:
            st.info("No AMP Customer ID available to fetch AMP activity.")

else:
    # --- MAIN SEARCH PAGE ---
    st.markdown("<h1 style='text-align:center; color:#003366;'>CUSTOMER 360</h1>", unsafe_allow_html=True)

    # --- FILTERS ROW ---
    st.markdown("<h3 style='color:black;'>🔍 Search Filters</h3>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        search_term = st.text_input("Account Name", placeholder="e.g. mazatlan", key="search_name")
    
    # Get filter options
    filter_options = get_filter_options()
    
    with col2:
        cities = ['All'] + sorted(filter_options['CITY'].unique().tolist())
        selected_city = st.selectbox("City", cities, key="filter_city")
    
    with col3:
        states = ['All'] + sorted(filter_options['STATE'].unique().tolist())
        selected_state = st.selectbox("State", states, key="filter_state")

    # --- MAIN LOGIC ---
    if len(search_term.strip()) >= 2 or selected_city != 'All' or selected_state != 'All':
        # Build dynamic query
        where_clauses = []
        params = []
        
        if len(search_term.strip()) >= 2:
            where_clauses.append("LOWER(NAME) LIKE %s")
            params.append(f"%{search_term.lower()}%")
        
        if selected_city != 'All':
            where_clauses.append("CITY = %s")
            params.append(selected_city)
        
        if selected_state != 'All':
            where_clauses.append("STATE = %s")
            params.append(selected_state)
        
        where_sql = " AND ".join(where_clauses)
        
        query = f"""
            SELECT 
                SF_ACCOUNT18_ID__C AS "Gamechanger ID",
                AMP_AMPCUSTOMER_ID AS "AMP Customer ID",
                FF_ID AS "Firefly ID",
                SF_PRIMARY_EMPLOYEE_NAME__C AS "Primary Employee",
                NAME AS "Name",
                ADDRESS AS "Address",
                CITY AS "City",
                STATE AS "State",
                SF_ZIP__C AS "Zip",
                SF_TF_PRIMARYDISTRIBUTORNAME__C AS "Primary Distributor",
                SF_LARGELEVERAGEOPERATOR__C AS "LLO",
                SF_GEOMARKET_NAME__C AS "Market",
                SF_GEOZONE_NAME__C AS "Zone",
                DS_ACCOUNT_TYPE AS "Account Type"
            FROM PROD_DWH.DWH.DIM_ACCOUNT
            WHERE {where_sql}
            ORDER BY NAME
        """
        
        # Get fresh connection and use try/finally
        conn = get_snowflake_connection()
        try:
            df = pd.read_sql(query, conn, params=tuple(params))
        finally:
            conn.close()

        if df.empty:
            st.warning("No matches found.")
            st.session_state.current_page = 0
        else:
            # Create priority column for sorting
            # Priority 1: Has all 3 IDs (Gamechanger + AMP Customer + Firefly)
            # Priority 2: Has Gamechanger + AMP Customer (no Firefly)
            # Priority 3: Has only Gamechanger
            # Priority 4: Everything else
            
            def get_priority(row):
                has_gamechanger = pd.notna(row['Gamechanger ID']) and row['Gamechanger ID'] != ''
                has_amp = pd.notna(row['AMP Customer ID']) and row['AMP Customer ID'] != '' and row['AMP Customer ID'] != 0
                has_firefly = pd.notna(row['Firefly ID']) and row['Firefly ID'] != ''
                
                if has_gamechanger and has_amp and has_firefly:
                    return 1  # Priority 1: All 3 IDs
                elif has_gamechanger and has_amp:
                    return 2  # Priority 2: Gamechanger + AMP
                elif has_gamechanger:
                    return 3  # Priority 3: Only Gamechanger
                else:
                    return 4  # Priority 4: Everything else
            
            # Create sort key by combining the 3 IDs
            def get_id_sort_key(row):
                gc_id = str(row['Gamechanger ID']) if pd.notna(row['Gamechanger ID']) else ''
                # Handle AMP ID as float, convert to int then string
                if pd.notna(row['AMP Customer ID']) and row['AMP Customer ID'] != 0:
                    amp_id = str(int(float(row['AMP Customer ID'])))
                else:
                    amp_id = ''
                ff_id = str(row['Firefly ID']) if pd.notna(row['Firefly ID']) else ''
                return f"{gc_id}_{amp_id}_{ff_id}"
            
            df['_priority'] = df.apply(get_priority, axis=1)
            df['_id_sort_key'] = df.apply(get_id_sort_key, axis=1)
            df = df.sort_values(by=['_priority', '_id_sort_key'])
            df = df.drop(columns=['_priority', '_id_sort_key'])
            
            # Pagination logic
            total_results = len(df)
            results_per_page = 50
            total_pages = (total_results - 1) // results_per_page + 1
            
            # Ensure current page is valid
            if st.session_state.current_page >= total_pages:
                st.session_state.current_page = 0
            
            start_idx = st.session_state.current_page * results_per_page
            end_idx = min(start_idx + results_per_page, total_results)
            
            st.success(f"Showing results {start_idx + 1}-{end_idx} of {total_results} total matches")

            # --- INTERACTIVE DATAFRAME WITH ROW SELECTION ---
            st.markdown("### 📋 Results")
            st.info("💡 **How to view activity:** Click the checkbox in any row to see that account's Salesforce and AMP activity details")
            
            page_df = df.iloc[start_idx:end_idx].reset_index(drop=True)
            
            # Check which accounts on this page have activity
            account_ids = []
            for _, row in page_df.iterrows():
                sf_id = row['Gamechanger ID'] if pd.notna(row['Gamechanger ID']) and row['Gamechanger ID'] != '' else None
                amp_id = row['AMP Customer ID']
                if pd.notna(amp_id) and amp_id != '' and amp_id != 0:
                    try:
                        amp_id = int(float(amp_id))
                    except:
                        amp_id = None
                else:
                    amp_id = None
                    
                account_ids.append({
                    'sf_id': sf_id,
                    'amp_id': amp_id
                })
            
           # Get activity status
            
            with st.spinner("Checking activity status..."):
                activity_status = check_activity_exists(account_ids)
            
            # Add green circle indicators to IDs that have activity
            for idx, row in page_df.iterrows():
                gc_id = row['Gamechanger ID']
                amp_id = row['AMP Customer ID']
                
                # Handle Gamechanger ID
                if pd.notna(gc_id) and gc_id != '':
                    status = activity_status.get(str(gc_id), {})
                    if status.get('has_sf'):
                        page_df.at[idx, 'Gamechanger ID'] = f"{gc_id} 🟢"
                
                # Handle AMP Customer ID
                if pd.notna(amp_id) and amp_id != '' and amp_id != 0:
                    try:
                        amp_id_int = int(float(amp_id))
                        status = activity_status.get(str(gc_id) if gc_id else str(amp_id_int), {})
                        if status.get('has_amp'):
                            page_df.at[idx, 'AMP Customer ID'] = f"{amp_id_int} 🟢"
                    except:
                        pass
            
            # Convert AMP Customer ID to string and replace None/NaN values with empty strings
            page_df['AMP Customer ID'] = page_df['AMP Customer ID'].apply(
                lambda x: '' if pd.isna(x) or x == 0 or x == '' else str(int(float(x)))
            )
            page_df = page_df.fillna('')
            
            # Create column configuration with proper sizing
            column_config = {
                "Gamechanger ID": st.column_config.TextColumn("Gamechanger ID", width="medium"),
                "Primary Employee": st.column_config.TextColumn("Primary Employee", width="medium"),
                "AMP Customer ID": st.column_config.TextColumn("AMP Cust ID", width="medium"),
                "Firefly ID": st.column_config.TextColumn("Firefly ID", width="medium"),
                "Name": st.column_config.TextColumn("Account Name", width="large"),
                "Address": st.column_config.TextColumn("Address", width="large"),
                "City": st.column_config.TextColumn("City", width="medium"),
                "State": st.column_config.TextColumn("State", width="small"),
                "Zip": st.column_config.TextColumn("Zip", width="small"),
                "LLO": st.column_config.TextColumn("LLO", width="medium"),
                "Market": st.column_config.TextColumn("Market", width="medium"),
                "Zone": st.column_config.TextColumn("Zone", width="medium"),
                "Account Type": st.column_config.TextColumn("Type", width="medium"),
                "Primary Distributor": st.column_config.TextColumn("Primary Distributor", width="medium")
            }
            
            # Display interactive dataframe
            selection = st.dataframe(
                page_df,
                use_container_width=False,
                height=600,
                on_select="rerun",
                selection_mode="single-row",
                hide_index=True,
                column_config=column_config
            )
            
            # Handle row selection
            if selection and "selection" in selection and "rows" in selection["selection"]:
                selected_rows = selection["selection"]["rows"]
                if len(selected_rows) > 0:
                    selected_idx = selected_rows[0]
                    selected_row = page_df.iloc[selected_idx]
                    
                    # Check if account has at least one ID
                    if pd.notna(selected_row["Gamechanger ID"]) or pd.notna(selected_row["AMP Source Customer ID"]) or pd.notna(selected_row["Firefly ID"]):
                        st.session_state.selected_account = selected_row.to_dict()
                        st.session_state.page = 'activity'
                        st.rerun()
                    else:
                        st.warning("This account has no IDs available to fetch activity.")

            # --- PAGINATION CONTROLS ---
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col1:
                if st.session_state.current_page > 0:
                    if st.button("← Previous"):
                        st.session_state.current_page -= 1
                        st.rerun()
            
            with col2:
                st.markdown(f"<div class='pagination'>Page {st.session_state.current_page + 1} of {total_pages}</div>", 
                          unsafe_allow_html=True)
            
            with col3:
                if st.session_state.current_page < total_pages - 1:
                    if st.button("Next →"):
                        st.session_state.current_page += 1
                        st.rerun()

    else:
        st.info("Start typing an account name (min 2 characters) or select a city/state filter...")
        st.session_state.current_page = 0
