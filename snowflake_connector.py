import snowflake.connector
import streamlit as st
import pandas as pd
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

def get_snowflake_connection():
    # Load the private key properly
    private_key_str = st.secrets["snowflake"]["private_key"]
    
    # Convert the private key string to bytes
    p_key = serialization.load_pem_private_key(
        private_key_str.encode(),
        password=None,
        backend=default_backend()
    )
    
    # Serialize to DER format (what Snowflake expects)
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"]["role"],
        private_key=pkb
    )

def get_product_activity_by_gamechanger_id(account18_id: str) -> pd.DataFrame:
    conn = get_snowflake_connection()
    query = """
        SELECT 
            p.TF_ACTIVITYSTARTDATE__C,
            p.TF_MEETINGCLOSEDDATEONLY__C,
            p.TF_ACTIVITYSTATUS__C,
            p.TF_PRODUCT_NAME__C,
            p.TF_PRODUCT_SKU__C,
            p.TF_PRODUCT_PACK__C,
            p.TF_PRODUCTCLIENTNAME__C,
            p.TF_PRODUCTCATEGORY__C,
            p.PIPELINE_ACTIVITY__C,
            p.PRODUCTSTATUS__C,
            p.QUANTITY_ENTERED__C,
            p.WHAT_ARE_NEXT_STEPS__C
        FROM PROD_DWH.DWH.DIM_ACCOUNT a
        JOIN PROD_DWH.DWH.DIM_PRODUCTACTIVITY p
            ON a.ACCOUNT_UUID = p.ACCOUNT_OPPERATOR_UUID
        WHERE a.SF_ACCOUNT18_ID__C = %s
        ORDER BY p.TF_ACTIVITYSTARTDATE__C DESC
    """
    try:
        df = pd.read_sql(query, conn, params=(account18_id,))
        
        # Rename columns after fetching
        df = df.rename(columns={
            'TF_ACTIVITYSTARTDATE__C': 'START_DATE',
            'TF_MEETINGCLOSEDDATEONLY__C': 'CLOSED_DATE',
            'TF_ACTIVITYSTATUS__C': 'ACTIVITY_STATUS',
            'TF_PRODUCT_NAME__C': 'PRODUCT_NAME',
            'TF_PRODUCT_SKU__C': 'PRODUCT_SKU',
            'TF_PRODUCT_PACK__C': 'PRODUCT_PACK',
            'TF_PRODUCTCLIENTNAME__C': 'CLIENT_NAME',
            'TF_PRODUCTCATEGORY__C': 'PRODUCT_CATEGORY',
            'PIPELINE_ACTIVITY__C': 'PIPELINE_ACTIVITY',
            'PRODUCTSTATUS__C': 'PRODUCT_STATUS',
            'QUANTITY_ENTERED__C': 'QUANTITY_SOLD',
            'WHAT_ARE_NEXT_STEPS__C': 'NEXT_STEPS'
        })
        
        return df
    finally:
        conn.close()

def get_amp_activity_by_customer_id(amp_ampcustomer_id) -> pd.DataFrame:
    conn = get_snowflake_connection()
    
    query = """
        WITH related_accounts AS (
            -- Get the FF_ID for the input customer
            SELECT DISTINCT a2.AMP_AMPCUSTOMER_ID
            FROM PROD_DWH.DWH.DIM_ACCOUNT a1
            JOIN PROD_DWH.DWH.DIM_ACCOUNT a2 
                ON a1.FF_ID = a2.FF_ID
            WHERE a1.AMP_AMPCUSTOMER_ID = %s
              AND a2.AMP_AMPCUSTOMER_ID IS NOT NULL
              AND a1.FF_ID IS NOT NULL
        )
        SELECT 
            amp.AMPCUSTOMER_ID,
            cust.AMP_DATA_SOURCE AS "GPO",
            mfr.AMP_CLIENTS_CLIENT AS "CLIENT_NAME",
            amp.DISTRIBUTOR,
            amp.ITEM_ID,
            prod.SKU,
            prod.PRODUCT_NAME,
            prod.AMP_CATEGORY AS "CATEGORY",
            prod.AMP_SUB_CATEGORY AS "SUB_CATEGORY",
            amp.YTD,
            amp.CYM,
            amp.MAGO_2 AS "2_MONTHS_AGO",
            amp.MAGO_3 AS "3_MONTHS_AGO",
            amp.MAGO_4 AS "4_MONTHS_AGO",
            amp.MAGO_5 AS "5_MONTHS_AGO",
            amp.MAGO_6 AS "6_MONTHS_AGO",
            amp.LYM,
            amp.LYTD,
            amp.PERIOD,
            amp.UOM
        FROM PROD_DWH.DWH.FACT_AMP_PURCHASE_DATA amp
        LEFT JOIN PROD_DWH.DWH.DIM_ACCOUNT cust
            ON amp.AMPCUSTOMER_ID = cust.AMP_AMPCUSTOMER_ID
        LEFT JOIN PROD_DWH.DWH.DIM_ACCOUNT mfr
            ON amp.CCODE = mfr.AMP_CLIENTS_CCODE
        LEFT JOIN PROD_DWH.DWH.DIM_PRODUCT prod
            ON amp.PRODUCT_UUID = prod.PRODUCT_UUID
        WHERE amp.AMPCUSTOMER_ID IN (SELECT AMP_AMPCUSTOMER_ID FROM related_accounts)
          AND (amp.YTD IS NOT NULL 
               OR amp.LYM IS NOT NULL 
               OR amp.LYTD IS NOT NULL
               OR amp.CYM IS NOT NULL)
        ORDER BY amp.PERIOD DESC
    """
    try:
        df = pd.read_sql(query, conn, params=(amp_ampcustomer_id,))
        
        # Rename columns to uppercase with proper spacing
        df = df.rename(columns={
            'AMPCUSTOMER_ID': 'AMP CUSTOMER ID',
            'CLIENT_NAME': 'CLIENT NAME',
            'PRODUCT_NAME': 'PRODUCT NAME',
            'SUB_CATEGORY': 'SUB CATEGORY',
            '2_MONTHS_AGO': '2 MONTHS AGO',
            '3_MONTHS_AGO': '3 MONTHS AGO',
            '4_MONTHS_AGO': '4 MONTHS AGO',
            '5_MONTHS_AGO': '5 MONTHS AGO',
            '6_MONTHS_AGO': '6 MONTHS AGO'
        })
        
        return df
    finally:
        conn.close()

def check_activity_exists(account_ids: list) -> dict:
    """
    Check which accounts have SF and/or AMP activity.
    Returns dict with format: {id: {'has_sf': bool, 'has_amp': bool}, ...}
    Now checks each individual AMP ID separately for individual green circle indicators.
    """
    conn = get_snowflake_connection()
    
    results = {}
    
    try:
        # Check for Salesforce activity
        sf_ids = [aid['sf_id'] for aid in account_ids if aid.get('sf_id')]
        if sf_ids:
            placeholders = ','.join(['%s'] * len(sf_ids))
            sf_query = f"""
                SELECT DISTINCT a.SF_ACCOUNT18_ID__C
                FROM PROD_DWH.DWH.DIM_ACCOUNT a
                JOIN PROD_DWH.DWH.DIM_PRODUCTACTIVITY p
                    ON a.ACCOUNT_UUID = p.ACCOUNT_OPPERATOR_UUID
                WHERE a.SF_ACCOUNT18_ID__C IN ({placeholders})
            """
            sf_df = pd.read_sql(sf_query, conn, params=tuple(sf_ids))
            sf_with_activity = set(sf_df['SF_ACCOUNT18_ID__C'].tolist())
        else:
            sf_with_activity = set()
        
        # Check for AMP activity - check EACH ID individually
        amp_ids_to_check = []
        for aid in account_ids:
            amp_id = aid.get('amp_id')
            if amp_id:
                # If comma-separated, split and add each
                amp_str = str(amp_id).strip()
                if ',' in amp_str:
                    for single_id in amp_str.split(','):
                        single_id = single_id.strip()
                        if single_id and single_id != '0':
                            amp_ids_to_check.append(single_id)
                else:
                    if amp_str and amp_str != '0':
                        amp_ids_to_check.append(amp_str)
        
        if amp_ids_to_check:
            # Remove duplicates and convert to integers
            amp_ids_to_check = list(set(amp_ids_to_check))
            
            # Convert string IDs to integers for the query
            amp_ids_numeric = []
            for amp_id in amp_ids_to_check:
                try:
                    amp_ids_numeric.append(int(float(amp_id)))
                except:
                    pass
            
            if amp_ids_numeric:
                placeholders = ','.join(['%s'] * len(amp_ids_numeric))
                amp_query = f"""
                    WITH related_accounts AS (
                        SELECT DISTINCT a2.AMP_AMPCUSTOMER_ID, a1.AMP_AMPCUSTOMER_ID as ORIGINAL_ID
                        FROM PROD_DWH.DWH.DIM_ACCOUNT a1
                        JOIN PROD_DWH.DWH.DIM_ACCOUNT a2 
                            ON a1.FF_ID = a2.FF_ID
                        WHERE a1.AMP_AMPCUSTOMER_ID IN ({placeholders})
                          AND a2.AMP_AMPCUSTOMER_ID IS NOT NULL
                          AND a1.FF_ID IS NOT NULL
                    )
                    SELECT DISTINCT 
                        ra.ORIGINAL_ID,
                        COUNT(DISTINCT amp.PURCHASE_UUID) as activity_count
                    FROM related_accounts ra
                    JOIN PROD_DWH.DWH.FACT_AMP_PURCHASE_DATA amp
                        ON ra.AMP_AMPCUSTOMER_ID = amp.AMPCUSTOMER_ID
                    GROUP BY ra.ORIGINAL_ID
                    HAVING COUNT(DISTINCT amp.PURCHASE_UUID) > 0
                """
                amp_df = pd.read_sql(amp_query, conn, params=tuple(amp_ids_numeric))
                
                # DEBUG - See what the query returned
                print(f"DEBUG AMP Query Results: {len(amp_df)} rows")
                if not amp_df.empty:
                    print(f"DEBUG Sample IDs with activity: {amp_df['ORIGINAL_ID'].head().tolist()}")
                
                # Safely convert IDs to strings
                amp_with_activity = set()
                if not amp_df.empty:
                    for x in amp_df['ORIGINAL_ID'].tolist():
                        try:
                            amp_with_activity.add(str(int(float(x))))
                        except:
                            pass
            else:
                amp_with_activity = set()
        else:
            amp_with_activity = set()
        
        # Build results dict - now includes ALL individual AMP IDs
        for aid in account_ids:
            sf_id = aid.get('sf_id')
            amp_id = aid.get('amp_id')
            
            # Add SF result
            if sf_id:
                results[str(sf_id)] = {
                    'has_sf': sf_id in sf_with_activity,
                    'has_amp': False
                }
            
            # Add AMP results - handle comma-separated
            if amp_id:
                amp_str = str(amp_id).strip()
                if ',' in amp_str:
                    for single_id in amp_str.split(','):
                        single_id = single_id.strip()
                        if single_id and single_id != '0':
                            results[single_id] = {
                                'has_sf': False,
                                'has_amp': single_id in amp_with_activity
                            }
                else:
                    if amp_str and amp_str != '0':
                        results[amp_str] = {
                            'has_sf': False,
                            'has_amp': amp_str in amp_with_activity
                        }
        
        return results
        
    finally:
        conn.close()
