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
            p.ACTIVITY_START_DATE__C,
            p.ACTIVITY_END_DATE__C,
            p.TF_PRODUCT_NAME__C,
            p.TF_PRODUCT_SKU__C,
            p.TF_PRODUCTCLIENTNAME__C,
            p.TF_PRODUCTCATEGORY__C,
            p.PIPELINE_ACTIVITY__C,
            p.PRODUCTSTATUS__C,
            p.QUANTITY_ENTERED__C,
            p.WHAT_ARE_NEXT_STEPS__C,
            p.BIG_HIT_CLIENT__C
        FROM PROD_DWH.DWH.DIM_ACCOUNT a
        JOIN PROD_DWH.DWH.DIM_PRODUCTACTIVITY p
            ON a.ACCOUNT_UUID = p.ACCOUNT_OPPERATOR_UUID
        WHERE a.SF_ACCOUNT18_ID__C = %s
        ORDER BY p.ACTIVITY_START_DATE__C DESC
    """
    try:
        df = pd.read_sql(query, conn, params=(account18_id,))
        
        # Rename columns after fetching
        df = df.rename(columns={
            'ACTIVITY_START_DATE__C': 'START_DATE',
            'ACTIVITY_END_DATE__C': 'END_DATE',
            'TF_PRODUCT_NAME__C': 'PRODUCT_NAME',
            'TF_PRODUCT_SKU__C': 'PRODUCT_SKU',
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
        SELECT 
            amp.AMPCUSTOMER_ID,
            cust.NAME AS "Customer Name",
            mfr.NAME AS "Client Name",
            amp.DISTRIBUTOR,
            amp.DIST_CODE,
            amp.ITEM_ID,
            amp.LYM,
            amp.LYTD,
            amp.MAGO_2,
            amp.MAGO_3,
            amp.MAGO_4,
            amp.MAGO_5,
            amp.MAGO_6,
            amp.PERIOD,
            amp.UOM,
            amp.YTD
        FROM PROD_DWH.DWH.FACT_AMP_PURCHASE_DATA amp
        LEFT JOIN PROD_DWH.DWH.DIM_ACCOUNT cust
            ON amp.AMPCUSTOMER_ID = cust.AMP_AMPCUSTOMER_ID
        LEFT JOIN PROD_DWH.DWH.DIM_ACCOUNT mfr
            ON amp.ACCOUNT_MANUFACTURER_UUID = mfr.ACCOUNT_UUID
        WHERE amp.AMPCUSTOMER_ID = %s
        ORDER BY amp.PERIOD DESC
    """
    try:
        df = pd.read_sql(query, conn, params=(amp_ampcustomer_id,))
        return df
    finally:
        conn.close()
