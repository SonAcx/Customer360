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
            p.CREATEDDATE,
            p.BIG_HIT_CLIENT__C,
            p.NAME,
            p.PIPELINE_ACTIVITY__C,
            p.PRODUCTSTATUS__C,
            p.QUANTITY_ENTERED__C
        FROM PROD_DWH.DWH.DIM_ACCOUNT a
        JOIN PROD_DWH.DWH.DIM_PRODUCTACTIVITY p
            ON a.ACCOUNT_UUID = p.ACCOUNT_OPPERATOR_UUID
        WHERE a.SF_ACCOUNT18_ID__C = %s
    """
    try:
        df = pd.read_sql(query, conn, params=(account18_id,))
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
