import pyodbc
import pandas as pd


conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\\SQLEXPRESS;"
    "DATABASE=MyDatabase;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

cursor = conn.cursor()

print("Database ready!")

df = pd.read_sql("SELECT * FROM ingestion.sales_details", conn)

# Convert dates
df["sls_order_dt"] = pd.to_datetime(df["sls_order_dt"], format="%Y%m%d", errors="coerce")
df["sls_ship_dt"]  = pd.to_datetime(df["sls_ship_dt"], format="%Y%m%d", errors="coerce")
df["sls_due_dt"]   = pd.to_datetime(df["sls_due_dt"], format="%Y%m%d", errors="coerce")

# Standardize order date per order number
order_counts = df.groupby("sls_ord_num")["sls_ord_num"].transform("count")
group_order_date = df.groupby("sls_ord_num")["sls_order_dt"].transform("min")

# Same order date for all items in the same order
df.loc[order_counts > 1, "sls_order_dt"] = group_order_date[order_counts > 1]

# For single-item orders, fill missing order date from ship date
df["sls_order_dt"] = df["sls_order_dt"].fillna(df["sls_ship_dt"])

# Price cleaning
df["sls_price"] = df["sls_price"].abs()
df["sls_price"] = df["sls_price"].fillna(
    df["sls_sales"] / df["sls_quantity"].replace(0, pd.NA)
)

# Recompute sales
df["sls_sales"] = df["sls_quantity"] * df["sls_price"]

cursor.execute("""
IF OBJECT_ID('transformation.sales_details', 'U') IS NOT NULL
DROP TABLE transformation.sales_details

CREATE TABLE transformation.sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id VARCHAR(50),
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales DECIMAL(12,2),
    sls_quantity INT,
    sls_price DECIMAL(12,2)
)
""")

df_to_load = df[
    [
        "sls_ord_num",
        "sls_prd_key",
        "sls_cust_id",
        "sls_order_dt",
        "sls_ship_dt",
        "sls_due_dt",
        "sls_sales",
        "sls_quantity",
        "sls_price"
    ]
].copy()

df_to_load = df_to_load.where(pd.notnull(df_to_load), None)

insert_query = """
INSERT INTO transformation.sales_details
(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

cursor.fast_executemany = True

print(f"Loading {len(df_to_load)} rows...")

cursor.executemany(insert_query, df_to_load.values.tolist())

conn.commit()

print("Success!")

conn.close()