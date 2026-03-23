import pandas as pd
import pyodbc

# ----- connect to new database -----
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\\SQLEXPRESS;"
    "DATABASE=MyDatabase;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

conn.autocommit = True
cursor = conn.cursor()

print("Connected to MyDatabase!")

cursor.execute("""
IF NOT EXISTS (
    SELECT * FROM sys.schemas WHERE name='curated'
)
EXEC('CREATE SCHEMA curated')
""")



customer_crm_df = pd.read_sql("SELECT * FROM transformation.cust_info", conn)
customer_erp_df = pd.read_sql("SELECT * FROM transformation.cust_az12", conn)
location_erp_df = pd.read_sql("SELECT * FROM transformation.loc_a101", conn)

df = pd.merge(
    left = customer_crm_df,
    right = customer_erp_df,
    how ="left",
    left_on ="cst_key", 
    right_on = "CID"
)

print(df)

df = pd.merge(
    left = df,
    right = location_erp_df,
    how = "left",
    left_on = "cst_key",
    right_on = "CID",
    suffixes = ("", "_loc")
)

print(df)


dim_customers = pd.DataFrame({
    "customer_id": df["cst_id"],
    "customer_number": df["cst_key"],
    "first_name": df["cst_firstname"],
    "last_name": df["cst_lastname"],
    "country": df["CNTRY"],
    "marital_status": df["cst_marital_status"],
    "gender": df["GEN"],
    "birthdate": df["BDATE"],
    "craete_date": df["cst_create_date"],
    
})

print(dim_customers)

cursor.execute("""
IF OBJECT_ID('curated.dim_customers', 'U') IS NOT NULL
    DROP TABLE curated.dim_customers

CREATE TABLE curated.dim_customers (
    customer_id INT,
    customer_number VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    country VARCHAR(100),
    marital_status VARCHAR(20),
    gender VARCHAR(20),
    birthdate DATE,
    create_date DATE
)
""")

# ----- prepare dataframe for insert -----
df_to_load = dim_customers.copy()

text_cols = [
    "customer_number",
    "first_name",
    "last_name",
    "country",
    "marital_status",
    "gender"
]

for col in text_cols:
    df_to_load[col] = df_to_load[col].astype(str)

df_to_load = df_to_load.where(pd.notnull(df_to_load), None)

# ----- insert query -----
insert_query = """
INSERT INTO curated.dim_customers
(customer_id, customer_number, first_name, last_name, country, marital_status, gender, birthdate, create_date)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

cursor.fast_executemany = True

print(f"Loading {len(df_to_load)} rows into curated.dim_customers...")

cursor.executemany(insert_query, df_to_load.values.tolist())
conn.commit()



###############################



product_crm_df = pd.read_sql("SELECT * FROM transformation.prd_info", conn)
category_erp_df = pd.read_sql("SELECT * FROM transformation.px_cat_g1v2", conn)

df = pd.merge(
    left=product_crm_df,
    right=category_erp_df,
    how="left",
    left_on="prd_id",
    right_on="ID"
)

print(df)

dim_products = pd.DataFrame({
    "product_number": df["prd_key"],
    "product_name": df["prd_nm"],
    "category_id": df["prd_id_category"],
    "category": df["CAT"],
    "subcategory": df["SUBCAT"],
    "maintenance": df["MAINTENANCE"],
    "cost": df["prd_cost"],
    "product_line": df["prd_line"],
    "start_date": df["prd_start_dt"],
    "end_date": df["prd_end_dt"]
})

print(dim_products)

cursor.execute("""
IF OBJECT_ID('curated.dim_products', 'U') IS NOT NULL
    DROP TABLE curated.dim_products

CREATE TABLE curated.dim_products (
    product_number VARCHAR(50),
    product_name VARCHAR(255),
    category_id VARCHAR(50),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    maintenance VARCHAR(20),
    cost FLOAT,
    product_line VARCHAR(100),
    start_date DATE,
    end_date DATE
)
""")

df_to_load = dim_products.copy()

text_cols = [
    "product_number",
    "product_name",
    "category_id",
    "category",
    "subcategory",
    "maintenance",
    "product_line"
]

for col in text_cols:
    df_to_load[col] = df_to_load[col].where(pd.notnull(df_to_load[col]), None)

df_to_load["cost"] = pd.to_numeric(df_to_load["cost"], errors="coerce")
df_to_load["start_date"] = pd.to_datetime(df_to_load["start_date"], errors="coerce").dt.date
df_to_load["end_date"] = pd.to_datetime(df_to_load["end_date"], errors="coerce").dt.date

df_to_load = df_to_load.where(pd.notnull(df_to_load), None)

insert_query = """
INSERT INTO curated.dim_products
(product_number, product_name, category_id, category, subcategory, maintenance, cost, product_line, start_date, end_date)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

cursor.fast_executemany = True

print(f"Loading {len(df_to_load)} rows into curated.dim_products...")

cursor.executemany(insert_query, df_to_load.values.tolist())
conn.commit()

print("curated.dim_products loaded successfully!")
print(dim_products)

####################################

sales_details_df = pd.read_sql("SELECT * FROM transformation.sales_details;", conn)

fact_sales = pd.DataFrame({
    "sales_key": range(1, len(sales_details_df) + 1),
    "product_number": sales_details_df["sls_prd_key"],
    "customer_number": sales_details_df["sls_cust_id"],
    "order_number": sales_details_df["sls_ord_num"],
    "order_date": sales_details_df["sls_order_dt"],
    "shipping_date": sales_details_df["sls_ship_dt"],
    "due_date": sales_details_df["sls_due_dt"],
    "sales": sales_details_df["sls_sales"],
    "quantity": sales_details_df["sls_quantity"],
    "price": sales_details_df["sls_price"]
})

print(fact_sales.head(10))

cursor.execute("""
IF OBJECT_ID('curated.fact_sales', 'U') IS NOT NULL
    DROP TABLE curated.fact_sales

CREATE TABLE curated.fact_sales (
    sales_key INT,
    product_number VARCHAR(50),
    customer_number VARCHAR(50),
    order_number VARCHAR(50),
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales FLOAT,
    quantity INT,
    price FLOAT
)
""")

insert_query = """
INSERT INTO curated.fact_sales
(sales_key, product_number, customer_number, order_number, order_date, shipping_date, due_date, sales, quantity, price)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

cursor.fast_executemany = True

print(f"Loading {len(fact_sales)} rows into curated.fact_sales...")

cursor.executemany(insert_query, fact_sales.values.tolist())
conn.commit()

print("curated.fact_sales loaded successfully!")