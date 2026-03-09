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

# ----- CREATE SCHEMA -----
cursor.execute("""
IF NOT EXISTS (
    SELECT * FROM sys.schemas WHERE name='ingestion'
)
EXEC('CREATE SCHEMA ingestion')
""")

# ----- CUSTOMER TABLE -----
cursor.execute("""
IF OBJECT_ID('ingestion.cust_info', 'U') IS NULL
CREATE TABLE ingestion.cust_info (
    cst_id VARCHAR(50),
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(20),
    cst_create_date DATE
)
""")

print("Customer table created!")

# ----- PRODUCT TABLE -----
cursor.execute("""
IF OBJECT_ID('ingestion.prd_info', 'U') IS NULL
CREATE TABLE ingestion.prd_info (
    prd_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(150),
    prd_cost DECIMAL(10,2),
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
)
""")

# ----- SALES TABLE -----
cursor.execute("""
IF OBJECT_ID('ingestion.sales_details', 'U') IS NULL
CREATE TABLE ingestion.sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id VARCHAR(50),
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales DECIMAL(12,2),
    sls_quantity INT,
    sls_price DECIMAL(12,2)
)
""")

# ----- CUSTOMER EXTRA TABLE -----
cursor.execute("""
IF OBJECT_ID('ingestion.cust_az12', 'U') IS NULL
CREATE TABLE ingestion.cust_az12 (
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(20)
)
""")

# ----- LOCATION TABLE -----
cursor.execute("""
IF OBJECT_ID('ingestion.loc_a101', 'U') IS NULL
CREATE TABLE ingestion.loc_a101 (
    CID VARCHAR(50),
    CNTRY VARCHAR(100)
)
""")

# ----- CATEGORY TABLE -----
cursor.execute("""
IF OBJECT_ID('ingestion.px_cat_g1v2', 'U') IS NULL
CREATE TABLE ingestion.px_cat_g1v2 (
    ID VARCHAR(50),
    CAT VARCHAR(100),
    SUBCAT VARCHAR(100),
    MAINTENANCE VARCHAR(10)
)
""")

print("All tables created!")

conn.close()
