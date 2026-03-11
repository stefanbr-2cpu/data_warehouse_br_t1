import pyodbc
import pandas as pd


def is_id_column(column_name):
    normalized = column_name.strip().lower()
    return normalized == "id" or normalized == "cid" or normalized.endswith("_id")


def clean_identifier(value):
    if pd.isna(value):
        return None

    text = str(value).strip()
    if text.endswith(".0") and text[:-2].replace("-", "", 1).isdigit():
        return text[:-2]
    return text


conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\\SQLEXPRESS;"
    "DATABASE=MyDatabase;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

conn.autocommit = True
cursor = conn.cursor()

cursor.execute("""
IF DB_ID('MyDatabase') IS NULL
CREATE DATABASE MyDatabase
""")

print("Database ready!")

df = pd.read_sql('SELECT * FROM ingestion.cust_info',conn)

id_columns = [col for col in df.columns if is_id_column(col)]
for col in id_columns:
    df[col] = df[col].map(clean_identifier)

df = df.dropna(subset='cst_id')
print('Number of NAs now:', df['cst_id'].isnull().sum())

df = df.drop_duplicates(subset='cst_id', keep='last')

marital_map = {
    "S": "Single",
    "M": "Married"
}

df["cst_marital_status"] = df["cst_marital_status"].map(marital_map).fillna("NA")


gender_map = {
    "M": "Male",
    "F": "Female"
}

df["cst_gndr"] = df["cst_gndr"].map(gender_map).fillna("NA")

cursor.execute("""               
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transformation')
BEGIN
    EXEC('CREATE SCHEMA [transformation]')
END""")

cursor.execute("""
IF OBJECT_ID('transformation.cust_info', 'U') IS NOT NULL
    DROP TABLE transformation.cust_info;

CREATE TABLE transformation.cust_info (
    cst_id NVARCHAR(MAX),
    cst_key NVARCHAR(MAX),
    cst_firstname NVARCHAR(MAX),
    cst_lastname NVARCHAR(MAX),
    cst_marital_status NVARCHAR(MAX),
    cst_gndr NVARCHAR(MAX),
    cst_create_date NVARCHAR(MAX)
)
""")

df_to_load = df[
    [
        "cst_id",
        "cst_key",
        "cst_firstname",
        "cst_lastname",
        "cst_marital_status",
        "cst_gndr",
        "cst_create_date",
    ]
].copy()
df_to_load = df_to_load.where(pd.notnull(df_to_load), None)
for col in id_columns:
    if col in df_to_load.columns:
        df_to_load[col] = df_to_load[col].map(clean_identifier)
data_list = [tuple(x) for x in df_to_load.values]

insert_query = """
    INSERT INTO transformation.cust_info 
    (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date) 
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
cursor.fast_executemany = True

print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("Success!") 

conn.close()
