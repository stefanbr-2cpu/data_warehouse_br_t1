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

df = pd.read_sql('SELECT * FROM ingestion.cust_az12',conn)

id_columns = [col for col in df.columns if is_id_column(col)]
for col in id_columns:
    df[col] = df[col].map(clean_identifier)


df["CID"] = df["CID"].astype(str)

df["CID"] = df["CID"].apply(
    lambda x: x[3:] if len(x) == 13 else x
)

gender_map = {
    "F": "Female",
    "Female": "Female",
    "M": "Male",
    "Male": "Male",
    " ": "Na",
    "NULL": "Na"
}
df["GEN"] = df["GEN"].replace(gender_map)


cursor.execute("""               
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transformation')
BEGIN
    EXEC('CREATE SCHEMA [transformation]')
END""")

cursor.execute("""
IF OBJECT_ID('transformation.cust_az12', 'U') IS NOT NULL
    DROP TABLE transformation.cust_az12;

CREATE TABLE transformation.cust_az12 (
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(20)
)
""")

df_to_load = df[
    [
        "CID", 
        "BDATE",
        "GEN" 
    ]
].copy()

text_cols = [
    "CID", 
    "BDATE",
    "GEN" 
]

for col in text_cols:
    df_to_load[col] = df_to_load[col].astype(str)

df_to_load = df_to_load.where(pd.notnull(df_to_load), None)


insert_query = """
INSERT INTO transformation.cust_az12
(CID, BDATE, GEN)
VALUES (?, ?, ?)
"""

cursor.fast_executemany = True

print(f"Loading {len(df_to_load)} rows...")

cursor.executemany(insert_query, df_to_load.values.tolist())

conn.commit()

print("Success!")

conn.close()