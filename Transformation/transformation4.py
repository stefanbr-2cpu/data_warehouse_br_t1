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


def clean_country(value):
    if pd.isna(value):
        return "NA"

    text = str(value).strip()

    if text == "" or text.upper() == "NULL":
        return "NA"

    country_map = {
        "DE": "Germany",
        "GERMANY": "Germany",
        "US": "United States",
        "USA": "United States",
        "UNITED STATES": "United States",
        "UNITED KINGDOM": "United Kingdom",
        "FRANCE": "France",
        "CANADA": "Canada",
        "AUSTRALIA": "Australia",
    }

    return country_map.get(text.upper(), text)


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

df = pd.read_sql('SELECT * FROM ingestion.loc_a101', conn)

df["CID"] = df["CID"].astype(str).str.replace("-", "", regex=False)

df["CNTRY"] = df["CNTRY"].apply(clean_country)

cursor.execute("""               
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transformation')
BEGIN
    EXEC('CREATE SCHEMA [transformation]')
END""")

cursor.execute("""
IF OBJECT_ID('transformation.loc_a101', 'U') IS NOT NULL
    DROP TABLE transformation.loc_a101;

CREATE TABLE transformation.loc_a101 (
    CID VARCHAR(50),
    CNTRY VARCHAR(100)
)
""")

df_to_load = df[
    [
        "CID",
        "CNTRY",
    ]
].copy()

text_cols = [
    "CID",
    "CNTRY",
]

for col in text_cols:
    df_to_load[col] = df_to_load[col].astype(str)

df_to_load = df_to_load.where(pd.notnull(df_to_load), None)

insert_query = """
INSERT INTO transformation.loc_a101
(CID, CNTRY)
VALUES (?, ?)
"""

cursor.fast_executemany = True

print(f"Loading {len(df_to_load)} rows...")

cursor.executemany(insert_query, df_to_load.values.tolist())

conn.commit()

print("Success!")

conn.close()