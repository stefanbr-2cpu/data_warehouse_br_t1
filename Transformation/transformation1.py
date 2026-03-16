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

cursor = conn.cursor()

print("Database ready!")



df = pd.read_sql("SELECT * FROM ingestion.prd_info", conn)



id_columns = [col for col in df.columns if is_id_column(col)]

for col in id_columns:
    df[col] = df[col].map(clean_identifier)


df = df.dropna(subset=["prd_id"])
df = df.drop_duplicates(subset="prd_id", keep="last")

print("Number of NAs now:", df["prd_id"].isnull().sum())



df["prd_id_category"] = df["prd_key"].str[:5]
df["prd_key"] = df["prd_key"].str[6:]

df["prd_id_category"] = df["prd_id_category"].str.replace("-", "_", regex=False)
df["prd_key"] = df["prd_key"].str.replace("-", "_", regex=False)


df["prd_cost"] = df["prd_cost"].fillna(0)



df["prd_line"] = df["prd_line"].str.strip().str.upper()

line_map = {
    "S": "Sport",
    "M": "Mountain",
    "T": "Touring",
    "R": "Road"
}

df["prd_line"] = df["prd_line"].map(line_map).fillna("Not Available")



df["prd_end_dt"] = (
    df.groupby("prd_key")["prd_start_dt"]
    .shift(-1) - pd.Timedelta(days=1)
)



cursor.execute("""
IF OBJECT_ID('transformation.prd_info', 'U') IS NOT NULL
DROP TABLE transformation.prd_info

CREATE TABLE transformation.prd_info (
    prd_id VARCHAR(MAX),
    prd_id_category VARCHAR(MAX),
    prd_key VARCHAR(MAX),
    prd_nm VARCHAR(MAX),
    prd_cost VARCHAR(MAX),
    prd_line VARCHAR(MAX),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
)
""")

df_to_load = df[
    [
        "prd_id",
        "prd_id_category",
        "prd_key",
        "prd_nm",
        "prd_cost",
        "prd_line",
        "prd_start_dt",
        "prd_end_dt"
    ]
].copy()

text_cols = [
    "prd_id",
    "prd_id_category",
    "prd_key",
    "prd_nm",
    "prd_cost",
    "prd_line"
]

for col in text_cols:
    df_to_load[col] = df_to_load[col].astype(str)

df_to_load = df_to_load.where(pd.notnull(df_to_load), None)


insert_query = """
INSERT INTO transformation.prd_info
(prd_id, prd_id_category, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

cursor.fast_executemany = True

print(f"Loading {len(df_to_load)} rows...")

cursor.executemany(insert_query, df_to_load.values.tolist())

conn.commit()

print("Success!")

conn.close()