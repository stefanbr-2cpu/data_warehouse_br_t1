import pyodbc
import os
import pandas as pd

contents_folder = "D:/datasets (1)"

# connect directly to SQL Server
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\\SQLEXPRESS;"
    "DATABASE=MyDatabase;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

conn.autocommit = True
cur = conn.cursor()
cur.fast_executemany = True


for root, dirs, files in os.walk(contents_folder):
    for file in files:

        if file.endswith(".csv"):

            print(f"Loading file: {file}")

            full_path = os.path.join(root, file)

            # read CSV
            df = pd.read_csv(full_path)

            # convert NaN → NULL for SQL
            df = df.astype(object).where(pd.notnull(df), None)

            # determine table name
            table_name = file.replace(".csv", "")

            # convert dataframe rows to list
            rows = df.values.tolist()

            # column names
            cols = ", ".join(df.columns)

            # SQL placeholders
            placeholders = ", ".join(["?"] * len(df.columns))

            # clear table first
            truncate_query = f"TRUNCATE TABLE ingestion.{table_name}"
            cur.execute(truncate_query)

            # insert query
            insert_query = f"""
            INSERT INTO ingestion.{table_name} ({cols})
            VALUES ({placeholders})
            """

            cur.executemany(insert_query, rows)

            conn.commit()

            print(f"{len(rows)} rows inserted into ingestion.{table_name}")

conn.close()
