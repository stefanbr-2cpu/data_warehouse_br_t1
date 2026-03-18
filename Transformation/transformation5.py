import pyodbc
import pandas as pd

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
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transformation')
BEGIN
    EXEC('CREATE SCHEMA [transformation]')
END
""")

cursor.execute("""
IF OBJECT_ID('transformation.px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE transformation.px_cat_g1v2;

CREATE TABLE transformation.px_cat_g1v2 (
    ID VARCHAR(50),
    CAT VARCHAR(100),
    SUBCAT VARCHAR(100),
    MAINTENANCE VARCHAR(10)
)
""")

df = pd.read_sql("""
    SELECT ID, CAT, SUBCAT, MAINTENANCE
    FROM ingestion.px_cat_g1v2
""", conn)

insert_query = """
INSERT INTO transformation.px_cat_g1v2
(ID, CAT, SUBCAT, MAINTENANCE)
VALUES (?, ?, ?, ?)
"""

cursor.fast_executemany = True
cursor.executemany(insert_query, df.values.tolist())

conn.commit()
conn.close()

print("transformation.px_cat_g1v2 loaded successfully!")