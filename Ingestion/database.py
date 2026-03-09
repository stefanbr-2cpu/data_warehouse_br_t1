import pyodbc

# ----- connect to master -----
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\\SQLEXPRESS;"
    "DATABASE=master;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

conn.autocommit = True
cursor = conn.cursor()

# create database safely
cursor.execute("""
IF DB_ID('MyDatabase') IS NULL
CREATE DATABASE MyDatabase
""")

print("Database ready!")

conn.close()
