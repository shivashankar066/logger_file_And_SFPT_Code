import pandas as pd
import urllib
from sqlalchemy import create_engine

# Define your connection parameters
server = '10.10.9.56'
database = 'AnalyticsDB'
username = 'taslanalytics'
password = 'Secure@2024$%!'

# Create a connection string
connection_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)

# Encode the connection string for SQLAlchemy
connection_url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"

# Create a SQLAlchemy engine
engine = create_engine(connection_url)
print("Database Connection is Successful")

# Read the Excel file into a DataFrame
df = pd.read_excel("E:/Logistic_Project/DHL_Forwarding/consolidated_file_D12_D28.xlsx")
print(df.head())
# Define your table name
table_name = "DHL_Forwarding_Logistic_data"
# Write the DataFrame to the SQL Server table
print(df.info())
try:
    df.to_sql(table_name, con=engine, index=False, if_exists='append')
    print("Data has been successfully inserted into the SQL Server table.")
except Exception as e:
    print(f"An error occurred: {e}")
