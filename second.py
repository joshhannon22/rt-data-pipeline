from dataModule import Analyze
import json
import pandas as pd
import pyodbc


with open('stock_data_compact.json', 'r') as file:
    data = json.load(file)
    
with open('app-secret.txt', 'r') as file:
    client_secret = file.read()

symbol = data["Meta Data"]["2. Symbol"]
time_series_data = data["Time Series (Daily)"]

analyze_client = Analyze(symbol,time_series_data)
clean_data = analyze_client.transform_data()

# Set up connection parameters
server = 'datapipelineserver22.database.windows.net'
database = 'data-pipeline-db'
tenant_id = 'b0f5bbda-5dc8-4ac8-8621-fb81b23db439'  # Azure Active Directory tenant ID
client_id = '9234a681-aeb8-4adc-8a30-6b0831317027'  # Service principal application ID
# Create the connection string using Azure AD service principal authentication
conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};PORT=1433;DATABASE={database};' \
           f'Authentication=ActiveDirectoryServicePrincipal;UID={client_id};PWD={client_secret};' \
           f'TenantID={tenant_id}'

# Establish the connection
conn = pyodbc.connect(conn_str)
analyze_client.import_data(data=clean_data,connection=conn)

cursor = conn.cursor()

# Example query
cursor.execute("SELECT * FROM StockData")
row = cursor.fetchone()
print(row)

# Commit and close
conn.commit()
cursor.close()
conn.close()

print ("here")