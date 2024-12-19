from dataModule import Analyze
import json
import pandas as pd
import pyodbc
from azureml.core import Workspace
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error
from azureml.core.model import Model
import joblib
from azureml.core.environment import Environment
from azureml.core.webservice import AciWebservice
from azureml.core.model import InferenceConfig
from azureml.core.webservice import Webservice
import requests


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
# Import Data
analyze_client.import_data(data=clean_data,connection=conn)

# Ana
# Load the workspace from the config.json file
ws = Workspace.from_config()
print(f"Connected to workspace: {ws.name}")

# Load the data
query = "SELECT Date, [Open], High, Low, [Close], Volume FROM StockData WHERE Symbol='SPY' ORDER BY Date"
data = pd.read_sql(query, conn)
# Convert Date column to datetime and sort
data['Date'] = pd.to_datetime(data['Date'])
data = data.sort_values(by='Date')

# Create lag features
for lag in range(1, 6):  # Lag 1 to 5 days
    data[f'Close_Lag{lag}'] = data['Close'].shift(lag)

# Drop rows with NaN due to lagging
data = data.dropna()

# Features and target
X = data[['Close_Lag1', 'Close_Lag2', 'Close_Lag3', 'Close_Lag4', 'Close_Lag5', 'Volume']]
y = data['Close']

# Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print(X_train.shape)
print(X_train.head())
print(y_train.head())

# Train Linear Regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Evaluate
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))

print(f"MAE: {mae:.2f}, RMSE: {rmse:.2f}")

joblib.dump(model, "stock_price_model.pkl")

# Register model in Azure ML Workspace
registered_model = Model.register(workspace=ws,
                                  model_path="stock_price_model.pkl",
                                  model_name="stock_price_predictor")
print(f"Model registered: {registered_model.name}")

# Define environment
env = Environment("sklearn-env")
env.python.conda_dependencies.add_pip_package("scikit-learn")
env.python.conda_dependencies.add_pip_package("numpy")

# Inference configuration
inference_config = InferenceConfig(entry_script="score.py", environment=env)

# Deploy to Azure Container Instance
deployment_config = AciWebservice.deploy_configuration(cpu_cores=1, memory_gb=1)

#service = Model.deploy(ws, "stock-price-service", [registered_model], inference_config, deployment_config)
service_name = "stock-price-service"
try:
    # Check if the service already exists
    service = Webservice(workspace=ws, name=service_name)
    print(f"Service URI: {service.scoring_uri}")
except Exception as e:
    print(f"Service '{service_name}' not found. Proceeding to deploy a new service.")
    service = Model.deploy(ws, service_name, [registered_model], inference_config, deployment_config)
    service.wait_for_deployment()
    print(f"Service URI: {service.scoring_uri}")

conn.close()

# Validate
scoring_uri = service.scoring_uri
headers = {"Content-Type": "application/json"}

# Replace with your test data
test_data = json.dumps({"data": [[588.0,592.0,606.0,603.0,601.0,599.0]]})
response = requests.post(scoring_uri, data=test_data, headers=headers)

print(response.status_code)
print(response.json())
