from dataModule import StockData
import json

# Initialize API with your key
with open('av-key.txt', 'r') as file:
    api_key = file.read()
    
stock_data_client = StockData(api_key=api_key)

data = stock_data_client.get_daily_time_series(symbol="SPY",outputSize="compact")

output_file = "stock_data_compact.json"  # Name of the file where data will be saved
with open(output_file, 'w') as json_file:
    json.dump(data, json_file, indent=4)