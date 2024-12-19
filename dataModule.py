import requests
import pyodbc

# class for interacting with Alpha Vantage
class StockData:
    # initialize class with api key used across functions
    def __init__(self, api_key: str):
        self.api_key = api_key
    # get time series data for given symbol
    def get_daily_time_series(self, symbol:str, outputSize:str):
        url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={}&outputsize={}&apikey={}'.format(symbol, outputSize, self.api_key)
        r = requests.get(url)
        data = r.json()
        return data

# class for transforming data
class Analyze:
    # intialize class
    def __init__(self, symbol:str, data:dict):
        self.symbol = symbol
        self.data = data
        
    def transform_data(self):
        rows = []
        for date, daily_data in self.data.items():
            rows.append({
                "Symbol": self.symbol,
                "Date": date,
                "Open": float(daily_data["1. open"]),
                "High":float(daily_data["2. high"]),
                "Low":float(daily_data["3. low"]),
                "Close":float(daily_data["4. close"]),
                "Volume":int(daily_data["5. volume"])
            })
        return rows
    
    def import_data(self, data:list, connection:pyodbc.Connection):
        cursor = connection.cursor()
        
        for item in data:
            cursor.execute("SELECT COUNT(*) FROM StockData WHERE Symbol = ? AND Date = ?", (self.symbol, item['Date']))
            exists = cursor.fetchone()[0]
            # add if it does not exist
            if not exists:
                sql_query = """
                INSERT INTO StockData (Symbol, Date, [Open], High, Low, [Close], Volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                # Execute the query with data values
                cursor.execute(sql_query, (item['Symbol'], item['Date'], item['Open'], item['High'], item['Low'], item['Close'], item['Volume']))
                
        # Commit the transaction
        connection.commit()
        cursor.close()
