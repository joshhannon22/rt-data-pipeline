import joblib
import json
import numpy as np
import os

def init():
    global model
    model_path = os.path.join(os.getenv("AZUREML_MODEL_DIR"), "stock_price_model.pkl")
    model = joblib.load(model_path)

def run(data):
    try:
        input_data = np.array(json.loads(data)['data'])
        predictions = model.predict(input_data)
        return json.dumps({"predictions": predictions.tolist()})
    except Exception as e:
        return json.dumps({"error": str(e)})
