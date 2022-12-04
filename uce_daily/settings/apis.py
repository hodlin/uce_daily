# Databases
from dotenv import load_dotenv
import os

load_dotenv()

# Forecast API
FORECAST_API_SETTINGS = {
    "host": os.environ.get("FORECAST_API_HOST"),
    "port": os.environ.get("FORECAST_API_PORT"),
    "api_key_name": os.environ.get("FORECAST_API_KEY_NAME"),
    "api_key_value": os.environ.get("FORECAST_API_KEY"),
}

# Forecast API
BORD_API_SETTINGS = {
    "url": os.environ.get("BORD_API_URL"),
    "user": os.environ.get("BORD_API_USER"),
    "password": os.environ.get("BORD_API_PASSWORD"),
}