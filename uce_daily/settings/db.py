# Databases
from dotenv import load_dotenv
import os

load_dotenv()

# Digital Ocean
DO_SETTINGS = {
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
    "database": os.environ.get("POSTGRES_DATABASE"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
}

DO_URL = "postgresql://{}:{}@{}:{}/{}"

DO_URL = DO_URL.format(
    DO_SETTINGS["user"],
    DO_SETTINGS["password"],
    DO_SETTINGS["host"],
    DO_SETTINGS["port"],
    DO_SETTINGS["database"],
)

WAREHOUSE_SETTINGS = {
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
    "database": os.environ.get("WAREHOUSE_DATABASE"),
    "user": os.environ.get("WAREHOUSE_USER"),
    "password": os.environ.get("WAREHOUSE_PASSWORD"),
}

WAREHOSUE_URL = "postgresql://{}:{}@{}:{}/{}"

WAREHOSUE_URL = WAREHOSUE_URL.format(
    WAREHOUSE_SETTINGS["user"],
    WAREHOUSE_SETTINGS["password"],
    WAREHOUSE_SETTINGS["host"],
    WAREHOUSE_SETTINGS["port"],
    WAREHOUSE_SETTINGS["database"],
)

FORECAST_DB_SETTINGS = {
    "host": os.environ.get("FORECAST_DB_HOST"),
    "port": os.environ.get("FORECAST_DB_PORT"),
    "database": os.environ.get("FORECAST_DB_DATABASE"),
    "user": os.environ.get("FORECAST_DB_USER"),
    "password": os.environ.get("FORECAST_DB_PASSWORD"),
}

FORECAST_DB_URL = "postgresql://{}:{}@{}:{}/{}"

FORECAST_DB_URL = FORECAST_DB_URL.format(
    FORECAST_DB_SETTINGS["user"],
    FORECAST_DB_SETTINGS["password"],
    FORECAST_DB_SETTINGS["host"],
    FORECAST_DB_SETTINGS["port"],
    FORECAST_DB_SETTINGS["database"],
)