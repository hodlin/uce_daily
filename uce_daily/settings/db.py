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