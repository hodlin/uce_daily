# FTPs
from dotenv import load_dotenv
import os

load_dotenv()

# CEG forecast_user
ceg_ftp_url = os.environ.get("CEG_FTP_URL")

FORECAST_FTP = {
    'host': ceg_ftp_url.split(':')[0],
    'port': int(ceg_ftp_url.split(':')[-1]),
    'user': os.environ.get("CEG_FTP_FORECAST_USER_NAME"),
    'passwd': os.environ.get("CEG_FTP_FORECAST_USER_PASSWORD"),
    'working_dir': os.environ.get("CEG_FTP_FORECAST_USER_FORECAST_DIR"),
}

METER_DATA_FTP = {
    'host': ceg_ftp_url.split(':')[0],
    'port': int(ceg_ftp_url.split(':')[-1]),
    'user': os.environ.get("CEG_FTP_UTP_DATA_USER_NAME"),
    'passwd': os.environ.get("CEG_FTP_UTP_DATA_USER_PASSWORD"),
    'working_dir': os.environ.get("CEG_FTP_UTP_DATA_USER_FORECAST_DIR"),
}