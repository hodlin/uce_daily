# FTPs
from dotenv import load_dotenv
import os

# CEG forecast_user
ceg_ftp_url = os.environ.get("CEG_FTP_URL")

ceg_fc_settings = {
    'host': ceg_ftp_url.split(':')[0],
    'port': int(ceg_ftp_url.split(':')[-1]),
    'user': os.environ.get("CEG_FTP_FORECAST_USER_NAME"),
    'passwd': os.environ.get("CEG_FTP_FORECAST_USER_PASSWORD"),
    'forecast_dir': os.environ.get("CEG_FTP_FORECAST_USER_FORECAST_DIR"),
}