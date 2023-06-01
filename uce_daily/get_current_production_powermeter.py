import json
import requests
from requests import HTTPError
import datetime as dt
import pandas as pd


class OperativeProduction:
    def __init__(self, url, user, password):
        self.url = url
        self.user = user
        self.password = password
        self.access_token = ""
        self.site_ids = dict()
        self.powermeters = dict()
        self.analyzers = dict()

        self.login_endpoint = "/api/v1/mainapi/login/"
        self.get_sites_endpoint = "/api/v1/mainapi/site/get/my"
        self.get_devices_endpoint = "/api/v1/mainapi/device/get/{}"
        self.get_data_endpoint = "/api/v1/mainapi/get/data/{}/{}"

        self.login()
        self.update_site_ids()
        self.get_devices()

    def login(self):
        header = {
            "Content-Type": "application/json"
        }
        data = {
            "user_name": self.user,
            "password": self.password
        }
        try:
            response = requests.post(self.url + self.login_endpoint, data=json.dumps(data), headers=header) #, verify=False)
            self.access_token = response.json()["access_token"]
            response.raise_for_status()
        except Exception as err:
            print(f'Exception raised: {err}')
        else:
            print('Token updated!')

    def update_site_ids(self):
        header = {
            "Content-Type": "application/json",
            "token": self.access_token
        }
        try:
            response = requests.get(self.url + self.get_sites_endpoint, headers=header) #, verify=True)
            
            for site in response.json():
                self.site_ids.update({site["w_code"]: site["station_id"]})
            response.raise_for_status()
        except Exception as err:
            print(f'Exception raised: {err}')
        else:
            print('Site ids updated!')

    def get_devices(self, site_id=None):
        if site_id is None:
            site_ids = self.site_ids.values()
        else:
            site_ids = [site_id]

        header = {
            "Content-Type": "application/json",
            "token": self.access_token
        }
        for site_id in site_ids:
            try:
                response = requests.get(self.url + self.get_devices_endpoint.format(site_id), headers=header) #, verify=False)
                powermeter = list()
                for device in response.json():
                    if device["device_name"][-3:] == "out":
                        powermeter.append(device["device_id"])           
                    self.powermeters.update({site_id: tuple(powermeter)})
                response.raise_for_status()
            except Exception as err:
                print(f'Exception raised: {err}')
            else:
                print('Devices updated!')
                
    @staticmethod
    def get_time_index(date, start=None, timezone='utc'):
        if start is None:
            start = dt.datetime(year=date.year, month=date.month, day=date.day, hour=0, minute=30)
        end = start + dt.timedelta(days=1)
        end = dt.datetime(year=end.year, month=end.month, day=end.day, hour=0, minute=30) - dt.timedelta(hours=1)
        index_in_kyiv = pd.date_range(start=start, end=end, freq='1H', tz='europe/kiev')
        index_in_utc = index_in_kyiv.tz_convert('utc')
        if timezone == 'utc':
            return index_in_utc.tz_localize(None)
        elif timezone == 'kyiv':
            return index_in_kyiv.tz_localize(None)
        else:
            raise ValueError

    def read_device_data(self, site_id, device_id, start_date_time, end_date_time, parameter_name):
        header = {
            "Content-Type": "application/json",
            "token": self.access_token
        }
        data = {
            "params_req": [parameter_name],
            "start_date_time": start_date_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date_time": end_date_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        try:
            response = requests.post(
                self.url + self.get_data_endpoint.format(site_id, device_id),
                data=json.dumps(data),
                headers=header
            #    verify=False
            )
            response.raise_for_status()
        except Exception as err:
            print(f'Exception raised: {err}')
            return None
        else:
            print(f'Data read successfully: {site_id}, {device_id}')

        data = pd.DataFrame.from_records(response.json(), coerce_float=True)
        try:
            data.index = data["time_metric"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"))
        except Exception as err:
            return None
        
        data.drop(columns=["time_metric"], inplace=True)
        data = data.resample("1H").mean()
        data.index = data.index + dt.timedelta(minutes=30)
        data.columns = ["yield"]
        # data = data.loc[data["yield"] >= 0.01]
        return data

    def prepare_site_data(self, site_id, utc_time_index, devices, parameter_name):
        data = pd.DataFrame(index=utc_time_index, data=0.0, columns=["yield"])
        data.index.name = "timestamp_utc"
        for device_id in devices:
            device_data = self.read_device_data(
                site_id,
                device_id,
                utc_time_index.min() - dt.timedelta(minutes=30),
                utc_time_index.max() + dt.timedelta(minutes=30),
                parameter_name
            )
            if device_data is not None:
                data = data + device_data

        data.dropna(axis=0, inplace=True)
        data = data.round(0).astype(int)
        data = data[data['yield'] != 0]
        return data

    def get_data(self, w_code, date):
        utc_time_index = self.get_time_index(date, timezone="utc")
        site_id = self.site_ids[w_code]
        try:
            data = self.prepare_site_data(site_id, utc_time_index, self.powermeters[site_id], "Active power, kW")
            return data
        except KeyError as err:
            print(f'Exception raised: {err}')
            return None


if __name__ == "__main__":
    from settings.apis import BORD_API_SETTINGS

    w_code = "62W767068546620M"
    print(type(w_code))
    date = dt.date(2023, 4, 13)

    PowermeterDataGetter = OperativeProduction(**BORD_API_SETTINGS)
    print(PowermeterDataGetter.get_data(w_code, date))