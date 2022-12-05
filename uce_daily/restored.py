import sys
import pandas as pd
import numpy as np
import datetime as dt
import time
import calendar

from uce_resources import get_site_id, get_mms_data
from uce_resources import get_forecast

from settings.sites import rengy_d as sites_list



def make_results(site_data, forecast_type, index):
    #print(site_data)
    
    if forecast_type == 'restored':
        data = site_data['restored_forecast_data'].loc[site_data['restored_forecast_data'].index.intersection(index)]
    else:
        data = site_data[forecast_type].loc[site_data[forecast_type].index.intersection(index)]

    data = pd.concat([data], axis=1, join='inner')

    if len(data.index) == 0:
        return None

    if len(data.index) != 24:
        print('{} index len is {}'.format(data.index.max().strftime('%Y-%m-%d'), len(data.index)))

    
    with np.errstate(divide='ignore'):
        result = dict()
        
        result['site'] = site_data['site']
        result['legal_entity'] = site_data['legal_entity']
        result['first_date'] = (data.index.min() + dt.timedelta(days=1)).date()
        result['last_date'] = data.index.max().date()
        result['number_of_values [records]'] = len(data.index)

        result['yield_data_version'] = site_data['mms_version']
        result['yielded [kWh]'] = data['yield [kWh]'].sum()

        result['forecast_type'] = forecast_type
        result['forecasted [kWh]'] = data['forecast [kWh]'].sum()

    #print(result)

    return pd.Series(result)




if __name__ == '__main__':

    assert len(sys.argv) == 3, 'Please specify target_year and target_month arguments'

    target_year = int(sys.argv[1])
    target_month = int(sys.argv[2])

    forecasts_types = ['real', 'naive', 'zero', '1_dah', 'pro', 'restored']

    # sites_list = ['Myroliubivka']
    sites_data = dict.fromkeys(sites_list)
    print(sites_data)
    print(len(sites_data.keys()))

    from sqlalchemy import create_engine, MetaData
    from sqlalchemy.pool import NullPool
    from settings.db import DO_URL

    engine = create_engine(DO_URL, poolclass=NullPool)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    # price_dir = 'data/results/2022-09/'
    # price_file = 'prices_2022_9_1-30.xlsx'
    # prices = pd.read_excel(price_dir + price_file, index_col= 0)

    # print(prices)

    target_dates = pd.date_range(start='2022-03-05',
                                end='2022-03-10', 
                                freq='D').to_pydatetime()

    with engine.connect() as connection:
            
        for site in sites_data.keys():
            start = time.time()
            print('-'*50)
            print(site)
            site_data = dict()
            site_data['site'] = site
            site_data['site_id'], site_data['legal_entity'] = get_site_id(site, connection, 
                                                                        metadata.tables['sites'],
                                                                        include_legal_entity_id=True)
                    
            mms_data, site_data['mms_version'] = get_mms_data(site_data['site_id'], 
                                                            target_year, target_month, 
                                                            connection, metadata.tables['mms_data'], include_prev=True,)
            # print(mms_data)
            print('MMS data | {} version | of | {} records |'.format(site_data['mms_version'], len(mms_data)))

            restored_forecast = get_forecast(site_data['site_id'], target_dates, 'restored', connection, metadata) * 1000
            restored_forecast = pd.concat([mms_data, restored_forecast.round(0)], axis=1, join='inner')
            site_data['restored_forecast_data'] = restored_forecast
            print('Restored forecast data prepared')

            sites_data.update({site: site_data})
            end = time.time()

            print('Processing took {} seconds'.format(round(end - start, 2)))

    print(sites_data)
    
    columns = ['site', 'legal_entity', 'first_date', 'last_date', 'number_of_values [records]', 'yield_data_version',
                'yielded [kWh]', 'forecast_type', 'forecasted [kWh]', ]
    
    daily_indexes = list()

    for day in range(1, calendar.monthrange(target_year, target_month)[-1] + 1):
        start = dt.datetime(year=target_year, month=target_month, day=day, hour=0, minute=30)
        end = dt.datetime(year=target_year, month=target_month, day=day, hour=23, minute=30)
        index_in_kyiv = pd.date_range(start=start, end=end, freq='1H', tz='europe/kiev')
        index_in_utc = index_in_kyiv.tz_convert('utc').tz_localize(None)
        daily_indexes.append(index_in_utc)

    print(len(daily_indexes))

    results_restored = pd.DataFrame(columns=columns)

    for site in sites_data.keys():
        
        
        for index in daily_indexes:
            result_restored = make_results(sites_data[site], 'restored', index)
            print('result_restored')
            print(result_restored)

            if not result_restored is None:
                 results_restored = results_restored.append(result_restored, ignore_index=True)


        sites_data[site]['results_restored'] = results_restored

        print(f'{site} - Results daily: Ok!')


    print('results restored\n')
    print(results_restored)

    results_daily = pd.concat([results_restored, ], axis=0)
    print('results daily\n')
    print(results_daily)

    days = '{}-{}'.format(results_daily['first_date'].min().day, results_daily['first_date'].max().day)

    with pd.ExcelWriter('data/results/{}-{:0>2}/restored_porohy_{}_{}_{}_UAH.xlsx'.format(target_year, target_month, target_year, target_month, days), engine="openpyxl") as writer:
        results_daily.to_excel(writer, 'results_daily')

    print('Saving results: ok!')
