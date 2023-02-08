import sys, os
import pandas as pd
import datetime as dt
import time
import calendar

from uce_resources import get_site_id, get_mms_data, get_applied_forecast, get_prices, get_green_tariff
from uce_resources import make_results, get_forecast

from settings.sites import ceg as sites_list

if __name__ == '__main__':
    
    sites_list = ['Vasylivka']

    assert len(sys.argv) == 3, 'Please specify target_year and target_month arguments'

    target_year = int(sys.argv[1])
    target_month = int(sys.argv[2])

    forecasts_types = ['real', '1_dah', 'restored']

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

    with engine.connect() as connection:
        
        prices = get_prices(target_year, target_month, connection, metadata.tables['electricity_market_prices'], currency='UAH')

    print(prices)

    target_dates = pd.date_range(start=prices.index.date.min() + dt.timedelta(days=1),
                                end=prices.index.date.max(), 
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
            applied_forecast = get_applied_forecast(site_data['site_id'], target_year, target_month, 
                                                    connection=connection, db_table=metadata.tables['forecasts_applied'])
            print('Forecast data of | {} records |'.format(len(applied_forecast)))
            print(applied_forecast)
            
            
            site_data['real_forecast_data'] = pd.concat([mms_data, applied_forecast], axis=1, join='inner')
            print('Real forecast data prepared')
            
            output_data = site_data['real_forecast_data']
            output_data.columns = ['yield [kWh]', 'applied_forecast']

            one_dah_forecast = get_forecast(site_data['site_id'], target_dates, '1dah', connection, metadata) * 1000
            one_dah_forecast = pd.concat([mms_data, one_dah_forecast.round()], axis=1, join='inner')
            # site_data['1dah_forecast'].to_csv('data/uce_update_frequency/separately/' + site + '/' + site + '_1dah_forecast.csv')
            site_data['1_dah_forecast_data'] = one_dah_forecast
            print('1dah forecast data prepared')

            output_data['1_dah_forecast_data'] = one_dah_forecast['forecast [kWh]']

            restored_forecast = get_forecast(site_data['site_id'], target_dates, 'restored', connection, metadata) * 1000
            restored_forecast = pd.concat([mms_data, restored_forecast.round(0)], axis=1, join='inner')
            site_data['restored_forecast_data'] = restored_forecast
            print('Restored forecast data prepared')

            output_data['restored_forecast_data'] = restored_forecast['forecast [kWh]']

            sites_data.update({site: site_data})
            end = time.time()

            print('Processing took {} seconds'.format(round(end - start, 2)))
            
            target_folder = f'data/forecasts/yield_forecasts/{site}/'

            if not os.path.exists(target_folder):
                os.makedirs(target_folder)

            with pd.ExcelWriter(target_folder + '{}_data_{}_{}.xlsx'.format(site, target_year, target_month), engine="openpyxl") as writer:
                output_data.to_excel(writer, 'results_daily')

            print('Saving results: ok!')
