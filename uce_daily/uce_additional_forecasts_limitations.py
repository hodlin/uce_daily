import sys
import pandas as pd
import datetime as dt
import time, pytz
import calendar

from uce_resources import get_site_id, get_mms_data, get_applied_forecast, get_prices, get_green_tariff
from uce_resources import make_results, get_forecast

from settings.sites import rengy_d as sites_list

if __name__ == '__main__':

    assert len(sys.argv) == 3, 'Please specify target_year and target_month arguments'

    target_year = int(sys.argv[1])
    target_month = int(sys.argv[2])

    forecasts_types = ['real', 'zero', 'restored']

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

    # price_dir = 'data/results/2022-10/'
    # price_file = 'prices_2022_10_1-19.xlsx'
    # prices = pd.read_excel(price_dir + price_file, index_col= 0)

    print(prices)

    target_dates = pd.date_range(start=prices.index.date.min() + dt.timedelta(days=1),
                                end=prices.index.date.max(), 
                                freq='D').to_pydatetime()

    
    limitations_file = './data/limitations/limitations.xlsx'

    limitation_data = pd.read_excel(
        limitations_file, 
        sheet_name='{}_{:02d}'.format(target_year, target_month),
        parse_dates=False
        )

    limitation_data = limitation_data.fillna(0)
    limitation_data.iloc[:, 2:] = limitation_data.iloc[:, 2:].astype(int)
    limitation_data['True_Time'] = (limitation_data['Time'] - 1).apply(lambda x: '{:02d}'.format(x))
    limitation_data.iloc[:, :2] = limitation_data.iloc[:, :2].astype(str)

    print(limitation_data.loc[limitation_data['Date'] == '2022-03-27'])

    index = pd.to_datetime(limitation_data['Date'] + ' ' + limitation_data['True_Time'], format='%Y-%m-%d %H') + dt.timedelta(minutes=30)
    index = pd.DatetimeIndex(data=index)
    index = index.tz_localize(pytz.timezone('europe/kiev'))
    index = index.tz_convert('utc')
    index = index.tz_localize(None)
    limitation_data.index = index
    limitation_data = limitation_data.drop(columns=['Date', 'Time', 'True_Time'])

    extended_limitations = pd.DataFrame(index=prices.index, columns=limitation_data.columns, data=0)
    extended_limitations.loc[extended_limitations.index.intersection(limitation_data.index)] = limitation_data.loc[limitation_data.index.intersection(extended_limitations.index)]
    limitation_data = extended_limitations
    limitat=limitation_data["Myroliubivka"]
    print(limitat)
    # limitation_data.to_excel("limitations123.xlsx")


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
                    
            site_data['green_tariff'] = get_green_tariff(site_data['site_id'], dt.date(year=target_year, month=target_month, day=1),
                                                        connection, metadata.tables['green_tariffs'], currency='UAH')
            print('Green tariff: {}'.format(site_data['green_tariff']))
            
            mms_data, site_data['mms_version'] = get_mms_data(site_data['site_id'], 
                                                            target_year, target_month, 
                                                            connection, metadata.tables['mms_data'], include_prev=True,)
            # print(mms_data)
            print('MMS data | {} version | of | {} records |'.format(site_data['mms_version'], len(mms_data)))
            applied_forecast = get_applied_forecast(site_data['site_id'], target_year, target_month, 
                                                    connection=connection, db_table=metadata.tables['forecasts_applied'])
            print('Forecast data of | {} records |'.format(len(applied_forecast)))
            #print(applied_forecast)
            
            site_data['real_forecast_data'] = pd.concat([mms_data, applied_forecast], axis=1, join='inner')
            print('Real forecast data prepared')

            limitation_forecast = applied_forecast.copy()
            limitation_forecast['forecast [kWh]'] = limitation_forecast['forecast [kWh]'] - limitation_data[site]

            site_data['limitation_forecast_data'] = pd.concat([mms_data, limitation_forecast], axis=1, join='inner')
            print('Limitation forecast data prepared')
        
            zero_forecast = applied_forecast * 0
            zero_forecast['forecast [kWh]'] = zero_forecast['forecast [kWh]'] - limitation_data[site]

            site_data['zero_forecast_data'] = pd.concat([mms_data, zero_forecast], axis=1, join='inner')
            print('Zero forecast data prepared')

            restored_forecast = get_forecast(site_data['site_id'], target_dates, 'restored', connection, metadata) * 1000
            restored_forecast = pd.concat([mms_data, restored_forecast.round(0)], axis=1, join='inner')
            site_data['restored_forecast_data'] = restored_forecast
            print('Restored forecast data prepared')

            restored_forecast_lim = restored_forecast['forecast [kWh]'].copy()
            restored_forecast_lim = pd.concat([mms_data, restored_forecast_lim.round(0)], axis=1, join='inner')
            restored_forecast_lim['forecast [kWh]'] = restored_forecast_lim ['forecast [kWh]'] - limitation_data[site]
            site_data['restored_forecast_data_lim'] = restored_forecast_lim
            print('Restored forecast data prepared')

            sites_data.update({site: site_data})
            end = time.time()

            print('Processing took {} seconds'.format(round(end - start, 2)))

    columns = ['site', 'legal_entity', 'first_date', 'last_date', 'number_of_values [records]', 'yield_data_version',
                'yielded [kWh]', 'forecast_type', 'forecasted [kWh]', 
                'green_tariff [UAH]', 'revenue [UAH]', 
                'error_u [kWh]', 'error_u [%]',
                'max_energy [kWh]', 'max_forecast [kWh]', 'max_error [kWh]',
                'mean_absolute_error [kWh]', 'median_absolute_error [kWh]', 
                'mean_square_error [kWh]', 'root_mean_square_error [kWh]', 'R^2 score',
                'dropped by alpha_u [records]', 'dropped by alpha_u [%]',
                'error_u (excess) [kWh]', 'error_u (excess) [%]',
                'error_u (shortage) [kWh]', 'error_u (shortage) [%]', 
                'cieq_641_rule (excess) [UAH]', 'cieq_641_rule (excess) [%]',
                'cieq_641_rule (shortage) [UAH]', 'cieq_641_rule (shortage) [%]',
                'cieq_641_rule (net) [UAH]', 'cieq_641_rule (net) [%]', 
                'imsp_avg_641_rule [UAH/MWh]',
                'cieq_641_rule* [UAH]', 'cieq_641_rule* [%]', 
                'imsp_avg_641_rule* [UAH/MWh]']

    daily_indexes = list()

    for day in range(1, calendar.monthrange(target_year, target_month)[-1] + 1):
        start = dt.datetime(year=target_year, month=target_month, day=day, hour=0, minute=30)
        end = dt.datetime(year=target_year, month=target_month, day=day, hour=23, minute=30)
        index_in_kyiv = pd.date_range(start=start, end=end, freq='1H', tz='europe/kiev')
        index_in_utc = index_in_kyiv.tz_convert('utc').tz_localize(None)
        daily_indexes.append(index_in_utc)

    print(len(daily_indexes))

    results_real = pd.DataFrame(columns=columns)
    results_zero = pd.DataFrame(columns=columns)
    results_limitation = pd.DataFrame(columns=columns)
    # results_pro = pd.DataFrame(columns=columns)
    results_restored = pd.DataFrame(columns=columns)
    results_restored_lim = pd.DataFrame(columns=columns)
    

    for site in sites_data.keys():
        
        
        for index in daily_indexes:
            # print(sites_data[site]['real_forecast_data'])
            result_real = make_results(sites_data[site], 'real', prices, index)
            #print(result_real)

            result_limitation = make_results(sites_data[site], 'limitation', prices, index)
            #print(result_real)

            result_zero = make_results(sites_data[site], 'zero', prices, index)      
            #print(result_zero)

            result_restored = make_results(sites_data[site], 'restored', prices, index)
            #print(result_restored)

            result_restored_lim = make_results(sites_data[site], 'restored_lim', prices, index)
            #print(result_restored)


            if not result_real is None:
                results_real = results_real.append(result_real, ignore_index=True)

            if not result_limitation is None:
                results_limitation = results_limitation.append(result_limitation, ignore_index=True)

            if not result_zero is None:
                results_zero = results_zero.append(result_zero, ignore_index=True)

            if not result_restored is None:
                results_restored = results_restored.append(result_restored, ignore_index=True)
                
            if not result_restored_lim is None:
                results_restored = results_restored.append(result_restored_lim, ignore_index=True)
            


        sites_data[site]['results_real'] = results_real
        sites_data[site]['results_limitation'] = results_limitation
        sites_data[site]['results_zero'] = results_zero
        sites_data[site]['restored'] = result_restored
        sites_data[site]['restored_lim'] = result_restored_lim


        print(f'{site} - Results daily: Ok!')


    results_daily = pd.concat([results_real, results_zero, results_restored, results_limitation, results_restored_lim], axis=0)

    days = '{}-{}'.format(results_daily['first_date'].min().day, results_daily['first_date'].max().day)

    with pd.ExcelWriter('data/results/{}-{:0>2}/uce_a_lim_daily_shyroke_{}_{}_{}_UAH.xlsx'.format(target_year, target_month, target_year, target_month, days), engine="openpyxl") as writer:
        results_daily.to_excel(writer, 'results_daily')

    print('Saving results: ok!')
