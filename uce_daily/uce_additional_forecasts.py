import sys
import pandas as pd
import datetime as dt
import time
import calendar

from uce_resources import get_site_id, get_mms_data, get_applied_forecast, get_prices, get_green_tariff
from uce_resources import make_results, get_forecast

from settings.sites import ceg_wo_occ as sites_list

if __name__ == '__main__':

    assert len(sys.argv) == 3, 'Please specify target_year and target_month arguments'

    target_year = int(sys.argv[1])
    target_month = int(sys.argv[2])

    forecasts_types = ['real', 'naive', 'zero', '1_dah', 'pro', 'raw']

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

    # price_dir = 'data/results/2024-05/'
    # price_file = 'prices_2024_5_1-31.xlsx'
    # prices = pd.read_excel(price_dir + price_file, index_col= 0)

    print(prices)

    target_dates = pd.date_range(start=prices.index.date.min() + dt.timedelta(days=1),
                                end=prices.index.date.max(), 
                                freq='D').to_pydatetime()

    min_price_day = (prices.index.min() + dt.timedelta(days=1)).day
    max_price_day = prices.index.max().day

    start_date = dt.date(year=target_year, month=target_month, day=min_price_day)
    end_date = dt.date(year=target_year, month=target_month, day=max_price_day)
    print(start_date, end_date)

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

            applied_forecast = get_applied_forecast(site_data['site_id'], start_date, end_date, 
                                                connection=connection, db_table=metadata.tables['forecasting_data'])
            print('Forecast data of | {} records |'.format(len(applied_forecast)))
            #print(applied_forecast)
            
            site_data['real_forecast_data'] = pd.concat([mms_data, applied_forecast], axis=1, join='inner')
            print('Real forecast data prepared')

            site_data['zero_forecast_data'] = pd.concat([mms_data, applied_forecast * 0], axis=1, join='inner')
            print('Zero forecast data prepared')

            raw_forecast = get_applied_forecast(site_data['site_id'], start_date, end_date, 
                                            connection=connection, db_table=metadata.tables['forecasting_data'],
                                            forecast_type='forecast_applied_raw')

            site_data['raw_forecast_data'] = pd.concat([mms_data, raw_forecast], axis=1, join='inner')
            print('Raw forecast data prepared')

            naive_forecast_data = pd.concat([mms_data, mms_data.shift(48)], axis=1, join='inner').dropna(axis=0, how='any')
            naive_forecast_data.columns = ['yield [kWh]', 'forecast [kWh]']
            naive_forecast_data['forecast [kWh]'] = naive_forecast_data['forecast [kWh]'].astype(int)
            site_data['naive_forecast_data'] = naive_forecast_data
            print('Naive forecast data prepared')

            one_dah_forecast = get_forecast(site_data['site_id'], target_dates, '1dah', connection, metadata) * 1000
            one_dah_forecast = pd.concat([mms_data, one_dah_forecast.round()], axis=1, join='inner')
            # site_data['1dah_forecast'].to_csv('data/uce_update_frequency/separately/' + site + '/' + site + '_1dah_forecast.csv')
            site_data['1_dah_forecast_data'] = one_dah_forecast
            print('1dah forecast data prepared')

            # pro_forecast = get_forecast(site_data['site_id'], target_dates, 'pro', connection, metadata) * 1000
            # pro_forecast = pd.concat([mms_data, pro_forecast.round()], axis=1, join='inner')
            # # site_data['pro_forecast'].to_csv('data/uce_update_frequency/separately/{}/{}_pro_forecast.csv'.format(site, site))
            # site_data['pro_forecast_data'] = pro_forecast
            # print('Pro forecast data prepared')

            
            decreased_70_forecast = raw_forecast['forecast [kWh]'].copy()
            decreased_70_forecast.loc[decreased_70_forecast > 0] = decreased_70_forecast.loc[decreased_70_forecast > 0] * 0.3
            decreased_70_forecast = pd.concat([mms_data, decreased_70_forecast.round(0)], axis=1, join='inner')
            site_data['decreased_70_forecast_data'] = decreased_70_forecast
            print('Decreased forecast -70pu data prepared')
            
            decreased_60_forecast = raw_forecast['forecast [kWh]'].copy()
            decreased_60_forecast.loc[decreased_60_forecast > 0] = decreased_60_forecast.loc[decreased_60_forecast > 0] * 0.4
            decreased_60_forecast = pd.concat([mms_data, decreased_60_forecast.round(0)], axis=1, join='inner')
            site_data['decreased_60_forecast_data'] = decreased_60_forecast
            print('Decreased forecast -60pu data prepared')

            decreased_50_forecast = raw_forecast['forecast [kWh]'].copy()
            decreased_50_forecast.loc[decreased_50_forecast > 0] = decreased_50_forecast.loc[decreased_50_forecast > 0] * 0.5
            decreased_50_forecast = pd.concat([mms_data, decreased_50_forecast.round(0)], axis=1, join='inner')
            site_data['decreased_50_forecast_data'] = decreased_50_forecast
            print('Decreased forecast -50pu data prepared')

            decreased_40_forecast = raw_forecast['forecast [kWh]'].copy()
            decreased_40_forecast.loc[decreased_40_forecast > 0] = decreased_40_forecast.loc[decreased_40_forecast > 0] * 0.6
            decreased_40_forecast = pd.concat([mms_data, decreased_40_forecast.round(0)], axis=1, join='inner')
            site_data['decreased_40_forecast_data'] = decreased_40_forecast
            print('Decreased forecast -40pu data prepared')

            decreased_30_forecast = raw_forecast['forecast [kWh]'].copy()
            decreased_30_forecast.loc[decreased_30_forecast > 0] = decreased_30_forecast.loc[decreased_30_forecast > 0] * 0.7
            decreased_30_forecast = pd.concat([mms_data, decreased_30_forecast.round(0)], axis=1, join='inner')
            site_data['decreased_30_forecast_data'] = decreased_30_forecast
            print('Decreased forecast -30pu data prepared')

            decreased_20_forecast = raw_forecast['forecast [kWh]'].copy()
            decreased_20_forecast.loc[decreased_20_forecast > 0] = decreased_20_forecast.loc[decreased_20_forecast > 0] * 0.8
            decreased_20_forecast = pd.concat([mms_data, decreased_20_forecast.round(0)], axis=1, join='inner')
            site_data['decreased_20_forecast_data'] = decreased_20_forecast
            print('Decreased forecast -20pu data prepared')

            decreased_10_forecast = raw_forecast['forecast [kWh]'].copy()
            decreased_10_forecast.loc[decreased_10_forecast > 0] = decreased_10_forecast.loc[decreased_10_forecast > 0] * 0.9
            decreased_10_forecast = pd.concat([mms_data, decreased_10_forecast.round(0)], axis=1, join='inner')
            site_data['decreased_10_forecast_data'] = decreased_10_forecast
            print('Decreased forecast -10pu data prepared')

            increased_10_forecast = raw_forecast['forecast [kWh]'].copy()
            increased_10_forecast.loc[increased_10_forecast > 0] = increased_10_forecast.loc[increased_10_forecast > 0] * 1.1
            increased_10_forecast = pd.concat([mms_data, increased_10_forecast.round(0)], axis=1, join='inner')
            site_data['increased_10_forecast_data'] = increased_10_forecast
            print('Increased forecast +10pu data prepared')

            increased_20_forecast = raw_forecast['forecast [kWh]'].copy()
            increased_20_forecast.loc[increased_20_forecast > 0] = increased_20_forecast.loc[increased_20_forecast > 0] * 1.2
            increased_20_forecast = pd.concat([mms_data, increased_20_forecast.round(0)], axis=1, join='inner')
            site_data['increased_20_forecast_data'] = increased_20_forecast
            print('Increased forecast +20pu data prepared')

            increased_30_forecast = raw_forecast['forecast [kWh]'].copy()
            increased_30_forecast.loc[increased_30_forecast > 0] = increased_30_forecast.loc[increased_30_forecast > 0] * 1.3
            increased_30_forecast = pd.concat([mms_data, increased_30_forecast.round(0)], axis=1, join='inner')
            site_data['increased_30_forecast_data'] = increased_30_forecast
            print('Increased forecast +30pu data prepared')

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
                'imsp_avg_641_rule* [UAH/MWh]',
                'cieq_641_rule_positive* [UAH]', 'cieq_641_rule_negative* [UAH]',
                'cieq_641_rule_positive* [%]', 'cieq_641_rule_negative* [%]']

    daily_indexes = list()

    for day in range(1, calendar.monthrange(target_year, target_month)[-1] + 1):
        start = dt.datetime(year=target_year, month=target_month, day=day, hour=0, minute=30)
        end = dt.datetime(year=target_year, month=target_month, day=day, hour=23, minute=30)
        index_in_kyiv = pd.date_range(start=start, end=end, freq='1H', tz='europe/kiev')
        index_in_utc = index_in_kyiv.tz_convert('utc').tz_localize(None)
        daily_indexes.append(index_in_utc)

    print(len(daily_indexes))

    results_real = pd.DataFrame(columns=columns)
    results_naive = pd.DataFrame(columns=columns)
    results_zero = pd.DataFrame(columns=columns)
    results_1_dah = pd.DataFrame(columns=columns)
    # results_pro = pd.DataFrame(columns=columns)
    results_raw = pd.DataFrame(columns=columns)
    results_decreased_70 = pd.DataFrame(columns=columns)
    results_decreased_60 = pd.DataFrame(columns=columns)
    results_decreased_50 = pd.DataFrame(columns=columns)
    results_decreased_40 = pd.DataFrame(columns=columns)
    results_decreased_30 = pd.DataFrame(columns=columns)
    results_decreased_20 = pd.DataFrame(columns=columns)
    results_decreased_10 = pd.DataFrame(columns=columns)
    results_increased_10 = pd.DataFrame(columns=columns)
    results_increased_20 = pd.DataFrame(columns=columns)
    results_increased_30 = pd.DataFrame(columns=columns)

    for site in sites_data.keys():
        
        
        for index in daily_indexes:
            # print(sites_data[site]['real_forecast_data'])
            result_real = make_results(sites_data[site], 'real', prices, index)
            #print(result_real)

            result_naive = make_results(sites_data[site], 'naive', prices, index)      
            #print(result_naive)

            result_zero = make_results(sites_data[site], 'zero', prices, index)      
            #print(result_zero)

            result_1_dah = make_results(sites_data[site], '1_dah', prices, index)      
            #print(result_1_dah)

            # result_pro = make_results(sites_data[site], 'pro', prices, index)      
            # #print(result_pro)

            result_raw = make_results(sites_data[site], 'raw', prices, index)
            #print(result_restored)

            result_decreased_70 = make_results(sites_data[site], 'decreased_70', prices, index)
            #print(result_increased_30)

            result_decreased_60 = make_results(sites_data[site], 'decreased_60', prices, index)
            #print(result_increased_30)

            result_decreased_50 = make_results(sites_data[site], 'decreased_50', prices, index)
            #print(result_increased_30)

            result_decreased_40 = make_results(sites_data[site], 'decreased_40', prices, index)
            #print(result_increased_30)

            result_decreased_30 = make_results(sites_data[site], 'decreased_30', prices, index)
            #print(result_increased_30)

            result_decreased_20 = make_results(sites_data[site], 'decreased_20', prices, index)
            #print(result_increased_30)

            result_decreased_10 = make_results(sites_data[site], 'decreased_10', prices, index)
            #print(result_increased_30)

            result_increased_10 = make_results(sites_data[site], 'increased_10', prices, index)
            #print(result_increased_10)

            result_increased_20 = make_results(sites_data[site], 'increased_20', prices, index)
            #print(result_increased_20)

            result_increased_30 = make_results(sites_data[site], 'increased_30', prices, index)
            #print(result_increased_30)

            if not result_real is None:
                results_real = results_real.append(result_real, ignore_index=True)

            if not result_naive is None:
                results_naive = results_naive.append(result_naive, ignore_index=True)
            
            if not result_zero is None:
                results_zero = results_zero.append(result_zero, ignore_index=True)
                
            if not result_1_dah is None:
                results_1_dah = results_1_dah.append(result_1_dah, ignore_index=True)

            # if not result_pro is None:
            #     results_pro = results_pro.append(result_pro, ignore_index=True)

            if not result_raw is None:
                results_raw = results_raw.append(result_raw, ignore_index=True)

                        
            if not result_decreased_70 is None:
                results_decreased_70 = results_decreased_70.append(result_decreased_70, ignore_index=True)

            if not result_decreased_60 is None:
                results_decreased_60 = results_decreased_60.append(result_decreased_60, ignore_index=True)

            if not result_decreased_50 is None:
                results_decreased_50 = results_decreased_50.append(result_decreased_50, ignore_index=True)

            if not result_decreased_40 is None:
                results_decreased_40 = results_decreased_40.append(result_decreased_40, ignore_index=True)

            if not result_decreased_30 is None:
                results_decreased_30 = results_decreased_30.append(result_decreased_30, ignore_index=True)
            
            if not result_decreased_20 is None:
                results_decreased_20 = results_decreased_20.append(result_decreased_20, ignore_index=True)

            if not result_decreased_10 is None:
                results_decreased_10 = results_decreased_10.append(result_decreased_10, ignore_index=True)

            if not result_increased_10 is None:
                results_increased_10 = results_increased_10.append(result_increased_10, ignore_index=True)

            if not result_increased_20 is None:
                results_increased_20 = results_increased_20.append(result_increased_20, ignore_index=True)

            if not result_increased_30 is None:
                results_increased_30 = results_increased_30.append(result_increased_30, ignore_index=True)



        sites_data[site]['results_real'] = results_real
        sites_data[site]['results_naive'] = results_naive
        sites_data[site]['results_zero'] = results_zero
        sites_data[site]['results_1_dah'] = results_1_dah
        # sites_data[site]['results_pro'] = results_pro
        sites_data[site]['results_raw'] = results_raw
        sites_data[site]['results_decreased_70'] = results_decreased_70
        sites_data[site]['results_decreased_60'] = results_decreased_60
        sites_data[site]['results_decreased_50'] = results_decreased_50
        sites_data[site]['results_decreased_40'] = results_decreased_40
        sites_data[site]['results_decreased_30'] = results_decreased_30
        sites_data[site]['results_decreased_20'] = results_decreased_20
        sites_data[site]['results_decreased_10'] = results_decreased_10
        sites_data[site]['results_increased_10'] = results_increased_10
        sites_data[site]['results_increased_20'] = results_increased_20
        sites_data[site]['results_increased_30'] = results_increased_30

        print(f'{site} - Results daily: Ok!')


    results_daily = pd.concat([results_real, results_naive, results_zero, results_1_dah, results_raw, results_increased_10, results_increased_20, results_increased_30, results_decreased_70, results_decreased_60, results_decreased_50, results_decreased_40, results_decreased_30, results_decreased_20, results_decreased_10], axis=0)

    days = '{}-{}'.format(results_daily['first_date'].min().day, results_daily['first_date'].max().day)

    with pd.ExcelWriter('C:/Users/o.babenko/projects/uce_daily/uce_daily/data/results/{}-{:0>2}/uce_a_daily_{}_{}_{}_UAH.xlsx'.format(target_year, target_month, target_year, target_month, days), engine="openpyxl") as writer:
        results_daily.to_excel(writer, 'results_daily')

    print('Saving results: ok!')
