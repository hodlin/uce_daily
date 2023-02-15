import io, os
import ftplib
from functools import lru_cache
import datetime as dt
import pytz
import calendar
import pandas as pd
import numpy as np
from sklearn.metrics import max_error, mean_absolute_error, mean_squared_error, median_absolute_error, r2_score
from math import sqrt

from sqlalchemy import create_engine, MetaData, desc
from sqlalchemy.schema import Table
from sqlalchemy.sql import select, extract, and_, or_, not_, asc

from settings.db import DO_SETTINGS as do_settings
from settings.ftp import FORECAST_FTP


def make_ftp(ftp_settings, supplier=None, logger=None):
    ftp = ftplib.FTP()
    ftp.connect(ftp_settings['host'], ftp_settings['port'])
    ftp.set_pasv(True)
    ftp.login(ftp_settings['user'], ftp_settings['passwd'])
    if logger: 
        logger.info(ftp.getwelcome())
    if supplier:
        ftp.cwd(ftp_settings['working_dir'] + supplier)
    if logger:
        logger.info('Working directory set as: {}'.format(ftp.pwd()))
    return ftp


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


def get_green_tariff(site_id, date, connection, db_table, currency='EUR'):
    
    green_tariffs = db_table
    date = date.strftime('%Y-%m-%d')
    query = select([green_tariffs.c.green_tariff])\
                  .where(and_(green_tariffs.c.valid_from <= date, green_tariffs.c.valid_to >= date, green_tariffs.c.site_id == site_id))
    response = connection.execute(query).fetchall()[0]
    if currency == 'EUR':
        exchange_rate = get_exchange_rate(date.year, date.month)
        green_tariff = response[0] / exchange_rate / 100
        return green_tariff
    elif currency == 'UAH':
        green_tariff = response[0] / 100
        return green_tariff
    else:
        return None


def get_exchange_rate(year, month):
    exchange_rates = pd.read_excel('UAH-EUR_.xlsx')
    rate = exchange_rates.loc[(exchange_rates['Год'] == year) & (exchange_rates['Месяц'] == month), 'UAH/EUR']
    return rate.values[0]


def get_prices(year, month, connection, db_table, currency='EUR'):
    prices_table = db_table
    query = select([prices_table.c.timestamps_utc, prices_table.c.dam, prices_table.c.imsp, 
                    prices_table.c.positive_unbalance, prices_table.c.negative_unbalance])\
                  .where(and_(prices_table.c.year == year, prices_table.c.month == month))
    response = connection.execute(query).fetchall()[0]
    dam = pd.DataFrame(response[1], index=response[0], columns=['dam'], dtype=float)
    imsp = pd.DataFrame(response[2], index=response[0], columns=['imsp'], dtype=float)
    positive_unbalance = pd.DataFrame(response[3], index=response[0], columns=['positive_unbalance'], dtype=float)
    negative_unbalance = pd.DataFrame(response[4], index=response[0], columns=['negative_unbalance'], dtype=float)
    if currency == 'EUR':
        exchange_rate = get_exchange_rate(year, month)
        prices = pd.concat([dam, imsp, positive_unbalance, negative_unbalance], axis=1).divide(exchange_rate).divide(1000)
        return prices
    if currency == 'UAH':
        prices = pd.concat([dam, imsp, positive_unbalance, negative_unbalance], axis=1).divide(1000)
        return prices
    else:
        return None


def get_site_id(site_name, connection, db_table, include_legal_entity_id=False, include_w_code=False):
    sites_table = db_table
    list_to_select = [sites_table.c.id, sites_table.c.legal_entity] if include_legal_entity_id else [sites_table.c.id]
    query = select(list_to_select).where(sites_table.c.displayable_name == site_name)
    site_id_response = connection.execute(query).fetchall()[0]
    site_id = site_id_response[0]
    return site_id_response if include_legal_entity_id else site_id


def get_site_info(site_name, connection, db_table):
    select_statement = f'''
    SELECT *
    FROM
    sites
    WHERE displayable_name = '{site_name}'
    ORDER BY 1;
    '''
    sites_info = connection.execute(select_statement)
    sites_info = list(map(dict,sites_info))[0]
    sites_info = (sites_info['id'], sites_info['legal_entity'], sites_info['w-code'])
    return sites_info


def get_mms_data(site_id, year, month, connection, db_table, include_prev=False):

    if month == 1:
        prev_month = 12
        prev_year = year - 1
    else:
        prev_month = month - 1
        prev_year = year 

    mms_data_table = db_table

    query = select([mms_data_table.c.timestamps_utc,
                    mms_data_table.c.total_v1, mms_data_table.c.total_v2]).\
                    where(and_(mms_data_table.c.site == site_id, mms_data_table.c.year == year, 
                    mms_data_table.c.month == month))
    
    response = connection.execute(query).fetchall()[0]
    version_target = 'v2' if response[-1] else 'v1'
    mms_data_target = pd.DataFrame(response[-2 if version_target == 'v1' else -1], index=response[0], columns=['yield [kWh]'])  

    
    if include_prev:
        query = select([mms_data_table.c.timestamps_utc,
                        mms_data_table.c.total_v1, mms_data_table.c.total_v2]).\
                        where(and_(mms_data_table.c.site == site_id, mms_data_table.c.year == prev_year, 
                        mms_data_table.c.month == prev_month))
        response = connection.execute(query).fetchall()[0]
        version_prev = 'v2' if response[-1] else 'v1'
        mms_data_prev = pd.DataFrame(response[-2 if version_prev == 'v1' else -1], index=response[0], columns=['yield [kWh]'])  
    
        mms_data = pd.concat((mms_data_prev, mms_data_target), axis=0).sort_index()
    else:
        mms_data = mms_data_target
    
    return mms_data, version_target


def get_current_forecast_1d(site_id, date, time_index, table, connection):
    
    query = select([table.c.time_indexes_utc, table.c.energy, table.c.update_version])\
                .where(and_(table.c.date == date.date(), table.c.site == site_id, table.c.update_version > 0))\
                .order_by(asc(table.c.update_version))
    applied_forecasts = connection.execute(query).fetchall()

    versions = list()
    current_forecast = pd.Series(data=0, index=time_index)
    for index, forecast_update, version in applied_forecasts:
        forecast = pd.Series(data=forecast_update, index=index)
        current_forecast.update(forecast)
        versions.append(version)
    return current_forecast.multiply(1000).astype(int)


def get_current_forecast(site_id, dates, connection, db_table):
    daily_forecasts = list()
    for date in dates:
        time_index = get_time_index(date)
        daily_forecast = get_current_forecast_1d(site_id, date, time_index, db_table, connection)
        daily_forecasts.append(daily_forecast)
        # print(date, daily_forecast)
    forecast_data = pd.concat(daily_forecasts)
    return forecast_data


def get_applied_forecast_old(site_id, year, month, connection, db_table):
    base = dt.date(year=year, month=month, day=1)
    dates_range = [base + dt.timedelta(days=days) for days in range(calendar.monthrange(year, month)[-1])]

    forecasts_applied_table = db_table
    query = select([forecasts_applied_table.c.time_indexes_utc, forecasts_applied_table.c.energy], forecasts_applied_table.c.date.in_(dates_range)).\
                    where(and_(forecasts_applied_table.c.site == site_id, forecasts_applied_table.c.update_version == 0))
    response = connection.execute(query).fetchall()
    forecast_data = pd.DataFrame(columns=['forecast [kWh]'])
    for record in response:
        data = pd.DataFrame(record[1], index=record[0], columns=['forecast [kWh]'])
        forecast_data = pd.concat([forecast_data, data])
    forecast_data = forecast_data.sort_index()
    return forecast_data.multiply(1000).astype(int)


def get_applied_forecast(site_id, first_date, last_date, connection, db_table, 
                         forecast_type='forecast_applied_corrected'):
    forecasting_data_table = db_table
    forecasting_data_query = select([
        forecasting_data_table.c.data_type,
        forecasting_data_table.c.data_timestamp_utc,
        forecasting_data_table.c.data_value
        ])\
        .filter(forecasting_data_table.c.date >= first_date)\
        .filter(forecasting_data_table.c.date <= last_date)\
        .filter(forecasting_data_table.c.data_type == forecast_type)\
        .filter(forecasting_data_table.c.site_id == site_id)\
        .order_by(forecasting_data_table.c.data_timestamp_utc.asc())
    forecasting_data = connection.execute(forecasting_data_query).fetchall()
    applied_forecast = pd.DataFrame(
        data=[r.data_value for r in forecasting_data],
        index=[r.data_timestamp_utc for r in forecasting_data],
        columns=['forecast [kWh]'],
        dtype='float')
    applied_forecast = applied_forecast.astype(int)
    return applied_forecast


def get_fc_info(site_id, date, available_before=None, connection=None, db_table=None):

    #print(site_id, date, available_before)

    available_before = available_before if available_before else dt.datetime.utcnow()
    
    forecast_logbook = db_table

    query = select([forecast_logbook.c.supplier, forecast_logbook.c.file_name])\
                    .where(and_(forecast_logbook.c.site_id == site_id,
                                forecast_logbook.c.from_date <= date,
                                forecast_logbook.c.issued_utc < available_before))\
                    .order_by(desc('issued_utc')).limit(1)
    #print(query.compile(connection))
    supplier, filename = connection.execute(query).fetchall()[0]
    #print(sites_names)
    return supplier, filename


def change_ftp_dir(ftp, path, logger=None):
    ftp.cwd(path)
    if logger:
        logger.info('Working directory set as: {}'.format(ftp.pwd()))
    return None


def read_forecast_data(file_name, ftp, ftp_dir):
    #print(file_name, ftp.pwd(), ftp_dir, ftp)
    
    if ftp.pwd() != ftp_dir:
        change_ftp_dir(ftp, ftp_dir)

    #print(file_name, ftp.pwd(), ftp_dir, ftp)

    content = io.BytesIO()
    ftp.retrbinary(f'RETR {file_name}', content.write)
    content.seek(0)
    return content


def number_rows_to_skip(reader):
    rows_to_skip = 0
    for row in reader.readlines():    
        if row[:1] == b'#':
            rows_to_skip = rows_to_skip + 1
    reader.seek(0)
    return rows_to_skip


def get_solargis_daily_forecast(file_name, index, ftp, ftp_dir):

    reader = read_forecast_data(file_name, ftp, ftp_dir)
    rows_to_skip = number_rows_to_skip(reader)

    #logger.info('File opened. Skipping {} rows'.format(rows_to_skip))
    
    data = pd.read_csv(reader, skiprows=rows_to_skip, sep=';', parse_dates={'DateTime': ['Date', 'Time']}, 
                       dayfirst=True)
    
    data['PVOUT'] = data['PVOUT']
    data = data.set_index(pd.to_datetime(data['DateTime']))['PVOUT']
    #print(data)
    if data.index[0].minute != 30:
        #print('File format is wrong: {}'.format(file_name))
        data.index = data.index - dt.timedelta(minutes=30)
        #print(data.index)
        data.index = data.index.tz_localize(pytz.timezone('Etc/GMT-2')).tz_convert('utc').tz_localize(None)
        print(data.index)

    data = data.loc[index]
    data.name = 'forecast [kWh]'

    return data


def get_solargis_forecast(file_name, index, ftp, ftp_dir):    
  
    reader = read_forecast_data(file_name, ftp, ftp_dir)
    rows_to_skip = number_rows_to_skip(reader)

    #logger.info('File opened. Skipping {} rows'.format(rows_to_skip))
    
    data = pd.read_csv(reader, skiprows=rows_to_skip, sep=';', parse_dates={'DateTime': ['Date', 'Time']}, 
                       dayfirst=True)
    
    data['PVOUT'] = data['PVOUT'] / 1000
    data = data.set_index(pd.to_datetime(data['DateTime']))['PVOUT']
    #print(data)
    if data.index[0].minute != 30:
        #print('File format is wrong: {}'.format(file_name))
        data.index = data.index + dt.timedelta(minutes=30)
        #print(data.index)
        #data.index = data.index.tz_localize(pytz.timezone('Etc/GMT-3')).tz_convert('utc').tz_localize(None)
        #print(data.index)

    data = data.loc[index]
    
    return data


def get_base_loads(site_id, date, connection, table):
    base_loads = table
    query = '''
    SELECT DISTINCT ON (site)
        id, base_load, site, update_date
    FROM public.base_loads
    WHERE site = {site_id}
    AND update_date <= '{date}'
    ORDER BY site, update_date DESC; 
    '''.format(site_id=site_id, date=date.strftime('%Y-%m-%d'))
    # print(query)
    base_loads = connection.execute(query).fetchall()[0][1]
    return base_loads


def get_2dah_forecast(site_id, dates, connection, ftp):
    
    forecasts = list()
    for date in dates:     
        availability = dt.datetime(year=date.year, month=date.month, day=date.day, hour=22) - dt.timedelta(days=2)
        time_index = get_time_index(date)

        supplier, file_name = get_fc_info(site_id, time_index.min().date(), available_before=availability, connection=connection)
        forecast = get_solargis_forecast(file_name, time_index, ftp, FORECAST_FTP['working_dir'] + supplier)

        forecasts.append(forecast)
    forecast = pd.concat(forecasts, axis=0)
    return forecast


def get_availability(date, type):
    if type == '2dah':
        return [dt.datetime(year=date.year, month=date.month, day=date.day, hour=14, minute=15) - dt.timedelta(days=2)]
    if type == '1dah':
        return [dt.datetime(year=date.year, month=date.month, day=date.day, hour=14, minute=15) - dt.timedelta(days=1)]
    if type == 'basic':
        initial_time = dt.datetime(year=date.year, month=date.month, day=date.day, hour=22) - dt.timedelta(days=1)
        start = dt.datetime(year=date.year, month=date.month, day=date.day, hour=0, minute=40)
        end = dt.datetime(year=date.year, month=date.month, day=date.day, hour=18, minute=40)
        return [initial_time, *pd.date_range(start, end, freq='6H').to_pydatetime()] 
    if type == 'pro':
        initial_time = dt.datetime(year=date.year, month=date.month, day=date.day, hour=22, minute=15) - dt.timedelta(days=1)
        start = dt.datetime(year=date.year, month=date.month, day=date.day, hour=4, minute=10)
        end = dt.datetime(year=date.year, month=date.month, day=date.day, hour=19, minute=10)
        return [initial_time, *pd.date_range(start, end, freq='1H').to_pydatetime()]
    if type == 'restored':
        initial_time = dt.datetime(year=date.year, month=date.month, day=date.day, hour=22, minute=15) - dt.timedelta(days=1)
        start = dt.datetime(year=date.year, month=date.month, day=date.day, hour=3, minute=10)
        end = dt.datetime(year=date.year, month=date.month, day=date.day, hour=14, minute=10)
        return [initial_time, *pd.date_range(start, end, freq='1H').to_pydatetime()]
    if type == 'last':
        return [dt.datetime(year=date.year, month=date.month, day=date.day, hour=22)]


def get_forecast(site_id, dates, type, connection, metadata, timezone='utc', ftp=None):
    logbook_table = metadata.tables['forecast_logbook']
    base_loads_table = metadata.tables['base_loads']
    if ftp is None:
        ftp = make_ftp(FORECAST_FTP)
    print(type)
    forecasts = list()
    base_loads = get_base_loads(site_id, min(dates), connection, base_loads_table)
    for date in dates:
        print(date.date())
        time_index = get_time_index(date, timezone=timezone)
        availabilities = get_availability(date, type)
        #print(availabilities)
        daily_forecast_updates = dict()
        #daily_forecast = list()
        daily_forecast = pd.Series(index=time_index, data=-1000, name='forecast [kWh]')
        
        available_first = availabilities.pop(0)
        try:
            supplier, file_name = get_fc_info(site_id, time_index.min().date(), available_before=available_first, connection=connection, db_table=logbook_table)
            print(file_name)
            forecast = get_solargis_forecast(file_name, time_index, ftp, FORECAST_FTP['working_dir'] + supplier)
            base_load_mask = forecast <= 0
            forecast = forecast + base_load_mask * base_loads[date.month - 1] / 1000
            daily_forecast.update(forecast)
            daily_forecast_updates.update({available_first: forecast})

            for available in availabilities:
                available = available
                print(available)
                av_time_index = time_index[time_index > available + dt.timedelta(hours=3)]
                first_record_stamp = av_time_index.min().date()
                if pd.isnull(first_record_stamp):
                    continue
                supplier, file_name = get_fc_info(site_id, first_record_stamp, available_before=available, connection=connection, db_table=logbook_table)
                print(file_name)
                forecast = get_solargis_forecast(file_name, av_time_index, ftp, FORECAST_FTP['working_dir'] + supplier)
                
                base_load_mask = forecast <= 0
                forecast = forecast + base_load_mask * base_loads[date.month - 1] / 1000
                daily_forecast.update(forecast)
                daily_forecast_updates.update({available: forecast})
                #daily_forecast.append(forecast)
        except IndexError:
            continue

        #forecast = pd.concat(daily_forecast, axis=0)
        forecasts.append(daily_forecast)
    forecast = pd.concat(forecasts, axis=0)
    return forecast


def get_enercast_forecast(site_name, target_year, target_month, type='1_hah'):
    data_folder = 'data/enercast_forecast/{}-{:0>2}/{}/'.format(target_year, target_month, type)
    data_file = [file for file in os.listdir(data_folder) if os.path.isfile(os.path.join(data_folder, file)) and file[10:].split(' ')[0] == site_name][0]
    forecast = pd.read_csv(os.path.join(data_folder, data_file), skiprows=1, header=None, names=['start_time', 'end_time', 'forecast [kWh]'])
    forecast.index = pd.DatetimeIndex(data=forecast['start_time'])
    forecast.index.name = None
    forecast.drop(columns=['start_time', 'end_time'], inplace=True)
    forecast['forecast [kWh]'] = forecast['forecast [kWh]'] / 60 * 15
    forecast = forecast.resample('1H', loffset=dt.timedelta(minutes=30)).sum()
    try:
        forecast.index = forecast.index.tz_localize(pytz.timezone('europe/kiev')).tz_convert(pytz.utc).tz_localize(None)
    except pytz.exceptions.NonExistentTimeError as e:
        forecast = forecast.drop(dt.datetime.strptime(str(e), '%Y-%m-%d %H:%M:%S'))
        forecast.index = forecast.index.tz_localize(pytz.timezone('europe/kiev')).tz_convert(pytz.utc).tz_localize(None)
    forecast = forecast.squeeze()
    print(forecast)
    base = dt.date(year=target_year, month=target_month, day=1)
    dates_range = [base + dt.timedelta(days=days) for days in range(calendar.monthrange(target_year, target_month)[-1])]

    daily_forecast = [pd.Series(index=get_time_index(date, timezone='utc'), data=0, name='forecast [kWh]') for date in dates_range]
    daily_forecast = pd.concat(daily_forecast)
    daily_forecast.update(forecast)
    print(daily_forecast)
    return daily_forecast
    


def make_results(site_data, forecast_type, prices, index):
    #print(site_data)
    if forecast_type == 'real':
        data = site_data['real_forecast_data'].loc[site_data['real_forecast_data'].index.intersection(index)]
    elif forecast_type == 'limitation':
        data = site_data['limitation_forecast_data'].loc[site_data['limitation_forecast_data'].index.intersection(index)]
    elif forecast_type == 'naive':
        data = site_data['naive_forecast_data'].loc[site_data['naive_forecast_data'].index.intersection(index)]
    elif forecast_type == 'zero':
        data = site_data['zero_forecast_data'].loc[site_data['zero_forecast_data'].index.intersection(index)]
    elif forecast_type == '1_dah':
        data = site_data['1_dah_forecast_data'].loc[site_data['1_dah_forecast_data'].index.intersection(index)]
    elif forecast_type == 'pro':
        data = site_data['pro_forecast_data'].loc[site_data['pro_forecast_data'].index.intersection(index)]
    elif forecast_type == 'restored':
        data = site_data['restored_forecast_data'].loc[site_data['restored_forecast_data'].index.intersection(index)]
    elif forecast_type == 'restored_lim':
        data = site_data['restored_forecast_data_lim'].loc[site_data['restored_forecast_data_lim'].index.intersection(index)]
    elif forecast_type == 'enercast':
        data = site_data['enercast_forecast_data'].loc[site_data['enercast_forecast_data'].index.intersection(index)]
    elif forecast_type == 'decreased_70':
        data = site_data['decreased_70_forecast_data'].loc[site_data['decreased_70_forecast_data'].index.intersection(index)]
    elif forecast_type == 'decreased_60':
        data = site_data['decreased_60_forecast_data'].loc[site_data['decreased_60_forecast_data'].index.intersection(index)]
    elif forecast_type == 'decreased_50':
        data = site_data['decreased_50_forecast_data'].loc[site_data['decreased_50_forecast_data'].index.intersection(index)]
    elif forecast_type == 'decreased_40':
        data = site_data['decreased_40_forecast_data'].loc[site_data['decreased_40_forecast_data'].index.intersection(index)]
    elif forecast_type == 'decreased_30':
        data = site_data['decreased_30_forecast_data'].loc[site_data['decreased_30_forecast_data'].index.intersection(index)]
    elif forecast_type == 'decreased_20':
        data = site_data['decreased_20_forecast_data'].loc[site_data['decreased_20_forecast_data'].index.intersection(index)]
    elif forecast_type == 'decreased_10':
        data = site_data['decreased_10_forecast_data'].loc[site_data['decreased_10_forecast_data'].index.intersection(index)]
    elif forecast_type == 'increased_10':
        data = site_data['increased_10_forecast_data'].loc[site_data['increased_10_forecast_data'].index.intersection(index)]
    elif forecast_type == 'increased_20':
        data = site_data['increased_20_forecast_data'].loc[site_data['increased_20_forecast_data'].index.intersection(index)]
    elif forecast_type == 'increased_30':
        data = site_data['increased_30_forecast_data'].loc[site_data['increased_30_forecast_data'].index.intersection(index)]
    else:
        data = site_data[forecast_type].loc[site_data[forecast_type].index.intersection(index)]

    data = pd.concat([prices, data], axis=1, join='inner')

    if len(data.index) == 0:
        return None

    if len(data.index) != 24:
        print('{} index len is {}'.format(data.index.max().strftime('%Y-%m-%d'), len(data.index)))

    
        
    with np.errstate(divide='ignore'):
        green_tariff = site_data['green_tariff'][data.index.max().month] if type(site_data['green_tariff']) == type(dict()) else site_data['green_tariff']
        data['revenue [UAH]'] = data['yield [kWh]'] * green_tariff

        data['error_u [kWh]'] = data['yield [kWh]'] - data['forecast [kWh]']
        data['error_u [%]'] = data['error_u [kWh]'] / data['forecast [kWh]'] * 100

        excess_mask = data['error_u [kWh]'] >= 0
        shortage_mask = data['error_u [kWh]'] < 0

        data['error_u (excess) [kWh]'] = data['error_u [kWh]'] * excess_mask
        data['error_u (shortage) [kWh]'] = data['error_u [kWh]'] * shortage_mask

        data['alfa_u_mask'] = data['error_u [%]'].apply(abs) > 5.0

        data['cieq_641_rule (excess) [UAH]'] = data['error_u (excess) [kWh]'] * data['alfa_u_mask'] * \
                                        (data['dam'] - data['positive_unbalance']) 

        data['cieq_641_rule (shortage) [UAH]'] = data['error_u (shortage) [kWh]'] * data['alfa_u_mask'] * \
                                        (data['dam'] - data['negative_unbalance'])

        data['cieq_641_rule (net) [UAH]'] = data['cieq_641_rule (excess) [UAH]'] + data['cieq_641_rule (shortage) [UAH]']

        mask_641_1 = (data['imsp'] < data['dam']) & (data['error_u [kWh]'] > 0)
        mask_641_2 = (data['imsp'] > data['dam']) & (data['error_u [kWh]'] < 0)
        mask_641 = mask_641_1 | mask_641_2
        
        data['641_mask'] = mask_641 & data['alfa_u_mask']
        data['641_mask_positive'] = mask_641_1 & data['alfa_u_mask']
        data['641_mask_negative'] = mask_641_2 & data['alfa_u_mask']
        
        data['641_price'] = data['dam'] - data['imsp']
        data['cieq_641_rule* [UAH]'] = data['error_u [kWh]'] * data['641_price'] * data['641_mask'] * data['alfa_u_mask']
        data['cieq_641_rule_positive* [UAH]'] = data['error_u [kWh]'] * data['641_price'] * data['641_mask_positive'] * data['alfa_u_mask']
        data['cieq_641_rule_negative* [UAH]'] = data['error_u [kWh]'] * data['641_price'] * data['641_mask_negative'] * data['alfa_u_mask']

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
        
        result['green_tariff [UAH]'] = green_tariff
        result['revenue [UAH]'] = data['revenue [UAH]'].sum()

        result['error_u [kWh]'] = data['error_u [kWh]'].apply(abs).sum()
        result['error_u [%]'] = data['error_u [kWh]'].apply(abs).sum() / data['forecast [kWh]'].sum() * 100

        result['max_energy [kWh]'] = data['yield [kWh]'].max()
        result['max_forecast [kWh]'] = data['forecast [kWh]'].max()
        result['max_error [kWh]'] = max_error(data['yield [kWh]'], data['forecast [kWh]'])

        result['mean_absolute_error [kWh]'] = mean_absolute_error(data['yield [kWh]'], data['forecast [kWh]'])
        result['median_absolute_error [kWh]'] = median_absolute_error(data['yield [kWh]'], data['forecast [kWh]'])
        result['mean_square_error [kWh]'] = mean_squared_error(data['yield [kWh]'], data['forecast [kWh]'])
        result['root_mean_square_error [kWh]'] = sqrt(mean_squared_error(data['yield [kWh]'], data['forecast [kWh]']))
        result['R^2 score'] = r2_score(data['yield [kWh]'], data['forecast [kWh]'])

        result['dropped by alpha_u [records]'] = len(data['alfa_u_mask']) - data['alfa_u_mask'].sum()
        result['dropped by alpha_u [%]'] = 100 - data['alfa_u_mask'].sum() / len(data['alfa_u_mask']) * 100

        result['error_u (excess) [kWh]'] = data['error_u (excess) [kWh]'].sum()
        result['error_u (excess) [%]'] = data['error_u (excess) [kWh]'].sum() / data['forecast [kWh]'].sum() * 100
        result['error_u (shortage) [kWh]'] = data['error_u (shortage) [kWh]'].sum()
        result['error_u (shortage) [%]'] = data['error_u (shortage) [kWh]'].sum() / data['forecast [kWh]'].sum() * 100

        result['cieq_641_rule (excess) [UAH]'] = data['cieq_641_rule (excess) [UAH]'].sum()
        result['cieq_641_rule (excess) [%]'] = data['cieq_641_rule (excess) [UAH]'].sum() / \
                                                        data['revenue [UAH]'].sum() * 100
        result['cieq_641_rule (shortage) [UAH]'] = data['cieq_641_rule (shortage) [UAH]'].sum()
        result['cieq_641_rule (shortage) [%]'] = data['cieq_641_rule (shortage) [UAH]'].sum() / \
                                                        data['revenue [UAH]'].sum() * 100
        result['cieq_641_rule (net) [UAH]'] = data['cieq_641_rule (net) [UAH]'].sum()
        result['cieq_641_rule (net) [%]'] = data['cieq_641_rule (net) [UAH]'].sum() / \
                                                        data['revenue [UAH]'].sum() * 100

        result['imsp_avg_641_rule [UAH/MWh]'] = data['cieq_641_rule (net) [UAH]'].sum() / \
                                                        result['yielded [kWh]'] * 1000

        result['cieq_641_rule* [UAH]'] = data['cieq_641_rule* [UAH]'].sum()
        result['cieq_641_rule_positive* [UAH]'] = data['cieq_641_rule_positive* [UAH]'].sum()
        result['cieq_641_rule_negative* [UAH]'] = data['cieq_641_rule_negative* [UAH]'].sum()
        result['cieq_641_rule* [%]'] = data['cieq_641_rule* [UAH]'].sum() / data['revenue [UAH]'].sum() * 100
        result['cieq_641_rule_positive* [%]'] = data['cieq_641_rule_positive* [UAH]'].sum() / data['revenue [UAH]'].sum() * 100
        result['cieq_641_rule_negative* [%]'] = data['cieq_641_rule_negative* [UAH]'].sum() / data['revenue [UAH]'].sum() * 100

        result['imsp_avg_641_rule* [UAH/MWh]'] = data['cieq_641_rule* [UAH]'].sum() / \
                                                        result['yielded [kWh]'] * 1000

    # print(result)

    return pd.Series(result)


def make_forecasting_results(site_data, forecast_type, index):
    if forecast_type == 'real':
        data = site_data['real_forecast_data'].loc[site_data['real_forecast_data'].index.intersection(index)]
    elif forecast_type == 'limitation':
        data = site_data['limitation_forecast_data'].loc[site_data['limitation_forecast_data'].index.intersection(index)]
    elif forecast_type == 'naive':
        data = site_data['naive_forecast_data'].loc[site_data['naive_forecast_data'].index.intersection(index)]
    elif forecast_type == 'gpee':
        data = site_data['gpee_forecast_data'].loc[site_data['gpee_forecast_data'].index.intersection(index)]
    elif forecast_type == 'enercast':
        data = site_data['enercast_forecast_data'].loc[site_data['enercast_forecast_data'].index.intersection(index)]
    else:
        data = site_data[forecast_type].loc[site_data[forecast_type].index.intersection(index)]

    if len(data.index) == 0:
        return None

    data['revenue [EUR]'] = data['yield [kWh]'] * site_data['green_tariff']

    data['error_u [kWh]'] = data['yield [kWh]'] - data['forecast [kWh]']
    data['error_u [%]'] = data['error_u [kWh]'] / data['forecast [kWh]'] * 100

    excess_mask = data['error_u [kWh]'] >= 0
    shortage_mask = data['error_u [kWh]'] < 0

    data['error_u (excess) [kWh]'] = data['error_u [kWh]'] * excess_mask
    data['error_u (shortage) [kWh]'] = data['error_u [kWh]'] * shortage_mask

    data['alfa_u_mask'] = data['error_u [%]'].apply(abs) > 5.0

    result = dict()
    
    result['site'] = site_data['site']
    result['first_date'] = (data.index.min() + dt.timedelta(days=1)).date()
    result['last_date'] = data.index.max().date()
    result['number_of_values [records]'] = len(data.index)

    result['yield_data_version'] = site_data['mms_version']
    result['yielded [kWh]'] = data['yield [kWh]'].sum()

    result['forecast_type'] = forecast_type
    result['forecasted [kWh]'] = data['forecast [kWh]'].sum()
    
    result['green_tariff [EUR]'] = site_data['green_tariff']
    result['revenue [EUR]'] = data['revenue [EUR]'].sum()

    result['error_u [kWh]'] = data['error_u [kWh]'].apply(abs).sum()
    result['error_u [%]'] = data['error_u [kWh]'].apply(abs).sum() / data['forecast [kWh]'].sum() * 100

    result['max_energy [kWh]'] = data['yield [kWh]'].max()
    result['max_error [kWh]'] = max_error(data['yield [kWh]'], data['forecast [kWh]'])

    result['mean_absolute_error [kWh]'] = mean_absolute_error(data['yield [kWh]'], data['forecast [kWh]'])
    result['median_absolute_error [kWh]'] = median_absolute_error(data['yield [kWh]'], data['forecast [kWh]'])
    result['mean_square_error [kWh]'] = mean_squared_error(data['yield [kWh]'], data['forecast [kWh]'])
    result['root_mean_square_error [kWh]'] = sqrt(mean_squared_error(data['yield [kWh]'], data['forecast [kWh]']))
    result['R^2 score'] = r2_score(data['yield [kWh]'], data['forecast [kWh]'])

    result['dropped by alpha_u [records]'] = len(data['alfa_u_mask']) - data['alfa_u_mask'].sum()
    result['dropped by alpha_u [%]'] = 100 - data['alfa_u_mask'].sum() / len(data['alfa_u_mask']) * 100

    result['error_u (excess) [kWh]'] = data['error_u (excess) [kWh]'].sum()
    result['error_u (excess) [%]'] = data['error_u (excess) [kWh]'].sum() / data['forecast [kWh]'].sum() * 100
    result['error_u (shortage) [kWh]'] = data['error_u (shortage) [kWh]'].sum()
    result['error_u (shortage) [%]'] = data['error_u (shortage) [kWh]'].sum() / data['forecast [kWh]'].sum() * 100

    #print(result)

    return pd.Series(result)


def format_excel(writer, df):
    workbook = writer.book
    #worksheet_real = writer.sheets['results_real']
    #worksheet_naive = writer.sheets['results_naive']

    money_fmt = workbook.add_format({'num_format': '# ### ##0'})
    percent_fmt = workbook.add_format({'num_format': '0.0'})
    percent_bold_fmt = workbook.add_format({'num_format': '0.0', 'bold': True})
    category_fmt = workbook.add_format({'align': 'center'})
    title_fmt = workbook.add_format({'align': 'center', 'text_wrap': True,  'valign': 'center', 'bold': True})

    for worksheet in [writer.sheets[sheet] for sheet in writer.sheets]:
        worksheet.set_column('A:A', 4)
        worksheet.set_column('B:B', 14)
        worksheet.set_column('C:C', 10)
        worksheet.set_column('D:D', 10)
        worksheet.set_column('E:E', 10)
        worksheet.set_column('F:F', 10)
        worksheet.set_column('G:G', 8, category_fmt)
        worksheet.set_column('H:H', 8, money_fmt)
        worksheet.set_column('I:I', 8, category_fmt)
        worksheet.set_column('K:K', 8, money_fmt)
        worksheet.set_column('L:L', 8, money_fmt)
        worksheet.set_column('M:M', 8, money_fmt)
        worksheet.set_column('N:N', 8, percent_fmt)
        worksheet.set_column('O:T', 8, money_fmt)
        worksheet.set_column('U:U', 8, percent_fmt)
        worksheet.set_column('W:W', 8, percent_fmt)
        worksheet.set_column('X:X', 8, money_fmt)
        worksheet.set_column('Y:Y', 8, percent_fmt)
        worksheet.set_column('Z:Z', 8, money_fmt)
        worksheet.set_column('AA:AA', 8, percent_fmt)
        worksheet.set_column('AB:AB', 8, money_fmt)
        worksheet.set_column('AC:AC', 8, percent_fmt)
        worksheet.set_column('AD:AD', 8, money_fmt)
        worksheet.set_column('AE:AE', 8, percent_fmt)
        worksheet.set_column('AF:AF', 8, money_fmt)
        worksheet.set_column('AG:AG', 8, percent_fmt)
        worksheet.set_column('AH:AH', 8, percent_bold_fmt)
        worksheet.set_column('AI:AI', 8, money_fmt)
        worksheet.set_column('AJ:AJ', 8, percent_fmt)
        worksheet.set_column('AK:AK', 8, percent_bold_fmt)

        for col, value in enumerate(['index'] + list(df.columns.values)):
            worksheet.write(0, col, value, title_fmt)

    return writer


def save_results(results, results_names, target_year, target_month, results_type, site='', period=''):
    #print(results, results_names)
    if results_type == 'daily':
        writer = pd.ExcelWriter('results/{}-{:0>2}/sites_results/uce_daily_{}_{}_{}.xlsx'.format(target_year, target_month, 
                                site.lower(), target_year, target_month))
        for results, results_name in zip(results, results_names):
            
            results.to_excel(writer, results_name)
        format_excel(writer, results).save()
        return True

    if results_type == 'period':
        writer = pd.ExcelWriter('results/{}-{:0>2}/uce_period_{}_{}_{}.xlsx'.format(target_year, target_month, target_year, target_month, period))
        for results, results_name in zip(results, results_names): 
            results.to_excel(writer, results_name)
        format_excel(writer, results).save()
        return True

def get_gpee_data_files(target_period, root_folder):
    data_folder = root_folder + target_period + '/'
    data_files = [f for f in os.listdir(data_folder) if os.path.isfile(os.path.join(data_folder, f)) and f[-4:] == 'xlsx']
    gpee_files = dict()
    for data_file in data_files:
        site = data_file.split('_')[0].replace('-', '_')
        gpee_files.update({site: data_file})
    return gpee_files

def read_gpee_data(site_name, data_file, data_folder, sheet_name='Остаточний прогноз'):
    data = pd.read_excel(data_folder + data_file, sheet_name=sheet_name, header=None)
    data = data.drop(index=[0]).reset_index()
    dates = pd.to_datetime(data[1], dayfirst=True).tolist()
    indexes = [get_time_index(date).to_series() for date in dates]
    index = pd.concat(indexes, axis=0).index
    data = data.drop(columns=['index', 0, 1])
    data = data.stack().reset_index(drop=True).squeeze() * 1000
    data.name = 'forecast [kWh]'
    data.index = index
    return data

def get_gpee_final_forecast(site_name, target_period, data_folder='./data/forecasts/gpee/'):
    data_files = get_gpee_data_files(target_period, data_folder)
    if site_name not in data_files.keys():
        return None
    else:
        return read_gpee_data(site_name, data_files[site_name], data_folder + target_period + '/')

def get_gpee_final_forecast_cor(site_name, target_period, data_folder='./data/forecasts/gpee/'):
    data_files = get_gpee_data_files(target_period, data_folder)
    if site_name not in data_files.keys():
        return None
    else:
        return read_gpee_data(site_name, data_files[site_name], data_folder + target_period + '/', sheet_name='Остаточний прогноз з обмеженням')


if __name__ == '__main__':
    data = get_gpee_final_forecast('Afanasiivka', '2021-09_1-20')
    print(data.head(20))

