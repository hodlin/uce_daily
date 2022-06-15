import io, os
import pandas as pd
import datetime as dt
import pytz
import calendar
import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, and_
from sqlalchemy.sql.expression import func

def clean_file(file_name):
    output_file = io.StringIO()
    with open(file_name, encoding='utf8') as read_file:
        lines = read_file.readlines()
        for line in lines:
            line = list(line)
            date = line[:16]
            line[0:4] = date[6:10]
            line[4:8] = date[2:6]
            line[8:10] = date[0:2]
            #line[10] = '-'
            line[14] = '3'
            del line[16:24]
            line = ''.join(line)
            line = line.replace('.', '-')
            line = line.replace('\u00A0', '')
            line = line.replace('\u0412', '')
            #print(line)
            output_file.write(line)
    output_file.seek(0)
    #print(output_file.getvalue())
    return output_file


def get_record_index(site_id, date, table, connection):
    index = None

    query_1 = select([table.c.id, table.c.completed]).where(and_(table.c.site == site_id, table.c.date == date))
    record_to_update = connection.execute(query_1).fetchall()

    if record_to_update:
        return record_to_update[0][0], True
    else:
        max_index_response = connection.execute('SELECT MAX(id) FROM public.{0}'.format(table))
        index = list(max_index_response)[0][0]
        index = 1 if index is None else int(index) + 1
        return index, False


def get_site_id(site_name, connection):
    query = 'SELECT id FROM public.sites where displayable_name = \'{0}\''.format(site_name.title())
    site_id_response = connection.execute(query)
    site_id = list(site_id_response)[0][0]
    return site_id


def get_corrections_list(date, engine, metadata, type='='):
    Forecast_corrections = metadata.tables['forecast_corrections']
    Sites = metadata.tables['sites']
        
    Session = sessionmaker(bind=engine)
    session = Session()

    if type == '>=':
        response = session.query(Sites.c.displayable_name, Forecast_corrections)\
                        .filter(Forecast_corrections.c.site == Sites.c.id)\
                        .filter(Forecast_corrections.c.date >= date)\
                        .filter(Forecast_corrections.c.is_valid == True)\
                        .all()
    elif type == '=':
        response = session.query(Sites.c.displayable_name, Forecast_corrections)\
                        .filter(Forecast_corrections.c.site == Sites.c.id)\
                        .filter(Forecast_corrections.c.date == date)\
                        .filter(Forecast_corrections.c.is_valid == True)\
                        .all()
    else:
        response = None

    return response


def add_correction_(site, start_time, end_time, correction_type, factor=None, level=None, repeat=False,
                    session=None, Forecast_corrections=None, Sites=None):
    

    site_id = session.query(Sites).filter(Sites.c.displayable_name == site).scalar()

    if start_time.date() == end_time.date():
        
        if start_time.tzinfo is None:
                start_time.replace(tzinfo=pytz.timezone('europe/kiev'))
        if end_time.tzinfo is None:
                end_time.replace(tzinfo=pytz.timezone('europe/kiev'))
        
        #print(start_time)   
        start_time = start_time.astimezone(pytz.utc)
        #print(start_time)
        start_time.replace(tzinfo=None)
        end_time = end_time.astimezone(pytz.utc)
        end_time.replace(tzinfo=None)

        #print(start_time, end_time)

        max_id = session.query(func.max(Forecast_corrections.c.id)).scalar()
        record = {'id': max_id + 1, 'site': site_id, 'type': correction_type,
                  'factor': factor, 'level': level, 'date': start_time.date(), 
                  'from_time_utc': start_time.time(), 'to_time_utc': end_time.time(), 
                  'is_valid': True, 
                  'comment': 'Record crated at {}\n'.format(dt.datetime.now(dt.timezone.utc).isoformat())}
        print(record)

        insert_stm = Forecast_corrections.insert().values(**record)
        session.execute(insert_stm)
        session.commit()
        
    else:
        dates = pd.date_range(start_time.date(), end_time.date(), freq='1D').tolist()
        
        start_time_ = dt.datetime.combine(dates[0].date(), start_time.time())
        start_time_.replace(tzinfo=pytz.utc)
        end_time_ = dt.datetime.combine(dates[0].date(), end_time.time() if repeat else dt.time(hour=18))
        
        add_correction_(site, start_time_, end_time_,
                        correction_type, factor, level, repeat, session, Forecast_corrections, Sites)

        for date in dates[1:-1]:
            start_time_ = dt.datetime.combine(date, start_time.time() if repeat else dt.time(hour=6))
            end_time_ = dt.datetime.combine(date.date(), end_time.time() if repeat else dt.time(hour=18))
            add_correction_(site, start_time_, end_time_,
                        correction_type, factor, level, repeat, session, Forecast_corrections, Sites)
        
        start_time_ = dt.datetime.combine(dates[-1].date(), start_time.time() if repeat else dt.time(hour=6))
        end_time_ = dt.datetime.combine(dates[-1].date(), end_time.time())
        end_time_.replace(tzinfo=pytz.timezone('UTC'))

        add_correction_(site, start_time_, end_time_,
                        correction_type, factor, level, repeat, session, Forecast_corrections, Sites)

 

def add_correction(site, start_time, end_time, correction_type, factor=None, level=None, repeat=False, engine=None, metadata=None):
    Forecast_corrections = metadata.tables['forecast_corrections']
    Sites = metadata.tables['sites']
    
    Session = sessionmaker(bind=engine)
    session = Session()
    add_correction_(site, start_time, end_time, correction_type, factor, level, repeat, session, Forecast_corrections, Sites)
    session.close()
