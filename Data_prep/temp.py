import os
import pandas as pd
from sqlalchemy import create_engine
from pyeto import thornthwaite, monthly_mean_daylight_hours, deg2rad
import numpy as np

engine = create_engine('postgresql://postgres:kalman@192.168.0.19:8888/climate')

query = """select * from "HES" h ;"""
all_hes = pd.read_sql(query, engine)

for hes in all_hes.iterrows():
    bid = hes[1]['bid']
    longtitude = hes[1]['longtitude']
    latitude = hes[1]['latitude']

    query = f"""select * from "MGM_Temp" mt where mt.gridno = ((SELECT mg.gridno  FROM "MGM_Grid" mg  ORDER BY mg.geom <-> ST_GeogFromText('POINT({longtitude} {latitude})')) LIMIT 1);"""

    df = pd.read_sql(query, engine)
    df = df.set_index(['yil', 'ay'])
    temp['at'] = df.apply(lambda row: thorn(row.name[0]), axis=1)

pd.Series([str(i) for i in range(1, 13)], index=['size_kb', 'size_mb', 'size_gb'])

ll = [str(i) for i in range(1, 13)]

at = np.array(mmdlh).reshape(-1, len(mmdlh))


def thorn(year, df):
    data = df.loc[df['yil'] == year]['hg45sck'].values
    lat = 37
    lat = deg2rad(lat)
    mmdlh = monthly_mean_daylight_hours(lat, year)
    evapo = thornthwaite(data, mmdlh)
    return evapo
    # df = pd.DataFrame(np.array(mmdlh).reshape(-1, len(mmdlh)), columns=[str(i) for i in range(1, 13)])


at = pd.DataFrame()
at['at'] = df.groupby('yil').apply(lambda x: thorn(x.name))


def doCalculation(df):
    groupCount = df.size
    groupSum = df['my_labels'].notnull().sum()
    return groupCount / groupSum


year = 2014
lat = deg2rad(57.1526)
mmdlh = monthly_mean_daylight_hours(lat, year)
monthly_t = [3.1, 3.5, 5.0, 6.7, 9.3, 12.1, 14.3, 14.1, 11.8, 8.9, 5.5, 3.8]
thornthwaite(monthly_t, mmdlh)

# tt = df[['ay', 'yil', 'hg45sck']]
years = df['yil'].unique()
years = years[:-1]
df = df.loc[df['yil'].isin(years)]
result = [thorn(i, df) for i in years]
result = np.array(result).flatten()
df['Evaporation'] = result

