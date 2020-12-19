import os
import pandas as pd
from sqlalchemy import create_engine
from pyeto import thornthwaite, monthly_mean_daylight_hours, deg2rad
import numpy as np
import itertools

# engine = create_engine('postgresql://postgres:kalman@192.168.0.19:8888/climate')
engine = create_engine('postgresql://postgres:kalman@46.197.216.155:8888/climate')


query = """select * from "HES" h ;"""
all_hes = pd.read_sql(query, engine)
n = len(all_hes)


def thorn(year, latitude, df, model):
    data = df.loc[df['yil'] == year][model].values
    # lat = 37
    lat = deg2rad(latitude)
    mmdlh = monthly_mean_daylight_hours(lat, year)
    evapo = thornthwaite(data, mmdlh)
    return evapo


def thorn_clima(year, latitude, df, model, senario):
    data = df.loc[(df['Yıl'] == year) & (df['Model'] == model) & (df['Senaryo'] == senario), 'Ortalama_Sıcaklık'].values
    # lat = 37
    if len(data) != 12:
        data = data[0:12]
    lat = deg2rad(latitude)
    mmdlh = monthly_mean_daylight_hours(lat, year)
    evapo = thornthwaite(data, mmdlh)
    return evapo


def export_MGM():
    models = ['hg45sck', 'hg85sck', 'mpi45sck', 'mpi85sck']
    df_result = pd.DataFrame()
    for i, hes in enumerate(all_hes.iterrows()):
        bid = hes[1]['bid']
        longtitude = hes[1]['longtitude']
        latitude = hes[1]['latitude']

        # query = f"""-- select * from "MGM_Temp" mt where mt.gridno = ((SELECT mg.gridno  FROM "MGM_Grid" mg  ORDER BY mg.geom <-> ST_GeogFromText('POINT({longtitude} {latitude})')) LIMIT 1);""" # for MGM Temp
        query = f"""select * from "Clima_Temp" ct where ct."Grid" = ((SELECT cg."Grid"  FROM "Clima_Grid" cg  ORDER BY cg.geom <-> ST_GeogFromText('POINT({longtitude} {latitude})')) LIMIT 1);"""

        df = pd.read_sql(query, engine)

        years = df['yil'].unique()
        years = years[:-1]
        df = df.loc[df['yil'].isin(years)]

        for model in models:
            result = [thorn(year, latitude, df, model) for year in years]
            result = np.array(result).flatten()
            df[model + '_evaporation'] = result
        df['bid'] = bid
        df_result = df_result.append(df)
        print(i, n)

    df_result.to_csv('/home/cak/Desktop/KD_Dashboard/Data_prep/mgm_evaporation.csv')


def export_Clima():
    df_result = pd.DataFrame()
    dublicate_grids = pd.DataFrame()
    senarios = ['RCP4.5', 'RCP8.5']
    models = ['HadGEM2-ES', 'MPI-ESM-MR', 'CNRM-CM5']
    AY = [i for i in range(1, 13)]

    for i, hes in enumerate(all_hes.iterrows()):
        bid = hes[1]['bid']
        longtitude = hes[1]['longtitude']
        latitude = hes[1]['latitude']

        # query = f"""-- select * from "MGM_Temp" mt where mt.gridno = ((SELECT mg.gridno  FROM "MGM_Grid" mg  ORDER BY mg.geom <-> ST_GeogFromText('POINT({longtitude} {latitude})')) LIMIT 1);""" # for MGM Temp
        query = f"""select * from "Clima_Temp" ct where ct."Grid" = ((SELECT cg."Grid"  FROM "Clima_Grid" cg  ORDER BY cg.geom <-> ST_GeogFromText('POINT({longtitude} {latitude})')) LIMIT 1);"""

        df = pd.read_sql(query, engine)

        years = df['Yıl'].unique()
        years.sort()
        years = years[:-1]
        df = df.loc[df['Yıl'].isin(years)]

        # check duplicate grids

        if len(df.Havza.unique()) != 1:
            tt = {'HES_ID': bid, 'Grid_ID': df['Grid'].unique()[0], 'Havza': list(df['Havza'].unique())}
            temp = pd.DataFrame([tt])
            dublicate_grids = dublicate_grids.append(temp)
            continue

        date = list(itertools.product(years, AY))
        df_temp = pd.DataFrame(date, columns=['Year', 'Month'])

        for model in models:
            for senario in senarios:
                result = [thorn_clima(year, latitude, df, model, senario) for year in years]
                result = np.array(result).flatten()

                df_temp[model + '_' + senario + '_evaporation'] = result
        df_temp['bid'] = bid
        df_result = df_result.append(df_temp)
        print(i, n)

    df_result.to_csv('/home/cak/Desktop/KD_Dashboard/Data_prep/clima_evaporation.csv')
    dublicate_grids.to_csv('/home/cak/Desktop/KD_Dashboard/Data_prep/dublicate_grids.csv')


export_Clima()
