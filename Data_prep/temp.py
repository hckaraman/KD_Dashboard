import os
import pandas as pd
from sqlalchemy import create_engine
from pyeto import thornthwaite, monthly_mean_daylight_hours, deg2rad
import numpy as np
import itertools

engine = create_engine('postgresql://postgres:kalman@192.168.0.19:8888/climate')

query = """select * from "HES" h ;"""
all_hes = pd.read_sql(query, engine)
n = len(all_hes)

Havza_id = ["B_0", "B_1", "B_2", "B_18", "B_42", "B_66", "B_77", "B_84", "B_98", "B_149", "B_152", "B_155", "B_156",
            "B_158",
            "B_164", "B_166", "B_186", "B_198", "B_221", "B_230", "B_236", "B_271", "B_285", "B_287", "B_300", "B_317"]

Havza_basin = ["Doğu Karadeniz", "Çoruh", "Doğu Karadeniz", "Çoruh", "Kızılırmak", "Doğu Karadeniz", "Yeşilırmak",
               "Dicle-Fırat", "Susurluk", "Doğu Karadeniz", "Antalya", "Seyhan", "Seyhan", "Seyhan", "Ceyhan", "Seyhan",
               "Seyhan", "Seyhan", "Batı Karadeniz", "Konya", "Seyhan", "Seyhan", "Doğu Karadeniz", "Batı Karadeniz",
               "Doğu Karadeniz", "Seyhan"]

havza_cnt = dict(zip(Havza_id, Havza_basin))


def thorn(year, latitude, df, model):
    data = df.loc[df['yil'] == year][model].values
    # lat = 37
    lat = deg2rad(latitude)
    mmdlh = monthly_mean_daylight_hours(lat, year)
    evapo = np.array(thornthwaite(data, mmdlh))
    return evapo


def thorn_clima(year, latitude, df, model, senario):
    data = df.loc[(df['Yıl'] == year) & (df['Model'] == model) & (df['Senaryo'] == senario), 'Ortalama_Sıcaklık'].values
    # lat = 37
    if len(data) != 12:
        data = data[0:12]
    lat = deg2rad(latitude)
    mmdlh = monthly_mean_daylight_hours(lat, year)
    evapo = np.array(thornthwaite(data, mmdlh))
    return evapo


def export_MGM():
    models = ['hg45sck', 'hg85sck', 'mpi45sck', 'mpi85sck']
    df_result = pd.DataFrame()
    for i, hes in enumerate(all_hes.iterrows()):
        bid = hes[1]['bid']
        longtitude = hes[1]['longtitude']
        latitude = hes[1]['latitude']

        query = f"""select * from "MGM_Temp" mt where mt.gridno = ((SELECT mg.gridno  FROM "MGM_Grid" mg  ORDER BY mg.geom <-> ST_GeogFromText('POINT({longtitude} {latitude})')) LIMIT 1);"""  # for MGM Temp
        # query = f"""select * from "MGM_Temp" ct where ct."Grid" = ((SELECT cg."Grid"  FROM "MGM_Grid" cg  ORDER BY cg.geom <-> ST_GeogFromText('POINT({longtitude} {latitude})')) LIMIT 1);"""

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

    df_result.to_csv('/mnt/s/KD_Dashboard/Data_prep/Data/mgm_evaporation.csv')


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

        # if len(df.Havza.unique()) != 1:
        #     tt = {'HES_ID': bid, 'Grid_ID': df['Grid'].unique()[0], 'Havza': list(df['Havza'].unique())}
        #     temp = pd.DataFrame([tt])
        #     dublicate_grids = dublicate_grids.append(temp)
        #     continue

        date = list(itertools.product(years, AY))
        df_temp = pd.DataFrame(date, columns=['Year', 'Month'])

        if bid in havza_cnt.keys():
            df = df.loc[df['Havza'] == havza_cnt[bid]]
            print(f'BID of {bid} is changed')
            if bid in ['B_84', 'B_149', 'B_300']:
                continue

        for model in models:
            for senario in senarios:
                result = [thorn_clima(year, latitude, df, model, senario) for year in years]
                result = np.array(result).flatten()

                df_temp[model + '_' + senario + '_evaporation'] = result
        df_temp['bid'] = bid
        df_result = df_result.append(df_temp)
        print(i, n)

    df_result.to_csv('/mnt/s/KD_Dashboard/Data_prep/Data/clima_evaporation.csv')
    dublicate_grids.to_csv('/mnt/s/KD_Dashboard/Data_prep/Data/dublicate_grids.csv')


# export_MGM()
export_Clima()
