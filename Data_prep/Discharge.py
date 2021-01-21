import psycopg2
import shapely
from shapely import wkb
import geopandas as gpd
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import os, time, glob
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import concurrent.futures
from itertools import cycle, zip_longest

start = time.time()

global df_havza
df_havza = pd.read_csv('/mnt/c/Users/cagri/Desktop/KD/Clima/Basins/basins.csv')

global df
df = pd.read_pickle('/mnt/s/KD_Dashboard/Data_prep/data.pkl')

models = ['MPI-ESM-MR', 'CNRM-CM5', 'HadGEM2-ES']
senaryos = ['RCP4.5', 'RCP8.5']
years = [year for year in range(2015, 2017)]
havzas = list(df_havza.bid.values)
months = [m for m in range(1, 13)]

id = 1
rows_list = []


def zip_cycle(*iterables, empty_default=None):
    cycles = [cycle(i) for i in iterables]
    for _ in zip_longest(*iterables):
        yield tuple(next(i, empty_default) for i in cycles)


params = []

for i in zip_cycle(models, senaryos, havzas, years, months):
    params.append(i)


def get_basins(basin_id):
    temp = df_havza.loc[df_havza['bid'] == basin_id]
    return list(temp.objectid.values)


def wait(seconds):
    print(f'Waiting {seconds} seconds...')
    time.sleep(seconds)
    return f'Done'


def extract(model, senaryo, h_, y, m):
    basin_lists = get_basins(h_)
    data = df.loc[(df['model'] == model) & (df['senaryo'] == senaryo) & (df['yıl'] == y) & (df['ay'] == m) & (
        df['drenajalanno'].isin(basin_lists))]

    # disc = df.sum

    data = {'Havza': h_, 'Ay': m, 'Yil': y,
            'Discharge': data['toplam_akış'].sum(),
            'Model': model, 'Senaryo': senaryo}
    # rows_list.append(data)
    # end = time.time()
    # hours, rem = divmod(end - start, 3600)
    # minutes, seconds = divmod(rem, 60)
    print(h_, model, senaryo, y, m)
    # print("Elapsed Time : {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
    return data


# extract(*params[0])

def app_data(data):
    rows_list.append(data)


def start_processing():
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        # breakpoint()
        futures = []
        for par in params:
            futures.append(executor.submit(extract, *par))

        for future in concurrent.futures.as_completed(futures):
            print(future.result())


        # future_proc = {executor.submit(extract, *f): f for f in params}
        # for future in concurrent.futures.as_completed(future_proc):
        #     #     app_data(future.result())
        #     print(future)
        # results = executor.map(extract, *params)
        # for res in results:
        #     print(res)


def run():
    # result = map(extract, *params)
    for par in params:
        rows_list.append(extract(*par))


# df_all = pd.DataFrame(rows_list)
# df_all.to_csv('result.csv')
#
if __name__ == "__main__":
    start_processing()
    # print(len(rows_list))
    # run()
