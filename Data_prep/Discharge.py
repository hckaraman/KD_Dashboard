import pandas as pd
import os, time, glob
import concurrent.futures
import logging
from itertools import cycle, zip_longest
import itertools

start = time.time()

folder = os.path.dirname(os.path.realpath(__file__))

global df_havza
df_havza = pd.read_csv(os.path.join(folder, 'basins.csv'))

global df
df = pd.read_csv(os.path.join(folder, 'data.csv'))

models = ['MPI-ESM-MR', 'CNRM-CM5', 'HadGEM2-ES']
senaryos = ['RCP4.5', 'RCP8.5']
years = [year for year in range(2015, 2090)]
havzas = list(df_havza.bid.values)
havzas = list(set(havzas))
months = [m for m in range(1, 13)]

p = [models, senaryos, havzas, years, months]

params = []
for element in itertools.product(*p):
    params.append(element)


rows_list = []

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(folder
                                         , "debug.log")),
        logging.StreamHandler()
    ]
)



def get_basins(basin_id):
    temp = df_havza.loc[df_havza['bid'] == basin_id]
    return list(temp.objectid.values)


def extract(model, senaryo, h_, y, m):
    basin_lists = get_basins(h_)
    data = df.loc[(df['model'] == model) & (df['senaryo'] == senaryo) & (df['yıl'] == y) & (df['ay'] == m) & (
        df['drenajalanno'].isin(basin_lists))]

    data = {'Havza': h_, 'Ay': m, 'Yil': y,
            'Discharge': data['toplam_akış'].sum(),
            'Model': model, 'Senaryo': senaryo}

    print(h_, model, senaryo, y, m)
    # logging.debug(h_, model, senaryo, y, m)
    name = f"{h_}_{model}_{senaryo}_{y}_{m}.csv"
    # try:
    #     res = pd.DataFrame([data])
    # except:
    #     res = None
    # print("Elapsed Time : {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
    return data


def app_data(data):
    rows_list.append(data)


def start_processing():
    with concurrent.futures.ProcessPoolExecutor(max_workers=16) as executor:
        # breakpoint()
        futures = []
        for par in params:
            futures.append(executor.submit(extract, *par))

        for future in concurrent.futures.as_completed(futures):
            # tt = pd.DataFrame(future.result()[0])
            # tt.to_csv(os.path.join(folder,'res',future.result()[1]))
            result = future.result()
            #
            # if result is not None:
            app_data(future.result())

            # print(result[0])

        # if future.done():

        # future_proc = {executor.submit(extract, *f): f for f in params}
        # for future in concurrent.futures.as_completed(future_proc):
        #     #     app_data(future.result())
        #     print(future)
        # results = executor.map(extract, *params)
        # for res in results:
        #     print(res)

    df_all = pd.DataFrame(rows_list)
    df_all.to_csv(os.path.join(folder,'res',"result.csv"))
    # print(rows_list)


def run():
    for par in params:
        rows_list.append(extract(*par))


if __name__ == "__main__":
    # param = params[0]
    # t = extract(*param)
    start_processing()
    # print(len(rows_list))
    # run()
