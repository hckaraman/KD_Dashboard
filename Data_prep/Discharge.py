import pandas as pd
import os, time, glob
import concurrent.futures
from itertools import cycle, zip_longest

start = time.time()

folder = os.path.dirname(os.path.realpath(__file__))

global df_havza
df_havza = pd.read_csv(os.path.join(folder, 'basins.csv'))

global df
df = pd.read_pickle(os.path.join(folder, 'data.pkl'))

models = ['MPI-ESM-MR', 'CNRM-CM5', 'HadGEM2-ES']
senaryos = ['RCP4.5', 'RCP8.5']
years = [year for year in range(2015, 2016)]
havzas = list(df_havza.bid.values)[0:12]
months = [m for m in range(1, 2)]

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
            app_data(future.result())

        if future.done():
            df_all = pd.DataFrame(rows_list)
            df_all.to_csv(os.path.join(folder, "result.csv"))
            executor.shutdown()
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
