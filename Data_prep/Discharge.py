import psycopg2
import shapely
from shapely import wkb
import geopandas as gpd
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import os, time,glob

start = time.time()

res_dir = r"D:\Database\Discharge"
poly_dir = r"D:\Database\Discharge\Basins_Polygon"
con = psycopg2.connect("dbname='iklim' user='postgres' host='127.0.0.1' password='admin'")

date = datetime.datetime(2015, 1, 1)
# y = '2015'
senaryo = 'RCP4.5'
model = 'MPI-ESM-MR'

havzas = glob.glob1(poly_dir,'*.shp')


models = ['MPI-ESM-MR', 'CNRM-CM5', 'HadGEM2-ES']
senaryos = ['RCP4.5', 'RCP8.5']

id = 1
rows_list = []

for model in models:
    for senaryo in senaryos:
        for h in havzas:
            havza = h.strip(".shp")
            for y in range(2015, 2017):
                for m in range(1, 13):
                    res = []
                    cur = con.cursor()
                    cur.execute(
                        "select sum(d2.discharge) from discharge d2,havza h2 where d2.yil = {} and d2.ay={} and d2.senaryo = '{}' and d2.model = '{}' and d2.havza = 'Fırat-Dicle' and d2.drenajno = h2.objectid and d2.drenajno IN( SELECT h2.objectid FROM havza h2 JOIN basin_polygon_name_4326 bpn ON ST_Intersects(ST_Buffer(bpn.geom,-0.02), h2.geom) WHERE bpn.rteno = '{}')".format(
                            y, m, senaryo, model, havza))
                    rows = cur.fetchall()

                    for row in rows:
                        data = {'id': id, 'Havza': 'Firat-Dicle', 'Drenaj_No': havza, 'Ay': m, 'Yil': y,
                                'Discharge': rows[0][-1],
                                'Model': model, 'Senaryo': senaryo}
                        rows_list.append(data)
                    print(id, m, y, senaryo, model,havza)
                    id += 1

con.commit()
cur.close()
con.close()

df = pd.DataFrame(rows_list)
df.to_csv(os.path.join(res_dir, 'result.csv'))

end = time.time()
hours, rem = divmod(end - start, 3600)
minutes, seconds = divmod(rem, 60)
print("Elapsed Time : {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
