import psycopg2
import shapely
from shapely import wkb
import geopandas as gpd
import datetime
from dateutil.relativedelta import relativedelta
import os

os.chdir(r"D:\Database\TEMP\HadGEM2-ES\RCP4.5")
con = psycopg2.connect("dbname='iklim' user='postgres' host='127.0.0.1' password='admin'")

date = datetime.datetime(2015, 1, 1)
# y = '2015'

for y in range(2015,2030):
    for m in range(1, 13):
        res = []
        cur = con.cursor()
        cur.execute(
            "select * from temp t2  where t2.yil = {} and t2.ay={} and t2.senaryo = 'RCP4.5' and t2.model = 'HadGEM2-ES' and t2.havza = 'Fırat-Dicle'".format(
                y, m))
        rows = cur.fetchall()

        rows_list = []
        for row in rows:
            data = {'id': row[0], 'Havza': row[1], 'Grid': row[2], 'Ay': row[3], 'Yil': row[4], 'Temp': row[5],
                    'Model': row[6], 'Senaryo': row[7], 'geometry': wkb.loads(row[10], hex=True)}
            rows_list.append(data)
            gdf = gpd.GeoDataFrame(rows_list, crs='epsg:4326').set_index('id')

        filename = date.strftime('%Y-%m-%d')
        date += relativedelta(months=1)
        gdf.to_file(filename + '.shp')
        print("{} done".format(filename))

con.commit()
cur.close()
con.close()

# wbt.idw_interpolation('2015-06-01.shp','Temp','2015-06-01.tif',use_z=False, weight=2.0, radius=None,min_points=None,cell_size=None)
