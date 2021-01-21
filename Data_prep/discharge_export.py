import psycopg2
import shapely
from shapely import wkb
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
import datetime
from dateutil.relativedelta import relativedelta
import os, time, glob
import whitebox

wbt = whitebox.WhiteboxTools()

start = time.time()

res_dir = "/mnt/c/Users/cagri/Desktop/Temp/BB/res"
wbt.set_working_dir(res_dir)
os.chdir(res_dir)

engine = create_engine('postgresql://postgres:kalman@192.168.0.19:8888/climate')

query = """select * from "HES" h ;"""
all_hes = pd.read_sql(query, engine)
n = len(all_hes)

df_polygon = gpd.GeoDataFrame()

for i, hes in all_hes.iterrows():
    bid = hes.bid

    hes = pd.DataFrame([hes])
    gdf = gpd.GeoDataFrame(
        hes, geometry=gpd.points_from_xy(hes.longtitude, hes.latitude))
    gdf.to_file(os.path.join(res_dir, f'temp_{bid}.shp'))

    try:
        wbt.snap_pour_points(
            f'temp_{bid}.shp',
            "accum.tif",
            f'snapped_{bid}.shp',
            snap_dist=0.01,
        )

        wbt.watershed(
            "pointer.tif",
            f'snapped_{bid}.shp',
            f"watershed_{bid}.tif"
        )

        code = f"""gdal_polygonize.py /mnt/c/Users/cagri/Desktop/Temp/BB/res/watershed_{bid}.tif /mnt/c/Users/cagri/Desktop/Temp/BB/res/watershed_{bid}.shp -b 1 -f "ESRI Shapefile" OUTPUT DN"""
        os.system(code)

        df = gpd.read_file(os.path.join(res_dir, f"watershed_{bid}.shp"))
        df['bid'] = bid
        df_polygon = df_polygon.append(df)

        os.system(f"""rm /mnt/c/Users/cagri/Desktop/Temp/BB/res/temp*""")
        os.system(f"""rm /mnt/c/Users/cagri/Desktop/Temp/BB/res/snapped_* """)
        os.system(f"""rm /mnt/c/Users/cagri/Desktop/Temp/BB/res/watershed_* """)

    except:
        print(f"bid {bid} not avaliable ")

    print(i + 1, len(df_polygon))

df_polygon.to_file(os.path.join(res_dir, 'Hes_basins.shp'))
