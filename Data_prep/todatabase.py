#!/home/cak/Desktop/Jupyter-lumped-models/venv/bin/python
import os, glob
import pandas as pd
from sqlalchemy import create_engine

folder = '/media/cak/AT/Database/sıcaklık'
folder = '/mnt/D/KD/Clima'
files = glob.glob1(folder, '*.csv')
n = len(files)
engine = create_engine('postgresql://postgres:kalman@192.168.0.19:8888/climate')
file = files[0]
for i, file in enumerate(files):
    df = pd.read_csv(os.path.join(folder, file))
    df.to_sql("Temp_Clima", engine, index =  False, if_exists = 'append')
    print(i, file, n)
