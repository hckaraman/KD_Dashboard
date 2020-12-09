import os,re
import glob
import pandas as pd


dir = r"D:\OneDrive - metu.edu.tr\Projeler\KD\Data\TOPLAM AKIŞ\HADGEM2-ES & RCP8.5"
os.chdir(dir)
files = [f for f in glob.glob("*.csv")]


for file in files:

    with open(file, 'r') as f:
        my_csv_text = f.read()

    find_str = ','
    replace_str = '.'
    my_csv_text = re.sub(find_str, replace_str, my_csv_text)
    find_str = ';'
    replace_str = ','
    my_csv_text = re.sub(find_str, replace_str, my_csv_text)
    find_str = '"'
    replace_str = ''
    my_csv_text = re.sub(find_str, replace_str, my_csv_text)
    df = pd.DataFrame([x.split(',') for x in my_csv_text.split('\n')])
    print(file,',',df[2][4])