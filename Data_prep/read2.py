import os,re
import glob
import pandas as pd


dir = r"D:\Database\akış"
os.chdir(dir)
files = [f for f in glob.glob("*.txt")]


for file in files:

    with open(file, 'r') as f:
        my_csv_text = f.read()

    find_str = ','
    replace_str = '.'
    my_csv_text = re.sub(find_str, replace_str, my_csv_text)
    # find_str = 'Havzası'
    # replace_str = '...'
    # my_csv_text = re.sub(find_str, replace_str, my_csv_text)
    find_str = ' '
    replace_str = ','
    my_csv_text = re.sub(find_str, replace_str, my_csv_text)
    find_str = '	'
    replace_str = ','
    my_csv_text = re.sub(find_str, replace_str, my_csv_text)
    find_str = ',Havzası,'
    replace_str = ','
    my_csv_text = re.sub(find_str, replace_str, my_csv_text)

    name = file + '.csv'
    with open(os.path.join(dir,name), 'w') as f:
        f.write(my_csv_text)



with open(r"D:\Database\sıcaklık\New folder\ort-sicaklik-Kızılırmak Havzası.txt.csv",'r') as f:
    with open(r"D:\Database\sıcaklık\New folder\updated_test.csv",'w') as f1:
        next(f) # skip header line
        for line in f:
            f1.write(line)

from glob import glob

with open('singleDataFile.csv', 'a') as singleFile:
    for csvFile in glob('*.csv'):
        next(csvFile)
        for line in open(csvFile, 'r'):
            singleFile.write(line)