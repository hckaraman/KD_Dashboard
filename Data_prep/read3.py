import os, re
import glob
import pandas as pd

dir = r"D:\Database\akış\at"
os.chdir(dir)
files = [f for f in glob.glob("*.csv")]

# for file in files:
# #
# #     new_file = file + '_new.csv'
# #     with open(file,'r') as f:
# #         with open(new_file,'w') as f1:
# #             next(f) # skip header line
# #             for line in f:
# #                 f1.write(line)

from glob import glob

with open('singleDataFile.csv', 'a') as singleFile:
    for csvFile in glob('*.csv'):
        for line in open(csvFile, 'r'):
            singleFile.write(line)


path = '/mnt/D/KD/Clima'
all_files = glob.glob(os.path.join(path, "*.csv"))     # advisable to use os.path.join as this makes concatenation OS independent

df_from_each_file = (pd.read_csv(f) for f in all_files)
concatenated_df   = pd.concat(df_from_each_file, ignore_index=True)
# doesn't create a list, nor does it append to one


from PyPDF2 import PdfFileMerger
import os

os.chdir('/media/cak/EA9CDC769CDC3EAF/Users/cagri/Documents/My Pictures/2020-12-04')

pdfs = ['Scan1.PDF','Scan10001.PDF']

merger = PdfFileMerger()

for pdf in pdfs:
    merger.append(pdf)

merger.write("result.pdf")
merger.close()
