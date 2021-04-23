import os
import pandas as pd

os.chdir('/mnt/v1/GTE/KD_Data/MGM_Data')

df_had_ref = pd.read_csv('HadGEM_Referans_donem.csv',sep=';')
df_had_future = pd.read_csv('HadGEM_Gelecek_Donem.csv',sep=';')
df_mpi_ref = pd.read_csv('MPI_Referans.csv',sep=';')
df_mpi_future = pd.read_csv('MPI_Gelecek_Donem.csv',sep=';')

# new_df = pd.merge(df_had_future, df_had_ref,  how='inner', left_on=['gridno','yil','ay'], right_on = ['B_c1','c2'])

# df_had = pd.concat([df_had_future, df_had_ref], axis=1, join="inner")
# df_mpi = pd.concat([df_mpi_future, df_mpi_ref], axis=1, join="inner")
# df = pd.concat([df_had_future, df_mpi_ref], axis=1, join="inner")
#
# df = pd.concat([df_had, df_mpi], axis=1, join="inner")
# df.to_csv('test.csv')

df3 = df_had_future.merge(df_mpi_future, on=['gridno','yil','ay'], how='inner')

df4 = df3.merge(df_had_ref,on=['gridno','ay'],how='inner')
df5 = df4.merge(df_mpi_ref,on=['gridno','ay'],how='inner')