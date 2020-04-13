import pandas as pd
import os

path_matches_cluster_dataframe = '/mnt/disk1/args-me-matching/lukas-argument-matches.csv'
path_best_matches= '/mnt/disk1/args-me-matching/lukas-argument-best-matches.txt'

df_matches = pd.read_csv(path_matches_cluster_dataframe,encoding='utf-8',sep=',')

with open(path_best_matches,'w') as best_matches:
    for old_argument_id, matches_df in df_matches.groupby('old-argument-id'):
        best_argument_id= matches_df['new-argument-id'].loc[matches_df['similarity'].idxmax()]
        best_matches.write("%s\t%s\n"%(best_argument_id,old_argument_id))

