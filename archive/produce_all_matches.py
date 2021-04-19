import pandas as pd
import os

path_matches_cluster_dataframe = '/mnt/ceph/storage/data-in-progress/args-me-document-matching/2020-4-21/lukas-argument-matches.csv'
path_best_matches= '/mnt/ceph/storage/data-in-progress/args-me-document-matching/2020-4-21/lukas-best-argument-matches.txt'

df_matches = pd.read_csv(path_matches_cluster_dataframe,encoding='utf-8',sep=',')
df_matches['similarity']=df_matches['similarity'].astype(float)
with open(path_best_matches,'w') as best_matches:
    for old_argument_id, matches_df in df_matches.groupby('old-argument-id'):
        best_argument_id= matches_df['new-argument-id'].loc[matches_df['similarity'].idxmax()]
        best_matches.write("%s\t%s\n"%(best_argument_id,old_argument_id))

