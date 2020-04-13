import pandas as pd
import os
matches_cluster = '/mnt/disk1/args-me-matching/lukas-argument-matches.txt/'
matches_cluster_dataframe_path = '/mnt/disk1/args-me-matching/lukas-argument-matches.csv'
old_argument_ids = []
new_argument_ids = []
similarities = []
for root,files,dirs in os.walk(matches_cluster):
    for dir in dirs:
        with open(os.path.join(root,dir),'r') as f:
            for line in f:
                line_without_brackets= line[1:-2]
                tokens = line_without_brackets.split(',')
                old_argument_ids.append(tokens[0].replace('\'',''))
                new_argument_ids.append(tokens[1].replace('\'',''))
                similarities.append(tokens[2])
df=pd.DataFrame({"old-argument-id":old_argument_ids,"new-argument-id":new_argument_ids,'similarity':similarities})
df.to_csv(matches_cluster_dataframe_path,encoding='utf-8',sep=",")