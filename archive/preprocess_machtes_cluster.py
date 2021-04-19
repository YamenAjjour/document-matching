import pandas as pd
import os
import sparkpickle
matches_cluster = '/mnt/ceph/storage/data-in-progress/args-me-document-matching/2020-4-21/lukas-argument-matches.pkl'
matches_cluster_dataframe_path = '/mnt/ceph/storage/data-in-progress/args-me-document-matching/2020-4-21/lukas-argument-matches.csv'
def read_argument_id_matches():
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

def read_argument_id_matches_pickle():
    old_argument_ids = []
    new_argument_ids = []
    similarities = []
    for root,files,dirs in os.walk(matches_cluster):
        for dir in dirs:
            if 'part' in dir:
                with open(os.path.join(root,dir),'rb') as file:

                    file_old_argument_ids,file_new_argument_ids,file_similarities=zip(*sparkpickle.load(file))
                    old_argument_ids.extend(file_old_argument_ids)
                    new_argument_ids.extend(file_new_argument_ids)
                    similarities.extend(file_similarities)
    df=pd.DataFrame({"old-argument-id":old_argument_ids,"new-argument-id":new_argument_ids,'similarity':similarities})
    df.to_csv(matches_cluster_dataframe_path,encoding='utf-8',sep=",")
read_argument_id_matches_pickle()