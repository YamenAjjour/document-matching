import pandas as pd
from conf.configuration import *

path = get_rankings_path()
rankings_df = pd.read_csv(path,encoding="utf-8",sep=",")

def load_matches(matches_file):
    matches_file=open(matches_file,'r')
    args_me_argument_ids=[]
    old_argument_ids= []
    for line in matches_file:
        ids= line.split("\t")
        old_argument_id = ids[1].strip()
        args_me_argument_id = ids[0].strip()
        args_me_argument_ids.append(args_me_argument_id)
        old_argument_ids.append(old_argument_id)
    return zip(old_argument_ids,args_me_argument_ids)

matches_1 = load_matches("/mnt/disk1/args-me-matching/lukas-argument-best-matches.txt")

ids_map = {}


all_matches = []
all_matches.extend(matches_1)

for old_arugment_id, args_id in all_matches:
    ids_map[old_arugment_id]=args_id

old_argument_ids= list( rankings_df['Argument ID'])
old_discussion_ids = list(rankings_df['Discussion ID'])
args_ids = []
for i, argument_id in enumerate(old_argument_ids):
    discussion_id = old_discussion_ids[i]
    id = str(argument_id) +" "+ str(discussion_id)
    try:
        args_id = ids_map[id]
        args_ids.append(args_id)
    except Exception as error:
        args_ids.append("None")
rankings_df['args-id']=args_ids
rankings_df.to_csv("rankings-rags-ids.csv",sep=",",encoding='utf-8')
