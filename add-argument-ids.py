import pandas as pd
from conf.configuration import *

path = get_rankings_path()
rankings_df = pd.reas_csv(path,encoding="utf-8",sep=",")

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

matches_1 = load_matches("matches-1.txt")
matches_2 = load_matches("matches-2.txt")
matches_3 = load_matches("matches-3.txt")
exact_matches = load_matches("argument_exact_matches.txt")
ids_map = {}


all_matches = []
all_matches.extend(matches_1)
all_matches.extend(matches_2)
all_matches.extend(matches_3)
all_matches.extend(exact_matches)
for old_arugment_id, args_id in all_matches:
    ids_map[old_arugment_id]=args_id

old_argument_ids= list( rankings_df['Argument ID'])
old_discussion_ids = list(rankings_df['Document ID'])
for i, argument_id in old_argument_ids:
    discussion_id = old_discussion_ids[i]
    id = str(int(argument_id))+" "+ discussion_id
    args_id = ids_map[id]
    print(args_id)
