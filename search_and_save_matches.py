import os
from conf.configuration import *
import pandas as pd
import csv
def load_args_me():
    args_argument_map={}
    path_df_arguments_preprocessed = get_preprocessed_path('args-me')
    dataframe_arguments = pd.read_csv(path_df_arguments_preprocessed,encoding="utf-8",quotechar='"',sep="|",quoting=csv.QUOTE_ALL).dropna()
    conclusions = list(dataframe_arguments['conclusion'])
    premises = list(dataframe_arguments['premise'])
    ids = list(dataframe_arguments['argument_id'])

    for i,conclusion in enumerate(conclusions):
        conclusion=conclusion
        premise = premises[i]
        argument=  conclusion + premise
        id = ids[i]
        args_argument_map[id] = argument
    return args_argument_map

def load_old_arguments():
    old_arguments_path = get_old_arguments_path()
    arguments_df = pd.read_csv(old_arguments_path)

    conclusions = list(arguments_df['Conclusion'])
    premises = list(arguments_df['Premise'])
    argument_id = list(arguments_df['Argument ID'])
    discussion_id = list(arguments_df['Discussion ID'])
    old_argument_map = {}
    for i,conclusion in enumerate(conclusions):

        conclusion=conclusion
        premise = premises[i]
        argument =  conclusion + " " + premise
        id = str(discussion_id[i]) +" "+ str(argument_id[i])
        old_argument_map[id]= argument
    return old_argument_map

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
    return zip(args_me_argument_ids,old_argument_ids)

def search_and_save_matches(matches_file,args_argument_map,old_argument_map):
    matches = load_matches(matches_file)
    matched_arguments_file = open("argument-"+matches_file,'w')
    for args_me_argument_id, old_argument_id in matches:
        args_argument = args_argument_map[args_me_argument_id]
        old_argument = old_argument_map[old_argument_id]
        matched_arguments_file.write(args_argument+"\n"+old_argument+"\n")
        matched_arguments_file.write("=================\n")

old_argument_map = load_old_arguments()
args_argument_map = load_args_me()
search_and_save_matches('exact-matches.txt',args_argument_map,old_argument_map)

