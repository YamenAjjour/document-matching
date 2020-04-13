import os
import pandas as pd
import json

import re
from difflib import SequenceMatcher
dirname = os.path.dirname(__file__)
import logging
import csv
import tqdm

def preprocess(conclusion):

    conclusion=conclusion.lower()
    conclusion=re.sub('[;^&%$@#_+-.,.!$")()]', '', conclusion)
    conclusion=re.sub('this house', '', conclusion)
    return conclusion.strip()


def load_args_me():
    argument_map={}
    path_df_arguments_preprocessed = get_preprocessed_path('args-me')
    dataframe_arguments = pd.read_csv(path_df_arguments_preprocessed,encoding="utf-8",quotechar='"',sep="|",quoting=csv.QUOTE_ALL).dropna()
    conclusions = list(dataframe_arguments['conclusion'])
    premises = list(dataframe_arguments['premise'])
    ids = list(dataframe_arguments['argument_id'])

    for i,conclusion in enumerate(conclusions):
        conclusion=preprocess(conclusion)
        premise = preprocess(premises[i])
        argument=  conclusion + premise
        id = ids[i]
        argument_map[argument] = id
    return argument_map

def load_old_arguments():
    old_arguments_path = get_old_arguments_path()
    arguments_df = pd.read_csv(old_arguments_path)

    conclusions = list(arguments_df['Conclusion'])
    premises = list(arguments_df['Premise'])
    argument_id = list(arguments_df['Argument ID'])
    discussion_id = list(arguments_df['Discussion ID'])
    arguments = {}
    for i,conclusion in enumerate(conclusions):

        conclusion=preprocess(conclusion)
        premise = preprocess(premises[i])
        argument =  conclusion + premise
        id = str(discussion_id[i]) +" "+ str(argument_id[i])
        arguments[argument]=id
    return arguments

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def match_arguments_exact(arguments_map,old_arguments,exact_matches_file,not_found_file):
    exact_matches_file=open(exact_matches_file,'w')
    not_found_file=open(not_found_file,'w')
    count_all_arguments=float(len(old_arguments))
    for i,old_argument in enumerate(old_arguments):
        logging.warning("%f finished"%(i/count_all_arguments))
        if old_argument in arguments_map:
            exact_matches_file.write('%s\t%s\n'%(arguments_map[old_argument],old_arguments[old_argument]))
        else:
            not_found_file.write('%s\n'%old_arguments[old_argument])
            continue

def match_arguments(arguments_map,old_arguments,argument_ids_file,argument_matches_file):
    argument_ids_file = open(argument_ids_file,'r')
    argument_ids_to_match =[]
    for argument_id_to_match in argument_ids_file:
        argument_ids_to_match.append(argument_id_to_match.strip())

    matches_file=open(argument_matches_file,'w')

    count_all_arguments=float(len(argument_ids_to_match))
    for i,old_argument in tqdm.tqdm(enumerate(old_arguments)):
        if old_arguments[old_argument] in argument_ids_to_match:
            logging.warning("%f finished"%(i/count_all_arguments))
            for argument in arguments_map:
                if(similar(old_argument,argument)>0.8):
                    matches_file.write('%s\t%s\n'%(arguments_map[argument],old_arguments[old_argument]))
                continue

argument_map = load_args_me()
old_arguments = load_old_arguments()



argument_map = load_args_me()
old_arguments = load_old_arguments()
match_arguments(argument_map,old_arguments,'not-exact-matches-1.txt','matches-1.txt')
#match_arguments_exact(argument_map,old_arguments,'exact-matches.txt','not-exact-machted.txt')
