import simplejson

import pandas as pd

from conf.configuration import *
import csv
import os
from mylogging.mylogging import *
from collections import namedtuple, defaultdict
import hashlib
import ijson
dataset='args-me-local'
import re
def clean(text):
    text = re.sub(r"https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+", " ", text)
    tokens= re.findall(r"\w+",text)
    return " ".join(tokens)

def entry2argument(entry):

    conclusion = entry['conclusion'].lower().replace("\t"," ")
    premise = entry['premises'][0]['text'].lower().replace("\t"," ")
    argument_id = entry['id']
    Argument = namedtuple('Argument', 'id conclusion premise hash url')
    argument_text = conclusion + " " + premise
    argument_text = clean(argument_text)
    if 'sourceUrl' in entry['context']:
        url = entry['context']['sourceUrl']
    else:
        url=None
    hash = hashlib.md5((argument_text).encode()).hexdigest()
    return Argument(argument_id,conclusion,premise,hash,url)


def preprocess_source():
    path_source = get_source_path(dataset)
    path_preprocessed =get_preprocessed_path(dataset)
    preprocess(path_source,path_preprocessed,save_duplicate_ids=True,save_duplicate_hash=False)

def preprocess_cleaned_id():
    path_cleaned=get_cleaned_path(dataset,'id')
    path_preprocessed_cleaned=get_cleaned_path(dataset,'id-preprocessed')
    preprocess(path_cleaned,path_preprocessed_cleaned,save_duplicate_ids=False, save_duplicate_hash=True)

def prepprocess_cleaned_hash():
    path_cleaned=get_cleaned_path(dataset,'hash')
    path_preprocessed_cleaned=get_cleaned_path(dataset,'hash-preprocessed')
    preprocess(path_cleaned,path_preprocessed_cleaned,save_duplicate_ids=False, save_duplicate_hash=False)




def generate_duplicated_hash_grouped(preprocessed_data_frame):
    duplicated_hash=preprocessed_data_frame[preprocessed_data_frame.duplicated('hash',keep=False)]
    duplicated_hash_groups=defaultdict(list)
    for hash, arguments_by_hash in duplicated_hash.groupby('hash'):
        for index,argument in arguments_by_hash.iterrows():
            id = argument['id']
            duplicated_hash_groups[hash].append(id)
    path_duplicated_hash= get_duplicated_path(dataset,'hash-grouped')
    with open(path_duplicated_hash,'w') as outfile:
        simplejson.dump(duplicated_hash_groups,outfile)

def preprocess(path_source,path_preprocessed,save_duplicate_ids,save_duplicate_hash):
    path_duplicated_ids= get_duplicated_path(dataset,'id')
    path_duplicated_hash= get_duplicated_path(dataset,'hash')
    all_parsed_arguments=[]
    for file in os.listdir(path_source):
        #todo remove following line
        if file.endswith("json"):
            path_dataset= os.path.join(path_source,file)
            log_message(path_dataset)
            with open(path_dataset,encoding='utf-8') as json_file:
                arguments= ijson.items(json_file,'arguments.item')
                parsed_arguments= [entry2argument(argument) for argument in arguments]
                all_parsed_arguments.extend(parsed_arguments)
    preprocessed_data_frame= pd.DataFrame({"conclusion":[parsed_argument.conclusion for parsed_argument in all_parsed_arguments],
                                                   "premise":[parsed_argument.premise for parsed_argument in all_parsed_arguments],
                                                   "id":[parsed_argument.id for parsed_argument in all_parsed_arguments],
                                                    "hash":[parsed_argument.hash for parsed_argument in all_parsed_arguments],
                                                    "url":[parsed_argument.url for parsed_argument in all_parsed_arguments]
                                           })
    preprocessed_data_frame[['id','hash']]
    duplicated_ids=preprocessed_data_frame[preprocessed_data_frame.duplicated('id')]
    generate_duplicated_hash_grouped(preprocessed_data_frame)
    duplicated_hash=preprocessed_data_frame[preprocessed_data_frame.duplicated('hash')]

    if save_duplicate_ids:
        duplicated_ids.to_csv(path_duplicated_ids,sep="\t",index=False)
    if save_duplicate_hash:
        duplicated_hash.to_csv(path_duplicated_hash,sep="\t",index=False)
    preprocessed_data_frame.to_csv(path_preprocessed,quotechar='"',sep="\t",quoting=csv.QUOTE_ALL,encoding="utf-8",index=False
                                           ,columns=['id','hash','conclusion','premise'])


