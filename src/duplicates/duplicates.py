import ijson
from mylogging.mylogging import *
from conf.configuration import *
from preprocessing.preprocess_args_me import entry2argument
import simplejson
import pandas as pd
import hashlib
import os
import random
#import matplotlib
def argument_id_exists(argument, argument_ids_to_drop):
    argument_id_exists = entry2argument(argument).id in argument_ids_to_drop
    return argument_id_exists

def save_duplicates_hash(match_variable):

    path_preprocessed =get_preprocessed_path('args-me')
    path_duplicated=get_duplicated_path('args-me',match_variable)

    df_duplicated = pd.read_csv(path_duplicated,sep='\t')
    df_duplicated = df_duplicated[['id']]
    df_preprocessed_arguments = pd.read_csv(path_preprocessed,sep='\t')
    df_duplicates=df_preprocessed_arguments.merge(df_duplicated,on='id')
    df_aggregated_duplicates=df_duplicates.groupby('id').agg({'hash':'nunique'}).rename(columns={"hash":"argument_count"})
    print(df_aggregated_duplicates['argument_count'].value_counts())
    df_preprocessed_arguments.to_csv(path_duplicates,sep="\t",index=False)


def get_duplicate_arguments_are_not_identical(duplicated_arguments):
    ids,argument_dict_hashes=zip(*duplicated_arguments)
    df=pd.DataFrame({'id':ids,'hash':argument_dict_hashes})
    df=df.groupby('id').agg(lambda x:list(x))
    df.info()
    ids=[]
    for id,record in df.iterrows():
        for argument_1 in record['hash']:
            for argument_2 in record['hash']:
                if argument_1!=argument_2:
                    ids.append(id)
    return ids

def hash_str(str):
    return hashlib.md5(str.encode()).hexdigest()

def drop_duplicates(match_variable,log_arguments_text=False):

    path_source = get_source_path('args-me')
    path_duplicated=get_duplicated_path('args-me',match_variable)
    path_cleaned=get_cleaned_path('args-me',match_variable)
    df_duplicated = pd.read_csv(path_duplicated,sep='\t')
    argument_ids_to_drop = df_duplicated['id'].values
    count_arguments_dropped=0
    count_arguments_kept=0
    for file in os.listdir(path_source):
        if  file.endswith("json"):
            log_message(f"droping duplicate {match_variable}s from {file}")
            path_dataset= os.path.join(path_source,file)

            with open(path_dataset,encoding='utf-8') as json_file:
                arguments= ijson.items(json_file,'arguments.item')
                kept_argument_ids=set()
                cleaned_arguments=[]
                duplicated_arguments=[]
                for argument in arguments:
                    if argument_id_exists(argument,argument_ids_to_drop):
                        parsed_argument= entry2argument(argument)
                        argument_id = parsed_argument.id
                        if argument_id in kept_argument_ids:
                            log_message(f"dropping {argument_id}")
                            count_arguments_dropped=count_arguments_dropped+1
                            if log_arguments_text:
                                log_message(simplejson.dumps(argument,indent=4,sort_keys=True))
                            duplicated_arguments.append((argument_id,argument))
                            continue
                        else:
                            log_message(f"keeping {argument_id}")
                            count_arguments_kept=count_arguments_kept+1
                            if log_arguments_text:
                                log_message(simplejson.dumps(argument,indent=4,sort_keys=True))
                            kept_argument_ids.add(argument_id)
                    cleaned_arguments.append(argument)

                if len(duplicated_arguments)>0:
                    ids=get_duplicate_arguments_are_not_identical(duplicated_arguments)
                    print(list(set(ids)))
                path_cleaned_dataset=os.path.join(path_cleaned,file)
                log_message(path_cleaned_dataset)
                with open(path_cleaned_dataset,'w') as json_file_cleaned:
                    data={}
                    data['arguments']=cleaned_arguments
                    simplejson.dump(data,json_file_cleaned)
            log_message(f"{count_arguments_dropped} drooped and {count_arguments_kept} kept")

def save_arguments(ids):
    path_source = get_source_path('args-me')
    for file in os.listdir(path_source):
        if  file.endswith("json"):
            path_dataset= os.path.join(path_source,file)
            with open(path_dataset,encoding='utf-8') as json_file:
                arguments= ijson.items(json_file,'arguments.item')
                filtered_arguments={entry2argument(argument).id:argument for argument in arguments if entry2argument(argument).id in ids}
                for id in filtered_arguments:
                    seed=random.randint(0,100)
                    s= f"../../tmp/{id}-{seed}.json"
                    with open(s,'w') as argument_file:
                        simplejson.dump(filtered_arguments[id],argument_file,indent=4, sort_keys=True)
