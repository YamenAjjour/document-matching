import simplejson

from src.conf.configuration import *

dataset='args-me-local'
path_duplicated_hash= get_duplicated_path(dataset,'hash-grouped')
with open(path_duplicated_hash) as file:
    data=simplejson.load(file)
    hash_counter=0
    counter=0
    for hash in data:
        hash_counter = hash_counter+1
        for id in data[hash]:
            counter= counter+1
    print(f"{hash_counter} hashes exist with {counter} different ids")
