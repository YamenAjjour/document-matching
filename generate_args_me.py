from conf.configuration import *
import json
import logging
logging.buildConfigu(filename="generate-args-me.log",level=logging.DEBUG)
import os
path_source_args_me=get_source_path("args-me")
print(path_source_args_me)
root_path="/mnt/ceph/storage/data-in-progress/args-me/2020-04-11/"
portals=['debateorg','debatepedia','debatewise','idebate']
all_data=[]
for portal in portals:

    path=root_path+portal+".json"
    logging.warning("loading up %s"%path)
    with open(path) as json_file:
        data = json.load(json_file)
        all_data.extend(data)
logging.warning("writing %s"%path_source_args_me)
with open(path_source_args_me,'w') as outfile:
    json.dump(data,outfile)