from duplicates.duplicates import drop_duplicates
from preprocessing.preprocess_args_me import *
from mylogging.mylogging import *
import os

path_dir = os.path.dirname(__file__)
setup_logging(path_dir+"../../../logs/drop-duplicates-ids.log")

path_source = get_source_path(dataset)
#drop_duplicates(path_source,'id')
preprocess_cleaned_id()
