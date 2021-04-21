from duplicates.duplicates import drop_duplicates
from preprocessing.preprocess_args_me import *
from mylogging.mylogging import *
setup_logging("../../logs/drop-duplicates-ids-text.log")

path_source = get_source_path(dataset)
drop_duplicates(path_source,'id')
preprocess_cleaned_id()
