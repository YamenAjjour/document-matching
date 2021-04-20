from preprocessing.preprocess_args_me import *
from duplicates.duplicates import drop_duplicates
from mylogging.mylogging import *
setup_logging("../../logs/drop-duplicates-hash.log")

drop_duplicates('hash')
prepprocess_cleaned_hash()