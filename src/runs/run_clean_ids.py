from duplicates.duplicates import drop_duplicates
from preprocessing.preprocess_args_me import *
from mylogging.mylogging import *
setup_logging("../../logs/drop-duplicates-ids-text.log")
drop_duplicates('id')
preprocess_cleaned_id()
