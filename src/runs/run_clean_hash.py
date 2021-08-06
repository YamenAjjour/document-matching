from preprocessing.preprocess_args_me import *
from duplicates.duplicates import drop_duplicates
from mylogging.mylogging import *

path_dir = os.path.dirname(__file__)
setup_logging(path_dir+"../../../logs/drop-duplicates-hash.log")

path_cleaned=get_cleaned_path(dataset,'id')
drop_duplicates(path_cleaned,'hash',False)
prepprocess_cleaned_hash()