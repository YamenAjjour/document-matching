import os
source_label ="source"
sample_label ="sample"
preprocessed = "preprocessed"
duplicated_template="duplicated-%s"
cleaned_template="cleaned-%s"
dirname = os.path.dirname(__file__)


def get_dataset_conf_path(dataset_name):
    dataset_conf = dirname+("/%s.conf"%dataset_name)
    return dataset_conf

def get_ids_path(dataset_name):
    dataset_conf_path = get_dataset_conf_path(dataset_name)
    dataset_ids_path = get_property_value(dataset_conf_path,ids_label)
    return dataset_ids_path

def get_source_path(dataset_name):
    dataset_conf_path = get_dataset_conf_path(dataset_name)
    dataset_source_path = get_property_value(dataset_conf_path,source_label)
    return dataset_source_path

def get_sample_path(dataset_name):
    dataset_conf_path = get_dataset_conf_path(dataset_name)
    dataset_sample_path = get_property_value(dataset_conf_path,sample_label)
    return dataset_sample_path

def get_old_arguments_path():
    dataset_conf_path = get_dataset_conf_path('args-me')
    old_argument_path = get_property_value(dataset_conf_path,old_arguments_label)
    return old_argument_path

def get_rankings_path():
    dataset_conf_path = get_dataset_conf_path('args-me')
    rankings_path = get_property_value(dataset_conf_path,rankings_label)
    return rankings_path

def get_preprocessed_path(dataset_name):
    dataset_conf_path = get_dataset_conf_path(dataset_name)
    dataset_preprocessed_path = get_property_value(dataset_conf_path,preprocessed)
    return dataset_preprocessed_path

def get_duplicated_path(dataset_name, match_variable):
    dataset_conf_path = get_dataset_conf_path(dataset_name)
    duplicated= duplicated_template % match_variable
    duplicated_path = get_property_value(dataset_conf_path,duplicated)
    return duplicated_path

def get_cleaned_path(dataset_name, match_variable):
    dataset_conf_path = get_dataset_conf_path(dataset_name)
    cleaned= cleaned_template % match_variable
    cleaned_path=get_property_value(dataset_conf_path,cleaned)
    return cleaned_path

def get_property_value(dataset_conf_path,property_label):
    conf_file = open(dataset_conf_path,'r')
    for line in conf_file:
        label = line.split("=")[0].strip()
        value = line.split("=")[1].strip()
        if label == property_label:
            return value