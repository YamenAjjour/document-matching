#docker exec -u befi8957 -it webis-datascience-image-befi8957 bash -l -c 'cd; exec /opt/spark/bin/spark-submit --deploy-mode cluster  /home/yamenajjour/git/args-me-matching/matching-cluster-lukas.py'
from pyspark.sql import SparkSession
import re
import pandas as pd

from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import levenshtein
spark = SparkSession.builder.appName('args-argument-matching').config('master','yarn').getOrCreate()
old_arguments_df = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("/user/befi8957/lukas-arguments.csv").na.drop()
args_me_arguments_df  = spark.read.format("csv").option("header", "true").option("delimiter", "\t", ).option('quote', '"').load("/user/befi8957/args-me.csv").na.drop()
from difflib import SequenceMatcher

def preprocess(text):

    text=text.lower()
    text=re.sub('[;^&%$@#_+-.,.!$")()]', '', text)
    text=re.sub('this house', '', text)
    return text.strip()


old_premises =old_arguments_df.select("Premise").rdd.map(lambda r: r[0])
old_arguments = old_premises
old_argument_ids = old_arguments_df.select("Argument ID").rdd.map(lambda r: r[0])
discussion_ids = old_arguments_df.select("Discussion ID").rdd.map(lambda r: r[0])
old_ids = old_argument_ids.zip(discussion_ids).map(lambda r:r[0] +" " + r[1])

old_arguments_pairs = old_ids.zip(old_arguments).filter(lambda r: isinstance(r[1],str)).filter(lambda r: r[1] != None).map(lambda r: (r[0],preprocess(r[1]))).collect()
old_arguments_broadcasted = spark.sparkContext.broadcast(old_arguments_pairs)

args_me_premises = args_me_arguments_df.select('premise').rdd.map(lambda r: r[0])

args_me_ids = args_me_arguments_df.select('argument_id').rdd.map(lambda r: r[0])
args_me_arguments = args_me_premises
args_me_arguments = args_me_ids.zip(args_me_arguments).filter(lambda r: isinstance(r[1],str)).filter(lambda r: r[1] != None).map(lambda r: (r[0],preprocess(r[1])))

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def find_match(argument):
    best_similarity=0
    id=-1
    for old_argument in old_arguments_broadcasted.value:
        similarity = similar(old_argument[1],argument[1])
        if similarity > best_similarity:
            id = old_argument[0]
            best_similarity=similarity
    return (id,argument[0],best_similarity)

argument_pairs = args_me_arguments.repartition(200).map(lambda argument:find_match(argument))
argument_pairs.saveAsPickleFile('/user/befi8957/lukas-argument-matches.pkl')
#exist.show()
#for premise in premises:
#    print(premise)
#    args_me_arguments[args_me_arguments['premise']==premise].show()
#argument_pairs_retrieved = argument_pairs.collect()
#arg_me_ids,argument_ids,similarties=unzip(*argument_pairs)
#similarity_df = pd.DataFrame({'args-me-id':args_me_ids,'argument-id':argument_ids,'similarity':similarties})
#similarity_df.to_csv("lukas-arguments-matches.csv",sep=",",encoding='utf-8')