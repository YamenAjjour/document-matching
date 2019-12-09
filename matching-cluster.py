from pyspark.sql import SparkSession
import re
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import levenshtein
spark = SparkSession.builder.appName('sub-ques-tax').config('master','yarn').getOrCreate()
old_arguments_df = spark.read.format("csv").option("header", "true").option("delimiter", ",").load("/user/befi8957/old-arguments.csv").na.drop()
args_me_arguments_df  = spark.read.format("csv").option("header", "true").option("delimiter", "|", ).option('quote', '"').load("/user/befi8957/args-me.csv").na.drop()
from difflib import SequenceMatcher

def preprocess(text):

    text=text.lower()
    text=re.sub('[;^&%$@#_+-.,.!$")()]', '', text)
    text=re.sub('this house', '', text)
    return text.strip()

old_conclusions = old_arguments_df.select("Conclusion").rdd.map(lambda r: r[0])
old_premises =old_arguments_df.select("Premise").rdd.map(lambda r: r[0])
old_arguments = old_conclusions.zip(old_premises).map(lambda r: r[0] + " " + r[1])
old_argument_ids = old_arguments_df.select("Argument ID").rdd.map(lambda r: r[0])
discussion_ids = old_arguments_df.select("Discussion ID").rdd.map(lambda r: r[0])
old_ids = old_argument_ids.zip(discussion_ids).map(lambda r:r[0] +" " + r[1])

old_arguments_pairs = old_ids.zip(old_arguments).filter(lambda r: isinstance(r[1],str)).filter(lambda r: r[1] != None).map(lambda r: (r[0],preprocess(r[1]))).collect()
old_arguments_broadcasted = spark.sparkContext.broadcast(old_arguments_pairs)

args_me_premises = args_me_arguments_df.select('premise').rdd.map(lambda r: r[0])
args_me_conclusions = args_me_arguments_df.select('conclusion').rdd.map(lambda r: r[0])

args_me_ids = args_me_arguments_df.select('argument_id').rdd.map(lambda r: r[0])
args_me_arguments = args_me_conclusions.zip(args_me_premises).map(lambda r: r[0] + " " + r[1])
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

argument_pairs = args_me_arguments.map(lambda argument:find_match(argument))
#exist.show()
#for premise in premises:
#    print(premise)
#    args_me_arguments[args_me_arguments['premise']==premise].show()
print(argument_pairs.collect())
argument_pairs.coalesce(1).saveAsTextFile("/user/befi8957/mathces3.txt")