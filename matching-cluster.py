from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import levenshtein
spark = SparkSession.builder.appName('sub-ques-tax').config('master','yarn').getOrCreate()
old_arguments = spark.read.format("csv").option("header", "true").option("delimiter",",").load("/user/befi8957/old-arguments.csv")
args_me_arguments  = spark.read.format("csv").option("header", "true").option("delimiter","|",).option('quote', '"').load("/user/befi8957/args-me.csv")

conclusions = old_arguments.select("Conclusion").rdd.map(lambda r: r[0]).collect()
premises =old_arguments.select("Premise").rdd.map(lambda r: r[0]).map(lambda r: r.lower()).collect()
argument_ids = old_arguments.select("Argument ID").rdd.map(lambda r: r[0]).flatMap(lambda x:x).collect()
discussion_ids = old_arguments.select("Discussion ID").rdd.map(lambda r: r[0]).flatMap(lambda x:x).collect()
newDF=old_arguments.join(args_me_arguments,levenshtein(old_arguments['Premise'], args_me_arguments['premise']) < 3)
newDF.show()
for premise in premises:
    print(premise)
    args_me_arguments[args_me_arguments['premise']==premise].show()