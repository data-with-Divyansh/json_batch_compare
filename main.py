import json
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

os.environ['PYSPARK_PYTHON'] = r'C:\Users\Quantumn\AppData\Local\Programs\Python\Python310\python.exe'
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk1.8.0_202'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

print("================= PYSPARK Started =====================")
print()

############ Write a function to compare json strings in a columns ################

def compare_json(yvalue: str,value:str) -> bool:
    json1 = json.loads(yvalue)
    json2 = json.loads(value)
    return json1==json2

compare_json_udf = udf(compare_json, BooleanType()) # Register the function so that spark can use it on dataframes

# Reading first batch hence all are new records

df = spark.read.csv("batch1.csv",header="true").drop("batchid").withColumnRenamed("value","yvalue")
fdf = (df.withColumn("itr",lit(1))
      .withColumn("type",lit("new"))
      .drop("batchid","id")
      )
# fdf.show(truncate=False)
df.show(truncate=False)

df.coalesce(1).write.format("csv").mode("overwrite").save("output/batch1")

################  DAY 2 Batch 2 ######################

df2 = spark.read.csv("batch2.csv", header="true").drop("batchid")
df2.show(truncate=False)

joindf = df.join(df2,"id","full")
joindf.show(truncate=False)

# ========= DELETED RECORDS =============

delete_df = (joindf.filter("value is null").select("id","yvalue")
             .withColumnRenamed("yvalue","value")
             ) # Finding deleted records which got deleted

# ========= NEW RECORDS =============
new_records_df = joindf.filter("yvalue is null").select("id","value") # Finding new records which got added
new_records_df.show()

######### Using the compare_udf to compare json string values in yvalue and values columns
json_compare_df = (joindf.filter("yvalue is not null and value is not null")
                   .withColumn("isequal",compare_json_udf(col("yvalue"),col("value")))
                   )
json_compare_df.show(truncate=False)

############ WRITING THE OUTPUT RESULTS #################

print("========= DELETED RECORDS =============")
delete_df.show()
delete_df.coalesce(1).write.csv("output/batch2/deleted_records",mode="overwrite",header="true")

print("========= New/Updated RECORDS =============")
new_updated_df = json_compare_df.filter("isequal == false").select("id","value").union(new_records_df)
new_updated_df.show()
new_updated_df.coalesce(1).write.csv("output/batch2/new_updated_records",mode="overwrite",header="true")

print("========= Existing RECORDS =============")
existing_df = json_compare_df.filter("isequal == 'true'").select("id","value")
existing_df.show()
existing_df.coalesce(1).write.csv("output/batch2/existing_records",mode="overwrite",header="true")