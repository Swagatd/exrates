from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("ex processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the  json file from the HDFS
df = spark.read.json('hdfs://namenode:9000/ex/ex_rates.json')

# Drop the duplicated rows based on the base and other columns
ex_rates = df.select('base', 'rates.eur', 'rates.usd', 'rates.jpy', 'rates.cad', 'rates.nzd', 'rates.gbp','rates.ind') \
    .dropDuplicates() \
    .fillna(0, subset=['EUR', 'USD', 'JPY', 'CAD', 'GBP', 'NZD','IND'])

# Export the dataframe into the Hive table
ex_rates.write.mode("append").insertInto("ex_rates")