import pyspark

from pyspark.sql.session import SparkSession

sc = SparkSession.builder.appName('aviva').getOrCreate()

sdf = sc.read.json('./data/input_data.json')