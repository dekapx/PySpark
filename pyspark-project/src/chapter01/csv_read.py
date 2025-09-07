from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("First Spark application")
         .getOrCreate())
csvFile = 'src/chapter01/data.csv'

dataframe = (spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(csvFile))
dataframe.printSchema()
dataframe.show()

