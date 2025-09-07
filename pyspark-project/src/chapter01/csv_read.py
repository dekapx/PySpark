from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit

spark = (SparkSession.builder
         .appName("PySparkDemo")
         .getOrCreate())

csvFile = 'src/chapter01/data.csv'
dataframe = (spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .option("mode", "PERMISSIVE")
             .csv(csvFile))
dataframe.printSchema()
dataframe.show()

dataframe = dataframe.withColumn('FullName', concat(
    col('first_name'), 
    lit(','), 
    col('last_name'))).select('FullName', 'age', 'city')
dataframe.printSchema()
dataframe.show()




