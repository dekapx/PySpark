from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("PySparkDemo")
         .getOrCreate())

jsonFile = 'src/chapter02/student-data.json'
dataframe = (spark.read
             .option("multiline", "true")
             .json(jsonFile))
dataframe.printSchema()
dataframe.show()

