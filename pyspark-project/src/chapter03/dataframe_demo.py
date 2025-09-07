from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("PySparkDemo")
         .getOrCreate())

# Creating DataFrame with colmns and rows with headers
columns = ["firstname", "lastname", "country", "state"]
data = [("James", "Smith", "USA", "CA"),
        ("Michael", "Rose", "USA", "NY"),
        ("Robert", "Williams", "USA", "CA"),
        ("Maria", "Jones", "USA", "FL")]

dataframe = spark.createDataFrame(data, schema=columns)
dataframe.printSchema()
dataframe.show()