from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = (SparkSession.builder
         .appName("PySparkDemo")
         .getOrCreate())

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("age", IntegerType(), True)
])

data = [("James", "Smith", "USA", "CA", 30),
        ("Michael", "Rose", "USA", "NY", 40),
        ("Robert", "Williams", "USA", "CA", 50),
        ("Maria", "Jones", "USA", "FL", 60)]    
dataframe = spark.createDataFrame(data, schema=schema)
dataframe.printSchema()
dataframe.show()
