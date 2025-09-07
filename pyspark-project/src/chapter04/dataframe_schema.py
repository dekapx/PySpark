from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = (SparkSession.builder
         .appName("PySparkDemo")
         .getOrCreate())

# define schema
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("age", IntegerType(), True)
])

# create dataframe
data = [("James", "Smith", "USA", "CA", 30),
        ("Michael", "Rose", "USA", "NY", 40),
        ("Robert", "Williams", "USA", "CA", 50),
        ("Maria", "Jones", "USA", "FL", 60)]    
dataframe = spark.createDataFrame(data, schema=schema)
dataframe.printSchema()
dataframe.show()

csvFilePath = "src/chapter04/data/people.csv"
# write data to csv
dataframe.write.csv(csvFilePath, header=True)

# read data from csv
csv_df = spark.read.csv(csvFilePath, header=True, schema=schema)
csv_df.printSchema()
csv_df.show()