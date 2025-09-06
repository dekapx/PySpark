from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("First Spark application")
         .getOrCreate())

data = [("James", "Smith", "USA", "CA"),
        ("Michael", "Rose", "USA", "NY"),
        ("Robert", "Williams", "USA", "CA"),
        ("Maria", "Jones", "USA", "FL")
]

df = spark.createDataFrame(data)
df.printSchema()
df.show()