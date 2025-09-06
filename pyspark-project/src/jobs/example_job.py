class ExampleJob:
    def __init__(self, spark):
        self.spark = spark

    def run(self):
        # Sample data processing job
        data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
        df = self.spark.createDataFrame(data, ["Name", "Value"])

        # Perform transformations
        df_transformed = df.groupBy("Name").sum("Value")

        # Show the results
        df_transformed.show()