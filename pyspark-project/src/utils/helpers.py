def load_data(spark, file_path, file_format="csv", options=None):
    if options is None:
        options = {}
    return spark.read.format(file_format).options(**options).load(file_path)

def save_data(dataframe, file_path, file_format="csv", options=None):
    if options is None:
        options = {}
    dataframe.write.format(file_format).options(**options).save(file_path)