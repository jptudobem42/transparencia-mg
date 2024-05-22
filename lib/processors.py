class Processors:
    def __init__(self, spark, raw_dir, bronze_table):
        self.spark = spark
        self.raw_dir = raw_dir
        self.bronze_table = bronze_table

    def process_bronze(self):
        df_raw = self.spark.read.csv(self.raw_dir, sep=';', header=True, inferSchema=True)
        return df_raw.write.mode("overwrite").saveAsTable(self.bronze_table)