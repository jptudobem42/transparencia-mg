# Databricks notebook source
from pyspark.sql import SparkSession

import os
import sys
sys.path.insert(0, "../../../lib")

from processors import Processors

# COMMAND ----------

spark = SparkSession.builder.appName("ProcessBronze").getOrCreate()

area = "dados_violencia_mulheres_ses"
raw_dir = f"/mnt/datalake/transparencia-mg/raw/{area}/data"
bronze_table = f"bronze.transparencia_mg.{area}"

processors = Processors(spark, raw_dir, bronze_table)
processors.process_bronze()

# COMMAND ----------

display(spark.read.table(bronze_table))
