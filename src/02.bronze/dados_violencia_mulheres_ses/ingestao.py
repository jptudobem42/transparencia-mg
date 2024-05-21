# Databricks notebook source
from pyspark.sql import SparkSession

import os
import sys
sys.path.insert(0, "../../../lib")

from ingestors import IngestionBronze

# COMMAND ----------

spark = SparkSession.builder.appName("IngestionBronze").getOrCreate()

area = 'dados_violencia_mulheres_ses'
api_url = f'https://dados.mg.gov.br/api/3/action/package_show?id={area}'
output_dir = f"/dbfs/mnt/datalake/transparencia-mg/bronze/{area}/data"
metadata_table = f'bronze.transparencia_mg.metadata_{area}'

bronze = IngestionBronze(api_url, spark, metadata_table)
bronze.get_and_save(output_dir)

# COMMAND ----------

df = spark.read.csv(f"/mnt/datalake/transparencia-mg/bronze/{area}/data", inferSchema=True, sep=';', header=True)
df.display()
