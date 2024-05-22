# Databricks notebook source
from pyspark.sql import SparkSession

import os
import sys
sys.path.insert(0, "../../../lib")

from collector import Raw

# COMMAND ----------

spark = SparkSession.builder.appName("Raw").getOrCreate()

area = 'dados_violencia_mulheres_ses'
api_url = f'https://dados.mg.gov.br/api/3/action/package_show?id={area}'
output_dir = f"/dbfs/mnt/datalake/transparencia-mg/raw/{area}/data"
metadata_table = f'raw.transparencia_mg.metadata_{area}'

raw = Raw(api_url, spark, metadata_table)
raw.get_and_save(output_dir)
