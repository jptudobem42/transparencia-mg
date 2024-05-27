# Databricks notebook source
from pyspark.sql import SparkSession

import os
import sys
sys.path.insert(0, "../../../lib")

from collector import Collector

# COMMAND ----------

spark = SparkSession.builder.appName("Collector").getOrCreate()

area = 'dados_violencia_mulheres_ses'
api_url = f'https://dados.mg.gov.br/api/3/action/package_show?id={area}'
output_dir = f"/dbfs/mnt/datalake/transparencia-mg/raw/{area}/data"
metadata_table = f'raw.transparencia_mg.metadata_{area}'

collect = Collector(api_url, spark, metadata_table)
collect.get_and_save(output_dir)
