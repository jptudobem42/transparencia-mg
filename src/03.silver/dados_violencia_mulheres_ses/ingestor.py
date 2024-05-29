# Databricks notebook source
!pip install unidecode rapidfuzz --quiet

from pyspark.sql import SparkSession

import os
import sys
sys.path.insert(0, "../../../lib")

from processors import ProcessSilver

# COMMAND ----------

spark = SparkSession.builder.appName("ProcessSilver").getOrCreate()

area = "dados_violencia_mulheres_ses"
bronze_table = f"bronze.transparencia_mg.{area}"
silver_table = f"silver.transparencia_mg.{area}"

processors = ProcessSilver(spark, silver_table)
processors.process_silver(bronze_table)
