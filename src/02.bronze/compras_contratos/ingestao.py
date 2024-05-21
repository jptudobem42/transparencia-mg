# Databricks notebook source
import sys
sys.path.insert(0, '..\..\..\lib')
from ingestors import IngestionBronze

# COMMAND ----------

area = 'compras_contratos'
api_url = f'https://dados.mg.gov.br/api/3/action/package_show?id={area}'
output_dir = f"/dbfs/mnt/datalake/transparencia-mg/{area}/data"

bronze = IngestionBronze(api_url)
bronze.get_and_save(output_dir)
