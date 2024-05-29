# Databricks notebook source
#!pip install tqdm
import requests
import csv
from tqdm import tqdm
import time

# COMMAND ----------

def get_codigos_ibge():
    """
    Obtém os códigos IBGE dos municípios.
    """
    estado = "MG"
    url_municipios_mg = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{estado}/municipios"
    
    response = requests.get(url_municipios_mg)

    if response.status_code == 200:
        municipios = response.json()
        codigos_ibge = [municipio['id'] for municipio in municipios]
        return codigos_ibge
    else:
        print(f"Erro ao obter lista de municípios: {response.status_code}")
        return []

def get_lat_lon(codigos_ibge):
    """
    Obtém a latitude e longitude dos municípios usando os códigos IBGE.
    """
    url_base = "https://servicodados.ibge.gov.br/api/v1/bdg/municipio"
    file = "/dbfs/mnt/datalake/censo-ibge/data/output/lat_lon_cidades_ibge.csv"
    seconds = 2

    with open(file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)

        writer.writerow(["codigo_ibge", "latitude", "longitude"])

        for codigo_ibge in tqdm(codigos_ibge):
            url = f"{url_base}/{codigo_ibge}/estacoes"
            print(url)

            resposta = requests.get(url, time.sleep(seconds))

            if resposta.status_code == 200:
                dados_municipio = resposta.json()

                if dados_municipio: 
                    codigo_ibge = dados_municipio[0].get("municipio").get("geocodigo")
                    latitude = dados_municipio[0].get("latitude")
                    longitude = dados_municipio[0].get("longitude")

                    writer.writerow([codigo_ibge, latitude, longitude])
                else:
                    print(f"Nenhum dado encontrado para o município {codigo_ibge}")
            else:
                print(f"Erro ao consultar município {codigo_ibge}: {resposta.status_code}")

if __name__ == "__main__":
    codigos_ibge = get_codigos_ibge()
    
    if codigos_ibge:
        get_lat_lon(codigos_ibge)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([ \
    StructField("codigo_ibge",StringType(),True), \
    StructField("latitude",DoubleType(),True), \
    StructField("longitude",DoubleType(),True), \
  ])

df = spark.read.csv("/mnt/datalake/censo-ibge/data/output/lat_lon_cidades_ibge.csv", sep=',', header=True, schema=schema)
df.display()

# COMMAND ----------

table_gold = "gold.ibge.lat_lon_cidades_ibge"
df.write.mode("overwrite").saveAsTable(table_gold)
