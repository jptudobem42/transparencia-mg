# Databricks notebook source
silver_table = "silver.transparencia_mg.dados_violencia_mulheres_ses"
silver_table = spark.table(silver_table)

# COMMAND ----------

from pyspark.sql.functions import when, col

gold_df = silver_table.withColumn(
    "local_ocorrencia",
    when(col("local_ocorrencia") == "Residencia", "Na residência")
    .otherwise("Fora da residência")
)

gold_df = gold_df.withColumn(
    "faixa_etaria",
    when(col("idade").between(0, 14), "0 a 14 anos")
    .when(col("idade").between(15, 24), "15 a 24 anos")
    .when(col("idade").between(25, 29), "25 a 29 anos")
    .when(col("idade").between(30, 40), "30 a 40 anos")
    .when(col("idade").between(41, 50), "41 a 50 anos")
    .when(col("idade").between(51, 60), "51 a 60 anos")
    .when(col("idade").between(61, 70), "61 a 70 anos")
    .otherwise("71 anos ou mais")
).drop("idade")

gold_df = gold_df.withColumn(
    "raca_cor",
    when(col("raca_cor").isin("Preta", "Parda"), "Negras")
    .when(col("raca_cor").isin("Branca", "Amarela", "Indígena"), "Não Negras")
    .otherwise(col("raca_cor"))
)

gold_df = gold_df.withColumn(
    "aconteceu_outras_vezes",
    when(col("aconteceu_outras_vezes") == "Sim", "Reincidente")
    .when(col("aconteceu_outras_vezes") == "Não", "Primeira vez")
    .otherwise(col("aconteceu_outras_vezes"))
)

# COMMAND ----------

gold_df.display()

# COMMAND ----------

gold_table = "gold.transparencia_mg.dados_violencia_mulheres_ses"
gold_df.write.mode("overwrite").saveAsTable(gold_table)
