from pyspark.sql.functions import col, when, to_date
from pyspark.sql.types import IntegerType
import pandas as pd
from rapidfuzz import fuzz
from rapidfuzz import process
from unidecode import unidecode

class ProcessBronze:
    def __init__(self, spark, raw_dir, bronze_table):
        self.spark = spark
        self.raw_dir = raw_dir
        self.bronze_table = bronze_table

    def process_bronze(self):
        df_bronze = self.spark.read.csv(self.raw_dir, sep=';', header=True, inferSchema=True)
        df_bronze.write.mode("overwrite").saveAsTable(self.bronze_table)

class ProcessSilver:
    def __init__(self, spark, silver_table):
        self.spark = spark
        self.silver_table = silver_table

    def process_silver(self, bronze_table):
        df_bronze = self.spark.read.table(bronze_table)

        df_silver_cleaned = (
            df_bronze
            .na.drop(subset=['ID_MN_RESI'])
            .filter(~(col('DT_NASC').isNull() & col('NU_IDADE_N').isNull()))
            .drop('DT_NASC')
            .fillna('Ignorado')
            .withColumn('NU_IDADE_N', col('NU_IDADE_N').cast(IntegerType()))
            .withColumn('DT_NOTIFIC', to_date(col('DT_NOTIFIC'), 'dd/MM/yyyy'))
            .orderBy(col('DT_NOTIFIC').desc())
        )

        columns = {'DT_NOTIFIC':'data_notificacao',
                   'NU_IDADE_N':'idade',
                   'CS_SEXO':'sexo',
                   'CS_RACA': 'raca_cor',
                   'ID_MN_RESI':'cidade_residencia',
                   'LOCAL_OCOR':'local_ocorrencia',
                   'OUT_VEZES':'aconteceu_outras_vezes',
                   'LES_AUTOP':'lesao_autoprovocada',
                   'VIOL_FISIC':'violencia_fisica',
                   'VIOL_PSICO':'violencia_psicologica',
                   'VIOL_SEXU':'violencia_sexual',
                   'NUM_ENVOLV':'numero_envolvidos',
                   'AUTOR_SEXO':'sexo_autor',
                   'ORIENT_SEX':'orientacao_sexual',
                   'IDENT_GEN':'identidade_genero'}

        for old_col, new_col in columns.items():
            df_silver_cleaned = df_silver_cleaned.withColumnRenamed(old_col, new_col)

        silver_df = df_silver_cleaned.toPandas()
        gold_df = self.spark.table("gold.ibge.populacao_estimada").toPandas()

        silver_df['cidade_residencia_normalized'] = silver_df['cidade_residencia'].apply(self.normalize_city_name)
        gold_df['city_normalized'] = gold_df['city'].apply(self.normalize_city_name)

        unique_cities = silver_df['cidade_residencia_normalized'].unique()
        city_choices = gold_df['city_normalized'].tolist()

        city_matches = {city: self.get_best_match(city, city_choices) for city in unique_cities}

        silver_df['city_match'] = silver_df['cidade_residencia_normalized'].map(city_matches)

        silver_df['year'] = pd.to_datetime(silver_df['data_notificacao']).dt.year

        gold_df['year'] = gold_df['year'].astype(int)

        gold_df_unique = gold_df.drop_duplicates(subset=['city_normalized', 'year'])
        merged_df = pd.merge(silver_df, gold_df_unique, how='left', left_on=['city_match', 'year'], right_on=['city_normalized', 'year'])

        selected_columns = [
            'data_notificacao', 'idade', 'sexo', 'raca_cor', 'cidade_residencia',
            'local_ocorrencia', 'aconteceu_outras_vezes', 'lesao_autoprovocada',
            'violencia_fisica', 'violencia_psicologica', 'violencia_sexual',
            'numero_envolvidos', 'sexo_autor', 'orientacao_sexual',
            'identidade_genero', 'estimated_population'
        ]
        column_renames = {'estimated_population': 'populacao_estimada'}

        df_final = merged_df[selected_columns].rename(columns=column_renames).dropna()

        # Convert back to Spark DataFrame
        df_final_spark = self.spark.createDataFrame(df_final)

        # Write the final DataFrame to the silver table
        df_final_spark.write.mode("overwrite").saveAsTable(self.silver_table)

    @staticmethod
    def normalize_city_name(name):
        name = unidecode(name)  # Remove acentuação
        name = name.lower()  # Converte para minúsculas
        name = name.strip()  # Remove espaços extras
        return name

    @staticmethod
    def get_best_match(city, choices):
        match = process.extractOne(city, choices, score_cutoff=90)
        return match[0] if match else None