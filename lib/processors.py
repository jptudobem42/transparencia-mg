from pyspark.sql.functions import col, when, to_date
from pyspark.sql.types import IntegerType

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

        # Renomear colunas
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

        df_silver_cleaned.write.mode("overwrite").saveAsTable(self.silver_table)