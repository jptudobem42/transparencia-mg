class Processors:
    def __init__(self, spark, raw_dir, bronze_table, silver_table):
        self.spark = spark
        self.raw_dir = raw_dir
        self.bronze_table = bronze_table
        self.silver_table = silver_table

    def process_bronze(self):
        df_bronze = self.spark.read.csv(self.raw_dir, sep=';', header=True, inferSchema=True)
        df_bronze.write.mode("overwrite").saveAsTable(self.bronze_table)

    def process_silver(self):
        import pandas as pd
        df_bronze = self.spark.read.table(self.bronze_table)
        df_silver = df_bronze.toPandas()

        df_silver_cleaned = (
            df_silver
            .dropna(thresh=3)  # Dropa linhas com menos de 3 valores não nulos
            .loc[~(df_silver['DT_NASC'].isna() & df_silver['NU_IDADE_N'].isna())]  # Dropa linhas onde ambas as colunas 'DT_NASC' e 'NU_IDADE_N' são NaN
            .drop(columns='DT_NASC')  # Dropa a coluna 'DT_NASC'
            .dropna(subset=['ID_MN_RESI'])  # Dropa linhas onde a coluna 'ID_MN_RESI' é NaN
            .fillna('Ignorado')  # Substitui os valores restantes NaN por 'Ignorado'
            .sort_values(by='DT_NOTIFIC', ascending=False)  # Organiza os dados pela data de notificação em ordem decrescente
            .reset_index(drop=True)  # Reseta o índice
        )

        # Converte a coluna 'NU_IDADE_N' para inteiro
        df_silver_cleaned['NU_IDADE_N'] = df_silver_cleaned['NU_IDADE_N'].astype(int)

        # Converte a coluna 'DT_NOTIFIC' para tipo date
        df_silver_cleaned['DT_NOTIFIC'] = pd.to_datetime(df_silver_cleaned['DT_NOTIFIC'], format='%d/%m/%Y').dt.date

        # Renomea colunas
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
        df_silver_cleaned = df_silver_cleaned.rename(columns=columns)

        # Converte DataFrame Pandas em Spark
        df_silver_spark = self.spark.createDataFrame(df_silver_cleaned)
        df_silver_spark.write.mode("overwrite").saveAsTable(self.silver_table)        