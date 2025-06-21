from pyspark.sql.dataframe import DataFrame


class UtilFunctions:

    @staticmethod
    def write_df_into_directory(df: DataFrame):

        df.write.format("parquet").mode('overwrite').save(
            '/home/administrador/Área de trabalho/estudos/projetos/projeto_api_brasil/dados_results/')