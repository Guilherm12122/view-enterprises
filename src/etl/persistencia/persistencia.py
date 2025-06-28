from pyspark.sql.dataframe import DataFrame

from src.etl.i_etapa_etl import EtapaEtl


class Persistencia(EtapaEtl):

    def __init__(self, spark_op, s3_path):
        self.spark_op = spark_op
        self.s3_path = s3_path

    def executar(self, df_receita_brasil_api: DataFrame):

        df_receita_brasil_api.write.mode('overwrite').partitionBy('data_processamento').parquet(self.s3_path)

