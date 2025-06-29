from etl.extracao.extracao import Extracao
from etl.persistencia.persistencia import Persistencia
from etl.transformacao.transformacao import Transformacao
from scoped_context import ScopedContext
from pyspark.sql import SparkSession

from service.cnpj_service import CnpjService
from service.dynamo_service import DynamoService


class Execucao:

    def __init__(self, spark_op):

        self.api_service = CnpjService()
        self.dynamo_service = DynamoService()
        self.scoped_context = ScopedContext()

        self.extracao = Extracao(spark_op, self.api_service, self.dynamo_service)
        self.transformacao = Transformacao(spark_op)

        self.persistencia = Persistencia(spark_op, self.scoped_context.s3_path_destiny)

    def executar_processamento(self):

        df_receita, df_brasil_api = self.extracao.executar(
            self.scoped_context.url_api_receita,
            self.scoped_context.url_brasil_api
        )

        df_dados_empresas = self.transformacao.executar(
            df_receita, df_brasil_api
        )

        self.persistencia.executar(df_dados_empresas)

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("Job for View Enterprise") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    execucao = Execucao(spark)
    execucao.executar_processamento()



