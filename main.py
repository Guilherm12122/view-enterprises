from etl.extracao.extracao import Extracao
from scoped_context import ScopedContext
from pyspark.sql import SparkSession

class Execucao:

    def __init__(self, spark_op):
        
        self.scoped_context = ScopedContext()
        self.extracao = Extracao(spark_op)
        #self.transformacao = Transformacao(spark_op)
        #self.persistencia = Persistencia(spark_op)

    def executar_processamento(self):

        df_receita, df_brasil_api = self.extracao.executar(
            self.scoped_context.url_api_receita,
            self.scoped_context.url_brasil_api
        )

        #df_dados_empresas = self.transformacao.executar(
        #    df_brasil, df_receita
        #)

        #self.persistencia.executar(df_dados_empresas)

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("Job for View Enterprise") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    execucao = Execucao(spark)
    execucao.executar_processamento()



