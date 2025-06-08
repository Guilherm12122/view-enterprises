from pyspark.sql import DataFrame, Row

from etl.const.schemas.schema_dados_brasil_api import schema_dados_brasil_api
from etl.const.schemas.schema_dados_receita import schema_dados_receita
from etl.i_etapa_etl import EtapaEtl
from service.cnpj_service import CnpjService
from service.dynamo_service import DynamoService


class Extracao(EtapaEtl):

    def __init__(self, spark_op):
        self.spark_op = spark_op
        self.api_service = CnpjService()
        self.dynamo_service = DynamoService()


    def executar(self, url_api_receita: str, url_brasil_api: str):

        # CNPJ DE TESTE:
        # BRASI_API: 19131243000197, 33372251006278
        # RECEITA_API: 19131243000197, 33372251006278

        lista_cnpj = self.dynamo_service.obter_cnpjs_para_processamento()

        dados_brasil_api = [self.api_service.obter_dados_cnpj(url_brasil_api, cpnj)
                            for cpnj in lista_cnpj]

        dados_api_receita = [self.api_service.obter_dados_cnpj(url_api_receita, cpnj)
                            for cpnj in lista_cnpj]

        df_api_receita: DataFrame = self.obter_dataframe_dados_api(dados_api_receita, schema_dados_receita)
        df_brasil_api: DataFrame = self.obter_dataframe_dados_api(dados_brasil_api, schema_dados_brasil_api)

        df_brasil_api.show()
        df_api_receita.show()

        return df_api_receita, df_brasil_api


    def obter_dataframe_dados_api(self, dados, schema):

        return self.spark_op.createDataFrame(dados, schema)