from pyspark.sql import DataFrame

from src.etl.const.schemas.schema_dados_brasil_api import schema_dados_brasil_api
from src.etl.const.schemas.schema_dados_receita import schema_dados_receita
from src.etl.i_etapa_etl import EtapaEtl


class Extracao(EtapaEtl):

    def __init__(self, spark_op, api_service, dynamo_service):
        self.spark_op = spark_op
        self.api_service = api_service
        self.dynamo_service = dynamo_service

    def executar(self, url_api_receita: str, url_brasil_api: str):

        # CNPJ DE TESTE:
        # BRASI_API: 19131243000197, 33372251006278
        # RECEITA_API: 19131243000197, 33372251006278

        dados_api_receita, dados_brasil_api = self.obter_dicts_api_receita_brasil(url_api_receita, url_brasil_api)
        df_api_receita: DataFrame = self.obter_dataframe_dados_api(dados_api_receita, schema_dados_receita)
        df_brasil_api: DataFrame = self.obter_dataframe_dados_api(dados_brasil_api, schema_dados_brasil_api)

        return df_api_receita, df_brasil_api



    def obter_dicts_api_receita_brasil(self, url_api_receita, url_brasil_api):

        lista_cnpj = self.dynamo_service.obter_cnpjs_para_processamento()

        dados_brasil_api = self.obter_lista_dicts_cnpj(url_brasil_api, lista_cnpj)

        dados_api_receita = self.obter_lista_dicts_cnpj(url_api_receita, lista_cnpj)

        return  dados_api_receita, dados_brasil_api



    def obter_lista_dicts_cnpj(self, url, lista_cnpj):

        return [self.api_service.obter_dados_cnpj(url, cpnj) for cpnj in lista_cnpj]


    def obter_dataframe_dados_api(self, dados, schema):

        return self.spark_op.createDataFrame(dados, schema)