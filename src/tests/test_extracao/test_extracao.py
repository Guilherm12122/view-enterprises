import unittest
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from src.etl.extracao.extracao import Extracao
from src.tests.test_extracao.constantes import DADOS_TESTE, SCHEMA_TESTE
import os

# Enxergar variável de ambiente do JAVA
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    '--conf "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED '
    '--add-opens=java.base/java.nio=ALL-UNNAMED" '
    '--conf "spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED '
    '--add-opens=java.base/java.nio=ALL-UNNAMED" '
    'pyspark-shell'
)


def obter_dados_cnpj(url, cnpj):

    return {
        "url": url,
        "cnpj": cnpj
    }


class TestExtracao(unittest.TestCase):

    @classmethod
    @patch('service.cnpj_service.CnpjService')
    @patch('service.dynamo_service.DynamoService')
    def setUpClass(cls, mock_api_service, mock_dynamo_service):

        cls.spark = SparkSession.builder \
            .appName("Teste Extracao") \
            .master("local[1]") \
            .getOrCreate()

        mock_api_service().obter_dados_cnpj.side_effect = obter_dados_cnpj

        cls.extracao = Extracao(cls.spark, mock_api_service(), mock_dynamo_service())


    def test_obter_dataframe_dados_api(self):

        df = self.extracao.obter_dataframe_dados_api(DADOS_TESTE, SCHEMA_TESTE)

        self.assertIsInstance(df, DataFrame)

    def test_obter_lista_dicts_cnpj(self):

        lista_dict_cnpj = self.extracao.obter_lista_dicts_cnpj("teste_url", ["12345", "54321"])

        self.assertEqual(lista_dict_cnpj,
                         [
                             {
                                "url": "teste_url",
                                "cnpj": "12345"
                             },
                             {
                                "url": "teste_url",
                                "cnpj": "54321"
                             }
                         ])