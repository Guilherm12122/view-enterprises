import os
import unittest
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from etl.transformacao.transformacao import Transformacao

# Enxergar variável de ambiente do JAVA
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    '--conf "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED '
    '--add-opens=java.base/java.nio=ALL-UNNAMED" '
    '--conf "spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED '
    '--add-opens=java.base/java.nio=ALL-UNNAMED" '
    'pyspark-shell'
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-pyspark") \
        .master("local[2]") \
        .getOrCreate()

@pytest.fixture
def transformacao_class(spark):
    return Transformacao(spark)


def test_preparar_cnpj_receita_join(spark, transformacao_class):

    df_entrada = spark.createDataFrame(
        [('123.543-12/-0', 'v1')],['cnpj', 'campo_aux'])

    df_esperado = spark.createDataFrame(
        [('123543120', 'v1')], ['cnpj', 'campo_aux']
    )

    resultado = transformacao_class.preparar_cnpj_receita_join(df_entrada)

    assert resultado.collect() == df_esperado.collect()


def test_obter_primeira_ocorrencia_atividade(spark, transformacao_class):

    df_entrada = spark.createDataFrame(
        [('123', 'aux1', 'at1'), ('456', 'aux1','at1'),
         ('123', 'aux2', 'at1'), ('456', 'aux2' , 'at1')],
        ['cnpj_atividade', 'campo_aux', 'at1']
    )

    df_esperado = spark.createDataFrame(
        [('123', 'aux1', 'at1'), ('456', 'aux1','at1')],
        ['cnpj_atividade', 'campo_aux', 'at1']
    )

    resultado = transformacao_class.obter_primeira_ocorrencia_atividade(
        df_entrada, 'at1'
    )

    assert resultado.collect() == df_esperado.collect()


def test_criacao_atividade_tratada(spark, transformacao_class):

    df_entrada = spark.createDataFrame(
        [('12345', '1', 'descricao')],
        ['cnpj_atividade', 'code', 'text']
    )

    df_esperado = spark.createDataFrame(
        [('12345', '1 - descricao')], ['cnpj_atividade', 'at1']
    )

    resultado = transformacao_class.criacao_atividade_tratada(df_entrada, 'at1')

    assert resultado.collect() == df_esperado.collect()


def test_explode_df_atividade(spark, transformacao_class):

    df_entrada = spark.createDataFrame(
        [(
            '12345',
            [{
                'code': '1',
                'text': 'text1'
            },
            {
                'code': '2',
                'text': 'text2'
            }]
        )], ['cnpj', 'at1']
    )

    df_esperado = spark.createDataFrame(
        [('12345',
          {
              'code': '1',
              'text': 'text1'
          },
          '1', 'text1'),
         ('12345',
          {
              'code': '2',
              'text': 'text2'
          },
          '2', 'text2')
         ], ['cnpj_atividade', 'at1', 'code', 'text']
    )

    resultado = transformacao_class.explode_df_atividade(df_entrada, 'at1')

    assert resultado.collect() == df_esperado.collect()


def test_selecionar_colunas_receita(spark, transformacao_class):

    df_entrada = spark.createDataFrame([('', '', '', '', '')],
                                       ['cnpj', 'nome', 'atividade_principal',
                                        'atividades_secundarias', 'telefone'])

    resultado = transformacao_class.selecionar_colunas_receita(df_entrada)

    assert isinstance(resultado, DataFrame)


def test_filtrar_code_text_nao_vazio(spark, transformacao_class):

    df_entrada = spark.createDataFrame(
        [("123", "Texto válido"), ("", "Texto presente"), (None, "Outro texto"),
         ("456", ""), ("789", None), ("  ", "Espaço"), ("XYZ", " ")
        ], ["code", "text"]
    )

    df_esperado = spark.createDataFrame(
            [("123", "Texto válido")],
            ["code", "text"]
        )

    resultado = transformacao_class.filtrar_code_text_nao_vazio(df_entrada)

    assert resultado.collect() == df_esperado.collect()
