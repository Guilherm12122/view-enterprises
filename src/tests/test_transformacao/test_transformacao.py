import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from src.etl.transformacao.transformacao import Transformacao

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


def test_join_receita_atividade(spark, transformacao_class):

    df_entrada_1 = spark.createDataFrame(
        [('12345', 'val1', 'val2')], ['cnpj', 'campo1', 'campo2']
    )

    df_entrada_2 = spark.createDataFrame(
        [('12345', 'val4', 'val22'),
         ('5432', '', '')], ['cnpj_atividade', 'campo4', 'campo2']
    )

    df_esperado = spark.createDataFrame(
        [('12345', 'val1', 'val22')],
        ['cnpj', 'campo1', 'campo2']
    )

    resultado = transformacao_class.join_receita_atividade(df_entrada_1, df_entrada_2, 'campo2')

    assert resultado.collect() == df_esperado.collect()


def test_join_dados_receita_brasil_api(spark, transformacao_class):

    df_entrada_1 = spark.createDataFrame(
        [('123', '', '', '+551198', 'empresa_ol', ''),
         ('456', '', '', None, 'empresa_ol', ''),
         ('789', '', '', None, None, '')],
        ['cnpj', 'atividade_principal', 'atividades_secundarias',
         'telefone', 'nome', 'campo_more']
    )

    df_entrada_2 = spark.createDataFrame(
        [('123', 'br', '+5567', '+5590', 'empresa_olll', '1212', 'hey', 100, ''),
         ('456', 'br', '+5567', '+5590', 'empresa_olll', '1212', 'hey', 100, ''),
         ('789', 'br', None, '+5590', 'empresa_olll', '1212', 'hey', 100, '')],
        ['cnpj','pais', 'ddd_telefone_1', 'ddd_telefone_2', 'razao_social',
         'data_inicio_atividade', 'cnae_fiscal_descricao', 'capital_social', 'campo_more']
    )

    df_esperado = spark.createDataFrame(
        [('123', 'br', '', '', '+551198', 'empresa_ol', '1212', 'hey', 100),
         ('789', 'br', '', '', '+5590', 'empresa_olll', '1212', 'hey', 100),
         ('456', 'br', '', '', '+5567', 'empresa_ol', '1212', 'hey', 100)],
        ['cnpj', 'pais', 'atividade_principal', 'atividades_secundarias',
         'telefone', 'nome', 'data_inicio_atividade', 'cnae_fiscal_descricao',
         'capital_social']
    )

    resultado = transformacao_class.join_dados_receita_brasil_api(df_entrada_1, df_entrada_2)

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

    df_esperado.show(truncate=False)

    resultado = transformacao_class.filtrar_code_text_nao_vazio(df_entrada)

    resultado.show(truncate=False)

    assert resultado.collect() == df_esperado.collect()
