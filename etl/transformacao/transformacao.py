from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode, concat, lit, row_number, regexp_replace, coalesce, trim
from pyspark.sql import Window

from devutils.UtilFunctions import UtilFunctions
from etl.i_etapa_etl import EtapaEtl


class Transformacao(EtapaEtl):

    def __init__(self, spark_op):
        self.spark_op = spark_op

    def executar(self, df_receita_api: DataFrame, df_brasil_api: DataFrame) -> DataFrame:

        df_dados_receita_tratados = self.selecionar_colunas_receita(self.tratar_dados_receita(df_receita_api))

        df_dados_receita_tratados = self.preparar_cnpj_receita_join(df_dados_receita_tratados)

        df_dados_receita_brasil_api = self.join_dados_receita_brasil_api(df_dados_receita_tratados, df_brasil_api)

        return df_dados_receita_brasil_api


    def join_dados_receita_brasil_api(self, df_receita: DataFrame, df_brasil_api: DataFrame) -> DataFrame:

        df_join = df_receita.join(df_brasil_api, df_receita['cnpj'] == df_brasil_api['cnpj'], 'left') \
                                 .select(
                                        df_receita['cnpj'],
                                        df_brasil_api['pais'],
                                        df_receita['atividade_principal'],
                                        df_receita['atividades_secundarias'],
                                        coalesce(df_receita['telefone'],
                                                 df_brasil_api['ddd_telefone_1'],
                                                 df_brasil_api['ddd_telefone_2']).alias('telefone'),
                                        coalesce(df_receita['nome'],
                                                 df_brasil_api['razao_social']).alias('nome'),
                                        df_brasil_api['data_inicio_atividade'],
                                        df_brasil_api['cnae_fiscal_descricao'],
                                        df_brasil_api['capital_social'])

        return df_join


    def preparar_cnpj_receita_join(self, df_receita: DataFrame) -> DataFrame:

        return df_receita.withColumn('cnpj', regexp_replace(col('cnpj'), "[\./-]", ""))


    def selecionar_colunas_receita(self, df_receita: DataFrame) -> DataFrame:

        return df_receita \
                .select(
                    col('cnpj'),
                    col('nome'),
                    col('atividade_principal'),
                    col('atividades_secundarias'),
                    col('telefone')
                )


    def tratar_dados_receita(self, df_receita_api: DataFrame):

        df_atividade_pr, df_atividade_sec = self.obter_dados_atividade_primaria_secundaria(df_receita_api)

        df_receita_join_1 = self.join_receita_atividade(df_receita_api, df_atividade_pr, 'atividade_principal')

        df_receita_join_2 = self.join_receita_atividade(df_receita_join_1, df_atividade_sec, 'atividades_secundarias')

        return df_receita_join_2


    def join_receita_atividade(self, df_receita: DataFrame, df_atividade: DataFrame,
                                      atividade_nome: str):

        colunas_df_receita = [col(c) for c in df_receita.columns if c != atividade_nome]

        df_join = df_receita.join(df_atividade, df_receita.cnpj == df_atividade.cnpj_atividade, 'left') \
                .select(*colunas_df_receita, df_atividade[atividade_nome])

        return  df_join


    def obter_dados_atividade_primaria_secundaria(self, df_receita):

        atividade_primaria = 'atividade_principal'
        atividades_secundaria = 'atividades_secundarias'

        df_atividade_primaria = self.explode_df_atividade(df_receita, atividade_primaria)
        df_atividade_secundaria = self.explode_df_atividade(df_receita, atividades_secundaria)

        df_atividade_primaria = self.filtrar_code_text_nao_vazio(df_atividade_primaria)
        df_atividade_secundaria = self.filtrar_code_text_nao_vazio(df_atividade_secundaria)

        df_atividade_primaria = self.criacao_atividade_tratada(df_atividade_primaria, atividade_primaria)
        df_atividade_secundaria = self.criacao_atividade_tratada(df_atividade_secundaria, atividades_secundaria)

        df_atividade_secundaria = self.obter_primeira_ocorrencia_atividade(df_atividade_secundaria, atividades_secundaria)

        return df_atividade_primaria, df_atividade_secundaria


    def explode_df_atividade(self, df_receita_api: DataFrame,
                                   atividade_nome: str) -> DataFrame:

        df_explode_receita_atividade = df_receita_api \
            .select(col('cnpj').alias('cnpj_atividade'),
                    explode(col(atividade_nome)).alias(atividade_nome)) \
            .withColumn('code', col(f"{atividade_nome}.code")) \
            .withColumn('text', col(f"{atividade_nome}.text"))

        return df_explode_receita_atividade

    def filtrar_code_text_nao_vazio(self, df_code_text: DataFrame) -> DataFrame:

        return df_code_text.filter(
            (col("code").isNotNull()) &
            (trim(col("code")) != '') &
            (col("text").isNotNull()) &
            (trim(col("text")) != '')
        )

    def criacao_atividade_tratada(self, df_receita_code_text: DataFrame,
                                        atividade_nome: str) -> DataFrame:

        df_receita_atividade_tratada = df_receita_code_text \
                .select(col('cnpj_atividade'),
                        concat(col("code"), lit(" - "), col("text")).alias(atividade_nome))

        return df_receita_atividade_tratada

    def obter_primeira_ocorrencia_atividade(self, df_receita_atividade_tratada: DataFrame,
                                                  atividade_nome: str):

        window_cnpj = Window.partitionBy("cnpj_atividade").orderBy(atividade_nome)

        df_rankeado = df_receita_atividade_tratada \
                        .withColumn('rank', row_number().over(window_cnpj))

        return df_rankeado.filter('rank = 1').drop('rank')