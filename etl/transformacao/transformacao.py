from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode, concat, lit, row_number
from pyspark.sql import Window
from etl.i_etapa_etl import EtapaEtl


class Transformacao(EtapaEtl):

    def __init__(self, spark_op):
        self.spark_op = spark_op

    def executar(self, df_receita_api: DataFrame, df_brasil_api: DataFrame) -> DataFrame:

        self.tratar_dados_receita(df_receita_api)

    def tratar_dados_receita(self, df_receita_api):

        df_atividade_pr, df_atividade_sec = self.obter_dados_atividade_primaria_secundaria(df_receita_api)

        df_receita_join_1 = self.join_receita_atividade(df_receita_api, df_atividade_pr, 'atividade_principal')

        df_receita_join_2 = self.join_receita_atividade(df_receita_join_1, df_atividade_sec, 'atividades_secundarias')

        df_receita_join_2.show(truncate=False)


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

        df_atividade_primaria.show()
        df_atividade_secundaria.show()

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
            (col("code") != '') &
            (col("text").isNotNull()) &
            (col("text") != '')
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