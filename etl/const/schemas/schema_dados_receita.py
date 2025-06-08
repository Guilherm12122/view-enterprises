from pyspark.sql.types import *

schema_dados_receita = StructType([
    StructField("status", StringType(), True),
    StructField("ultima_atualizacao", StringType(), True),
    StructField("cnpj", StringType(), True),
    StructField("tipo", StringType(), True),
    StructField("porte", StringType(), True),
    StructField("nome", StringType(), True),
    StructField("fantasia", StringType(), True),
    StructField("abertura", StringType(), True),

    StructField("atividade_principal", ArrayType(
        StructType([
            StructField("code", StringType(), True),
            StructField("text", StringType(), True)
        ])
    ), True),

    StructField("atividades_secundarias", ArrayType(
        StructType([
            StructField("code", StringType(), True),
            StructField("text", StringType(), True)
        ])
    ), True),

    StructField("natureza_juridica", StringType(), True),
    StructField("logradouro", StringType(), True),
    StructField("numero", StringType(), True),
    StructField("complemento", StringType(), True),
    StructField("cep", StringType(), True),
    StructField("bairro", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("email", StringType(), True),
    StructField("telefone", StringType(), True),
    StructField("efr", StringType(), True),
    StructField("situacao", StringType(), True),
    StructField("data_situacao", StringType(), True),
    StructField("motivo_situacao", StringType(), True),
    StructField("situacao_especial", StringType(), True),
    StructField("data_situacao_especial", StringType(), True),
    StructField("capital_social", StringType(), True),

    StructField("qsa", ArrayType(
        StructType([
            StructField("nome", StringType(), True),
            StructField("qual", StringType(), True),
            StructField("pais_origem", StringType(), True),
            StructField("nome_rep_legal", StringType(), True),
            StructField("qual_rep_legal", StringType(), True)
        ])
    ), True),

    StructField("simples", StructType([
        StructField("optante", BooleanType(), True),
        StructField("data_opcao", StringType(), True),
        StructField("data_exclusao", StringType(), True),
        StructField("ultima_atualizacao", StringType(), True)
    ]), True),

    StructField("simei", StructType([
        StructField("optante", BooleanType(), True),
        StructField("data_opcao", StringType(), True),
        StructField("data_exclusao", StringType(), True),
        StructField("ultima_atualizacao", StringType(), True)
    ]), True),

    StructField("billing", StructType([
        StructField("free", BooleanType(), True),
        StructField("database", BooleanType(), True)
    ]), True)
])
