from pyspark.sql.types import *

schema_dados_brasil_api = StructType([
    StructField("uf", StringType(), True),
    StructField("cep", StringType(), True),
    StructField("cnpj", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("email", StringType(), True),
    StructField("porte", StringType(), True),
    StructField("bairro", StringType(), True),
    StructField("numero", StringType(), True),
    StructField("ddd_fax", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("logradouro", StringType(), True),
    StructField("cnae_fiscal", LongType(), True),
    StructField("codigo_pais", StringType(), True),
    StructField("complemento", StringType(), True),
    StructField("codigo_porte", IntegerType(), True),
    StructField("razao_social", StringType(), True),
    StructField("nome_fantasia", StringType(), True),
    StructField("capital_social", LongType(), True),
    StructField("ddd_telefone_1", StringType(), True),
    StructField("ddd_telefone_2", StringType(), True),
    StructField("opcao_pelo_mei", StringType(), True),
    StructField("descricao_porte", StringType(), True),
    StructField("codigo_municipio", IntegerType(), True),
    StructField("natureza_juridica", StringType(), True),
    StructField("situacao_especial", StringType(), True),
    StructField("opcao_pelo_simples", StringType(), True),
    StructField("situacao_cadastral", IntegerType(), True),
    StructField("data_opcao_pelo_mei", StringType(), True),
    StructField("data_exclusao_do_mei", StringType(), True),
    StructField("cnae_fiscal_descricao", StringType(), True),
    StructField("codigo_municipio_ibge", IntegerType(), True),
    StructField("data_inicio_atividade", StringType(), True),
    StructField("data_situacao_especial", StringType(), True),
    StructField("data_opcao_pelo_simples", StringType(), True),
    StructField("data_situacao_cadastral", StringType(), True),
    StructField("nome_cidade_no_exterior", StringType(), True),
    StructField("codigo_natureza_juridica", IntegerType(), True),
    StructField("data_exclusao_do_simples", StringType(), True),
    StructField("motivo_situacao_cadastral", IntegerType(), True),
    StructField("ente_federativo_responsavel", StringType(), True),
    StructField("identificador_matriz_filial", IntegerType(), True),
    StructField("qualificacao_do_responsavel", IntegerType(), True),
    StructField("descricao_situacao_cadastral", StringType(), True),
    StructField("descricao_tipo_de_logradouro", StringType(), True),
    StructField("descricao_motivo_situacao_cadastral", StringType(), True),
    StructField("descricao_identificador_matriz_filial", StringType(), True),

    # Lista de sócios
    StructField("qsa", ArrayType(StructType([
        StructField("pais", StringType(), True),
        StructField("nome_socio", StringType(), True),
        StructField("codigo_pais", StringType(), True),
        StructField("faixa_etaria", StringType(), True),
        StructField("cnpj_cpf_do_socio", StringType(), True),
        StructField("qualificacao_socio", StringType(), True),
        StructField("codigo_faixa_etaria", IntegerType(), True),
        StructField("data_entrada_sociedade", StringType(), True),
        StructField("identificador_de_socio", IntegerType(), True),
        StructField("cpf_representante_legal", StringType(), True),
        StructField("nome_representante_legal", StringType(), True),
        StructField("codigo_qualificacao_socio", IntegerType(), True),
        StructField("qualificacao_representante_legal", StringType(), True),
        StructField("codigo_qualificacao_representante_legal", IntegerType(), True)
    ])), True),

    # Lista de CNAEs secundários
    StructField("cnaes_secundarios", ArrayType(StructType([
        StructField("codigo", LongType(), True),
        StructField("descricao", StringType(), True)
    ])), True),

    # Lista de regimes tributários
    StructField("regime_tributario", ArrayType(StructType([
        StructField("ano", IntegerType(), True),
        StructField("cnpj_da_scp", StringType(), True),
        StructField("forma_de_tributacao", StringType(), True),
        StructField("quantidade_de_escrituracoes", IntegerType(), True)
    ])), True),
])
