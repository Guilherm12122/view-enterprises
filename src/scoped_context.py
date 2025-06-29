import sys
from awsglue.utils import getResolvedOptions

class ScopedContext:

    def __init__(self):

        args = getResolvedOptions(sys.argv, ['URL_API_RECEITA', 'URL_BRASIL_API', 'S3_PATH_DESTINY',
                                                    'TABELA_DYNAMO_NAME'])

        # Obtêndo parâmetros do Glue
        self.url_api_receita: str = args['URL_API_RECEITA']
        self.url_brasil_api: str = args['URL_BRASIL_API']
        self.s3_path_destiny: str = args['S3_PATH_DESTINY']
        self.tabela_dynamo_name: str = args['TABELA_DYNAMO_NAME']

