import boto3
from boto3.dynamodb.conditions import Key

from src.etl.const.vars.consts import DATA_ATUAL


class DynamoService:

    def __init__(self, table_dynamo_name: str):

        self.table_dynamo = self.inicializar_tabela_dynamo(table_dynamo_name)

    def inicializar_tabela_dynamo(self, table_dynamo_name: str):

        dynamodb = boto3.resource('dynamodb', region_name="us-east-1")

        return dynamodb.Table(table_dynamo_name)

    def obter_cnpjs_dynamo(self):

        response = self.table_dynamo.get_item(
            Key={
                'data': DATA_ATUAL
            }
        )

        return response.get('Item')

    def obter_cnpjs_para_processamento(self):

        '''
        Esse método vai verificar se existe processamento para o dia de hoje, e vai retornar a lista de CNPJ.
        SE existir: retorna CNPJs
        SENÃO: Raise Exception
        :return:
        '''

        return self.formatar_resposta_dynamo(self.obter_cnpjs_dynamo())

    # 123,234,1232
    def formatar_resposta_dynamo(self, cnpjs_string: str):

        return cnpjs_string.strip().split(sep=',')