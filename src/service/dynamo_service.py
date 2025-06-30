import pymongo

from src.etl.const.vars.consts import DATA_ATUAL


class DynamoService:

    def __init__(self, url_connection, database):

        self.mongo_repository = self.inicializar_mongo_repository(
            url_connection,
            database,
            'dynamo_db_simulation'
        )


    def inicializar_mongo_repository(self, url_connection: str, database: str, collection: str):

        return pymongo.MongoClient(url_connection)[database][collection]

    # Exceções:
    # - SE não existir dados para o dia especificado, lança exceção e para a aplicação.
    def obter_cnpjs_para_processamento(self):

        cnpjs = self.validar_cnpjs()

        return cnpjs.split(sep=',')

    def validar_cnpjs(self):

        cnpjs = self.obter_dict_data_cnpj().get('cnpj').strip()

        if cnpjs == '':
            raise Exception(f'Não existe CPNJ especificado para a data {DATA_ATUAL}')

        return cnpjs

    def obter_dict_data_cnpj(self):

        list_dicts_data = list(self.mongo_repository.find({"data": DATA_ATUAL}, {"cnpj": 1, "_id": 0}))

        if len(list_dicts_data) == 0:
            raise Exception(f"Não existe processamento para a data especificada: {DATA_ATUAL}")

        return list_dicts_data[0]