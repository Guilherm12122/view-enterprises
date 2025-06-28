import requests

from src.service.i_api_service_cnpj import ApiServiceCnpj


class CnpjService(ApiServiceCnpj):

    def __init__(self):
        pass

    def obter_dados_cnpj(self, url: str, cpnj: str):

        return requests.get(f"{url}/{cpnj}").json()