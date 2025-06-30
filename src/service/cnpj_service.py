from http.client import responses

import requests
import time
from src.service.i_api_service_cnpj import ApiServiceCnpj


class CnpjService(ApiServiceCnpj):

    def __init__(self):
        pass


    # Exceções:
    # -> SE o servidor não responder, espere alguns minutos e solicite de novo.
    def obter_dados_cnpj(self, url: str, cpnj: str):

        response = {}

        while True:

            try:
                response = requests.get(f"{url}/{cpnj}")
                break
            except Exception as e:
                print(e)
                time.sleep(15)

        return response