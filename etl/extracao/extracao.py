from etl.i_etapa_etl import EtapaEtl
from service.cnpj_service import CnpjService
from service.dynamo_service import DynamoService


class Extracao(EtapaEtl):

    def __init__(self):
        self.api_service = CnpjService()
        self.dynamo_service = DynamoService()


    def executar(self, url_api_receita: str, url_brasil_api: str):

        # CNPJ DE TESTE:
        # BRASI_API: 19131243000197, 33372251006278
        # RECEITA_API: 19131243000197, 33372251006278

        lista_cnpj = self.dynamo_service.obter_cnpjs_para_processamento()

        dados_brasil_api = [self.api_service.obter_dados_cnpj(url_brasil_api, cpnj)
                            for cpnj in lista_cnpj]

        dados_api_receita = [self.api_service.obter_dados_cnpj(url_api_receita, cpnj)
                            for cpnj in lista_cnpj]
        
        #for cnpj in lista_cnpj:
        #    print(cnpj)

        #dados_brasil_api = self.api_service.obter_dados_cnpj(url_brasil_api, lista_cnpj[0])
        #dados_api_receita = self.api_service.obter_dados_cnpj(url_api_receita, lista_cnpj[1])

        print(len(dados_api_receita))
        print(len(dados_brasil_api))
