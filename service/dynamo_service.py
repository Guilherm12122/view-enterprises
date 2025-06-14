

class DynamoService:

    def __init__(self):
        pass


    def obter_cnpjs_para_processamento(self):

        '''
        Esse método vai verificar se existe processamento para o dia de hoje, e vai retornar a lista de CNPJ.
        SE existir: retorna CNPJs
        SENÃO: Raise Exception
        :return:
        '''
        # TESTE
        return ['19131243000197', '33372251006278']