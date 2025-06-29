

class DynamoService:

    def __init__(self):
        pass


    # Exceções:
    # - SE não existir dados para o dia especificado, lança exceção e para a aplicação.
    def obter_cnpjs_para_processamento(self):

        '''
        Esse método vai verificar se existe processamento para o dia de hoje, e vai retornar a lista de CNPJ.
        SE existir: retorna CNPJs
        SENÃO: Raise Exception
        :return:
        '''
        # TESTE
        return ['1913124300019007', '33372251006270008']