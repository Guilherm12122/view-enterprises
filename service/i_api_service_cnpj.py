from abc import ABC, abstractmethod

class ApiServiceCnpj(ABC):

    @abstractmethod
    def obter_dados_cnpj(self, url: str, cpnj: str):
        """
        Consome os dados de uma empresa por meio de uma API, usando como parâmetro o CPNJ.
        """