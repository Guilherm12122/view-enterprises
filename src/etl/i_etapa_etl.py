from abc import ABC, abstractmethod


class EtapaEtl(ABC):
    
    @abstractmethod
    def executar(self, *args, **kwargs):
        '''
        Realiza alguma etapa do processo de ETL 
        (Extração, Tranformação ou Carga)
        '''
        pass