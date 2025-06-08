from etl.extracao.extracao import Extracao
from scoped_context import ScopedContext


class Execucao:

    def __init__(self):
        
        self.scoped_context = ScopedContext()
        self.extracao = Extracao()
        #self.transformacao = Transformacao()
        #self.persistencia = Persistencia()

    def executar_processamento(self):

        self.extracao.executar(
            self.scoped_context.url_api_receita,
            self.scoped_context.url_brasil_api
        )

        #df_dados_empresas = self.transformacao.executar(
        #    df_brasil, df_receita
        #)

        #self.persistencia.executar(df_dados_empresas)

if __name__ == "__main__":
    execucao = Execucao()
    execucao.executar_processamento()



