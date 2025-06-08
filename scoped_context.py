from dotenv import load_dotenv
import os

class ScopedContext:

    def __init__(self):

        load_dotenv()

        self.url_api_receita: str = os.getenv('URL_API_RECEITA')
        self.url_brasil_api: str = os.getenv('URL_BRASIL_API')

