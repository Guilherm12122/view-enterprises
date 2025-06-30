from dotenv import load_dotenv
import os

class ScopedContext:

    def __init__(self):

        load_dotenv()

        self.url_api_receita: str = os.getenv('URL_API_RECEITA')
        self.url_brasil_api: str = os.getenv('URL_BRASIL_API')
        self.s3_path_destiny: str = os.getenv('S3_PATH_DESTINY')
        self.url_mongo: str = os.getenv('MONGO_DB_CONNECTION')
        self.mongo_database: str = os.getenv('MONGO_DB_DATABASE')

