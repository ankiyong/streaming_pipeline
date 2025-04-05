import psycopg2,os
from dotenv import load_dotenv
from pathlib import Path


class Database():
    def __init__(self):
        env_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(dotenv_path=env_path)
        self.db = psycopg2.connect(
                host=os.environ.get('DB_HOST'),
                dbname=os.environ.get('DB_NAME'),
                user=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASSWORD'),
                port=os.environ.get('DB_PORT')
            )
        self.cursor = self.db.cursor()

    def __del__(self):
        self.db.close()
        self.cursor.close()
    
    def execute(self,query,args={}):
        self.cursor.execute(query,args)
        row = self.cursor.fetchall()
        return row
