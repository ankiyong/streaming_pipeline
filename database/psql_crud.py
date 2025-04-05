import psycopg2,os
from .psql_conn import Database

class CRUD(Database):
    def read_db(self,sql):
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return result
        except Exception as e:
            return f"Error {e}"
    
    def create_db(self,sql):
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            return f"Error {e}"
        
    def insert_data(self,ticker,sector):
        sql = f"""
            INSERT INTO public.sectors(ticker,sector)
            VALUES(%s,%s); 
        """
        self.cursor.execute(sql,(ticker,sector))
        self.db.commit()