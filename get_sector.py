import yfinance as yf
from dotenv import load_dotenv
from database import psql_crud
load_dotenv()

def main(ticker):
    try:
        company = yf.Ticker(ticker)
        sector = company.info.get("sector", "Unknows")
        return sector
    except Exception as e:
        return "Unknowns"

if __name__ == "__main__":
    db = psql_crud.CRUD()
    sql = 'select DISTINCT as2."Name"  from all_stocks as2'
    create_sql = """
        CREATE TABLE SECTORS
        ticker char(20) NOT NULL,
        sector char(30)
    """
    db.create_db(create_sql)
    names = [name[0] for name in db.read_db(sql=sql)]
    sector_info = []
    for ticker in names:
        sector = main(ticker)
        db.insert_data(ticker, sector)