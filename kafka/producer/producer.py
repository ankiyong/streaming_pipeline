from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
from pathlib import Path
import os,sys,json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from database import psql_crud
from datetime import datetime, timedelta

def convertor(data):
    if data:
        message = {
            "date": data[0],
            "open": data[1],
            "high": data[2],
            "low": data[3],
            "close": data[4],
            "volume": data[5],
            "ticker": data[7].strip(),
            "sector": data[8].strip()
        }
        return message

def main():
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    topic_name = os.environ.get("TOPIC_NAME")
    server_str = os.environ.get("BOOTSTRAP_SERVER","")
    server_list = [s.strip() for s in server_str.split(",")if s]
    producer = KafkaProducer(
        bootstrap_servers=server_list,
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
    )
    start_date = datetime.strptime("2013-02-08", "%Y-%m-%d")
    while True:
        query_date = start_date.strftime("%Y-%m-%d")
        sql = f"SELECT * FROM stocks AS as2 WHERE as2.date = '{query_date}'"
        crud = psql_crud.CRUD()
        data = crud.read_db(sql)
        if data:
            for d in data:
                value = convertor(d)
                key_bytes = value["ticker"]
                try:
                    future = producer.send(topic=topic_name,key=key_bytes,value=value)
                    record_metadata = future.get(timeout=10)
                    print(f"Message sent successfully to partition {record_metadata.partition} "
                        f"with offset {record_metadata.offset}")
                except KafkaError as e:
                    print(f"Failed to send message: {e}")
            is_continue = input("Do you want to send another message? (y/n): ").strip().lower()
        if is_continue == 'n':
            break
        start_date += timedelta(days=1)
    producer.close()
if __name__ == "__main__":
    main()
