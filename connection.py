import os
import pika
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    url = os.getenv("CLOUDAMQP_URL")
    if not url: 
        raise RuntimeError("CLOUDAMQP_URL não definido no .env")

    params = pika.URLParameters(url)
    return pika.BlockingConnection(params)

# usa a função anterior para cirar um canal para uso dos produtores e consumidores
def get_channel():
    conn = get_connection()
    return conn, conn.channel()


