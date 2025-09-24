# connection.py
import os
import pika
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    """Cria e retorna uma conexão RabbitMQ usando a URL do .env"""
    url = os.getenv("CLOUDAMQP_URL")
    if not url:
        raise RuntimeError("CLOUDAMQP_URL não definido no .env")
    params = pika.URLParameters(url)
    return pika.BlockingConnection(params)

def get_channel():
    """Retorna um canal já aberto"""
    conn = get_connection()
    return conn, conn.channel()
