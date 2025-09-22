# consumer_notification.py
import os, json
import pika
from dotenv import load_dotenv
load_dotenv()

url = os.getenv("CLOUDAMQP_URL")
params = pika.URLParameters(url)
conn = pika.BlockingConnection(params)
ch = conn.channel()

def send_notification(payload):
    # Simula envio de email/SMS
    print(f"Notifying customer about transaction {payload['id']} - amount {payload['amount']}")

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        send_notification(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("Notification error:", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

ch.basic_qos(prefetch_count=5)
ch.basic_consume(queue='fila.notificacoes', on_message_callback=callback)

print("Notification consumer started")
ch.start_consuming()
