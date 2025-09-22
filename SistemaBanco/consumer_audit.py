# consumer_audit.py
import os, json
import pika
from dotenv import load_dotenv
load_dotenv()

url = os.getenv("CLOUDAMQP_URL")
params = pika.URLParameters(url)
conn = pika.BlockingConnection(params)
ch = conn.channel()

def write_audit_log(payload):
    # apenas simula gravação em log (escreva em DB/arquivo append)
    print("AUDIT:", payload['id'], payload['from'], "->", payload['to'], payload['amount'])

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        write_audit_log(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("Audit error:", e)
        # NACK sem requeue envia pra DLX se configurado; aqui apenas nack e requeue=False para DLQ
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

ch.basic_qos(prefetch_count=5)
ch.basic_consume(queue='fila.auditoria', on_message_callback=callback)

print("Audit consumer started")
ch.start_consuming()
