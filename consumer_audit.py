import json
from connection import get_channel

conn, ch = get_channel()

def registrar_auditoria(payload):
    print("AUDITORIA:", payload)

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        registrar_auditoria(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("Erro de auditoria:", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

ch.basic_qos(prefetch_count=5)
ch.basic_consume(queue='fila.auditoria', on_message_callback=callback)
print("Auditoria do consumidor iniciada...")
ch.start_consuming()
