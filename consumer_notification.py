import json, traceback
from connection import get_channel

conn, ch = get_channel()

def enviar_notificacao(payload):
    status = payload.get("status", "OK")
    print(f"Notificando cliente: ID {payload.get('id')} - Valor R$ {payload.get('valor')} - Status: {status}")

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        enviar_notificacao(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("Erro de notificação:", e)
        print("Problema de Payload:", body)
        traceback.print_exc()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

ch.basic_qos(prefetch_count=5)
ch.basic_consume(queue='fila.notificacoes', on_message_callback=callback)
print("Notificacao do consumidor iniciada...")
ch.start_consuming()
