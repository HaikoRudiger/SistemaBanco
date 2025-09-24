import json
from connection import get_channel

conn, ch = get_channel()

def registrar_auditoria(payload):
    # apenas simula gravação em log (escreva em DB/arquivo append)
    print("AUDITORIA:", payload['id'], payload['conta_origem'], "->", payload['conta_destino'], payload['valor'])

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        registrar_auditoria(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("Audit error:", e)
        # NACK sem requeue envia pra DLX se configurado; aqui apenas nack e requeue=False para DLQ
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

ch.basic_qos(prefetch_count=5)
ch.basic_consume(queue='fila.auditoria', on_message_callback=callback)

print("Audit consumer started")
ch.start_consuming()
