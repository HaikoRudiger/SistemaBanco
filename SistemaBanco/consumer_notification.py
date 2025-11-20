import json
import traceback
from connection import get_channel

conn, ch = get_channel()

def enviar_notificacao(payload):
    print(
        f"Notificando cliente: ID {payload.get('id')} | "
        f"Origem: {payload.get('valor')} {payload.get('moeda_origem')} → "
        f"Destino: {payload.get('valor_destino')} {payload.get('moeda_destino')} | "
        f"Taxa: {payload.get('fx_rate')} | "
        f"Status: {payload.get('status', 'OK')}"
    )

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        enviar_notificacao(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except:
        traceback.print_exc()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

ch.basic_qos(prefetch_count=5)
ch.basic_consume(queue="fila.notificacoes", on_message_callback=callback)
print("Notificação iniciada...")
ch.start_consuming()
