import json
import traceback
from connection import get_channel
conn, ch = get_channel()

def enviar_notificacao(payload):
    # Simula envio de email/SMS
    print(f"Notificando cliente sobre operação {payload['id']} - valor {payload['valor']}")

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        enviar_notificacao(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
          print("Notification error:", e)
          print("Payload problem:", body)   # mostra a mensagem original
          traceback.print_exc()             # mostra onde o erro aconteceu
          ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

ch.basic_qos(prefetch_count=5)
ch.basic_consume(queue='fila.notificacoes', on_message_callback=callback)

print("Notification consumer started")
ch.start_consuming()
