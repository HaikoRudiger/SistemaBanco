import json, uuid
import pika
import time
from connection import get_channel
conn, ch = get_channel()

def enviar_operacao(conta_origem, conta_destino, valor):
    payload = {
        "id": str(uuid.uuid4()),
        "conta_origem": conta_origem,
        "conta_destino": conta_destino,
        "valor": valor,
        "data_hora": time.time()
    }
    body = json.dumps(payload)
    ch.basic_publish(
        exchange='exchange.transacoes',
        routing_key='transacao.transferencia',
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistente
            content_type='application/json',
            headers={'x-retries': 0}
        )
    )
    print("Enviado:", payload)

if __name__ == "__main__":
    enviar_operacao("111-222", "333-444", 5000)
    conn.close()
