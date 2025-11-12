import json, uuid, pika
from datetime import datetime
from connection import get_channel

conn, ch = get_channel()

def enviar_operacao(conta_origem, conta_destino, valor, moeda="BRL"):
    payload = {
        "id": str(uuid.uuid4()),
        "conta_origem": conta_origem,
        "conta_destino": conta_destino,
        "valor": valor,
        "moeda": moeda,
        "data_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    body = json.dumps(payload)
    ch.basic_publish(
        exchange='exchange.principal',
        routing_key='transacao.transferencia',
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type='application/json',
            headers={'x-retries': 0}
        )
    )
    print("Enviado:", payload)

if __name__ == "__main__":
    enviar_operacao("111-222", "333-444", 100, "BRL")
    conn.close()
