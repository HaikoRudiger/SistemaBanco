# producer.py
import os, json, uuid
import pika
from dotenv import load_dotenv
load_dotenv()

url = os.getenv("CLOUDAMQP_URL")
params = pika.URLParameters(url)
conn = pika.BlockingConnection(params)
ch = conn.channel()

def send_transaction(from_acc, to_acc, amount):
    payload = {
        "id": str(uuid.uuid4()),
        "from": from_acc,
        "to": to_acc,
        "amount": amount,
        "timestamp": __import__('time').time()
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
    print("Sent:", payload)

if __name__ == "__main__":
    send_transaction("111-222", "333-444", 150.75)
    conn.close()
