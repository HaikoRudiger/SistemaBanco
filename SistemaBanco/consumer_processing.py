# consumer_processing.py
import os, json
import pika
from dotenv import load_dotenv
load_dotenv()

url = os.getenv("CLOUDAMQP_URL")
params = pika.URLParameters(url)
conn = pika.BlockingConnection(params)
ch = conn.channel()

MAX_RETRIES = 3

def process_transaction(payload):
    # Simula processamento — aqui você colocaria validação, débito/crédito etc.
    # Simule um erro para demonstrar retry:
    if payload.get("amount", 0) > 1000:
        raise Exception("Valor acima do limite para processamento automático.")
    # caso ok, retorna True
    return True

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
    except Exception as e:
        print("JSON parse error:", e)
        # encaminhar direto para DLX
        ch.basic_publish(exchange='exchange.dlx', routing_key='', body=body, properties=properties)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    retries = properties.headers.get('x-retries', 0) if properties.headers else 0
    try:
        print("Processing:", data)
        success = process_transaction(data)
        if success:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print("Processed OK:", data['id'])
    except Exception as e:
        print("Processing error:", e)
        retries = (retries or 0) + 1
        if retries > MAX_RETRIES:
            print("Max retries exceeded, sending to DLQ:", data['id'])
            # envia para DLX
            ch.basic_publish(exchange='exchange.dlx', routing_key='', body=body,
                             properties=pika.BasicProperties(
                                 delivery_mode=2,
                                 content_type='application/json',
                                 headers={'x-retries': retries}
                             ))
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            # envia para exchange.retry com routing para fila de retry apropriada (exponential)
            retry_routing = f'retry.{retries}'  # primeiro erro -> retry.1 (30s), segundo -> retry.2 etc
            print(f"Requeueing to retry queue {retry_routing}, retries={retries}")
            ch.basic_publish(exchange='exchange.retry', routing_key=retry_routing, body=body,
                             properties=pika.BasicProperties(
                                 delivery_mode=2,
                                 content_type='application/json',
                                 headers={'x-retries': retries}
                             ))
            ch.basic_ack(delivery_tag=method.delivery_tag)

ch.basic_qos(prefetch_count=1)  # fairness / load distribution
ch.basic_consume(queue='fila.transacoes', on_message_callback=callback)

print("Consumer (processing) started. Waiting messages...")
ch.start_consuming()
