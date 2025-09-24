import json
import pika
from connection import get_channel

conn, ch = get_channel()

MAX_RETRIES = 3

def processar_operacao(payload):
    """Simula processamento — aqui você colocaria validação, débito/crédito etc."""
    if payload.get("valor", 0) > 1000:
        raise Exception("Valor acima do limite para processamento automático.")
    return True

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
    except Exception as e:
        print("JSON parse error:", e)
        # encaminhar direto para DLX
        ch.basic_publish(
            exchange='exchange.dlx',
            routing_key='',
            body=body,
            properties=properties
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Garante que headers nunca será None
    headers = properties.headers or {}
    retries = headers.get('x-retries', 0)

    try:
        print("Processando:", data)
        sucesso = processar_operacao(data)
        if sucesso:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print("Processado OK:", data['id'])
    except Exception as e:
        print("Processing error:", e)
        retries += 1

        if retries > MAX_RETRIES:
            print("Max retries exceeded, sending to DLQ:", data['id'])
            ch.basic_publish(
                exchange='exchange.dlx',
                routing_key='',
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json',
                    headers={'x-retries': retries}
                )
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            retry_routing = f'retry.{retries}'
            print(f"Requeueing to retry queue {retry_routing}, retries={retries}")
            ch.basic_publish(
                exchange='exchange.retry',
                routing_key=retry_routing,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json',
                    headers={'x-retries': retries}
                )
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)

ch.basic_qos(prefetch_count=1)
ch.basic_consume(queue='fila.transacoes', on_message_callback=callback)

print("Consumer (processing) started. Waiting messages...")
ch.start_consuming()
