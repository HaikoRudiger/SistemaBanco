# setup_topology.py
import os
import pika
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()
url = os.getenv("CLOUDAMQP_URL")
params = pika.URLParameters(url)
conn = pika.BlockingConnection(params)
ch = conn.channel()

# Exchanges
ch.exchange_declare(exchange='exchange.transacoes', exchange_type='topic', durable=True)
ch.exchange_declare(exchange='exchange.retry', exchange_type='direct', durable=True)
ch.exchange_declare(exchange='exchange.dlx', exchange_type='fanout', durable=True)

# Filas principais
ch.queue_declare(queue='fila.transacoes', durable=True, arguments={
    'x-dead-letter-exchange': 'exchange.retry'  # envie temporariamente para retry quando necessário
})
ch.queue_declare(queue='fila.auditoria', durable=True)
ch.queue_declare(queue='fila.notificacoes', durable=True)

# DLQ
ch.queue_declare(queue='fila.dlq', durable=True)
# Bind DLX -> DLQ
ch.queue_bind(exchange='exchange.dlx', queue='fila.dlq')

# Retry queues com TTL (exponential backoff: 30s, 60s, 120s)
retry_ttls = [30000, 60000, 120000]  # ms
for i, ttl in enumerate(retry_ttls, start=1):
    qname = f'fila.retry.{i}'
    ch.queue_declare(queue=qname, durable=True, arguments={
        'x-dead-letter-exchange': 'exchange.transacoes',  # após TTL retorna para exchange principal
        'x-message-ttl': ttl
    })
    ch.queue_bind(exchange='exchange.retry', queue=qname, routing_key=f'retry.{i}')

# Bindings da exchange.transacoes
# routing keys exemplo: transacao.*  (você pode usar transacao.transferencia, transacao.deposito etc)
ch.queue_bind(exchange='exchange.transacoes', queue='fila.transacoes', routing_key='transacao.#')
ch.queue_bind(exchange='exchange.transacoes', queue='fila.auditoria', routing_key='transacao.#')
ch.queue_bind(exchange='exchange.transacoes', queue='fila.notificacoes', routing_key='transacao.#')

print("Topology created.")
conn.close()
