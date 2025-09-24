from connection import get_channel

conn, ch = get_channel()

# Exchanges
ch.exchange_declare(exchange='exchange.transacoes', exchange_type='topic', durable=True)
ch.exchange_declare(exchange='exchange.retry', exchange_type='direct', durable=True)
ch.exchange_declare(exchange='exchange.dlx', exchange_type='fanout', durable=True)

# Filas principais
ch.queue_declare(queue='fila.transacoes', durable=True, arguments={
    'x-dead-letter-exchange': 'exchange.retry'
})
ch.queue_declare(queue='fila.auditoria', durable=True)
ch.queue_declare(queue='fila.notificacoes', durable=True)

# DLQ
ch.queue_declare(queue='fila.dlq', durable=True)
ch.queue_bind(exchange='exchange.dlx', queue='fila.dlq')

# Retry queues com TTL (30s, 60s, 120s)
retry_ttls = [3000, 6000, 12000]  # em ms
for i, ttl in enumerate(retry_ttls, start=1):
    qname = f'fila.retry.{i}'
    ch.queue_declare(queue=qname, durable=True, arguments={
        'x-dead-letter-exchange': 'exchange.transacoes',      # volta pro fluxo normal
        'x-dead-letter-routing-key': 'transacao.transferencia',  # garante roteamento correto
        'x-message-ttl': ttl
    })
    ch.queue_bind(exchange='exchange.retry', queue=qname, routing_key=f'retry.{i}')

# Bindings da exchange.transacoes
ch.queue_bind(exchange='exchange.transacoes', queue='fila.transacoes', routing_key='transacao.#')
ch.queue_bind(exchange='exchange.transacoes', queue='fila.auditoria', routing_key='transacao.#')
ch.queue_bind(exchange='exchange.transacoes', queue='fila.notificacoes', routing_key='transacao.#')

print("Topology created.")
conn.close()
