from connection import get_channel

conn, ch = get_channel()

# -------------------------
# Exchanges
# -------------------------

ch.exchange_declare(exchange='exchange.principal', exchange_type='topic', durable=True)
ch.exchange_declare(exchange='exchange.retry', exchange_type='direct', durable=True)
ch.exchange_declare(exchange='exchange.dlx', exchange_type='fanout', durable=True)
ch.exchange_declare(exchange='exchange.cluster', exchange_type='direct', durable=True)

# -------------------------
# Filas principais
# -------------------------

ch.queue_declare(
    queue='fila.transacoes',
    durable=True,
    arguments={'x-dead-letter-exchange': 'exchange.retry'}
)

ch.queue_declare(queue='fila.auditoria', durable=True)
ch.queue_declare(queue='fila.notificacoes', durable=True)
ch.queue_declare(queue='fila.dlq', durable=True)

# Fila de trabalho do cluster (líder distribui; workers consomem)
ch.queue_declare(queue='fila.cluster.work', durable=True)
ch.queue_bind(
    exchange='exchange.cluster',
    queue='fila.cluster.work',
    routing_key='work'
)

# -------------------------
# Bindings do principal
# -------------------------

ch.queue_bind(
    exchange='exchange.principal',
    queue='fila.transacoes',
    routing_key='transacao.#'
)

ch.queue_bind(
    exchange='exchange.principal',
    queue='fila.auditoria',
    routing_key='audit.#'
)

ch.queue_bind(
    exchange='exchange.principal',
    queue='fila.notificacoes',
    routing_key='notify.#'
)

# -------------------------
# DLQ
# -------------------------

ch.queue_bind(
    exchange='exchange.dlx',
    queue='fila.dlq'
)

# -------------------------
# Retries progressivos (3 tentativas)
# -------------------------

retry_ttls = [3000, 6000, 12000]

for i, ttl in enumerate(retry_ttls, start=1):
    qname = f'fila.retry.{i}'

    ch.queue_declare(
        queue=qname,
        durable=True,
        arguments={
            'x-dead-letter-exchange': 'exchange.principal',
            'x-dead-letter-routing-key': 'transacao.transferencia',
            'x-message-ttl': ttl,
        }
    )

    ch.queue_bind(
        exchange='exchange.retry',
        queue=qname,
        routing_key=f'retry.{i}'
    )

print("Topologia Criada")
conn.close()
