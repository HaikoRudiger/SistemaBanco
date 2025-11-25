import json
from connection import get_channel
from db import registrar_auditoria

conn, ch = get_channel()

def print_banner(title):
    print("\n" + "=" * 60)
    print(f"{title}")
    print("=" * 60)

def imprimir_auditoria(data):
    evento = data.get("evento")

    print_banner("📘 EVENTO DE AUDITORIA")

    print(f"ID da Transação : {data.get('id')}")
    print(f"Evento          : {evento}")
    print(f"Serviço         : {data.get('servico')}")
    print(f"Timestamp       : {data.get('ts')}")

    # Recebido pelo Líder
    if evento == "recebido-lider":
        print(f"Líder           : {data.get('lider')}")

    # Pré-processamento
    if evento == "pre-processamento":
        print(f"Valor Origem    : {data.get('valor_origem')} {data.get('moeda_origem')}")
        print(f"Valor Destino   : {data.get('valor_destino')} {data.get('moeda_destino')}")
        print(f"Taxa FX         : {data.get('fx_rate')}")
        print(f"Moeda Prov.     : {data.get('fx_provider')}")

    # Pós-processamento
    if evento == "pos-processamento":
        print("Status          : PROCESSADO ✔️")

    # Retry
    if evento == "retry":
        print_banner("RETRY")
        print(f"Tentativa       : {data.get('tentativa')}")
        print(f"Próxima fila    : {data.get('proxima_fila')}")
        print(f"Próximo TTL(ms) : {data.get('proximo_ttl_ms')}")
        print(f"Erro original   : {data.get('erro')}")
        print("=" * 60)

    # Falha lógica
    if evento == "falha-logica":
        print_banner("FALHA LÓGICA (DLQ DIRETO)")
        print(f"Erro            : {data.get('erro')}")
        print("Ação           : Enviado para fila DLQ")

    # Falha definitiva
    if evento == "falha-banco-definitiva":
        print_banner("FALHA DEFINITIVA APÓS RETRY")
        print(f"Tentativas      : {data.get('tentativas')}")
        print(f"Erro            : {data.get('erro')}")
        print("Ação           : DLQ após esgotar tentativas")

    print("=" * 60 + "\n")


def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        evento = data.get("evento")

        # NÃO SALVAR NO BANCO quando o DB pode estar travado
        if evento in ("retry", "falha-logica", "falha-banco-definitiva"):
            imprimir_auditoria(data)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # ✔ Eventos seguros — pode salvar no SQLite
        registrar_auditoria(data)
        imprimir_auditoria(data)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print("ERRO NO CONSUMIDOR DE AUDITORIA:", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


ch.basic_qos(prefetch_count=5)
ch.basic_consume(queue="fila.auditoria", on_message_callback=callback)

print(">>> Auditoria iniciada (com salvamento seguro) <<<")
ch.start_consuming()
