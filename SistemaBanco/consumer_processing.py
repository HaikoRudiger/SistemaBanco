# consumer_processing.py
import json
import os
import time
import random
import threading
import socket
import sqlite3
from datetime import datetime, timezone

import pika
import currencyapicom

from connection import get_channel
from db import obter_conta, atualizar_saldo, registrar_transacao
from crypto_utils import encrypt_value

# -------------------------
# Configurações Globais
# -------------------------
MAX_RETRIES = 3

SERVICE_ID = os.getenv("SERVICE_ID", f"svc-{random.randint(1000,9999)}")
START_TS = time.time()

def uptime():
    return int(time.time() - START_TS)

# CurrencyAPI
API_KEY = os.getenv("CURRENCYAPI_KEY")
if not API_KEY:
    raise RuntimeError("CURRENCYAPI_KEY não definido no .env")

_fx_client = currencyapicom.Client(API_KEY)
_FX_CACHE = {}
_FX_TTL = 60

# Heartbeat
AVAILABLE_PORTS = [5001, 5002, 5003, 5004]
HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 1
FAIL_THRESHOLD = 3


# -------------------------
# Publicação RabbitMQ
# -------------------------
def publicar(ch, rk, payload, headers=None):
    ch.basic_publish(
        exchange="exchange.principal",
        routing_key=rk,
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            headers=headers or {}
        )
    )


# -------------------------
# FX (plano free)
# -------------------------
def fx_rate(from_currency, to_currency):
    f = from_currency.upper()
    t = to_currency.upper()

    if f == t:
        return 1.0

    key = (f, t)
    now = time.time()

    cached = _FX_CACHE.get(key)
    if cached and now - cached[1] < _FX_TTL:
        return cached[0]

    resp = _fx_client.latest()
    data = resp["data"]

    usd_to = 1.0 if t == "USD" else float(data[t]["value"])
    usd_from = 1.0 if f == "USD" else float(data[f]["value"])
    rate = usd_to / usd_from

    _FX_CACHE[key] = (rate, now)
    return rate


# -------------------------
# Processamento real (Banco + Criptografia)
# -------------------------
def processar_operacao(payload):
    try:
        conta_origem = int(payload["conta_origem"])
        conta_destino = int(payload["conta_destino"])
        valor = float(payload["valor_convertido"])

        origem = obter_conta(conta_origem)
        destino = obter_conta(conta_destino)

        if origem is None:
            raise ValueError("Conta de origem inexistente")

        if destino is None:
            raise ValueError("Conta de destino inexistente")

        saldo_origem = float(origem[2])
        saldo_destino = float(destino[2])

        if saldo_origem < valor:
            raise ValueError("Saldo insuficiente")

        # Debita e Credita
        atualizar_saldo(conta_origem, saldo_origem - valor)
        atualizar_saldo(conta_destino, saldo_destino + valor)

        # Criptografar valor para banco
        valor_cript = encrypt_value(valor)

        registrar_transacao(conta_origem, conta_destino, valor_cript, "transferencia")

        return True

    except sqlite3.Error as e:
        raise RuntimeError(f"ErroBD: {e}")  # retry
    except Exception as e:
        raise ValueError(str(e))  # DLQ


# -------------------------
# Worker
# -------------------------
def worker_consume():
    conn_w, ch_w = get_channel()
    ch_w.basic_qos(prefetch_count=1)

    def cb(ch, method, properties, body):
        try:
            data = json.loads(body)
        except:
            ch.basic_publish(exchange="exchange.dlx", routing_key="", body=body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        headers = properties.headers or {}
        retries = int(headers.get("x-retries", 0))

        # FX
        try:
            valor_original = float(data["valor"])
            moeda_origem = data["moeda"]
            moeda_base = os.getenv("CURRENCY_BASE", "USD")

            taxa = fx_rate(moeda_origem, moeda_base)
            valor_conv = valor_original * taxa

            data["valor_convertido"] = round(valor_conv, 6)
            data["moeda_base"] = moeda_base
            data["fx_rate"] = taxa

        except Exception as e:
            publicar(ch, "audit.falha", {
                "evento": "falha-fx",
                "id": data["id"],
                "erro": str(e),
                "ts": datetime.now(timezone.utc).isoformat()
            })
            ch.basic_publish(exchange="exchange.dlx", routing_key="", body=json.dumps(data))
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # auditoria pre
        publicar(ch, "audit.pre", {
            "evento": "pre-processamento",
            "id": data["id"],
            "servico": SERVICE_ID,
            "valor_convertido": data["valor_convertido"],
            "moeda_base": data["moeda_base"],
            "ts": datetime.now(timezone.utc).isoformat()
        })

        # exec
        try:
            processar_operacao(data)

            publicar(ch, "audit.post", {
                "evento": "pos-processamento",
                "id": data["id"],
                "servico": SERVICE_ID,
                "ts": datetime.now(timezone.utc).isoformat()
            })

            data["status"] = "SUCESSO"
            publicar(ch, "notify.transacao", data)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except RuntimeError as e:
            retries += 1

            if retries > MAX_RETRIES:
                publicar(ch, "audit.falha", {
                    "evento": "falha-banco-definitiva",
                    "id": data["id"],
                    "erro": str(e),
                    "ts": datetime.now(timezone.utc).isoformat()
                })
                ch.basic_publish(exchange="exchange.dlx", routing_key="", body=json.dumps(data))
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                rk = f"retry.{retries}"
                ch.basic_publish(
                    exchange="exchange.retry",
                    routing_key=rk,
                    body=json.dumps(data),
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=2,
                        headers={"x-retries": retries}
                    )
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)

        except ValueError as e:
            publicar(ch, "audit.falha", {
                "evento": "falha-logica",
                "id": data["id"],
                "erro": str(e),
                "ts": datetime.now(timezone.utc).isoformat()
            })
            ch.basic_publish(exchange="exchange.dlx", routing_key="", body=json.dumps(data))
            ch.basic_ack(delivery_tag=method.delivery_tag)

    ch_w.basic_consume(queue="fila.cluster.work", on_message_callback=cb)
    print(f"[{SERVICE_ID}] Worker ON consumindo fila.cluster.work")
    ch_w.start_consuming()

# -------------------------
# Líder (exclusive consumer)
# -------------------------
def leader_consume():
    conn_l, ch_l = get_channel()
    ch_l.basic_qos(prefetch_count=1)

    def cb(ch, method, properties, body):
        try:
            data = json.loads(body)
        except:
            ch.basic_publish(exchange="exchange.dlx", routing_key="", body=body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        publicar(ch, "audit.recebido_lider", {
            "evento": "recebido-lider",
            "id": data["id"],
            "lider": SERVICE_ID,
            "ts": datetime.now(timezone.utc).isoformat()
        })

        ch.basic_publish(
            exchange="exchange.cluster",
            routing_key="work",
            body=json.dumps(data),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                headers=properties.headers or {}
            )
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    try:
        ch_l.basic_consume(
            queue="fila.transacoes",
            on_message_callback=cb,
            exclusive=True
        )
        print(f"[{SERVICE_ID}] >>> LÍDER ELEITO")
        ch_l.start_consuming()
    finally:
        try: conn_l.close()
        except: pass

# -------------------------
# HEARTBEAT TCP
# -------------------------
def start_heartbeat_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    my_port = None
    for p in AVAILABLE_PORTS:
        try:
            s.bind(("0.0.0.0", p))
            my_port = p
            break
        except:
            pass

    if my_port is None:
        raise RuntimeError("Sem portas livres")

    s.listen(5)

    def loop():
        while True:
            conn, _ = s.accept()
            try:
                msg = conn.recv(1024)
                if msg.strip() == b"PING":
                    conn.sendall(b"PONG\n")
            except:
                pass
            finally:
                conn.close()

    threading.Thread(target=loop, daemon=True).start()
    print(f"[{SERVICE_ID}][HB] Servidor HB porta {my_port}")
    return my_port


def heartbeat_client(my_port):
    falhas = {}

    while True:
        for p in AVAILABLE_PORTS:
            if p == my_port:
                continue

            try:
                with socket.create_connection(("127.0.0.1", p), timeout=HEARTBEAT_TIMEOUT) as sock:
                    sock.sendall(b"PING\n")
                    resp = sock.recv(1024)
                    if resp.strip() == b"PONG":
                        falhas[p] = 0
                        print(f"[{SERVICE_ID}][HB] PING OK -> {p}")
                    else:
                        raise Exception()
            except:
                falhas[p] = falhas.get(p, 0) + 1
                print(f"[{SERVICE_ID}][HB] Falha #{falhas[p]} porta {p}")
                if falhas[p] == FAIL_THRESHOLD:
                    print(f"[{SERVICE_ID}][HB] Nó {p} OFFLINE")

        time.sleep(HEARTBEAT_INTERVAL)


# -------------------------
# ELEIÇÃO
# -------------------------
def election_loop():
    threading.Thread(target=worker_consume, daemon=True).start()

    my_port = start_heartbeat_server()
    threading.Thread(target=heartbeat_client, args=(my_port,), daemon=True).start()

    while True:
        try:
            leader_consume()
        except pika.exceptions.ChannelClosedByBroker:
            print(f"[{SERVICE_ID}] Outro líder ativo → permaneço worker")
        except Exception as e:
            print(f"[{SERVICE_ID}] Erro líder: {e}")

        time.sleep(random.uniform(2, 4))


# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    print(f"[{SERVICE_ID}] Iniciando (ELEIÇÃO + FX + DB + CRIPTO + HEARTBEAT)")
    election_loop()
