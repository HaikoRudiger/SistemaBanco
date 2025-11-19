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

# =====================================================================================
# CONFIGURAÇÕES GLOBAIS
# =====================================================================================

NODE_ID = random.randint(1000, 9999)  # usado no Bully (quanto maior, mais forte)
SERVICE_ID = os.getenv("SERVICE_ID", f"svc-{NODE_ID}")
START_TS = time.time()

API_KEY = os.getenv("CURRENCYAPI_KEY")
if not API_KEY:
    raise RuntimeError("CURRENCYAPI_KEY não definido no .env")

_fx_client = currencyapicom.Client(API_KEY)
_FX_CACHE = {}
_FX_TTL = 60

MAX_RETRIES = 3

AVAILABLE_PORTS = [5501, 5502, 5503, 5504, 5505]
HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 1
FAIL_THRESHOLD = 3


def uptime():
    return int(time.time() - START_TS)


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


# =====================================================================================
# FX CONVERSION
# =====================================================================================

def fx_rate(origem, destino):
    if origem.upper() == destino.upper():
        return 1.0

    key = (origem, destino)
    now = time.time()

    if key in _FX_CACHE and now - _FX_CACHE[key][1] < _FX_TTL:
        return _FX_CACHE[key][0]

    resp = _fx_client.latest()
    data = resp["data"]

    usd_to = 1.0 if destino == "USD" else float(data[destino]["value"])
    usd_from = 1.0 if origem == "USD" else float(data[origem]["value"])

    rate = usd_to / usd_from
    _FX_CACHE[key] = (rate, now)
    return rate


# =====================================================================================
# PROCESSAMENTO REAL (BANCO + CRIPTOGRAFIA)
# =====================================================================================

def processar_operacao(payload):
    try:
        conta_origem = int(payload["conta_origem"])
        conta_destino = int(payload["conta_destino"])
        valor = float(payload["valor_convertido"])

        origem = obter_conta(conta_origem)
        destino = obter_conta(conta_destino)

        if origem is None:
            raise ValueError("Conta origem inexistente")

        if destino is None:
            raise ValueError("Conta destino inexistente")

        saldo_origem = float(origem[2])
        saldo_destino = float(destino[2])

        if saldo_origem < valor:
            raise ValueError("Saldo insuficiente")

        atualizar_saldo(conta_origem, saldo_origem - valor)
        atualizar_saldo(conta_destino, saldo_destino + valor)

        valor_cript = encrypt_value(valor)
        registrar_transacao(conta_origem, conta_destino, valor_cript, "transferencia")

        return True

    except sqlite3.Error as e:
        raise RuntimeError(f"ErroBD: {e}")
    except Exception as e:
        raise ValueError(str(e))

# =====================================================================================
# WORKER (NÃO É O LÍDER)
# =====================================================================================

def worker_consume():
    conn_w, ch_w = get_channel()
    ch_w.basic_qos(prefetch_count=1)

    def cb(ch, method, props, body):
        try:
            data = json.loads(body)
        except:
            ch.basic_publish(exchange="exchange.dlx", routing_key="", body=body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        headers = props.headers or {}
        retries = headers.get("x-retries", 0)

        try:
            taxa = fx_rate(data["moeda"], "USD")
            convertido = float(data["valor"]) * taxa

            data["valor_convertido"] = round(convertido, 6)
            data["moeda_base"] = "USD"
            data["fx_rate"] = taxa

            publicar(ch, "audit.pre", {
                "evento": "pre-processamento",
                "id": data["id"],
                "servico": SERVICE_ID,
                "ts": datetime.now(timezone.utc).isoformat()
            })

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
                        delivery_mode=2,
                        content_type="application/json",
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
    print(f"[{SERVICE_ID}] Worker ON")
    ch_w.start_consuming()


# =====================================================================================
# LÍDER
# =====================================================================================

def leader_consume():
    conn_l, ch_l = get_channel()
    ch_l.basic_qos(prefetch_count=1)

    def cb(ch, method, props, body):
        data = json.loads(body)

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
                delivery_mode=2,
                headers=props.headers or {}
            )
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    try:
        ch_l.basic_consume(
            queue="fila.transacoes",
            on_message_callback=cb,
            exclusive=True
        )
        print(f"[{SERVICE_ID}] SOU O LÍDER (ID {NODE_ID})")
        ch_l.start_consuming()

    except pika.exceptions.ChannelClosedByBroker:
        print(f"[{SERVICE_ID}] Não virei líder")

# =====================================================================================
# BULLY ALGORITHM
# =====================================================================================

current_leader = None
election_in_progress = False

def send_election(ch):
    global election_in_progress
    election_in_progress = True
    msg = {
        "type": "ELECTION",
        "node_id": NODE_ID
    }
    ch.basic_publish(exchange="exchange.election", routing_key="", body=json.dumps(msg))
    print(f"[{SERVICE_ID}] → ELECTION enviada")


def send_ok(ch, target):
    msg = {"type": "OK", "node_id": NODE_ID, "to": target}
    ch.basic_publish(exchange="exchange.election", routing_key="", body=json.dumps(msg))
    print(f"[{SERVICE_ID}] → OK para {target}")


def send_coordinator(ch):
    global current_leader, election_in_progress
    current_leader = NODE_ID
    election_in_progress = False
    msg = {"type": "COORDINATOR", "node_id": NODE_ID}
    ch.basic_publish(exchange="exchange.election", routing_key="", body=json.dumps(msg))
    print(f"[{SERVICE_ID}] → COORDINATOR enviado (eu sou o líder)")


# =====================================================================================
# BULLETIN LISTENER
# =====================================================================================

def bully_listener():
    conn, ch = get_channel()
    result = ch.queue_declare(queue="", exclusive=True)
    qname = result.method.queue

    ch.queue_bind(exchange="exchange.election", queue=qname)

    def cb(ch, method, props, body):
        global current_leader, election_in_progress

        msg = json.loads(body)
        msg_type = msg["type"]
        sender = msg["node_id"]

        if msg_type == "ELECTION":
            if sender < NODE_ID:
                send_ok(ch, sender)
                send_election(ch)

        elif msg_type == "OK":
            election_in_progress = False

        elif msg_type == "COORDINATOR":
            current_leader = sender
            election_in_progress = False
            print(f"[{SERVICE_ID}] → líder atualizado para {sender}")

    ch.basic_consume(queue=qname, on_message_callback=cb, auto_ack=True)
    print(f"[{SERVICE_ID}] Bully Listener ativo")
    ch.start_consuming()


# =====================================================================================
# HEARTBEAT
# =====================================================================================

def heartbeat_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    my_port = None
    for p in AVAILABLE_PORTS:
        try:
            s.bind(("0.0.0.0", p))
            my_port = p
            break
        except OSError:
            continue  # porta ocupada → tenta a próxima

    if my_port is None:
        raise RuntimeError("Nenhuma porta disponível para heartbeat!")

    print(f"[{SERVICE_ID}] HB server porta {my_port}")
    s.listen(5)

    def loop():
        while True:
            try:
                conn, _ = s.accept()
                msg = conn.recv(1024)
                if msg.strip() == b"PING":
                    conn.sendall(b"PONG\n")
            except:
                pass
            finally:
                conn.close()

    threading.Thread(target=loop, daemon=True).start()
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
                    resp = sock.recv(100)
                    if resp.strip() == b"PONG":
                        falhas[p] = 0
                        continue
                    raise Exception()
            except:
                falhas[p] = falhas.get(p, 0) + 1
                if falhas[p] == FAIL_THRESHOLD:
                    print(f"[{SERVICE_ID}] DETECTADO NÓ {p} CAÍDO → iniciando Bully")
                    conn, ch = get_channel()
                    send_election(ch)

        time.sleep(HEARTBEAT_INTERVAL)


# =====================================================================================
# ELECTION LOOP
# =====================================================================================

def election_loop():
    threading.Thread(target=worker_consume, daemon=True).start()
    threading.Thread(target=bully_listener, daemon=True).start()

    my_port = heartbeat_server()
    threading.Thread(target=heartbeat_client, args=(my_port,), daemon=True).start()

    while True:
        try:
            leader_consume()
        except pika.exceptions.ChannelClosedByBroker:
            pass
        time.sleep(random.uniform(2, 4))


# =====================================================================================
# MAIN
# =====================================================================================

if __name__ == "__main__":
    print(f"[{SERVICE_ID}] inicializado (Bully + FX + DB + Cripto + Heartbeat)")
    election_loop()

