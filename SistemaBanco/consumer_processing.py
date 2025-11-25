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
from db import (
    obter_conta,
    atualizar_saldo,
    registrar_transacao,
    obter_codigo_moeda_conta,
)
from crypto_utils import encrypt_value

# CONFIGURAÇÃO GLOBAL
MAX_RETRIES = 3

# TTLs configurados nas filas de retry (ms)
RETRY_TTLS_MS = {
    1: 3000,
    2: 6000,
    3: 12000,
}

RAW_ID = random.randint(1000, 9999)
SERVICE_ID = f"svc-{RAW_ID}"
NODE_ID = RAW_ID  # usado pelo algoritmo Bully

START_TS = time.time()

def uptime():
    return int(time.time() - START_TS)

# CurrencyAPI – Conversão de moedas
API_KEY = os.getenv("CURRENCYAPI_KEY")
if not API_KEY:
    raise RuntimeError("CURRENCYAPI_KEY não definido no .env")

_fx_client = currencyapicom.Client(API_KEY)
_FX_CACHE = {}
_FX_TTL_SECONDS = 60


def _fx_get_rate(from_currency: str, to_currency: str) -> float:
    """
    Converte via USD → plano free da API.
    Fórmula:
      taxa(from→to) = taxa(USD→to) / taxa(USD→from)
    """
    f = from_currency.upper()
    t = to_currency.upper()

    if f == t:
        return 1.0

    key = (f, t)
    now = time.time()

    # cache
    cached = _FX_CACHE.get(key)
    if cached and (now - cached[1]) < _FX_TTL_SECONDS:
        return cached[0]

    resp = _fx_client.latest()
    data = resp["data"]

    usd_to = 1.0 if t == "USD" else float(data[t]["value"])
    usd_from = 1.0 if f == "USD" else float(data[f]["value"])

    rate = usd_to / usd_from

    _FX_CACHE[key] = (rate, now)
    return rate


def converter_valor(valor: float, moeda_origem: str, moeda_destino: str) -> tuple[float, float]:
    """
    Conversão real usando moeda das CONTAS no DB.
    """
    if moeda_origem.upper() == moeda_destino.upper():
        return valor, 1.0

    rate = _fx_get_rate(moeda_origem, moeda_destino)
    return valor * rate, rate



# PROCESSAMENTO REAL (DB + Criptografia)
def processar_operacao(payload):
    """
    Debita valor_original na conta origem.
    Credita valor_destino (convertido) na conta destino.
    Valor creditado é criptografado para o banco.
    """
    try:
        conta_origem = int(payload["conta_origem"])
        conta_destino = int(payload["conta_destino"])

        valor_origem = float(payload["valor"])
        valor_destino = float(payload["valor_destino"])

        origem = obter_conta(conta_origem)
        destino = obter_conta(conta_destino)

        if origem is None:
            raise ValueError("Conta de origem inexistente")
        if destino is None:
            raise ValueError("Conta de destino inexistente")

        saldo_origem = float(origem[2])
        saldo_destino = float(destino[2])

        if saldo_origem < valor_origem:
            raise ValueError("Saldo insuficiente na conta de origem")

        # Debitar e creditar
        novo_saldo_origem = saldo_origem - valor_origem
        novo_saldo_destino = saldo_destino + valor_destino

        atualizar_saldo(conta_origem, novo_saldo_origem)
        atualizar_saldo(conta_destino, novo_saldo_destino)

        # Criptografar apenas o valor depositado (destino)
        valor_criptografado = encrypt_value(valor_destino)
        registrar_transacao(
            conta_origem, conta_destino, valor_criptografado, "transferencia"
        )

        return True

    # Erros de infraestrutura (BD travado, locked, busy, etc.)
    except (sqlite3.Error, sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        raise RuntimeError(f"ErroBD: {e}")  # RETRY
    except Exception as e:
        # erro de regra → DLQ
        raise ValueError(str(e))



# FUNCIONALIDADE DE PUBLICAÇÃO NO RABBITMQ
def publicar(ch, rk, payload, headers=None):
    ch.basic_publish(
        exchange="exchange.principal",
        routing_key=rk,
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            headers=headers or {},
        ),
    )



# WORKER – CONSUMO DA FILA DE TRABALHO
def worker_consume():
    conn_w, ch_w = get_channel()
    ch_w.basic_qos(prefetch_count=1)

    def cb(ch, method, props, body):
        try:
            data = json.loads(body)
        except Exception:
            ch.basic_publish(exchange="exchange.dlx", routing_key="", body=body)
            ch.basic_ack(method.delivery_tag)
            return

        headers = props.headers or {}
        retries = headers.get("x-retries", 0)

        try:
            # ----------------------------------------
            # FX baseado nas MOEDAS das CONTAS
            # ----------------------------------------
            conta_origem = int(data["conta_origem"])
            conta_destino = int(data["conta_destino"])

            valor_original = float(data["valor"])

            moeda_origem = obter_codigo_moeda_conta(conta_origem)
            moeda_destino = obter_codigo_moeda_conta(conta_destino)

            valor_destino, fx_rate = converter_valor(
                valor_original, moeda_origem, moeda_destino
            )

            # Enriquecer payload
            data["moeda_origem"] = moeda_origem
            data["moeda_destino"] = moeda_destino
            data["valor_destino"] = round(valor_destino, 6)
            data["fx_rate"] = fx_rate
            data["fx_provider"] = "CurrencyAPI"
            data["fx_at"] = datetime.now(timezone.utc).isoformat()

            # Auditoria pré-processamento
            publicar(
                ch,
                "audit.pre",
                {
                    "evento": "pre-processamento",
                    "id": data["id"],
                    "servico": SERVICE_ID,
                    "valor_origem": valor_original,
                    "moeda_origem": moeda_origem,
                    "valor_destino": data["valor_destino"],
                    "moeda_destino": moeda_destino,
                    "fx_rate": fx_rate,
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
            )

            # PROCESSAMENTO REAL
            processar_operacao(data)

            # Auditoria pós
            publicar(
                ch,
                "audit.post",
                {
                    "evento": "pos-processamento",
                    "id": data["id"],
                    "servico": SERVICE_ID,
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
            )

            # Enviar notificação
            data["status"] = "SUCESSO"
            publicar(ch, "notify.transacao", data)

            ch.basic_ack(method.delivery_tag)

        except RuntimeError as e:
            # Erro de BD → RETRY / DLQ
            retries += 1

            if retries > MAX_RETRIES:
                # falha definitiva (não tenta mais)
                publicar(
                    ch,
                    "audit.falha",
                    {
                        "evento": "falha-banco-definitiva",
                        "id": data.get("id"),
                        "erro": str(e),
                        "tentativas": retries - 1,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    },
                )
                ch.basic_publish(
                    exchange="exchange.dlx",
                    routing_key="",
                    body=json.dumps(data),
                )
                ch.basic_ack(method.delivery_tag)
            else:
                # tentativa de retry com TTL
                ttl_ms = RETRY_TTLS_MS.get(retries)
                rk = f"retry.{retries}"

                publicar(
                    ch,
                    "audit.retry",
                    {
                        "evento": "retry",
                        "id": data.get("id"),
                        "erro": str(e),
                        "tentativa": retries,
                        "proxima_fila": f"fila.retry.{retries}",
                        "proximo_ttl_ms": ttl_ms,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    },
                )

                ch.basic_publish(
                    exchange="exchange.retry",
                    routing_key=rk,
                    body=json.dumps(data),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type="application/json",
                        headers={"x-retries": retries},
                    ),
                )
                ch.basic_ack(method.delivery_tag)

        except Exception as e:
            # Erro lógico → DLQ
            publicar(
                ch,
                "audit.falha",
                {
                    "evento": "falha-logica",
                    "id": data.get("id"),
                    "erro": str(e),
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
            )
            ch.basic_publish(
                exchange="exchange.dlx",
                routing_key="",
                body=json.dumps(data),
            )
            ch.basic_ack(method.delivery_tag)

    ch_w.basic_consume(queue="fila.cluster.work", on_message_callback=cb)
    print(f"[{SERVICE_ID}] Worker ON")
    ch_w.start_consuming()


# ==========================================================
# LÍDER – EXCLUSIVE CONSUMER
# ==========================================================
# variáveis globais para o Bully + heartbeat
AVAILABLE_PORTS = [5501, 5502, 5503, 5504, 5505]
HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 1
FAIL_THRESHOLD = 3

my_hb_port = None          # porta TCP desta instância
current_leader_id = None   # ID lógico do líder (NODE_ID)
current_leader_port = None # porta de heartbeat do líder
election_in_progress = False


def leader_consume():
    global current_leader_id, current_leader_port

    conn_l, ch_l = get_channel()
    ch_l.basic_qos(prefetch_count=1)

    def cb(ch, method, props, body):
        try:
            data = json.loads(body)
        except Exception:
            ch.basic_publish(exchange="exchange.dlx", routing_key="", body=body)
            ch.basic_ack(method.delivery_tag)
            return

        publicar(
            ch,
            "audit.recebido_lider",
            {
                "evento": "recebido-lider",
                "id": data["id"],
                "lider": SERVICE_ID,
                "ts": datetime.now(timezone.utc).isoformat(),
            },
        )

        ch.basic_publish(
            exchange="exchange.cluster",
            routing_key="work",
            body=json.dumps(data),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                headers=props.headers or {},
            ),
        )
        ch.basic_ack(method.delivery_tag)

    try:
        ch_l.basic_consume(
            queue="fila.transacoes",
            on_message_callback=cb,
            exclusive=True,
        )
        # Se chegamos aqui, viramos líder de fato
        current_leader_id = NODE_ID
        current_leader_port = my_hb_port
        print(f"[{SERVICE_ID}] AGORA SOU O LÍDER (ID {NODE_ID})")

        # Anuncia via Bully que somos coordenador
        send_coordinator(ch_l)

        ch_l.start_consuming()
    finally:
        try:
            conn_l.close()
        except Exception:
            pass


# ==========================================================
# HEARTBEAT TCP – monitor de atividade das nodes
# ==========================================================
def heartbeat_server():
    """
    Cada instância pega uma porta exclusiva da lista AVAILABLE_PORTS.
    """
    global my_hb_port

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    chosen = None
    for p in AVAILABLE_PORTS:
        try:
            s.bind(("0.0.0.0", p))
            chosen = p
            break
        except OSError:
            continue

    if chosen is None:
        raise RuntimeError("Nenhuma porta HB disponível")

    my_hb_port = chosen

    s.listen(5)
    print(f"[{SERVICE_ID}] HB server porta {my_hb_port}")

    def loop():
        while True:
            try:
                conn, _ = s.accept()
                msg = conn.recv(1024)
                if msg.strip() == b"PING":
                    conn.sendall(b"PONG\n")
            except Exception:
                pass
            finally:
                conn.close()

    threading.Thread(target=loop, daemon=True).start()
    return my_hb_port


def heartbeat_client(my_port):
    """
    Pinga todas as portas conhecidas.
    Só dispara eleição Bully quando:
      - temos líder conhecido (current_leader_port)
      - a porta que falhou é a porta do líder.
    """
    global current_leader_port

    falhas = {}

    while True:
        for p in AVAILABLE_PORTS:
            if p == my_port:
                continue

            try:
                with socket.create_connection(("127.0.0.1", p), timeout=1.0) as sock:
                    sock.sendall(b"PING\n")
                    resp = sock.recv(1024)
                    if resp.strip() == b"PONG":
                        falhas[p] = 0
                    else:
                        raise Exception()
            except Exception:
                falhas[p] = falhas.get(p, 0) + 1

                if falhas[p] == FAIL_THRESHOLD:
                    # Só inicia eleição se for o LÍDER que caiu
                    if current_leader_port is not None and p == current_leader_port:
                        print(
                            f"[{SERVICE_ID}] DETECTADO LÍDER NA PORTA {p} CAÍDO → iniciando eleição Bully"
                        )
                        iniciar_bully()
                    else:
                        print(
                            f"[{SERVICE_ID}] Nó comum na porta {p} considerado offline (sem eleição)"
                        )

        time.sleep(HEARTBEAT_INTERVAL)


# ==========================================================
# ALGORITMO BULLY (mensagens ELECTION / OK / COORDINATOR)
# ==========================================================
def send_election(ch):
    global election_in_progress
    election_in_progress = True

    payload = {
        "type": "ELECTION",
        "node_id": NODE_ID,
        "service": SERVICE_ID,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    ch.basic_publish(
        exchange="exchange.election", routing_key="", body=json.dumps(payload)
    )
    print(f"[{SERVICE_ID}] → ELECTION enviada")


def send_ok(ch, target_id):
    payload = {
        "type": "OK",
        "node_id": NODE_ID,
        "to": target_id,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    ch.basic_publish(
        exchange="exchange.election", routing_key="", body=json.dumps(payload)
    )
    print(f"[{SERVICE_ID}] → OK para {target_id}")


def send_coordinator(ch):
    """
    Anuncia que ESTE nó é o novo coordenador.
    Inclui também a porta de heartbeat.
    """
    global election_in_progress, current_leader_id, current_leader_port

    current_leader_id = NODE_ID
    current_leader_port = my_hb_port
    election_in_progress = False

    payload = {
        "type": "COORDINATOR",
        "node_id": NODE_ID,
        "service": SERVICE_ID,
        "port": my_hb_port,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    ch.basic_publish(
        exchange="exchange.election", routing_key="", body=json.dumps(payload)
    )
    print(f"[{SERVICE_ID}] → SOU O NOVO LÍDER (porta {my_hb_port})")


def iniciar_bully():
    """
    Dispara uma nova eleição via exchange.election.
    Evita iniciar se já existe uma eleição em progresso.
    """
    global election_in_progress
    if election_in_progress:
        return

    conn, ch = get_channel()
    try:
        send_election(ch)
    finally:
        conn.close()


def bully_consumer():
    """
    Escuta as mensagens do algoritmo Bully:
      - ELECTION
      - OK
      - COORDINATOR
    """
    global current_leader_id, current_leader_port, election_in_progress

    conn, ch = get_channel()

    # Garante que o exchange exista com mesmo tipo do setup_topology
    ch.exchange_declare(exchange="exchange.election", exchange_type="fanout", durable=True)

    result = ch.queue_declare(queue="", exclusive=True)
    qname = result.method.queue
    ch.queue_bind(exchange="exchange.election", queue=qname)

    def cb(ch, method, props, body):
        global current_leader_id, current_leader_port, election_in_progress

        try:
            msg = json.loads(body)
        except Exception:
            return

        tipo = msg.get("type")
        remetente = msg.get("node_id")

        if tipo == "ELECTION":
            # Regra do Bully:
            # se eu tenho ID MAIOR do que quem pediu eleição,
            # respondo OK e inicio nova eleição minha.
            if remetente is not None and remetente < NODE_ID:
                send_ok(ch, remetente)
                send_election(ch)

        elif tipo == "OK":
            # Só marca que existe nó mais forte na eleição
            election_in_progress = True

        elif tipo == "COORDINATOR":
            current_leader_id = remetente
            current_leader_port = msg.get("port")
            election_in_progress = False
            print(
                f"[{SERVICE_ID}] Coordenador eleito: ID {remetente}, porta {current_leader_port}"
            )

    print(f"[{SERVICE_ID}] Bully Listener ativo")
    ch.basic_consume(queue=qname, on_message_callback=cb, auto_ack=True)
    ch.start_consuming()


# ==========================================================
# LOOP PRINCIPAL
# ==========================================================
def election_loop():
    # Worker sempre ligado
    threading.Thread(target=worker_consume, daemon=True).start()

    # Heartbeat server + cliente
    my_port = heartbeat_server()
    threading.Thread(target=heartbeat_client, args=(my_port,), daemon=True).start()

    # Bully listener
    threading.Thread(target=bully_consumer, daemon=True).start()

    # Loop para disputar liderança via consumer EXCLUSIVO
    while True:
        try:
            leader_consume()
        except pika.exceptions.ChannelClosedByBroker:
            print(f"[{SERVICE_ID}] Não virei líder (já existe outro)")
        except Exception as e:
            print(f"[{SERVICE_ID}] Erro líder: {e}")

        time.sleep(random.uniform(2, 4))


# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    print(f"[{SERVICE_ID}] inicializado (Bully + FX + DB + Cripto + Heartbeat)")
    election_loop()
