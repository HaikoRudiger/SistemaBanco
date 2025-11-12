# consumer_processing.py
import json
import os
import time
import random
import threading
from datetime import datetime, timezone

import pika
import currencyapicom

from connection import get_channel

# -------------------------
# Configurações e globais
# -------------------------
MAX_RETRIES = 3
SERVICE_ID = os.getenv("SERVICE_ID")
if not SERVICE_ID or "python - <<" in SERVICE_ID:
    SERVICE_ID = f"svc-{random.randint(1000, 9999)}"

START_TS = time.time()

def uptime() -> int:
    return int(time.time() - START_TS)

# Currency API
API_KEY = os.getenv("CURRENCYAPI_KEY")
if not API_KEY:
    raise RuntimeError("CURRENCYAPI_KEY não definido no .env")
_fx_client = currencyapicom.Client(API_KEY)

# cache simples de taxas: { (from,to): (rate, ts) }
_FX_CACHE = {}
_FX_TTL_SECONDS = 60


# -------------------------
# Utilitários de publicação
# -------------------------
def publicar(ch, rk: str, payload: dict, headers: dict | None = None):
    """Publica no exchange.principal com routing key rk."""
    ch.basic_publish(
        exchange='exchange.principal',
        routing_key=rk,
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type='application/json',
            headers=headers or {}
        )
    )


# -------------------------
# Conversão de moeda (ajustada p/ plano free)
# -------------------------
def _fx_get_rate(from_currency: str, to_currency: str) -> float:
    """
    Calcula a taxa from->to usando latest() (base USD no plano free):
      rate(from->to) = rate(USD->to) / rate(USD->from)
    Cache por 60s.
    """
    f = from_currency.upper()
    t = to_currency.upper()
    key = (f, t)
    now = time.time()

    cached = _FX_CACHE.get(key)
    if cached and (now - cached[1]) < _FX_TTL_SECONDS:
        return cached[0]

    # Chama latest sem parâmetros (sempre base USD no free)
    resp = _fx_client.latest()
    try:
        data = resp["data"]
        usd_to   = 1.0 if t == "USD" else float(data[t]["value"])
        usd_from = 1.0 if f == "USD" else float(data[f]["value"])
        rate = usd_to / usd_from
    except Exception as e:
        raise RuntimeError(f"Falha ao calcular taxa FX {f}->{t}: {e}")

    _FX_CACHE[key] = (rate, now)
    return rate


def converter_moeda(amount: float, from_currency: str, to_currency: str) -> tuple[float, float]:
    """
    Converte amount de from_currency para to_currency.
    Retorna (valor_convertido, fx_rate).
    """
    if from_currency.upper() == to_currency.upper():
        return amount, 1.0
    rate = _fx_get_rate(from_currency, to_currency)
    return amount * rate, rate


# -------------------------
# "Negócio" / processamento
# -------------------------
def processar_operacao(payload: dict) -> bool:
    """
    Regras simples: valor convertido deve estar no intervalo [0, 1000].
    """
    valor_proc = float(payload.get("valor_convertido", 0))
    if valor_proc > 1000 or valor_proc < 0:
        raise Exception("Valor invalido para processamento automático.")
    # TODO: persistir no banco (próxima etapa)
    return True


# -------------------------
# Worker: consome fila.cluster.work
# -------------------------
def worker_consume():
    conn_w, ch_w = get_channel()
    ch_w.basic_qos(prefetch_count=1)

    def on_msg(ch, method, properties, body):
        # 1) Parse JSON ou DLQ se inválido
        try:
            data = json.loads(body)
        except Exception:
            ch.basic_publish(exchange='exchange.dlx', routing_key='', body=body, properties=properties)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        headers = (properties.headers or {})
        retries = int(headers.get('x-retries', 0))

        # 2) Conversão de moeda (antes de qualquer regra)
        origem = data.get("moeda", "BRL")
        base = os.getenv("CURRENCY_BASE", "USD")  # moeda base do sistema
        try:
            valor_original = float(data.get("valor", 0))
            valor_conv, fx_rate = converter_moeda(valor_original, origem, base)
            data["valor_convertido"] = round(valor_conv, 6)
            data["moeda_base"] = base
            data["fx_rate"] = fx_rate
            data["fx_provider"] = "currencyapi.com"
            data["fx_at"] = datetime.now(timezone.utc).isoformat()
        except Exception as e:
            print(f"[{SERVICE_ID}] falha conversão moeda: {e}")
            retries += 1
            if retries > MAX_RETRIES:
                publicar(ch, 'audit.falha', {
                    "evento": "falha-definitiva",
                    "id": data.get("id"),
                    "servico": SERVICE_ID,
                    "erro": f"FX: {e}",
                    "ts": datetime.now(timezone.utc).isoformat()
                })
                ch.basic_publish(
                    exchange='exchange.dlx',
                    routing_key='',
                    body=json.dumps(data),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type='application/json',
                        headers={'x-retries': retries}
                    )
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                rk_retry = f"retry.{retries}"
                ch.basic_publish(
                    exchange='exchange.retry',
                    routing_key=rk_retry,
                    body=json.dumps(data),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type='application/json',
                        headers={'x-retries': retries}
                    )
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # 3) Auditoria pré
        publicar(ch, 'audit.pre', {
            "evento": "pre-processamento",
            "id": data.get("id"),
            "servico": SERVICE_ID,
            "valor_original": data.get("valor"),
            "moeda_origem": origem,
            "valor_convertido": data.get("valor_convertido"),
            "moeda_base": base,
            "fx_rate": data.get("fx_rate"),
            "ts": datetime.now(timezone.utc).isoformat()
        })

        # 4) Processamento + Retry/DLQ + Auditoria pós/notificação
        try:
            ok = processar_operacao(data)
            if ok:
                publicar(ch, 'audit.post', {
                    "evento": "pos-processamento",
                    "id": data.get("id"),
                    "servico": SERVICE_ID,
                    "ts": datetime.now(timezone.utc).isoformat()
                })
                data_out = dict(data)
                data_out["status"] = "SUCESSO"
                publicar(ch, 'notify.transacao', data_out)
                ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"[{SERVICE_ID}] erro processamento: {e}")
            retries += 1
            if retries > MAX_RETRIES:
                publicar(ch, 'audit.falha', {
                    "evento": "falha-definitiva",
                    "id": data.get("id"),
                    "servico": SERVICE_ID,
                    "erro": str(e),
                    "ts": datetime.now(timezone.utc).isoformat()
                })
                ch.basic_publish(
                    exchange='exchange.dlx',
                    routing_key='',
                    body=json.dumps(data),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type='application/json',
                        headers={'x-retries': retries}
                    )
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                rk_retry = f"retry.{retries}"
                ch.basic_publish(
                    exchange='exchange.retry',
                    routing_key=rk_retry,
                    body=json.dumps(data),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type='application/json',
                        headers={'x-retries': retries}
                    )
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)

    ch_w.basic_consume(queue='fila.cluster.work', on_message_callback=on_msg)
    print(f"[{SERVICE_ID}] Worker ON consumindo fila.cluster.work")
    ch_w.start_consuming()


# -------------------------
# Líder: consome fila.transacoes e distribui
# -------------------------
def leader_loop():
    conn_l, ch_l = get_channel()
    ch_l.basic_qos(prefetch_count=1)

    def on_leader_msg(ch, method, properties, body):
        try:
            data = json.loads(body)
        except Exception:
            ch.basic_publish(exchange='exchange.dlx', routing_key='', body=body, properties=properties)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        publicar(ch, 'audit.recebido_lider', {
            "evento": "recebido-lider",
            "id": data.get("id"),
            "lider": SERVICE_ID,
            "ts": datetime.now(timezone.utc).isoformat()
        })

        ch.basic_publish(
            exchange='exchange.cluster',
            routing_key='work',
            body=json.dumps(data),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json',
                headers=properties.headers or {}
            )
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch_l.basic_consume(queue='fila.transacoes', on_message_callback=on_leader_msg)
    print(f"[{SERVICE_ID}] ATUANDO COMO LÍDER (distribuindo do principal -> cluster.work)")
    ch_l.start_consuming()


# -------------------------
# Eleição (startup, líder único)
# -------------------------
def try_become_leader():
    """
    Eleição de líder corrigida:
      - Testa existência de 'leader.lock' em modo passive.
      - Se não existe, cria fila exclusiva (auto_delete). Só 1 nó consegue.
      - Demais nós atuam como workers e ficam rechecando.
    """
    while True:
        time.sleep(random.uniform(1.0, 2.5))

        try:
            conn_chk, ch_chk = get_channel()
            try:
                # Se existir, já tem líder
                ch_chk.queue_declare(queue='leader.lock', passive=True)
                if not globals().get("_worker_started"):
                    globals()["_worker_started"] = True
                    threading.Thread(target=worker_consume, daemon=True).start()
                conn_chk.close()
                time.sleep(4.0)
                continue
            except pika.exceptions.ChannelClosedByBroker:
                # Não existe -> podemos tentar criar a fila lock
                try:
                    conn_lock, ch_lock = get_channel()
                    ch_lock.queue_declare(
                        queue='leader.lock',
                        durable=False,
                        exclusive=True,
                        auto_delete=True,
                        arguments={'x-expires': 15000}
                    )
                    print(f"[{SERVICE_ID}] ELEITO LÍDER (uptime={uptime()}s)")
                    t_worker = threading.Thread(target=worker_consume, daemon=True)
                    t_worker.start()
                    try:
                        leader_loop()
                    finally:
                        try: conn_lock.close()
                        except: pass
                except Exception:
                    if not globals().get("_worker_started"):
                        globals()["_worker_started"] = True
                        threading.Thread(target=worker_consume, daemon=True).start()
                    time.sleep(3.0)
        except Exception:
            if not globals().get("_worker_started"):
                globals()["_worker_started"] = True
                threading.Thread(target=worker_consume, daemon=True).start()
            time.sleep(3.0)


# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    print(f"[{SERVICE_ID}] iniciando serviço de transação (eleição + FX + processamento)")
    try_become_leader()
