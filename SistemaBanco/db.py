# db.py
import sqlite3
from threading import Lock
import os

_DB_LOCK = Lock()

# Caminho absoluto: SistemaBanco/Bancodedados/banco_de_dados.db
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "Bancodedados", "banco_de_dados.db")

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def obter_conta(conta_id):
    with _DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, cliente_id, saldo, tipo_conta, moeda FROM conta WHERE id=?",
            (conta_id,),
        )
        row = cur.fetchone()
        conn.close()
        return row

def atualizar_saldo(conta_id, novo_saldo):
    with _DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE conta SET saldo=? WHERE id=?",
            (novo_saldo, conta_id),
        )
        conn.commit()
        conn.close()

def registrar_transacao(origem, destino, valor_cript, tipo):
    with _DB_LOCK:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO transacao (conta_id_origem, conta_id_destino, valor, tipo_transacao)
            VALUES (?, ?, ?, ?)
            """,
            (origem, destino, valor_cript, tipo),
        )
        conn.commit()
        conn.close()
