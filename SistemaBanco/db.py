import sqlite3
from threading import Lock
import os

_DB_LOCK = Lock()

# Caminho absoluto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "Bancodedados", "banco_de_dados.db")

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def criar_tabela_auditoria():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS auditoria (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transacao_id TEXT,
                evento TEXT,
                servico TEXT,
                detalhes TEXT,
                timestamp TEXT
            )
        """)

        conn.commit()
        conn.close()
    except Exception as e:
        print("ERRO ao criar tabela auditoria:", e)

# Executa no import
criar_tabela_auditoria()

def registrar_auditoria(evento_dict: dict):
    """
    Salva eventos seguros no banco.
    NÃO será chamada para retry / falha-lógica / falha-banco-definitiva.
    """
    try:
        with _DB_LOCK:
            conn = get_conn()
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO auditoria (transacao_id, evento, servico, detalhes, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (
                evento_dict.get("id"),
                evento_dict.get("evento"),
                evento_dict.get("servico"),
                str(evento_dict),
                evento_dict.get("ts"),
            ))

            conn.commit()
            conn.close()

    except sqlite3.OperationalError as e:
        # Banco travado → não salvar auditoria
        print("ERRO Auditoria DB:", e)
    except Exception as e:
        print("ERRO inesperado ao salvar auditoria:", e)

def obter_conta(conta_id):
    try:
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

    except (sqlite3.Error, sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        raise RuntimeError(f"DB_LOCKED: {e}")


def atualizar_saldo(conta_id, novo_saldo):
    try:
        with _DB_LOCK:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                "UPDATE conta SET saldo=? WHERE id=?",
                (novo_saldo, conta_id),
            )
            conn.commit()
            conn.close()

    except (sqlite3.Error, sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        raise RuntimeError(f"DB_LOCKED: {e}")


def registrar_transacao(origem, destino, valor_criptografado, tipo):
    try:
        with _DB_LOCK:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO transacao 
                (conta_id_origem, conta_id_destino, valor, tipo_transacao)
                VALUES (?, ?, ?, ?)
            """, (origem, destino, valor_criptografado, tipo))
            conn.commit()
            conn.close()
    except (sqlite3.Error, sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        raise RuntimeError(f"DB_LOCKED: {e}")


def obter_codigo_moeda_conta(conta_id: int) -> str:
    try:
        with _DB_LOCK:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("""
                SELECT m.nome, m.simbolo
                FROM conta c
                JOIN moeda m ON m.id = c.moeda
                WHERE c.id = ?
            """, (conta_id,))
            row = cur.fetchone()
            conn.close()
    except (sqlite3.Error, sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        raise RuntimeError(f"DB_LOCKED: {e}")

    if row is None:
        raise ValueError(f"Conta {conta_id} não encontrada para obter moeda")

    nome, simbolo = row
    nome = nome.lower()
    simbolo = simbolo.upper()

    if "real" in nome or simbolo in ("BRL", "R$"):
        return "BRL"
    if "dólar" in nome or "dolar" in nome or simbolo in ("USD", "US$"):
        return "USD"
    if "euro" in nome or simbolo in ("EUR", "€"):
        return "EUR"

    return simbolo or "USD"
