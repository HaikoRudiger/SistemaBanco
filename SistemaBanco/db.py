import sqlite3
from threading import Lock
import os

_DB_LOCK = Lock()

# Caminho absoluto: SistemaBanco/Bancodedados/banco_de_dados.db
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "Bancodedados", "banco_de_dados.db")


def get_conn():
    """
    Cria a conexão SQLite com check_same_thread=False
    para permitir uso em threads paralelos.
    """
    return sqlite3.connect(DB_PATH, check_same_thread=False)



# FUNÇÕES DE ACESSO AO BANCO - TODAS COM TRATAMENTO DE ERRO CORRETAMENTE

def obter_conta(conta_id):
    """
    Retorna os dados da conta.
    Erros SQLite → RuntimeError → RETRY
    """
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
    """
    Atualiza o saldo de uma conta.
    Erros SQLite → RuntimeError → RETRY
    """
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
    """
    Registra uma transação no histórico.
    Erros SQLite → RuntimeError → RETRY
    """
    try:
        with _DB_LOCK:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO transacao (conta_id_origem, conta_id_destino, valor, tipo_transacao)
                VALUES (?, ?, ?, ?)
                """,
                (origem, destino, valor_criptografado, tipo),
            )
            conn.commit()
            conn.close()

    except (sqlite3.Error, sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        raise RuntimeError(f"DB_LOCKED: {e}")


def obter_codigo_moeda_conta(conta_id: int) -> str:
    """
    Retorna o código da moeda da conta (BRL, USD, EUR, etc.)
    baseado na tabela 'moeda'.
    """
    try:
        with _DB_LOCK:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT m.nome, m.simbolo
                FROM conta c
                JOIN moeda m ON m.id = c.moeda
                WHERE c.id = ?
                """,
                (conta_id,),
            )
            row = cur.fetchone()
            conn.close()

    except (sqlite3.Error, sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        raise RuntimeError(f"DB_LOCKED: {e}")

    if row is None:
        raise ValueError(f"Conta {conta_id} não encontrada para obter moeda")

    nome, simbolo = row  # exemplo: ("Real", "R$")

    # Mapeamentos aceitos
    nome_lower = (nome or "").lower()
    simbolo_upper = (simbolo or "").upper()

    if "real" in nome_lower or simbolo_upper in ("BRL", "R$"):
        return "BRL"
    if "dólar" in nome_lower or "dolar" in nome_lower or simbolo_upper in ("USD", "US$"):
        return "USD"
    if "euro" in nome_lower or simbolo_upper in ("EUR", "€"):
        return "EUR"

    return simbolo_upper or "USD"
