import sqlite3

# Nome do arquivo do banco (precisa bater com o DB_PATH do db.py)
DB_NAME = 'banco_de_dados.db'

conexao = sqlite3.connect(DB_NAME)
cursor = conexao.cursor()

# Tabela de clientes
cursor.execute('''
    CREATE TABLE IF NOT EXISTS cliente (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        cadastro_federal TEXT UNIQUE NOT NULL,
        email TEXT NOT NULL,
        telefone TEXT
    )
''')

# Tabela de moedas
cursor.execute('''
    CREATE TABLE IF NOT EXISTS moeda (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        simbolo TEXT NOT NULL
    )
''')

# Tabela de contas
cursor.execute('''
    CREATE TABLE IF NOT EXISTS conta (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER,
        saldo REAL DEFAULT 0,
        tipo_conta TEXT,
        moeda INTEGER,
        FOREIGN KEY (cliente_id) REFERENCES cliente(id),
        FOREIGN KEY (moeda) REFERENCES moeda(id)
    )
''')

# Tabela de transações (valores criptografados em 'valor')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS transacao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conta_id_origem INTEGER,
        conta_id_destino INTEGER,
        valor TEXT,                -- valor criptografado (Base64)
        tipo_transacao TEXT,
        data TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (conta_id_origem) REFERENCES conta(id),
        FOREIGN KEY (conta_id_destino) REFERENCES conta(id)
    )
''')

# Tabela de auditoria
cursor.execute('''
    CREATE TABLE IF NOT EXISTS auditoria (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transacao_id TEXT NOT NULL,   -- UUID da transação
        evento TEXT NOT NULL,         -- recebido-lider, pre-processamento, retry, falha-logica, etc
        servico TEXT,                 -- svc-XXXX do serviço que gerou o log
        timestamp TEXT,               -- ISO8601
        detalhes TEXT                 -- JSON completo do evento de auditoria
    )
''')

conexao.commit()
conexao.close()

print(f"Tabelas criadas/atualizadas no banco '{DB_NAME}'.")
