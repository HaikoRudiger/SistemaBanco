import sqlite3

conexao = sqlite3.connect('banco_de_dados.db')
cursor = conexao.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS cliente (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        cadastro_federal TEXT UNIQUE NOT NULL,
        email TEXT NOT NULL,
        telefone TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS moeda (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        simbolo TEXT NOT NULL
    )
''')

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

cursor.execute('''
    CREATE TABLE IF NOT EXISTS transacao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conta_id_origem INTEGER,
        conta_id_destino INTEGER,
        valor REAL,
        tipo_transacao TEXT,
        data TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (conta_id_origem) REFERENCES conta(id),
        FOREIGN KEY (conta_id_destino) REFERENCES conta(id)
    )
''')

conexao.commit()
conexao.close()