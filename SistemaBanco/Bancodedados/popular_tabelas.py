import sqlite3

conexao = sqlite3.connect('banco_de_dados.db')
cursor = conexao.cursor()

# Popular a tabela de moedas
moedas = [
    ('Real', 'R$'),
    ('Dólar', 'US$'),
    ('Euro', '€')
]

cursor.executemany('''
    INSERT INTO moeda (nome, simbolo)
    VALUES (?, ?)
''', moedas)

# Popular a tabela de clientes
clientes = [
    ('Daniel Neves', '123.456.789-00', 'daniel@example.com', '9999-9999'),
    ('Ana Julia da Cunha', '987.654.321-00', 'ana@example.com', '8888-8888'),
    ('Haiko Ruediger', '111.222.333-44', 'haiko@example.com', '7777-7777'),
    ('Beatriz Moresco', '555.666.777-88', 'beatriz@example.com', '6666-6666')
]

cursor.executemany('''
    INSERT INTO cliente (nome, cadastro_federal, email, telefone)
    VALUES (?, ?, ?, ?)
''', clientes)

contas = [
    (1, 1000000.00, 'poupanca', 1),
    (2, 1000000.00, 'corrente', 2),
    (3, 1000000.00, 'poupanca', 3),
    (4, 1000000.00, 'corrente', 1)
]

# Popular a tabela de contas
cursor.executemany('''
    INSERT INTO conta (cliente_id, saldo, tipo_conta, moeda)
    VALUES (?, ?, ?, ?)
''', contas)


conexao.commit()
conexao.close()