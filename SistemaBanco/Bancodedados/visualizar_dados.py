import sqlite3
conexao = sqlite3.connect('banco_de_dados.db')
cursor = conexao.cursor()

for linha in cursor.execute('SELECT * FROM cliente'):
    print(linha)

conexao.close()