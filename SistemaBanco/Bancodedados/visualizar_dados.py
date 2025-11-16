import sqlite3
conn = sqlite3.connect("banco_de_dados.db")
cur = conn.cursor()
cur.execute("SELECT id, saldo FROM conta")
print(cur.fetchall())
conn.close()