import sqlite3

from db import DB_PATH

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute("BEGIN EXCLUSIVE")
input("Banco travado. Pressione enter para liberar...")
conn.commit()