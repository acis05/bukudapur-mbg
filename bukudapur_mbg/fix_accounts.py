import sqlite3
import os

candidates = [
    "bukudapur.db",
    os.path.join("instance", "bukudapur.db"),
]

db_path = None
for c in candidates:
    if os.path.exists(c):
        db_path = c
        break

if not db_path:
    raise FileNotFoundError("Database tidak ditemukan: bukudapur.db (root) atau instance/bukudapur.db")

print("DB dipakai:", db_path)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accounts'")
print("Table accounts ada?", cur.fetchone())

cur.execute("SELECT DISTINCT type FROM accounts ORDER BY type")
print("Types sebelum:", [r[0] for r in cur.fetchall()])

cur.execute("""
UPDATE accounts
SET type='Pendapatan Lain'
WHERE TRIM(type)='Pendapatn Lain'
""")
conn.commit()

print("Rows updated:", conn.total_changes)

cur.execute("SELECT DISTINCT type FROM accounts ORDER BY type")
print("Types sesudah:", [r[0] for r in cur.fetchall()])

conn.close()
