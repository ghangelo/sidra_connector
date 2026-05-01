import sqlite3
import os

db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'agregados_ibge.db')
print(f"DB path: {db_path}")
print(f"Exists: {os.path.exists(db_path)}")
print(f"Size: {os.path.getsize(db_path) / 1024:.1f} KB")

conn = sqlite3.connect(db_path)
cur = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cur.fetchall()]
print(f"\nTabelas: {tables}")

for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"  {t}: {cur.fetchone()[0]} registros")

cur.execute("SELECT * FROM agregados LIMIT 3")
print(f"\nSample agregados: {cur.fetchall()}")

cur.execute("SELECT * FROM grupos LIMIT 3")
print(f"Sample grupos: {cur.fetchall()}")

# Check column info
cur.execute("PRAGMA table_info(agregados)")
print(f"\nColunas agregados: {cur.fetchall()}")

cur.execute("PRAGMA table_info(grupos)")
print(f"Colunas grupos: {cur.fetchall()}")

conn.close()
