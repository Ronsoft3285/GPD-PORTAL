import sqlite3

db='database/gpd_portal.db'
conn=sqlite3.connect(db)
cur=conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
tables=[r[0] for r in cur.fetchall()]
print('tables:', tables)
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM \"{t}\"")
    print(t, 'count=', cur.fetchone()[0])
conn.close()
