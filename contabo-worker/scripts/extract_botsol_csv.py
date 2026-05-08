"""
Extract Botsol's scrape data from db.sqlite into a CSV that matches
Botsol's export format (so csv_processor can ingest it normally).
Used when Botsol hangs and can't export via UI.
"""
import sqlite3, shutil, csv, os, sys
from pathlib import Path

DB_SRC = Path(os.environ['APPDATA']) / 'Botsol' / 'db.sqlite'
DB_SNAP = Path(os.environ['TEMP']) / 'botsol_export_snap.sqlite'
OUT_PATH = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(r'C:\Botsol\output\botsol_recovery.csv')

shutil.copy2(DB_SRC, DB_SNAP)
con = sqlite3.connect(DB_SNAP)
cur = con.cursor()

# Find the main scraping table (largest underscore-prefixed)
tables = [r[0] for r in cur.execute("select name from sqlite_master where type='table' and name like '\\_%' escape '\\'").fetchall()]
best = None
best_count = 0
for t in tables:
    c = cur.execute(f'select count(*) from "{t}"').fetchone()[0]
    if c > best_count:
        best_count = c
        best = t
if not best or best_count == 0:
    print('No data table with rows found')
    sys.exit(1)
print(f'Source table: {best}  rows={best_count}')

cur.execute(f'select * from "{best}"')
cols = [d[0] for d in cur.description]
header = [c[1:] if c.startswith('_') else c for c in cols]
out_dir = OUT_PATH.parent
out_dir.mkdir(parents=True, exist_ok=True)
with open(OUT_PATH, 'w', encoding='utf-8', newline='') as f:
    w = csv.writer(f, quoting=csv.QUOTE_ALL)
    w.writerow(header)
    n = 0
    for row in cur:
        w.writerow(['' if v is None else str(v) for v in row])
        n += 1
print(f'Wrote {n} rows -> {OUT_PATH}')
print(f'File size: {OUT_PATH.stat().st_size / 1024 / 1024:.1f} MB')
