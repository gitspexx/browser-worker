#!/usr/bin/env python3
"""
botsol-analyzer.py — per-CSV keyword efficiency analyzer.

Reads a Botsol-exported CSV + the keyword .txt that produced it.
Computes per-keyword unique-business contribution.
Writes <category>.pruned.txt covering >=95% of unique businesses for next-run optimization.

Usage:
    python botsol-analyzer.py <csv_path> <keyword_txt_path>
    python botsol-analyzer.py C:\\Botsol\\archive\\colombia_wellness.starter_20260505.csv \\
                              C:\\Botsol\\pipeline\\keywords_v2\\colombia\\done\\wellness.starter.txt

Output:
  - Markdown report to stdout
  - <input_keyword>.pruned.txt next to source (or done/ folder)
"""
import csv, sys, os, re
from pathlib import Path
from collections import defaultdict, Counter

if len(sys.argv) < 3:
    print(__doc__); sys.exit(1)

CSV_PATH = Path(sys.argv[1])
KW_PATH = Path(sys.argv[2])
COVERAGE_TARGET = float(os.environ.get('COVERAGE', '0.95'))

if not CSV_PATH.exists(): print(f'CSV not found: {CSV_PATH}'); sys.exit(1)
if not KW_PATH.exists(): print(f'Keyword file not found: {KW_PATH}'); sys.exit(1)

kws = [l.strip() for l in KW_PATH.read_text(encoding='utf-8').splitlines() if l.strip()]
print(f'Source CSV : {CSV_PATH.name}')
print(f'Keyword txt: {KW_PATH.name} ({len(kws)} keywords)')

# Read CSV. Botsol's exports use Data_cid as primary unique key (Google CID).
# Fall back to Name + Full_Address composite.
rows = []
with open(CSV_PATH, 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    for row in reader:
        rows.append(row)
print(f'CSV rows   : {len(rows)}')

def biz_key(row):
    cid = (row.get('Data_cid') or '').strip()
    if cid and cid.lower() != 'none' and cid.lower() != 'null':
        return ('cid', cid)
    name = (row.get('Name') or '').strip().lower()
    addr = (row.get('Full_Address') or '').strip().lower()
    return ('na', f'{name}|{addr}')

# Per-keyword: set of unique businesses
kw_to_bizs = defaultdict(set)
all_bizs = set()
for row in rows:
    kw = (row.get('Keyword') or '').strip()
    if not kw: continue
    bk = biz_key(row)
    kw_to_bizs[kw].add(bk)
    all_bizs.add(bk)

total_uniq = len(all_bizs)
print(f'Unique biz : {total_uniq}')
dup_rate = 1 - (total_uniq / len(rows)) if rows else 0
print(f'Dup rate   : {dup_rate * 100:.1f}%')

# Greedy set cover: keywords ranked by NEW unique contribution
covered = set()
ranked = []  # (kw, new_count_at_pick_time, total_count)
remaining_kws = {kw: set(bizs) for kw, bizs in kw_to_bizs.items()}
while remaining_kws and len(covered) < total_uniq:
    best_kw, best_new = None, -1
    for kw, bizs in remaining_kws.items():
        new = len(bizs - covered)
        if new > best_new:
            best_new = new; best_kw = kw
    if best_new <= 0: break
    ranked.append((best_kw, best_new, len(kw_to_bizs[best_kw])))
    covered.update(remaining_kws[best_kw])
    del remaining_kws[best_kw]

# Find cutoff for COVERAGE_TARGET
target = int(total_uniq * COVERAGE_TARGET)
cum = 0; cutoff_idx = len(ranked)
for i, (kw, new, _tot) in enumerate(ranked):
    cum += new
    if cum >= target:
        cutoff_idx = i + 1; break

# Stats per keyword (unordered)
hit_count = sum(1 for kw in kws if kw in kw_to_bizs and kw_to_bizs[kw])
no_hit = len(kws) - hit_count

print('')
print('## Efficiency report')
print(f'| Metric | Value |')
print(f'|---|---|')
print(f'| Total rows | {len(rows)} |')
print(f'| Unique businesses | {total_uniq} |')
print(f'| Duplicate rate | {dup_rate * 100:.1f}% |')
print(f'| Keywords in list | {len(kws)} |')
print(f'| Keywords with any hits | {hit_count} ({hit_count*100//max(len(kws),1)}%) |')
print(f'| Wasted keywords (0 hits) | {no_hit} ({no_hit*100//max(len(kws),1)}%) |')
print(f'| Top {cutoff_idx} covers | {COVERAGE_TARGET*100:.0f}% of uniques |')
speedup = len(kws) / max(cutoff_idx, 1)
print(f'| Estimated speedup | {speedup:.1f}x |')

print('')
print(f'## Top {min(20, cutoff_idx)} contributing keywords (greedy)')
print(f'| Rank | New uniques | Total hits | Keyword |')
print(f'|---|---|---|---|')
for i, (kw, new, tot) in enumerate(ranked[:20], 1):
    print(f'| {i} | {new} | {tot} | {kw} |')

# Write pruned list
out_path = KW_PATH.with_name(KW_PATH.name.replace('.starter.txt', '.pruned.txt')
                              if '.starter.txt' in KW_PATH.name
                              else KW_PATH.stem + '.pruned.txt')
# If KW was wellness.starter.txt, output is wellness.pruned.txt
# Land it next to the *country* dir, not in done/
if 'done' in str(out_path):
    out_path = out_path.parent.parent / out_path.name
out_path.write_text('\n'.join(kw for kw, _new, _tot in ranked[:cutoff_idx]) + '\n', encoding='utf-8')
print('')
print(f'Pruned list -> {out_path} ({cutoff_idx} keywords)')
