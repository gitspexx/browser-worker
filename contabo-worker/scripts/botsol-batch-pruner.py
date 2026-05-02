#!/usr/bin/env python3
"""
botsol-batch-pruner.py - Cross-country keyword pruner using historical Botsol CSVs.

Inputs (hardcoded refs, override via env var BOTSOL_PRUNER_REFS as JSON):
  Reference CSVs analyzed in pairs of (csv_path, country, category, source_txt_path)

For each reference (country, category):
  - Parse source .txt -> set of attempted (thing, city) pairs
  - Parse CSV -> unique-business count per keyword (Data_cid primary, Name+Address fallback)
  - Per "thing" (e.g. "tour", "yoga class"), compute avg unique businesses per city tried
  - "Dead things" = attempted but produced 0 unique results in any city, with no
    countervailing productive data from another reference

For all keywords_v2/<country>/<category>.txt (across all countries):
  - If category has reference data:
      * Drop keyword if its "thing" is in dead-set
      * Keep keyword if its "thing" has avg >= MIN_AVG_UNIQUE
      * Keep keyword if "thing" is unknown (conservative; no signal yet)
  - Else (no reference for this category): copy as-is (no pruning yet)

Outputs:
  - <txt>.pruned.txt next to original (originals untouched)
  - Stdout: per-file decisions + global summary
  - JSON dump of derived patterns to C:\\worker\\tmp\\thing_patterns.json
"""

import csv, json, os, re, sys
from collections import defaultdict
from pathlib import Path


KEYWORDS_ROOT = r'C:\Botsol\pipeline\keywords_v2'
PATTERNS_JSON = r'C:\worker\tmp\thing_patterns.json'

DEFAULT_REFS = [
    {
        'csv':     r'C:\Botsol\archive\cafe-colombia_20260418_053219.csv',
        'country': 'colombia',
        'category': 'cafe',
        'source_txt': r'C:\Botsol\pipeline\keywords_v2\colombia\cafe.txt',
    },
    {
        'csv':     r'C:\Botsol\archive\colombia_do_20260427_20260427_163742.csv',
        'country': 'colombia',
        'category': 'do',
        'source_txt': r'C:\Botsol\pipeline\keywords_v2\colombia\do.txt',
    },
]

MIN_AVG_UNIQUE = float(os.environ.get('PRUNER_MIN_AVG', '2.0'))


def parse_kw(kw, country):
    """Returns (thing, city_or_None) tuple, lowercased; or (None,None) if unparseable."""
    kw = (kw or '').strip().lower()
    if not kw:
        return (None, None)
    country_l = country.lower().replace('_', ' ').strip()
    m = re.match(rf'^(.+?)\s+near\s+(.+?)\s+{re.escape(country_l)}\s*$', kw)
    if m:
        return (m.group(1).strip(), m.group(2).strip())
    m = re.match(rf'^(.+?)\s+in\s+{re.escape(country_l)}\s*$', kw)
    if m:
        return (m.group(1).strip(), None)
    return (None, None)


def analyze_csv(csv_path):
    """Per-keyword unique-business counts (CIDs primary, Name+Address fallback)."""
    if not os.path.exists(csv_path):
        return {}, {}
    with open(csv_path, 'r', encoding='utf-8-sig', errors='replace', newline='') as f:
        first = f.readline()
        if not first.strip().lower().startswith('sep='):
            f.seek(0)
        reader = csv.DictReader(f)
        rows = list(reader)

    cid_populated = sum(1 for r in rows if (r.get('Data_cid') or '').strip())
    use_cid = cid_populated > (len(rows) * 0.5)

    seen = set()
    unique_per_kw = defaultdict(int)
    total_per_kw = defaultdict(int)
    for row in rows:
        kw = (row.get('Keyword') or '').strip()
        if use_cid:
            key = (row.get('Data_cid') or '').strip()
        else:
            nm = (row.get('Name') or '').strip().lower()
            ad = (row.get('Full_Address') or row.get('Full Address') or '').strip().lower()
            key = f'{nm}|{ad}' if (nm or ad) else ''
        total_per_kw[kw] += 1
        if key and key not in seen:
            seen.add(key)
            unique_per_kw[kw] += 1
    return dict(unique_per_kw), dict(total_per_kw)


def build_patterns(refs):
    """
    Returns:
      patterns[category] = {
        'productive_things': {thing: avg_unique_per_city_attempt},
        'dead_things': set of things (attempted but never produced any unique business),
        'cities_observed': {city: total_unique_seen_for_this_country},
        'refs_count': how many reference CSVs contributed to this category,
      }
    """
    by_cat = {}
    for ref in refs:
        csv_path = ref['csv']
        country = ref['country']
        cat = ref['category']
        txt_path = ref['source_txt']

        if not os.path.exists(csv_path):
            print(f'# WARN: skipping ref (CSV missing): {csv_path}', file=sys.stderr)
            continue
        if not os.path.exists(txt_path):
            print(f'# WARN: skipping ref (txt missing): {txt_path}', file=sys.stderr)
            continue

        # Source keywords -> attempted (thing, city) pairs
        attempted_things = defaultdict(set)
        with open(txt_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                t, c = parse_kw(line, country)
                if t:
                    attempted_things[t].add(c or '__generic__')

        # CSV -> productive things
        unique_per_kw, _ = analyze_csv(csv_path)
        productive = defaultdict(list)
        cities_seen = defaultdict(int)
        for kw, n in unique_per_kw.items():
            t, c = parse_kw(kw, country)
            if t:
                productive[t].append(n)
                if c:
                    cities_seen[c] += n

        prod_avg = {t: sum(v) / len(v) for t, v in productive.items()}
        # Dead things: attempted but never seen as productive (count=0 in CSV implicit)
        dead = set(attempted_things.keys()) - set(prod_avg.keys())

        if cat not in by_cat:
            by_cat[cat] = {
                'productive_things': {},
                'dead_things': set(),
                'cities_observed': {},
                'refs_count': 0,
            }
        rec = by_cat[cat]
        for t, e in prod_avg.items():
            # Merge across refs: keep max average (most generous)
            rec['productive_things'][t] = max(rec['productive_things'].get(t, 0.0), e)
        for t in dead:
            if t not in rec['productive_things']:
                rec['dead_things'].add(t)
        for c, n in cities_seen.items():
            rec['cities_observed'][c] = max(rec['cities_observed'].get(c, 0), n)
        rec['refs_count'] += 1

    return by_cat


def prune_file(txt_path, country, category, patterns):
    cat_p = patterns.get(category)
    if cat_p is None:
        return None

    with open(txt_path, 'r', encoding='utf-8', errors='replace') as f:
        lines = [l.strip() for l in f if l.strip()]

    productive = cat_p['productive_things']
    dead = cat_p['dead_things']

    kept, dropped = [], []
    for line in lines:
        t, c = parse_kw(line, country)
        if t is None:
            kept.append((line, 'unparseable'))
            continue
        if t in dead:
            dropped.append((line, f'thing-dead'))
            continue
        e = productive.get(t)
        if e is None:
            kept.append((line, 'thing-unknown'))
        elif e >= MIN_AVG_UNIQUE:
            kept.append((line, f'thing-avg={e:.2f}'))
        else:
            dropped.append((line, f'thing-low (avg={e:.2f})'))

    return lines, kept, dropped


def main():
    refs = DEFAULT_REFS
    refs_env = os.environ.get('BOTSOL_PRUNER_REFS')
    if refs_env:
        try:
            refs = json.loads(refs_env)
        except Exception as e:
            print(f'# WARN: bad BOTSOL_PRUNER_REFS json: {e}', file=sys.stderr)

    print('# Botsol batch pruner')
    print(f'# MIN_AVG_UNIQUE = {MIN_AVG_UNIQUE}')
    print(f'# References:')
    for r in refs:
        print(f'  - {r["country"]}/{r["category"]}: {r["csv"]}')

    patterns = build_patterns(refs)

    # Persist patterns JSON for later use
    Path(os.path.dirname(PATTERNS_JSON)).mkdir(parents=True, exist_ok=True)
    serialisable = {
        cat: {
            'productive_things': p['productive_things'],
            'dead_things': sorted(p['dead_things']),
            'cities_observed': p['cities_observed'],
            'refs_count': p['refs_count'],
        }
        for cat, p in patterns.items()
    }
    with open(PATTERNS_JSON, 'w', encoding='utf-8') as f:
        json.dump(serialisable, f, indent=2, ensure_ascii=False)

    print(f'\n# Derived patterns by category:')
    for cat in sorted(patterns.keys()):
        p = patterns[cat]
        prods = sorted(p['productive_things'].items(), key=lambda x: x[1], reverse=True)
        deads = sorted(p['dead_things'])
        print(f'\n## {cat} (from {p["refs_count"]} ref runs)')
        print(f'### Productive things (top 20 by avg unique per city)')
        for t, e in prods[:20]:
            print(f'  {e:6.2f}  {t}')
        print(f'### Dead things ({len(deads)})')
        for t in deads[:25]:
            print(f'  -- {t}')
        if len(deads) > 25:
            print(f'  ... and {len(deads) - 25} more')

    # Apply to all keywords_v2/<country>/<category>.txt
    print(f'\n# Pruning files in {KEYWORDS_ROOT}:')
    root = Path(KEYWORDS_ROOT)
    summary = []
    skipped_no_pattern = []
    if not root.exists():
        print(f'# ERROR: keywords root not found: {root}', file=sys.stderr)
        sys.exit(1)

    for country_dir in sorted(root.iterdir()):
        if not country_dir.is_dir():
            continue
        if country_dir.name.startswith(('_', '.')):
            continue
        country = country_dir.name.replace('_', ' ')
        for txt in sorted(country_dir.glob('*.txt')):
            cat = txt.stem
            if cat not in patterns:
                skipped_no_pattern.append(f'{country}/{cat}')
                continue
            res = prune_file(str(txt), country, cat, patterns)
            if res is None:
                continue
            lines, kept, dropped = res
            out_path = str(txt).replace('.txt', '.pruned.txt')
            with open(out_path, 'w', encoding='utf-8', newline='\n') as f:
                for k, _ in kept:
                    f.write(k + '\n')
            pct = (100.0 * (len(lines) - len(kept)) / len(lines)) if lines else 0
            summary.append({
                'country': country, 'category': cat,
                'orig': len(lines), 'kept': len(kept), 'dropped': len(dropped),
                'pct_dropped': pct, 'out': out_path,
            })
            print(f'  {country:>20s}/{cat:<10s}: {len(lines):>4d} -> {len(kept):>4d}  ({pct:5.1f}% dropped)')

    if skipped_no_pattern:
        print(f'\n# Skipped (no reference data for these categories):')
        for s in sorted(set([x.split("/")[1] for x in skipped_no_pattern])):
            cnt = sum(1 for x in skipped_no_pattern if x.split('/')[1] == s)
            print(f'  - {s}: {cnt} country files (no .pruned.txt emitted, originals untouched)')

    if summary:
        total_orig = sum(s['orig'] for s in summary)
        total_kept = sum(s['kept'] for s in summary)
        total_drop = total_orig - total_kept
        print(f'\n# TOTAL: {len(summary)} files pruned')
        print(f'  keywords: {total_orig:,} -> {total_kept:,}  ({total_drop:,} dropped, {100.0 * total_drop / total_orig:.1f}%)')
        print(f'\n# Patterns persisted to: {PATTERNS_JSON}')


if __name__ == '__main__':
    main()
