#!/usr/bin/env python3
"""
botsol-starter-generator.py - Generate starter keyword lists for first-pass scraping.

Strategy:
  Parse each keyword into (thing, city) for the target country.
  Group by city.
  Per city: keep top N keywords (sorted by thing-length-ascending so generic
    "tour near X" beats specialty "third wave coffee near X").
  Plus: keep top M country-wide generic keywords (same ranking rule).
  Also: keep all unparseable keywords (safety, rare).

Output: <input>.starter.txt with ~30-50 keywords per category.

Run on Colombia's untouched categories (drink, eat, essentials, explore, stay, wellness).
"""

import os, re, sys
from collections import defaultdict
from pathlib import Path

KEYWORDS_ROOT = r'C:\Botsol\pipeline\keywords_v2'
COUNTRY_TARGET = os.environ.get('STARTER_COUNTRY', 'colombia')
CATEGORIES = os.environ.get('STARTER_CATS',
    'drink,eat,essentials,explore,stay,wellness').split(',')
MAX_PER_CITY = int(os.environ.get('STARTER_PER_CITY', '3'))
MAX_GENERIC  = int(os.environ.get('STARTER_GENERIC', '5'))


def parse_kw(kw, country):
    kw = (kw or '').strip().lower()
    cl = country.lower().replace('_', ' ')
    m = re.match(rf'^(.+?)\s+near\s+(.+?)\s+{re.escape(cl)}\s*$', kw)
    if m: return (m.group(1).strip(), m.group(2).strip())
    m = re.match(rf'^(.+?)\s+in\s+{re.escape(cl)}\s*$', kw)
    if m: return (m.group(1).strip(), None)
    return (None, None)


def generate_starter(src_path, country, max_per_city=MAX_PER_CITY, max_generic=MAX_GENERIC):
    with open(src_path, 'r', encoding='utf-8', errors='replace') as f:
        lines = [l.strip() for l in f if l.strip()]

    by_city = defaultdict(list)
    generics = []
    unparseable = []
    seen_kw = set()

    for kw in lines:
        if kw in seen_kw:
            continue
        seen_kw.add(kw)
        thing, city = parse_kw(kw, country)
        if thing is None:
            unparseable.append(kw)
        elif city:
            by_city[city].append((thing, kw))
        else:
            generics.append((thing, kw))

    starter = []
    # Walk cities in order of first appearance in source file (preserves user's priority)
    seen_cities = []
    for kw in lines:
        _, c = parse_kw(kw, country)
        if c and c not in seen_cities:
            seen_cities.append(c)

    for city in seen_cities:
        cand = by_city[city]
        # Stable sort by (thing-length, thing) keeps short generic things first
        ranked = sorted(cand, key=lambda x: (len(x[0]), x[0]))
        for _, kw in ranked[:max_per_city]:
            starter.append(kw)

    ranked_generics = sorted(generics, key=lambda x: (len(x[0]), x[0]))
    for _, kw in ranked_generics[:max_generic]:
        starter.append(kw)

    starter.extend(unparseable)

    return {
        'orig_count': len(lines),
        'starter_count': len(starter),
        'starter_keywords': starter,
        'cities_seen': len(by_city),
        'generics_total': len(generics),
        'unparseable': len(unparseable),
    }


def main():
    print(f'# Starter generator')
    print(f'# Country: {COUNTRY_TARGET}')
    print(f'# Categories: {",".join(CATEGORIES)}')
    print(f'# Per-city cap: {MAX_PER_CITY}, generics cap: {MAX_GENERIC}')
    print()

    root = Path(KEYWORDS_ROOT) / COUNTRY_TARGET
    if not root.exists():
        print(f'# ERROR: {root} not found', file=sys.stderr)
        sys.exit(1)

    summary = []
    for cat in CATEGORIES:
        src = root / f'{cat}.txt'
        if not src.exists():
            print(f'# {cat}: SOURCE MISSING ({src})')
            continue
        result = generate_starter(str(src), COUNTRY_TARGET)
        out = root / f'{cat}.starter.txt'
        with open(out, 'w', encoding='utf-8', newline='\n') as f:
            for kw in result['starter_keywords']:
                f.write(kw + '\n')
        summary.append((cat, result['orig_count'], result['starter_count'],
                        result['cities_seen'], str(out)))
        print(f'# {cat}: {result["orig_count"]} -> {result["starter_count"]} keywords '
              f'(across {result["cities_seen"]} cities + {result["generics_total"]} generics) -> {out}')

    print()
    print('# Sample of one starter file:')
    for cat, _, _, _, out_path in summary[:1]:
        print(f'## {cat}.starter.txt:')
        with open(out_path, 'r', encoding='utf-8') as f:
            for line in f:
                print(f'  {line.rstrip()}')
        break


if __name__ == '__main__':
    main()
