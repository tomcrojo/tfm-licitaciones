"""Aggregate the two isolated v2 runs; never publishes raw notice payloads.

Usage: uv run --locked --with-editable . python docs/evidence/v2-2026-10-27/measure.py \
    /mnt/pop-work/tfm-v2-2026-10-27 /mnt/pop-work/tfm-e2e-from-source/raw/ted/ted-20260101-20260630.jsonl
"""
from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

import polars as pl


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def coverage(frame: pl.DataFrame, field: str) -> dict:
    total = frame.height
    present = total - frame[field].null_count()
    return dict(total=total, declared_date=present, missing_date=total - present,
                coverage=round(present / total, 6) if total else None)


def logical_sha(frame: pl.DataFrame, key: str) -> str:
    return hashlib.sha256(frame.sort(key).write_ndjson().encode()).hexdigest()


def measure(root: Path, old_ted: Path) -> dict:
    raw = root / 'raw'
    new_ted = raw / 'ted' / 'ted-20260101-20260630.jsonl'
    old = {r['ND']: r for r in map(json.loads, old_ted.read_text().splitlines())}
    new = {r['ND']: r for r in map(json.loads, new_ted.read_text().splitlines())}
    shared = old.keys() & new.keys()
    changed_fields = Counter()
    changed_notices = 0
    for nd in shared:
        changed = [k for k in old[nd].keys() | new[nd].keys()
                   if k != 'procedure-identifier' and old[nd].get(k) != new[nd].get(k)]
        changed_fields.update(changed)
        changed_notices += bool(changed)
    inputs = []
    for path in sorted(raw.rglob('*')):
        if path.is_file() and not path.name.endswith('.provenance.json'):
            sidecar = path.with_name(path.name + '.provenance.json')
            inputs.append(dict(path=str(path.relative_to(raw)), bytes=path.stat().st_size,
                               sha256=sha(path), provenance=json.loads(sidecar.read_text())))
    v1_hash_file = Path(__file__).parents[1] / 'real-run-2026-09-17/fresh_tableau_sha256.txt'
    v1_csv_hashes = {Path(path).name: digest for digest, path in
                     (line.split(maxsplit=1) for line in v1_hash_file.read_text().splitlines())}
    results = []
    for n in (1, 2):
        run = root / f'run{n}'
        bronze = pl.read_parquet(run / 'bronze/records.parquet')
        silver = pl.read_parquet(run / 'silver/procurement_events.parquet')
        gold = pl.read_parquet(str(run / 'gold2/open_opportunities/*.parquet'))
        placsp_bronze = bronze.filter(pl.col('source') == 'placsp').select(
            pl.col('payload_json').str.json_path_match('$.publication_date')
            .str.slice(0, 10).str.to_date('%Y-%m-%d', strict=False).alias('publication_date'))
        ted = silver.filter(pl.col('source') == 'ted')
        procedures = ted.filter(pl.col('procedure_id').is_not_null())
        groups = procedures.group_by('procedure_id').agg(
            pl.len().alias('events'),
            pl.col('publication_date').null_count().alias('missing_dates'),
            (pl.col('publication_date') == pl.col('publication_date').max()).sum().alias('latest_day_ties'),
        )
        ambiguous = groups.filter((pl.col('events') > 1) &
                                  ((pl.col('missing_dates') > 0) | (pl.col('latest_day_ties') > 1)))
        exports = {p.name: sha(p) for p in sorted((run / 'exports/tableau').glob('*.csv'))}
        expected_exports = {'buyer_summary.csv', 'cpv_summary.csv', 'open_opportunities.csv',
                            'opportunities_monthly.csv', 'opportunity_cpv.csv'}
        if exports.keys() != expected_exports:
            raise ValueError(f'Unexpected exports: {sorted(exports)}')
        results.append(dict(
            run=n,
            manifest_counts=json.loads((run / 'gold/run_manifest.json').read_text())['counts'],
            ingestion=json.loads((run / 'bronze/ingestion_report.json').read_text()),
            gold_counts=json.loads((run / 'gold2/gold_manifest.json').read_text())['counts'],
            gold_manifest=json.loads((run / 'gold2/gold_manifest.json').read_text()),
            silver_rows=silver.height, gold_rows=gold.height,
            gold_publication_date_range=dict(earliest=str(gold['publication_date'].min()),
                                             latest=str(gold['publication_date'].max())),
            export_counts={name: info['row_count'] for name, info in json.loads(
                (run / 'exports/tableau/tableau_export_manifest.json').read_text())['files'].items()},
            ted=dict(events=ted.height, resolved_events=procedures.height,
                     unresolved_events=ted.height - procedures.height, procedures=groups.height,
                     competing_procedures=groups.filter(pl.col('events') > 1).height,
                     ambiguous_procedures=ambiguous.height,
                     ambiguous_events=ambiguous['events'].sum(),
                     resolvable_procedures=groups.height - ambiguous.height),
            placsp_dates_by_status=silver.filter((pl.col('source') == 'placsp') &
                                                        (pl.col('source_event_type') == 'notice_snapshot'))
                .group_by('status').agg(pl.len().alias('events'),
                                       pl.col('publication_date').null_count().alias('missing_date'))
                .sort('status', nulls_last=True).to_dicts(),
            placsp_dates=dict(
                bronze_observations=coverage(placsp_bronze, 'publication_date'),
                silver_events=coverage(silver.filter((pl.col('source') == 'placsp') &
                                                    (pl.col('source_event_type') == 'notice_snapshot')), 'publication_date'),
                gold_opportunities=coverage(gold.filter(pl.col('source') == 'placsp'), 'publication_date')),
            hashes=dict(
                silver_parquet_sha256=sha(run / 'silver/procurement_events.parquet'),
                silver_logical_sha256=logical_sha(silver, 'event_id'),
                gold_logical_sha256=logical_sha(gold, 'procedure_id'),
                csv_sha256=exports),
        ))
    return dict(v1_csv_hashes=v1_csv_hashes,
                csv_changed_from_v1={name: digest != v1_csv_hashes[name]
                                     for name, digest in results[0]['hashes']['csv_sha256'].items()},
                code_revision=(root / 'code-revision.txt').read_text().strip(),
                as_of='2026-06-30', inputs=inputs,
                ted_retrieval=dict(before_events=len(old), after_events=len(new),
                                   before_resolved_events=sum(bool(r.get('procedure-identifier') or r.get('BT-04-notice'))
                                                              for r in old.values()),
                                   same_notice_ids=old.keys() == new.keys(),
                                   shared_fields_changed_notices=changed_notices,
                                   shared_fields_changed=dict(changed_fields),
                                   old_ids_resolved_after=sum(bool(new[nd].get('procedure-identifier')) for nd in shared),
                                   missing_procedure_by_notice_type=dict(Counter(
                                       r.get('notice-type') for r in new.values() if not r.get('procedure-identifier')))),
                runs=results, two_run_hashes_equal=results[0]['hashes'] == results[1]['hashes'],
                two_run_counts_equal=all(results[0][k] == results[1][k] for k in
                                         ('manifest_counts', 'ingestion', 'gold_counts', 'silver_rows', 'gold_rows', 'ted',
                                          'placsp_dates', 'placsp_dates_by_status', 'gold_publication_date_range', 'export_counts')))



if __name__ == '__main__':
    result = measure(Path(sys.argv[1]), Path(sys.argv[2]))
    output = Path(__file__).with_name('measurements.json')
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n')
    print(output)
    if not result['two_run_hashes_equal'] or not result['two_run_counts_equal']:
        raise SystemExit('Two-run hashes differ')
