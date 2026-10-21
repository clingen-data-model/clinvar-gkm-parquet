"""Delta chain check and replay into DuckDB (design plan §4.3).

Owned by the input-side work package. Produces one DuckDB table per bundle section (all 20; a section absent
from every input is an empty table), `bundle_<section>(key VARCHAR, record VARCHAR)`: `key` is the decoded bundle
key, `record` the exact record text. With no deltas this is the full as staged. With deltas: per key, the row
with the highest seq wins (0 = the full, i = the i-th delta); a manifest delete is a tombstone at that seq, and
within one delta an upsert beats a delete.

A delete of a key that is not in the state it applies to is logged as a warning, not an error. That is how
ClinVar-GKM treats it: the docs' replay example drops it (`pop(pk, None)`) and the delta-reconstruction oracle
(`oracle-delta-reconstruction.sh`, runbook 1.1.0 §4.4) applies deletes as a `NOT IN` filter, so a missing
key changes nothing and the oracle still passes. It removes no data, so nothing is dropped silently.
"""

from __future__ import annotations

import logging

import duckdb

from .bundle import SECTIONS
from .errors import BundleFormatError, ChainError, InputError
from .model import DeltaManifest, StagedBundle, StagedSection

log = logging.getLogger(__name__)

_MIN_MAX_LINE = 2_000_000  # DuckDB's default max_line_size


def check_chain(full_source_release: str | None, manifests: list[DeltaManifest]) -> None:
    """Raise ChainError unless manifests[0].baseline_release == full_source_release and each later
    baseline_release equals the previous release."""
    if not manifests:
        return
    if full_source_release is None:
        raise ChainError(
            "the full's source release is unknown, so the first delta can't be checked against it; pass "
            "--full-source-release or a line index that lists it"
        )
    prev, prev_what = full_source_release, f"the full (source release {full_source_release})"
    for i, m in enumerate(manifests):
        if m.baseline_release != prev:
            what = "first " if i == 0 else ""
            raise ChainError(
                f"broken delta chain: the {what}delta {m.release} has baseline_release {m.baseline_release}, but "
                f"it follows {prev_what}. A week is missing or the deltas are out of order."
            )
        if not m.release > prev:
            raise ChainError(f"broken delta chain: delta {m.release} is not later than {prev}")
        prev, prev_what = m.release, f"delta {m.release}"


def table_name(section: str) -> str:
    return f"bundle_{section}"


def _read_staging(sec: StagedSection) -> str:
    """SQL reading a staging file as (key, record): the key decoded from its JSON form, the record as is."""
    path = str(sec.path).replace("'", "''")
    max_line = max(_MIN_MAX_LINE, sec.max_line_bytes + 1024)
    return (
        f"SELECT json_extract_string(k, '$') AS key, record FROM read_csv('{path}', delim='\t', quote='', "
        f"escape='', header=false, auto_detect=false, new_line='\\n', strict_mode=true, compression='gzip', "
        f"columns={{'k': 'VARCHAR', 'record': 'VARCHAR'}}, max_line_size={max_line})"
    )


def _dup_check(con: duckdb.DuckDBPyConnection, table: str, staged: StagedBundle, section: str, where: str = ""):
    row = con.execute(
        f'SELECT key, count(*) FROM "{table}" {where} GROUP BY key HAVING count(*) > 1 ORDER BY key LIMIT 1'
    ).fetchone()
    if row:
        raise BundleFormatError(f"{staged.input.name}: section {section}, key {row[0]}: appears {row[1]} times")


def _check_count(con, table: str, staged: StagedBundle, section: str, expected: int, where: str = "") -> None:
    n = con.execute(f'SELECT count(*) FROM "{table}" {where}').fetchone()[0]
    if n != expected:
        raise InputError(f"{staged.input.name}: section {section}: staged {expected} records, loaded {n}")


def _manifest_sha(m: DeltaManifest) -> str | None:
    sha = m.raw.get("sha256") or (m.raw.get("bundle") or {}).get("sha256")
    return sha.lower() if isinstance(sha, str) else None


def load_state(
    con: duckdb.DuckDBPyConnection,
    full: StagedBundle,
    deltas: list[tuple[StagedBundle, DeltaManifest]],
) -> dict[str, int]:
    """Create the bundle_<section> tables holding the final state. Returns {section: row count}.

    The caller checks the chain first (check_chain). This checks that every input has the full's
    schema_version, that each delta is the one its manifest names (release, sha256), and that no key repeats
    within a section of one input."""
    for staged, man in deltas:
        if staged.schema_version != full.schema_version:
            raise ChainError(
                f"{staged.input.name} has schema_version {staged.schema_version}, the full "
                f"{full.input.name} has {full.schema_version}; every input in a chain must have the same one"
            )
        if staged.input.release and staged.input.release != man.release:
            raise ChainError(
                f"{staged.input.name} (release {staged.input.release}) is paired with the "
                f"manifest for {man.release}"
            )
        msha = _manifest_sha(man)
        if msha and msha != staged.sha256:
            raise ChainError(
                f"{staged.input.name}: sha256 {staged.sha256} differs from the one its manifest "
                f"({man.release}) names, {msha}"
            )

    counts: dict[str, int] = {}
    for section in SECTIONS:
        t = table_name(section)
        sec = full.sections.get(section)
        if sec is None:
            con.execute(f'CREATE OR REPLACE TABLE "{t}" (key VARCHAR, record VARCHAR)')
        else:
            con.execute(f'CREATE OR REPLACE TABLE "{t}" AS {_read_staging(sec)}')
            _check_count(con, t, full, section, sec.records)
            _dup_check(con, t, full, section)
        if deltas:
            _replay_section(con, section, t, deltas)
        counts[section] = con.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0]
    return counts


def _replay_section(con, section: str, t: str, deltas: list[tuple[StagedBundle, DeltaManifest]]) -> None:
    touched = f"_touched_{section}"
    con.execute(
        f'CREATE OR REPLACE TEMP TABLE "{touched}" (key VARCHAR, record VARCHAR, seq INTEGER, is_upsert BOOLEAN)'
    )
    any_rows = False
    for seq, (staged, man) in enumerate(deltas, start=1):
        sec = staged.sections.get(section)
        if sec is not None:
            any_rows = True
            con.execute(f'INSERT INTO "{touched}" SELECT key, record, {seq}, true FROM ({_read_staging(sec)})')
            where = f"WHERE seq = {seq} AND is_upsert"
            _check_count(con, touched, staged, section, sec.records, where)
            _dup_check(con, touched, staged, section, where)
        keys = man.deleted.get(section)
        if keys:
            any_rows = True
            con.execute(
                f'INSERT INTO "{touched}" SELECT DISTINCT unnest(?::VARCHAR[]), NULL, {seq}, false', [list(keys)]
            )
    if not any_rows:
        con.execute(f'DROP TABLE "{touched}"')
        return

    # A delete whose key is absent from the state it applies to (the state after the previous delta).
    missing = con.execute(
        f"""
        WITH tomb AS (SELECT DISTINCT key, seq FROM "{touched}" WHERE NOT is_upsert),
        ev AS (
            SELECT key, 0 AS seq, true AS present FROM "{t}" WHERE key IN (SELECT key FROM tomb)
            UNION ALL
            SELECT key, seq, bool_or(is_upsert) AS present FROM "{touched}"
            WHERE key IN (SELECT key FROM tomb) GROUP BY key, seq
        ),
        miss AS (
            SELECT tomb.key, tomb.seq FROM tomb LEFT JOIN ev ON ev.key = tomb.key AND ev.seq < tomb.seq
            GROUP BY tomb.key, tomb.seq HAVING arg_max(ev.present, ev.seq) IS NOT TRUE
        )
        SELECT seq, count(*), list(key ORDER BY key)[1:5] FROM miss GROUP BY seq ORDER BY seq
        """
    ).fetchall()
    for seq, n, keys in missing:
        log.warning(
            "delta %s: %d deleted %s key(s) not in the prior state (a no-op, as in ClinVar-GKM's replay), e.g. %s",
            deltas[seq - 1][1].release,
            n,
            section,
            ", ".join(keys),
        )

    con.execute(f'DELETE FROM "{t}" WHERE key IN (SELECT key FROM "{touched}")')
    con.execute(
        f"""
        INSERT INTO "{t}"
        SELECT key, record FROM "{touched}"
        QUALIFY row_number() OVER (PARTITION BY key ORDER BY seq DESC, is_upsert DESC) = 1 AND is_upsert
        """
    )
    con.execute(f'DROP TABLE "{touched}"')
