"""Delta chain check and replay into DuckDB (design plan §4.3).

Owned by the input-side work package. Produces one DuckDB table per bundle section,
`bundle_<section>(key VARCHAR, record VARCHAR)`: `key` is the decoded bundle key, `record` the exact record text.
With no deltas this is the full as staged. With deltas: per key, the row with the highest seq wins (0 = the full,
i = the i-th delta); a manifest delete is a tombstone at that seq, and within one delta an upsert beats a delete.
"""

from __future__ import annotations

import duckdb

from .model import DeltaManifest, StagedBundle


def check_chain(full_source_release: str | None, manifests: list[DeltaManifest]) -> None:
    """Raise ChainError unless manifests[0].baseline_release == full_source_release and each later
    baseline_release equals the previous release."""
    raise NotImplementedError


def load_state(
    con: duckdb.DuckDBPyConnection,
    full: StagedBundle,
    deltas: list[tuple[StagedBundle, DeltaManifest]],
) -> dict[str, int]:
    """Create the bundle_<section> tables holding the final state. Returns {section: row count}."""
    raise NotImplementedError
