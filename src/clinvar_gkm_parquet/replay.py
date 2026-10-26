"""Delta chain check and in-stream delta replay (design plan §4.3).

Owned by the input-side work package. Deltas are small (a weekly delta is about 10 MB gzip), so they are read
into memory before the full streams (`read_deltas`) and folded, oldest first, into one net change map per section
(`fold`): key -> its final record, or a tombstone. The rule is the one ClinVar-GKM's replay applies: per key, the
latest delta that touches it wins (seq 0 = the full, i = the i-th delta); within one delta the manifest's deletes
apply before the bundle's upserts, so an upsert beats a delete of the same key.

While the full streams past (build.py), `SectionReplay` applies a section's map: a full record whose key the map
holds is dropped (it is replaced or deleted), and at the section's end the map's upserts are appended, in the
map's order (the order the deltas first touch each key), so the staged section is the final state.

A delete of a key that is not in the state it applies to (the state after the previous delta) is logged as a
warning, not an error. That is how ClinVar-GKM treats it: the docs' replay example drops it (`pop(pk, None)`) and
the delta-reconstruction oracle (`oracle-delta-reconstruction.sh`, runbook 1.1.0 §4.4) applies deletes as a
`NOT IN` filter, so a missing key changes nothing and the oracle still passes. It removes no data, so nothing is
dropped silently.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from .bundle import canonical_key, stream_bundle
from .errors import BundleFormatError, ChainError
from .model import DeltaBundle, DeltaManifest, InputFile

log = logging.getLogger(__name__)

Write = Callable[[bytes, bytes], None]


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


# ---------------------------------------------------------------------------------------------------------------
# Reading the deltas


class _MemorySink:
    def __init__(self):
        self.sections: dict[str, list[tuple[bytes, bytes]]] = {}

    def begin(self, schema_version: str) -> None:
        pass

    def open_section(self, section: str) -> Write:
        recs: list[tuple[bytes, bytes]] = []
        self.sections[section] = recs
        return lambda k, r: recs.append((k, r))

    def close_section(self, section: str) -> None:
        pass


def _manifest_sha(m: DeltaManifest) -> str | None:
    sha = m.raw.get("sha256") or (m.raw.get("bundle") or {}).get("sha256")
    return sha.lower() if isinstance(sha, str) else None


def read_delta(inp: InputFile, man: DeltaManifest) -> DeltaBundle:
    """Read one delta bundle into memory and check it is the one its manifest names (release, sha256)."""
    sink = _MemorySink()
    streamed = stream_bundle(inp, sink)
    if inp.release and inp.release != man.release:
        raise ChainError(f"{inp.name} (release {inp.release}) is paired with the manifest for {man.release}")
    msha = _manifest_sha(man)
    if msha and msha != streamed.sha256:
        raise ChainError(
            f"{inp.name}: sha256 {streamed.sha256} differs from the one its manifest ({man.release}) names, {msha}"
        )
    return DeltaBundle(streamed=streamed, sections=sink.sections)


def check_schema_versions(full_name: str, full_version: str, deltas: list[tuple[DeltaBundle, DeltaManifest]]):
    """Every input in a chain must have the full's schema_version."""
    for d, _ in deltas:
        if d.schema_version != full_version:
            raise ChainError(
                f"{d.input.name} has schema_version {d.schema_version}, the full {full_name} has "
                f"{full_version}; every input in a chain must have the same one"
            )


# ---------------------------------------------------------------------------------------------------------------
# Folding the deltas into one change map per section


@dataclass
class SectionChanges:
    """The net effect of the deltas on one section."""

    section: str
    # canonical key (model: bundle.canonical_key) -> (key as JSON bytes, record bytes), or None for a tombstone
    final: dict[bytes, tuple[bytes, bytes] | None] = field(default_factory=dict)
    # delta seq -> deleted keys known (from earlier deltas) to be absent from the state the delete applies to
    absent_deletes: dict[int, list[bytes]] = field(default_factory=dict)
    # key -> seqs of the deletes that apply to the full's state (no earlier delta touched the key)
    full_deletes: dict[bytes, list[int]] = field(default_factory=dict)


def fold(deltas: list[tuple[DeltaBundle, DeltaManifest]]) -> dict[str, SectionChanges]:
    """Fold the deltas, oldest first, into one SectionChanges per section they touch. Fails on a key that
    repeats within one section of one delta."""
    out: dict[str, SectionChanges] = {}
    status: dict[str, dict[bytes, bool]] = {}  # section -> key -> present after the deltas folded so far
    for seq, (delta, man) in enumerate(deltas, start=1):
        sections = list(dict.fromkeys([*delta.sections, *man.deleted]))
        for section in sections:
            ch = out.setdefault(section, SectionChanges(section))
            st = status.setdefault(section, {})
            ups: dict[bytes, tuple[bytes, bytes]] = {}
            for kj, rec in delta.sections.get(section, ()):
                ck = canonical_key(kj)
                if ck in ups:
                    n = sum(1 for k, _ in delta.sections[section] if canonical_key(k) == ck)
                    raise BundleFormatError(
                        f"{delta.input.name}: section {section}, key {ck.decode('utf-8', 'replace')}: "
                        f"appears {n} times"
                    )
                ups[ck] = (kj, rec)
            dels = [k.encode("utf-8", "surrogatepass") for k in dict.fromkeys(man.deleted.get(section, ()))]
            for ck in dels:  # against the state after the previous delta
                if ck in st:
                    if not st[ck]:
                        ch.absent_deletes.setdefault(seq, []).append(ck)
                else:
                    ch.full_deletes.setdefault(ck, []).append(seq)
            for ck in dels:
                ch.final[ck] = None
                st[ck] = False
            for ck, v in ups.items():
                ch.final[ck] = v
                st[ck] = True
    return out


class SectionReplay:
    """Applies one section's SectionChanges while the full streams past (module docstring)."""

    def __init__(self, changes: SectionChanges, full_name: str, releases: list[str]):
        self.changes = changes
        self.full_name = full_name
        self.releases = releases  # releases[seq - 1] is delta seq's release
        self.seen: set[bytes] = set()

    def wrap(self, write: Write) -> Write:
        """The per-record function for the full's records of this section."""
        final, seen, section, name = self.changes.final, self.seen, self.changes.section, self.full_name

        def add(kj: bytes, rec: bytes) -> None:
            ck = kj[1:-1] if b"\\" not in kj else canonical_key(kj)
            if ck in final:
                if ck in seen:
                    raise BundleFormatError(
                        f"{name}: section {section}, key {ck.decode('utf-8', 'replace')}: appears more than once"
                    )
                seen.add(ck)  # replaced or deleted: the final version, if any, is appended by finish()
                return
            write(kj, rec)

        return add

    def finish(self, write: Write) -> None:
        """Append the net upserts and log the deletes of keys that were not there."""
        for v in self.changes.final.values():
            if v is not None:
                write(*v)
        missing = {seq: list(keys) for seq, keys in self.changes.absent_deletes.items()}
        for ck, seqs in self.changes.full_deletes.items():
            if ck not in self.seen:
                for seq in seqs:
                    missing.setdefault(seq, []).append(ck)
        for seq in sorted(missing):
            keys = sorted(missing[seq])
            log.warning(
                "delta %s: %d deleted %s key(s) not in the prior state (a no-op, as in ClinVar-GKM's replay), "
                "e.g. %s",
                self.releases[seq - 1],
                len(keys),
                self.changes.section,
                ", ".join(k.decode("utf-8", "replace") for k in keys[:5]),
            )
