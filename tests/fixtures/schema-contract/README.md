# schema-contract (a made-up schema version that meets the contract)

`9.9.0/` is a small schema set under a made-up bundle `schema_version`, written by hand, for nine real sections
(`sequenceReference`, `location`, `allele`, `copyNumberCount`, `copyNumberChange`, `gene`, `variation`, `condition`,
`conditionSet`). It meets the schema contract of ADR 0006 §4, so it compiles with no overlay: it allows only the
properties the producer emits, closes every extension value, writes each pointer as a string whose `pattern` names
its target sections, narrows an integer-or-Range to an integer where the producer writes no Range, and leaves no
class that reaches itself. Its `$id`s equal the URLs a real set would be fetched from, the alignment ADR 0006 asks
ClinVar-GKM for.

`tests/test_compiler.py` builds the real excerpt's records of these sections against it, with their keys sorted as
the contract requires. No record is hand-written. `9.9.0/MANIFEST.json` has each file's sha256; the set is read by
`schemas.read_set`.
