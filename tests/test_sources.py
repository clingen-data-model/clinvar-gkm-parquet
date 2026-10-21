"""sources: input resolution, checksum discovery, HTTP streaming, the line index, delta manifests.

No network: HTTP goes through a fake `urlopen`. The line index below follows the shape of `build_index` in
ClinVar-GKM's src/scripts/r2_layout.py (@ origin/1.0 25344ba); the releases and sha256s are synthetic.
"""

from __future__ import annotations

import email.message
import json
import shutil
import urllib.error

import pytest
from _input_helpers import FIXTURE, delta_manifest_json, local_input, sha256_of

from clinvar_gkm_parquet import sources
from clinvar_gkm_parquet.errors import InputError
from clinvar_gkm_parquet.model import InputFile
from clinvar_gkm_parquet.sources import (
    USER_AGENT,
    load_index,
    load_manifest,
    open_stream,
    parse_sidecar,
    resolve_input,
    resolve_release,
)

BASE = "https://data.clinvar-gkm.org"
L = "v1/1.1/"


def _sha(c: str) -> str:
    return c * 64


def _monthly(label, source, sha, *, archive=None, latest=False):
    path = f"{L}archive/{archive}/" if archive else L
    name = "clinvar-gkm_00-latest.json.gz" if latest else f"clinvar-gkm_{label}.json.gz"
    return {
        "name": name,
        "path": path + name,
        "size": 1000 + int(label[5:7]),
        "sha256": sha,
        "modified": None,
        "release": label,
        "source_release": source,
        "latest": latest,
    }


def _delta(release, *, folder=None, sha=None, manifest=True):
    compact = release[:7] + release[8:]
    folder = folder if folder is not None else "deltas/"
    stem = f"{L}{folder}clinvar-gkm-delta_{compact}"
    return {
        "release": release,
        "month": release[:7],
        "path": L + folder,
        "manifest": stem + ".manifest.json" if manifest else None,
        "bundle": {"path": stem + ".json.gz", "sha256": sha, "size": 10},
        "modified": None,
    }


def _weekly(release, sha, *, latest=False):
    compact = release[:7] + release[8:]
    name = "clinvar-gkm_00-latest_weekly.json.gz" if latest else f"clinvar-gkm_{compact}.json.gz"
    return {
        "name": name,
        "path": f"{L}weekly/{name}",
        "size": 500,
        "sha256": sha,
        "modified": None,
        "release": release,
        "latest": latest,
    }


@pytest.fixture
def index():
    """Line v1/1.1 on 2026-10-18 (synthetic): the 2026-10 monthly built from 2026-09-28 (ADR 0002), the 2026-08
    monthly from a non-month-aligned 2026-08-04, a 2025 monthly in the archive, the October weeklies."""
    return {
        "line": "v1/1.1",
        "status": "current",
        "schema_version": "1.1.0",
        "base_url": BASE,
        "current_month": "2026-10",
        "latest_release": "2026-10-18",
        "datasets": {
            "monthly": [
                _monthly("2026-08", "2026-08-04", _sha("8")),
                _monthly("2026-09", "2026-08-29", _sha("9")),
                _monthly("2026-10", "2026-09-28", _sha("a")),
                _monthly("2026-10", "2026-09-28", _sha("a"), latest=True),
            ],
            "weekly": [
                _weekly("2026-10-04", _sha("b")),
                _weekly("2026-10-11", _sha("c")),
                _weekly("2026-10-18", _sha("d")),
                _weekly("2026-10-18", _sha("d"), latest=True),
            ],
        },
        "deltas": [
            _delta("2026-08-11", folder="deltas/2026-08/", sha=_sha("1")),
            _delta("2026-08-22", folder="deltas/2026-08/", sha=_sha("2")),
            _delta("2026-08-29", folder="deltas/2026-08/", sha=_sha("3")),
            _delta("2026-09-06", folder="deltas/2026-09/", sha=_sha("4")),
            _delta("2026-09-28", folder="deltas/2026-09/", sha=_sha("5")),
            _delta("2026-10-04", sha=_sha("6")),
            _delta("2026-10-11", sha=_sha("7")),
            _delta("2026-10-18"),
        ],
        "archives": {
            "2025": {
                "monthly": [_monthly("2025-12", "2025-12-01", _sha("e"), archive="2025")],
                "deltas": [_delta("2025-12-08", folder="archive/2025/deltas/2025-12/")],
            }
        },
        "parquet": None,
    }


# ---------------------------------------------------------------------------------------------------------------
# Fake HTTP


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200, headers: dict | None = None, fail_after: int | None = None):
        self._body, self.status, self._pos, self._fail_after = body, status, 0, fail_after
        self.headers = email.message.Message()
        for k, v in {"Content-Length": str(len(body)), **(headers or {})}.items():
            self.headers[k] = v

    def readinto(self, b) -> int:
        if self._fail_after is not None and self._pos >= self._fail_after:
            raise ConnectionResetError("synthetic drop")
        end = len(self._body) if self._fail_after is None else self._fail_after
        n = min(len(b), end - self._pos, 7)
        b[:n] = self._body[self._pos : self._pos + n]
        self._pos += n
        return n

    def read(self) -> bytes:
        out, self._pos = self._body[self._pos :], len(self._body)
        return out

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


def _http_error(url, code, headers=None):
    msg = email.message.Message()
    for k, v in (headers or {}).items():
        msg[k] = v
    return urllib.error.HTTPError(url, code, "synthetic", msg, None)


@pytest.fixture
def fake_http(monkeypatch):
    """Install a scripted urlopen: `script` is a list of callables (request) -> response | raises."""
    calls, sleeps, script = [], [], []

    def urlopen(req, timeout=None):
        calls.append(req)
        return script.pop(0)(req)

    monkeypatch.setattr(sources, "_urlopen", urlopen)
    monkeypatch.setattr(sources, "_sleep", sleeps.append)
    return calls, sleeps, script


def _raise(exc):
    def f(req):
        raise exc

    return f


# ---------------------------------------------------------------------------------------------------------------
# resolve_input: local files and checksum discovery


def test_local_with_sidecar():
    inp = resolve_input(str(FIXTURE))
    assert inp.expected_sha256 == sha256_of(FIXTURE) and inp.sha256_source == "sidecar"
    assert inp.name == FIXTURE.name and inp.size == FIXTURE.stat().st_size and inp.release is None


def test_explicit_wins():
    inp = resolve_input(str(FIXTURE), sha256="A" * 64)
    assert inp.expected_sha256 == "a" * 64 and inp.sha256_source == "explicit"


def test_explicit_must_be_hex():
    with pytest.raises(InputError, match="64-digit"):
        resolve_input(str(FIXTURE), sha256="abc")


def test_missing_file(tmp_path):
    with pytest.raises(InputError, match="No such file"):
        resolve_input(str(tmp_path / "nope.json.gz"))


def test_unsupported_scheme():
    with pytest.raises(InputError, match="https"):
        resolve_input("http://data.clinvar-gkm.org/v1/1.1/clinvar-gkm_2026-10.json.gz")


@pytest.mark.parametrize(
    "text, ok",
    [
        (f"{_sha('a')}  clinvar-gkm_2026-10.json.gz\n", True),
        (f"{_sha('a')} *clinvar-gkm_2026-10.json.gz\n", True),
        (f"{_sha('A')}\n", True),
        (f"{_sha('a')}  other.json.gz\n", False),
        ("not a checksum\n", False),
        (f"{_sha('a')}  a\n{_sha('b')}  b\n", False),
        ("", False),
    ],
)
def test_parse_sidecar(text, ok):
    if ok:
        assert parse_sidecar(text, "clinvar-gkm_2026-10.json.gz", where="x") == _sha("a")
    else:
        with pytest.raises(InputError):
            parse_sidecar(text, "clinvar-gkm_2026-10.json.gz", where="x")


def test_index_sha_and_release_for_a_local_monthly(tmp_path, index):
    p = tmp_path / "clinvar-gkm_2026-10.json.gz"
    p.write_bytes(b"x")
    inp = resolve_input(str(p), index=index)
    assert (inp.expected_sha256, inp.sha256_source, inp.release) == (_sha("a"), "index", "2026-09-28")


def test_sidecar_beats_index(tmp_path, index):
    p = tmp_path / "clinvar-gkm_2026-10.json.gz"
    p.write_bytes(b"x")
    (tmp_path / (p.name + ".sha256")).write_text(f"{_sha('f')}  {p.name}\n")
    inp = resolve_input(str(p), index=index)
    assert (inp.expected_sha256, inp.sha256_source) == (_sha("f"), "sidecar")


def test_release_from_names_without_index(tmp_path):
    for name, rel in [
        ("clinvar-gkm_2026-1018.json.gz", "2026-10-18"),
        ("clinvar-gkm-delta_2026-1011.json.gz", "2026-10-11"),
        ("clinvar-gkm-delta_2026-1011.manifest.json", "2026-10-11"),
        ("clinvar-gkm_2026-10.json.gz", None),
    ]:
        p = tmp_path / name
        p.write_bytes(b"x")
        inp = resolve_input(str(p))
        assert inp.release == rel and inp.expected_sha256 is None and inp.sha256_source is None


def test_frozen_manifest(tmp_path):
    p = tmp_path / "clinvar-gkm_2026-10.json.gz"
    p.write_bytes(b"x")
    frozen = {
        "line": "v1/1.0",
        "files": [{"key": "v1/1.0/clinvar-gkm_2026-10.json.gz", "size": 1, "sha256": _sha("c")}],
    }
    inp = resolve_input(str(p), frozen_manifest=frozen)
    assert (inp.expected_sha256, inp.sha256_source) == (_sha("c"), "frozen-manifest")


def test_delta_sha_from_index(tmp_path, index):
    p = tmp_path / "clinvar-gkm-delta_2026-1011.json.gz"
    p.write_bytes(b"x")
    inp = resolve_input(str(p), index=index)
    assert (inp.expected_sha256, inp.sha256_source, inp.release) == (_sha("7"), "index", "2026-10-11")


# ---------------------------------------------------------------------------------------------------------------
# Aliases


def test_local_alias_resolves_to_the_dated_name(tmp_path, index):
    p = tmp_path / "clinvar-gkm_00-latest.json.gz"
    p.write_bytes(b"x" * 1010)  # the 2026-10 entry's size
    inp = resolve_input(str(p), index=index)
    assert inp.name == "clinvar-gkm_2026-10.json.gz" and inp.release == "2026-09-28"
    assert inp.expected_sha256 == _sha("a") and inp.source == str(p)


def test_local_alias_matched_by_its_sidecar(tmp_path, index):
    p = tmp_path / "clinvar-gkm_00-latest.json.gz"
    p.write_bytes(b"x" * 1009)
    (tmp_path / (p.name + ".sha256")).write_text(f"{_sha('9')}  {p.name}\n")  # an older copy: 2026-09
    inp = resolve_input(str(p), index=index)
    assert inp.name == "clinvar-gkm_2026-09.json.gz" and inp.release == "2026-08-29"


def test_alias_size_mismatch(tmp_path, index):
    p = tmp_path / "clinvar-gkm_00-latest.json.gz"
    p.write_bytes(b"x")
    with pytest.raises(InputError, match="not the current alias"):
        resolve_input(str(p), index=index)


def test_alias_needs_an_index(tmp_path):
    p = tmp_path / "clinvar-gkm_00-latest.json.gz"
    p.write_bytes(b"x")
    with pytest.raises(InputError, match="alias"):
        resolve_input(str(p))


def test_url_alias_reads_the_dated_url(index, fake_http):
    calls, _, script = fake_http
    script.append(_raise(_http_error("x", 404)))  # no sidecar for the alias
    inp = resolve_input(f"{BASE}/{L}weekly/clinvar-gkm_00-latest_weekly.json.gz", index=index)
    assert inp.source == f"{BASE}/{L}weekly/clinvar-gkm_2026-1018.json.gz"
    assert inp.name == "clinvar-gkm_2026-1018.json.gz" and inp.release == "2026-10-18"
    assert (inp.expected_sha256, inp.sha256_source) == (_sha("d"), "index")
    assert calls[0].full_url.endswith("clinvar-gkm_00-latest_weekly.json.gz.sha256")
    assert calls[0].get_header("User-agent") == USER_AGENT


def test_url_sidecar(fake_http):
    calls, _, script = fake_http
    name = "clinvar-gkm_2026-10.json.gz"
    script.append(lambda req: FakeResponse(f"{_sha('a')}  {name}\n".encode()))
    inp = resolve_input(f"{BASE}/{L}{name}")
    assert (inp.expected_sha256, inp.sha256_source) == (_sha("a"), "sidecar")


# ---------------------------------------------------------------------------------------------------------------
# open_stream over HTTP


def _url_input(url):
    return InputFile(
        source=url, name=url.rsplit("/", 1)[-1], release=None, expected_sha256=None, sha256_source=None
    )


def test_http_stream_resumes_with_range(fake_http):
    calls, sleeps, script = fake_http
    body = bytes(range(256)) * 40
    script.append(lambda req: FakeResponse(body, headers={"ETag": '"v1"'}, fail_after=1000))

    def resumed(req):
        assert req.get_header("Range") == "bytes=1000-" and req.get_header("If-range") == '"v1"'
        return FakeResponse(
            body[1000:], status=206, headers={"Content-Range": f"bytes 1000-{len(body) - 1}/{len(body)}"}
        )

    script.append(resumed)
    with open_stream(_url_input(f"{BASE}/{L}x.json.gz")) as fh:
        assert fh.read() == body
    assert len(calls) == 2 and len(sleeps) == 1
    assert all(c.get_header("User-agent") == USER_AGENT for c in calls)
    assert calls[0].get_header("Accept-encoding") == "identity"


def test_http_stream_refuses_a_changed_file(fake_http):
    _, _, script = fake_http
    body = b"y" * 100
    script.append(lambda req: FakeResponse(body, headers={"ETag": '"v1"'}, fail_after=50))
    script.append(lambda req: FakeResponse(b"z" * 120))  # If-Range failed: the server sends the whole new file
    with pytest.raises(InputError, match="could not resume at byte 50"):
        with open_stream(_url_input(f"{BASE}/{L}x.json.gz")) as fh:
            fh.read()


def test_retry_429_and_5xx(fake_http):
    calls, sleeps, script = fake_http
    script.append(_raise(_http_error("x", 429, {"Retry-After": "3"})))
    script.append(_raise(_http_error("x", 503)))
    script.append(_raise(urllib.error.URLError("synthetic")))
    script.append(lambda req: FakeResponse(b"ok"))
    with open_stream(_url_input(f"{BASE}/{L}x.json.gz")) as fh:
        assert fh.read() == b"ok"
    assert len(calls) == 4 and sleeps == [3.0, 2.0, 4.0]


def test_no_retry_on_404(fake_http):
    calls, _, script = fake_http
    script.append(_raise(_http_error("x", 404)))
    with pytest.raises(InputError, match="HTTP 404"):
        open_stream(_url_input(f"{BASE}/{L}x.json.gz"))
    assert len(calls) == 1


def test_gives_up(fake_http):
    calls, _, script = fake_http
    script.extend([_raise(_http_error("x", 500))] * sources.MAX_ATTEMPTS)
    with pytest.raises(InputError, match="giving up"):
        open_stream(_url_input(f"{BASE}/{L}x.json.gz"))
    assert len(calls) == sources.MAX_ATTEMPTS


def test_local_stream():
    with open_stream(local_input(FIXTURE)) as fh:
        assert fh.read() == FIXTURE.read_bytes()


# ---------------------------------------------------------------------------------------------------------------
# load_index, load_manifest


def test_load_index(tmp_path, index):
    p = tmp_path / "index.json"
    p.write_text(json.dumps(index))
    assert load_index(str(p)) == index


@pytest.mark.parametrize("text", ["[]", "{}", '{"datasets": {"monthly": []}, "deltas": []}', "not json"])
def test_load_index_malformed(tmp_path, text):
    p = tmp_path / "index.json"
    p.write_text(text)
    with pytest.raises(InputError):
        load_index(str(p))


def _manifest_file(tmp_path, data, name="clinvar-gkm-delta_2026-1011.manifest.json"):
    p = tmp_path / name
    p.write_text(json.dumps(data))
    return resolve_input(str(p))


def test_load_manifest(tmp_path):
    data = delta_manifest_json(
        "2026-10-11",
        "2026-10-04",
        {"scv": ["clinvar.submission:SCV000000001.1"], "vcv": []},
        checkpoint_full={
            "path": f"{L}clinvar-gkm_2026-10.json.gz",
            "release": "2026-09-28",
            "month": "2026-10",
            "sha256": _sha("a"),
        },
        schema_version="1.1.0",
        sha256=_sha("7"),
    )
    m = load_manifest(_manifest_file(tmp_path, data))
    assert (m.release, m.baseline_release) == ("2026-10-11", "2026-10-04")
    assert m.deleted == {"scv": ("clinvar.submission:SCV000000001.1",)}
    assert m.checkpoint_full["release"] == "2026-09-28" and m.raw == data


def test_load_manifest_first_release_has_null_baseline(tmp_path):
    m = load_manifest(_manifest_file(tmp_path, delta_manifest_json("2026-10-11", None)))
    assert m.baseline_release is None


@pytest.mark.parametrize(
    "mutate, match",
    [
        (lambda d: d["sections"].update(proposition={"deleted": []}), "unknown section 'proposition'"),
        (lambda d: d.update(release="2026-1011"), "release"),
        (lambda d: d.update(baseline_release=20261004), "baseline_release"),
        (lambda d: d["sections"].update(scv={"added": 1}), "deleted is missing"),
        (lambda d: d["sections"].update(scv={"deleted": [1]}), "deleted is missing"),
        (lambda d: d.pop("sections"), "sections"),
        (lambda d: d.update(checkpoint_full="2026-10"), "checkpoint_full"),
    ],
)
def test_load_manifest_malformed(tmp_path, mutate, match):
    data = delta_manifest_json("2026-10-11", "2026-10-04")
    mutate(data)
    with pytest.raises(InputError, match=match):
        load_manifest(_manifest_file(tmp_path, data))


def test_load_manifest_release_must_match_the_file(tmp_path):
    with pytest.raises(InputError, match="does not match"):
        load_manifest(_manifest_file(tmp_path, delta_manifest_json("2026-10-18", "2026-10-11")))


# ---------------------------------------------------------------------------------------------------------------
# resolve_release


def test_release_with_a_weekly_full(index):
    b = resolve_release(index, "2026-10-11", base_url=BASE)
    assert b.full.source == f"{BASE}/{L}weekly/clinvar-gkm_2026-1011.json.gz"
    assert (b.full.release, b.full_source_release, b.deltas) == ("2026-10-11", "2026-10-11", ())
    assert (b.full.expected_sha256, b.full.sha256_source) == (_sha("c"), "index")


def test_release_latest(index):
    b = resolve_release(index, "latest", base_url=BASE + "/")
    assert b.full.name == "clinvar-gkm_2026-1018.json.gz" and b.deltas == ()


def test_replay_rule_crosses_into_the_monthlys_own_month(index):
    # The weeklies are gone: the 2026-10 monthly is from 2026-09-28, so 10-04 and 10-11 must be replayed (the
    # docs' old `release[:7] > month` rule dropped them; design plan §4.3).
    for w in index["datasets"]["weekly"]:
        w["release"] = "2026-10-25"
    b = resolve_release(index, "2026-10-11", base_url=BASE)
    assert b.full.name == "clinvar-gkm_2026-10.json.gz"
    assert (b.full.release, b.full_source_release) == ("2026-09-28", "2026-09-28")
    assert [(d.release, m.release) for d, m in b.deltas] == [("2026-10-04",) * 2, ("2026-10-11",) * 2]
    d, m = b.deltas[0]
    assert d.source == f"{BASE}/{L}deltas/clinvar-gkm-delta_2026-1004.json.gz" and d.expected_sha256 == _sha("6")
    assert m.source == f"{BASE}/{L}deltas/clinvar-gkm-delta_2026-1004.manifest.json"
    assert m.expected_sha256 is None


def test_replay_rule_non_month_aligned_monthly(index):
    # 2026-08 is built from 2026-08-04: 2026-08-22 replays 08-11 and 08-22 onto it.
    b = resolve_release(index, "2026-08-22", base_url=BASE)
    assert b.full.name == "clinvar-gkm_2026-08.json.gz" and b.full_source_release == "2026-08-04"
    assert [d.release for d, _ in b.deltas] == ["2026-08-11", "2026-08-22"]
    assert b.deltas[0][0].source.endswith("deltas/2026-08/clinvar-gkm-delta_2026-0811.json.gz")


def test_replay_rule_target_is_a_monthly_source(index):
    b = resolve_release(index, "2026-08-29", base_url=BASE)
    assert b.full.name == "clinvar-gkm_2026-09.json.gz" and b.deltas == ()


def test_replay_rule_uses_archives(index):
    b = resolve_release(index, "2025-12-08", base_url=BASE)
    assert b.full.source == f"{BASE}/{L}archive/2025/clinvar-gkm_2025-12.json.gz"
    assert [d.source.rsplit("/", 2)[-2] for d, _ in b.deltas] == ["2025-12"]


def test_release_not_in_the_index(index):
    with pytest.raises(InputError, match="no such release.*nearest earlier release is 2026-10-11"):
        resolve_release(index, "2026-10-15", base_url=BASE)


def test_release_after_latest(index):
    with pytest.raises(InputError, match="newest release"):
        resolve_release(index, "2026-10-25", base_url=BASE)


def test_release_bad_format(index):
    with pytest.raises(InputError, match="YYYY-MM-DD"):
        resolve_release(index, "2026-10", base_url=BASE)


def test_release_before_any_monthly(index):
    index["archives"] = {}
    index["deltas"].insert(0, _delta("2026-07-27", folder="deltas/2026-07/"))
    with pytest.raises(InputError, match="no official monthly"):
        resolve_release(index, "2026-07-27", base_url=BASE)


def test_release_delta_without_manifest(index):
    index["datasets"]["weekly"] = []
    index["deltas"][6] = _delta("2026-10-11", manifest=False)
    with pytest.raises(InputError, match="2026-10-11 delta has no bundle or no manifest"):
        resolve_release(index, "2026-10-11", base_url=BASE)


def test_monthly_with_unknown_source_release_is_skipped(index, caplog):
    index["datasets"]["weekly"] = []
    index["datasets"]["monthly"][2]["source_release"] = None  # 2026-10
    b = resolve_release(index, "2026-10-11", base_url=BASE)
    assert b.full.name == "clinvar-gkm_2026-09.json.gz"
    assert [d.release for d, _ in b.deltas] == ["2026-09-06", "2026-09-28", "2026-10-04", "2026-10-11"]
    assert "source_release unknown for clinvar-gkm_2026-10.json.gz" in caplog.text


def test_copy_of_fixture_alias_end_to_end(tmp_path, index):
    """A local 00-latest copy with its own sidecar keeps its sidecar checksum and gets the dated name."""
    p = tmp_path / "clinvar-gkm_00-latest.json.gz"
    shutil.copy(FIXTURE, p)
    sha = sha256_of(FIXTURE)
    (tmp_path / (p.name + ".sha256")).write_text(f"{sha}  {p.name}\n")
    index["datasets"]["monthly"][2].update(sha256=sha, size=p.stat().st_size)
    inp = resolve_input(str(p), index=index)
    assert (inp.name, inp.expected_sha256, inp.sha256_source) == ("clinvar-gkm_2026-10.json.gz", sha, "sidecar")
