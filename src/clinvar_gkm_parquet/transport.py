"""Read bytes from a local path or a URL, for any producer's files.

Every request sends `User-Agent: <tool>/<version>`; one stream per large file; retry 429 and 5xx with backoff;
resume an interrupted large file with HTTP Range. Unit tests never touch the network (`_urlopen` and `_sleep` are
the indirections they replace).
"""

from __future__ import annotations

import http.client
import io
import logging
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import BinaryIO

from . import __version__
from .errors import InputError
from .model import InputFile

log = logging.getLogger(__name__)

TOOL_NAME = "clinvar-gkm-parquet"  # the distribution's name: the User-Agent and the cache folder say it
USER_AGENT = f"{TOOL_NAME}/{__version__}"
MAX_ATTEMPTS = 8
TIMEOUT_S = 60
_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})

# Indirections the unit tests replace (no network in tests).
_urlopen = urllib.request.urlopen
_sleep = time.sleep


def is_url(source: str) -> bool:
    if source.startswith("https://"):
        return True
    if "://" in source:
        raise InputError(f"{source}: only local paths and https:// URLs are supported")
    return False


def _backoff(attempt: int, retry_after: str | None = None) -> float:
    if retry_after and retry_after.strip().isdigit():
        return min(float(retry_after), 120.0)
    return min(2.0**attempt, 60.0)


def _request(url: str, headers: dict[str, str] | None = None, *, allow_404: bool = False):
    """GET `url` with retries on 429, 5xx and connection errors. Returns the response, or None on a 404 when
    allow_404."""
    hdrs = {"User-Agent": USER_AGENT, "Accept-Encoding": "identity", **(headers or {})}
    last: Exception | None = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            return _urlopen(urllib.request.Request(url, headers=hdrs), timeout=TIMEOUT_S)
        except urllib.error.HTTPError as e:
            if e.code == 404 and allow_404:
                return None
            if e.code not in _RETRY_STATUS:
                raise InputError(f"{url}: HTTP {e.code} {e.reason}") from e
            last = e
            _sleep(_backoff(attempt, e.headers.get("Retry-After") if e.headers else None))
        except (urllib.error.URLError, OSError, http.client.HTTPException) as e:
            last = e
            _sleep(_backoff(attempt))
    raise InputError(f"{url}: giving up after {MAX_ATTEMPTS} attempts: {last}")


def fetch_bytes(url: str, *, allow_404: bool = False) -> bytes | None:
    """A small file, whole (index, manifest, sidecar)."""
    for attempt in range(MAX_ATTEMPTS):
        resp = _request(url, allow_404=allow_404)
        if resp is None:
            return None
        try:
            with resp:
                return resp.read()
        except (OSError, http.client.HTTPException) as e:
            last = e
            _sleep(_backoff(attempt))
    raise InputError(f"{url}: giving up after {MAX_ATTEMPTS} attempts: {last}")


class _HttpStream(io.RawIOBase):
    """A GET that resumes with `Range` (guarded by `If-Range`) when the connection drops."""

    def __init__(self, url: str):
        self.url = url
        self.pos = 0
        self.length: int | None = None
        self.validator: str | None = None
        self._resp = self._open()

    def readable(self) -> bool:
        return True

    def _open(self):
        headers = {}
        if self.pos:
            headers["Range"] = f"bytes={self.pos}-"
            if self.validator:
                headers["If-Range"] = self.validator
        resp = _request(self.url, headers)
        status = getattr(resp, "status", 200)
        if not self.pos:
            if status != 200:
                resp.close()
                raise InputError(f"{self.url}: unexpected HTTP {status}")
            etag = resp.headers.get("ETag")
            self.validator = etag if etag and not etag.startswith("W/") else resp.headers.get("Last-Modified")
            cl = resp.headers.get("Content-Length")
            self.length = int(cl) if cl and cl.isdigit() else None
            return resp
        cr = resp.headers.get("Content-Range") or ""
        m = re.fullmatch(r"bytes (\d+)-\d+/(\d+|\*)", cr.strip())
        if status != 206 or not m or int(m.group(1)) != self.pos:
            resp.close()
            raise InputError(
                f"{self.url}: could not resume at byte {self.pos} (HTTP {status}, Content-Range {cr!r}); the "
                "file may have changed on the server. Retry the build."
            )
        return resp

    def readinto(self, b) -> int:
        for attempt in range(MAX_ATTEMPTS):
            try:
                n = self._resp.readinto(b)
            except (OSError, http.client.HTTPException) as e:
                err: object = e
            else:
                if n or not len(b) or self.length is None or self.pos >= self.length:
                    self.pos += n
                    return n
                err = f"connection closed at byte {self.pos} of {self.length}"
            log.warning("%s: %s; resuming (attempt %d)", self.url, err, attempt + 1)
            try:
                self._resp.close()
            except Exception:  # noqa: BLE001 (the connection is already broken)
                pass
            _sleep(_backoff(attempt))
            self._resp = self._open()
        raise InputError(f"{self.url}: giving up after {MAX_ATTEMPTS} resumes at byte {self.pos}")

    def close(self) -> None:
        if not self.closed:
            try:
                self._resp.close()
            finally:
                super().close()


def open_stream(inp: InputFile) -> BinaryIO:
    """A binary stream of the file's (compressed) bytes."""
    if is_url(inp.source):
        return io.BufferedReader(_HttpStream(inp.source), buffer_size=1 << 20)
    try:
        return open(inp.source, "rb")
    except OSError as e:
        raise InputError(f"{inp.source}: {e.strerror or e}") from e


def read_all(source: str) -> bytes:
    """A small file, whole, from a path or URL."""
    if is_url(source):
        return fetch_bytes(source)
    try:
        return Path(source).read_bytes()
    except OSError as e:
        raise InputError(f"{source}: {e.strerror or e}") from e
