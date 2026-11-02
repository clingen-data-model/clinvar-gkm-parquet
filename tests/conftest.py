"""The suite never touches the network and never reads a real cache: schema sets come from the shipped snapshot or
from a fixture a test puts in the cache directory. A network test opts in (`pytest -m network`)."""

from __future__ import annotations

import os
import urllib.error

import pytest

from clinvar_gkm_parquet import sources


@pytest.fixture(scope="session", autouse=True)
def _private_cache(tmp_path_factory):
    before = os.environ.get("XDG_CACHE_HOME")
    os.environ["XDG_CACHE_HOME"] = str(tmp_path_factory.mktemp("cache"))
    yield
    if before is None:
        del os.environ["XDG_CACHE_HOME"]
    else:
        os.environ["XDG_CACHE_HOME"] = before


@pytest.fixture(autouse=True)
def _no_network(request, monkeypatch):
    if request.node.get_closest_marker("network"):
        return

    def refuse(req, **kwargs):
        raise urllib.error.HTTPError(getattr(req, "full_url", str(req)), 404, "no network in unit tests", {}, None)

    monkeypatch.setattr(sources, "_urlopen", refuse)
