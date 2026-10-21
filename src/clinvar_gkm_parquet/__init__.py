"""Rebuild ClinVar-GKM's Parquet tables from its published JSON bundles."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("clinvar-gkm-parquet")
except PackageNotFoundError:  # running from a source tree without an install
    __version__ = "0.0.0+unknown"
