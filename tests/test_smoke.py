"""Smoke test — the package imports and exposes a version string."""

import labdata


def test_package_imports() -> None:
    """The package imports and surfaces a non-empty ``__version__``."""
    assert isinstance(labdata.__version__, str)
    assert labdata.__version__


def test_series_is_public() -> None:
    """``Series`` is re-exported at the top level."""
    assert labdata.Series.__name__ == "Series"
