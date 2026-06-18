"""Smoke test — the package imports and exposes a version string."""

import labdata


def test_package_imports() -> None:
    """The package imports and surfaces a non-empty ``__version__``."""
    assert isinstance(labdata.__version__, str)
    assert labdata.__version__


def test_geo_classes_are_public() -> None:
    """The GEO object-model classes are re-exported at the top level."""
    assert [c.__name__ for c in (labdata.Series, labdata.Sample, labdata.Platform)] == [
        "Series",
        "Sample",
        "Platform",
    ]
    assert labdata.Experiment.__name__ == "Experiment"
    assert labdata.Run.__name__ == "Run"
