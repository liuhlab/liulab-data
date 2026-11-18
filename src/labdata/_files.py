"""Write files whole or not at all, through a temporary name unique to this writer.

Also the UTC stamp the stores' records use, :func:`utc_stamp`.
"""

from __future__ import annotations

import datetime as dt
import os
import tomllib
from pathlib import Path
from typing import Any

from labdata.exceptions import LabdataError, NotAStoreError


def temp_beside(path: Path, tag: str = "tmp") -> Path:
    """Return a hidden temporary path next to ``path``, unique to this process."""
    return path.with_name(f".{path.name}.{os.getpid()}.{tag}")


def toml_line(key: str, value: str | int | bool) -> str:
    """Return ``key = value`` as one TOML line; a string becomes an escaped basic string."""
    if isinstance(value, bool):
        return f"{key} = {'true' if value else 'false'}"
    if isinstance(value, int):
        return f"{key} = {value}"
    if not isinstance(value, str):
        raise TypeError(f"no TOML form for {key} = {value!r}")
    escaped = "".join(_ESCAPES.get(c) or (f"\\u{ord(c):04x}" if _control(c) else c) for c in value)
    return f'{key} = "{escaped}"'


_ESCAPES = {'"': '\\"', "\\": "\\\\", "\n": "\\n", "\t": "\\t", "\r": "\\r"}


def _control(char: str) -> bool:
    return ord(char) < 0x20 or ord(char) == 0x7F


def write_atomic(path: Path, text: str) -> None:
    """Write ``text`` to a temporary file beside ``path`` and rename it over ``path``."""
    tmp = temp_beside(path)
    try:
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def read_toml_marker(root: Path, marker: str, what: str) -> dict[str, Any]:
    """Return a root's parsed marker file, ``marker``.

    Raises
    ------
    NotAStoreError
        If the root has no marker; the message says the root is not ``what``.
    """
    path = root / marker
    if not path.is_file():
        raise NotAStoreError(f"{root} is not {what}: it has no {marker}")
    with path.open("rb") as fh:
        return tomllib.load(fh)


def create(path: Path, text: str) -> bool:
    """Write ``path`` whole, unless it exists; return whether this call made it.

    Of two writers, only one makes it: the file appears by a hard link, which fails if it exists.
    """
    tmp = temp_beside(path, "new")
    tmp.write_text(text, encoding="utf-8")
    try:
        os.link(tmp, path)
    except FileExistsError:
        return False
    finally:
        tmp.unlink(missing_ok=True)
    return True


def create_marker(marker: Path, text: str, what: str) -> None:
    """Write a marker file whole, or fail if it exists, so two inits cannot both win.

    Raises
    ------
    LabdataError
        If the marker already exists; the message names ``what`` is initialised.
    """
    if not create(marker, text):
        raise LabdataError(f"{marker} already exists; this {what} is initialised")


def utc_stamp() -> str:
    """Return the time now in UTC, to the second, as ``YYYY-MM-DDTHH:MM:SSZ``."""
    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
