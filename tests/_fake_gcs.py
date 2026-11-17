"""``FakeGcs``: GCS behind the rclone seam, a folder that applies the rclone calls the code makes.

It knows ``copy``, ``copyto``, ``delete`` and ``lsjson``, with the flags the copies and the
restore pass, and fails on any other. Each object keeps its MD5, as GCS keeps it in object metadata.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest

#: What rclone prints when its Google login has expired.
EXPIRED = "Failed to create file system: couldn't fetch token: invalid_grant: maybe it has expired"

_VALUED = {"--max-depth"}
_FLAGS = {
    "--no-traverse",
    "--files-from-raw",
    "--max-depth",
    "--hash",
    "--filter",
    "--recursive",
    "--files-only",
    "--ignore-times",
}


class KilledError(Exception):
    """A copy cut off at the rclone seam, before the call that would send ``FakeGcs.kill``."""


@dataclass
class _Call:
    verb: str
    where: list[str] = field(default_factory=list)
    filters: list[str] = field(default_factory=list)
    listed: list[str] | None = None
    traverse: bool = True
    depth: int | None = None
    hashes: bool = False
    recursive: bool = False


class FakeGcs:
    """GCS at ``dest``, held in ``folder``: what the code sends lands there, and nothing else.

    ``expired`` makes every call meet an expired login, or, set to a verb, only that verb's;
    ``error`` makes every call fail with that rclone error; ``kill`` names an object, and the
    call that would send it raises :class:`KilledError` before it sends anything. ``log``
    lists each object sent (``put``), fetched (``get``) and deleted (``delete``), in order;
    ``walks`` counts the calls that listed GCS past its top level.
    """

    def __init__(self, folder: Path, dest: str) -> None:
        self.folder, self.dest = folder, dest.rstrip("/")
        self.folder.mkdir(parents=True, exist_ok=True)
        self.md5: dict[str, str] = {}
        self.calls: list[list[str]] = []
        self.log: list[tuple[str, str]] = []
        self.walks = 0
        self.expired: bool | str = False
        self.error = ""
        self.kill = ""

    @classmethod
    def install(cls, monkeypatch: pytest.MonkeyPatch, folder: Path, dest: str) -> FakeGcs:
        """Put a new fake behind the rclone seam."""
        fake = cls(folder, dest)
        monkeypatch.setattr("labdata._rclone.run", fake)
        return fake

    # ------------------------------------------------------------ what GCS holds

    def files(self) -> list[str]:
        """Every object GCS holds, as paths from ``dest``."""
        return sorted(_tree(self.folder))

    def read(self, path: str) -> bytes:
        return (self.folder / path).read_bytes()

    def write(self, path: str, data: bytes) -> None:
        """Hold ``data`` at ``path``, as an earlier copy would have left it."""
        target = self.folder / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        self.md5[path] = hashlib.md5(data).hexdigest()

    def remove(self, path: str) -> None:
        (self.folder / path).unlink()
        del self.md5[path]

    def sent(self) -> list[str]:
        return [path for op, path in self.log if op == "put"]

    def deleted(self) -> list[str]:
        return [path for op, path in self.log if op == "delete"]

    def phases(self, record: str, status: str = "") -> list[str]:
        """The log as the steps it took, in order: ``get``, ``files``, ``top`` (files at the
        top of ``dest``), ``delete``, ``record`` and ``status`` (those objects sent)."""
        steps: list[str] = []
        for op, path in self.log:
            if op == "put":
                named = {record: "record", status: "status"}
                op = named.get(path) or ("files" if "/" in path else "top")
            if not steps or steps[-1] != op:
                steps.append(op)
        return steps

    def forget(self) -> None:
        """Forget the calls so far; keep what GCS holds."""
        self.calls.clear()
        self.log.clear()
        self.walks = 0

    # ------------------------------------------------------------ the rclone seam

    def __call__(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(args)
        call = _parse(args)
        if self.error:
            return subprocess.CompletedProcess(args, 3, "", self.error)
        if self.expired is True or self.expired == call.verb:
            return subprocess.CompletedProcess(args, 1, "", EXPIRED)
        out = ""
        if call.verb == "lsjson":
            out = self._lsjson(call)
        elif call.verb == "delete":
            self._delete(call)
        elif call.verb == "copyto":
            src, dst = call.where
            self._transfer([(src, dst)])
        elif call.verb == "copy":
            self._copy(call)
        else:
            raise AssertionError(f"FakeGcs does not know rclone {call.verb}")
        return subprocess.CompletedProcess(args, 0, out, "")

    def _key(self, path: str) -> str | None:
        """The object path under ``dest`` for a GCS path; None for a work-disk path."""
        if path == self.dest or path.startswith(f"{self.dest}/"):
            return path[len(self.dest) + 1 :]
        assert not re.match(r"^[\w-]+:", path), f"FakeGcs holds only {self.dest}, not {path}"
        return None

    def _copy(self, call: _Call) -> None:
        src, dst = call.where
        from_gcs = self._key(src) is not None
        base = self.folder / (self._key(src) or "") if from_gcs else Path(src)
        names = call.listed
        if names is None:
            names = [n for n in _tree(base, call.depth) if _passes(n, call.filters)]
        names = [n for n in names if (base / n).is_file()]
        if call.traverse and call.depth != 1:
            self.walks += 1
        self._transfer([(f"{src.rstrip('/')}/{n}", f"{dst.rstrip('/')}/{n}") for n in names])

    def _transfer(self, pairs: list[tuple[str, str]]) -> None:
        if self.kill and self.kill in [self._key(dst) for _, dst in pairs]:
            raise KilledError(self.kill)
        for src, dst in pairs:
            key = self._key(src)
            if key is not None:
                target = Path(dst)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(self.read(key))
                self.log.append(("get", key))
            else:
                key = self._key(dst)
                assert key is not None, f"FakeGcs copies to or from GCS, not {src} to {dst}"
                self.write(key, Path(src).read_bytes())
                self.log.append(("put", key))

    def _delete(self, call: _Call) -> None:
        prefix = self._key(call.where[0])
        assert prefix is not None, "FakeGcs deletes only in GCS"
        base = self.folder / prefix
        names = list(_tree(base)) if call.listed is None else call.listed
        for name in names:
            key = f"{prefix}/{name}" if prefix else name
            (self.folder / key).unlink(missing_ok=True)
            self.md5.pop(key, None)
            self.log.append(("delete", key))

    def _lsjson(self, call: _Call) -> str:
        prefix = self._key(call.where[0])
        assert prefix is not None, "FakeGcs lists only GCS"
        base = self.folder / prefix
        if call.recursive:
            # Files only, as rclone lists them with --files-only.
            names = [n for n in _tree(base) if call.listed is None or n in call.listed]
            return json.dumps([self._entry(base, n, call.hashes) for n in sorted(names)])
        entries = []
        for path in sorted(base.iterdir()) if base.is_dir() else []:
            folder = path.is_dir()
            size = -1 if folder else path.stat().st_size
            entry: dict[str, object] = {"Path": path.name, "Name": path.name, "Size": size}
            entry["IsDir"] = folder
            if call.hashes and not folder:
                entry["Hashes"] = {"md5": self.md5[path.relative_to(self.folder).as_posix()]}
            entries.append(entry)
        return json.dumps(entries)

    def _entry(self, base: Path, name: str, hashes: bool) -> dict[str, object]:
        path = base / name
        entry: dict[str, object] = {"Path": name, "Size": path.stat().st_size, "IsDir": False}
        if hashes:
            entry["Hashes"] = {"md5": self.md5[path.relative_to(self.folder).as_posix()]}
        return entry


def _parse(args: list[str]) -> _Call:
    assert args[0] == "rclone", args
    call = _Call(args[1])
    rest = iter(args[2:])
    for arg in rest:
        if not arg.startswith("--"):
            call.where.append(arg)
            continue
        name, eq, value = arg.partition("=")
        if name in _VALUED and not eq:
            value = next(rest)
        if name.startswith("--stats"):
            continue
        assert name in _FLAGS, f"FakeGcs does not know {arg}"
        if name == "--filter":
            call.filters.append(value)
        elif name == "--files-from-raw":
            call.listed = Path(value).read_text(encoding="utf-8").splitlines()
        elif name == "--no-traverse":
            call.traverse = False
        elif name == "--max-depth":
            call.depth = int(value)
        elif name == "--hash":
            call.hashes = True
        elif name == "--recursive":
            call.recursive = True
    return call


def _tree(base: Path, depth: int | None = None) -> Iterator[str]:
    """Every file under ``base``, ``depth`` levels deep at most, as paths from it."""
    if not base.is_dir():
        return
    for path in base.rglob("*"):
        name = path.relative_to(base).as_posix()
        if path.is_file() and (depth is None or name.count("/") < depth):
            yield name


def _passes(name: str, filters: list[str]) -> bool:
    """Whether rclone's ``--filter`` rules let ``name`` through: the first match decides."""
    for rule in filters:
        sign, _, pattern = rule.partition(" ")
        if _glob(pattern).search(name):
            return sign == "+"
    return True


def _glob(pattern: str) -> re.Pattern[str]:
    """rclone's glob: ``*`` stays within a path part, ``**`` does not; ``/`` first anchors it."""
    anchored = pattern.startswith("/")
    body = ""
    for part in re.split(r"(\*\*|\*|\?)", pattern.lstrip("/")):
        body += {"**": ".*", "*": "[^/]*", "?": "[^/]"}.get(part, re.escape(part))
    return re.compile(("^" if anchored else "(^|/)") + body + "$")
