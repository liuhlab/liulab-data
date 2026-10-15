"""Find the store root and check its marker.

The per-user pointer file follows the ``ncbi.toml`` pattern and holds only the root;
store settings live in the store's own ``labdata-store.toml``.
"""

from __future__ import annotations

import json
import os
import tomllib
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from labdata._cache import ensure_cache_dir, liulab_data_cache_dir
from labdata.exceptions import (
    AccessionError,
    LabdataError,
    NotAStoreError,
    NotMirrored,
    StoreNotConfiguredError,
)
from labdata.geo.sra_records import Experiment, experiments_from_package_xml
from labdata.store._offline import OfflineEntrezClient, OfflineSdlClient
from labdata.store.model import (
    PAPER,
    RELATIONS,
    Link,
    RawText,
    Record,
    forward_name,
    kind_of,
)

if TYPE_CHECKING:
    from labdata.store.backend import DuckDbBackend

#: Environment variable naming the store root.
STORE_ENV = "LABDATA_STORE"

#: Basename of the per-user pointer file, beside ``ncbi.toml``.
POINTER_FILENAME = "store.toml"

#: The marker file every store root carries.
MARKER = "labdata-store.toml"


class Store:
    """An opened store, reading the build its marker named when it was opened.

    Parameters
    ----------
    root : pathlib.Path
        The store's root folder.
    settings : dict
        The parsed ``labdata-store.toml``.
    """

    def __init__(self, root: Path, settings: dict[str, Any]) -> None:
        self.root = root
        self.settings = settings
        self._backend: DuckDbBackend | None = None

    def __repr__(self) -> str:
        """Return the root and the build."""
        return f"Store({str(self.root)!r}, build={self.settings.get('build')!r})"

    @property
    def backend(self) -> DuckDbBackend:
        """The backend over this store's build, opened on first use.

        Raises
        ------
        NotAStoreError
            If the marker names no build.
        """
        if self._backend is None:
            build = self.settings.get("build")
            if not build:
                raise NotAStoreError(
                    f"{self.root / MARKER} names no build; run `labdata store build`"
                )
            from labdata.store.backend import DuckDbBackend

            self._backend = DuckDbBackend(self.root / "builds" / str(build))
        return self._backend

    def open_many(self, accessions: Iterable[str]) -> dict[str, Record | LabdataError]:
        """Open many records: one record lookup and one link lookup for the whole batch.

        Parameters
        ----------
        accessions : iterable of str
            Public accessions of any record type.

        Returns
        -------
        dict
            Each accession, in input order, mapped to its ``Record`` or to the error that
            stopped it (``AccessionError`` or ``NotMirrored``).
        """
        order = [_norm(a) for a in accessions]
        out: dict[str, Record | LabdataError] = {}
        valid: list[str] = []
        for accession in order:
            try:
                if kind_of(accession) == PAPER:
                    raise AccessionError(f"{accession} is a paper, not a record; follow 'PubMed'")
                valid.append(accession)
            except AccessionError as err:
                out[accession] = err
        rows = self.backend.get_many(valid) if valid else {}
        by_end: dict[str, list[Link]] = {}
        for row in self.backend.links_many(rows) if rows else []:
            for end, link in _ends(row):
                if end in rows:
                    by_end.setdefault(end, []).append(link)
        for accession in valid:
            row = rows.get(accession)
            if row is None:
                out[accession] = NotMirrored(f"not held in this store: {accession}")
                continue
            links = sorted(by_end.get(accession, []), key=lambda k: (k.relation, k.other, k.source))
            out[accession] = Record(
                accession=row[0],
                type=row[1],
                status=row[2],
                submitted=row[3],
                updated=row[4],
                published=row[5],
                fields=tuple((pair["name"], pair["value"]) for pair in row[6]),
                content_hash=row[7],
                snapshot_date=row[8],
                links=tuple(links),
            )
        return {a: out[a] for a in order}

    def open(self, accession: str) -> Record:
        """Open one record by public accession; ``open_many`` of one.

        Raises
        ------
        AccessionError
            If the accession is malformed or names a paper.
        NotMirrored
            If the store does not hold it.
        """
        [result] = self.open_many([accession]).values()
        if isinstance(result, LabdataError):
            raise result
        return result

    def follow(self, accessions: str | Iterable[str], relation: str) -> dict[str, list[str]]:
        """Map each accession to the sorted accessions at the far end of one relation.

        Reads links only, in one backend call, so a far end may be an accession the store
        does not hold. ``relation`` takes a forward or a reverse name.

        Raises
        ------
        ValueError
            If the relation name is unknown.
        """
        accs = [_norm(a) for a in ([accessions] if isinstance(accessions, str) else accessions)]
        forward = forward_name(relation)
        out: dict[str, set[str]] = {a: set() for a in accs}
        for row in self.backend.links_many(set(accs), [forward]):
            for end, link in _ends(row):
                if end in out and link.relation == relation:
                    out[end].add(link.other)
        return {a: sorted(v) for a, v in out.items()}

    def raw(self, accessions: Iterable[str]) -> dict[str, RawText]:
        """Map each held accession to its raw source text and ``content_hash``, in one call.

        Accessions the store does not hold are left out.
        """
        rows = self.backend.raw_many({_norm(a) for a in accessions})
        return {a: RawText(text, digest) for a, (text, digest) in sorted(rows.items())}

    def experiments_for_many(
        self, accessions: Iterable[str], *, partial: bool = False
    ) -> dict[str, list[Experiment]]:
        """Map each accession to the SRA experiments under it, built from stored packages.

        Walks down one level at a time, with one link lookup and one holdings lookup per
        level for all roots together.

        Raises
        ------
        AccessionError
            If an accession is malformed.
        NotMirrored
            If a root or a record on the way down is not held, unless ``partial``. The
            message lists every missing accession.
        """
        roots = [_norm(a) for a in accessions]
        for root in roots:
            kind_of(root)
        missing = set(roots) - self.backend.held_many(set(roots))
        found: dict[str, set[str]] = {r: set() for r in roots}
        level = {(r, r) for r in roots if r not in missing}
        seen: set[tuple[str, str]] = set()
        while level:
            seen |= level
            todo = set()
            for root, acc in level:
                if kind_of(acc) == "SRA experiment":
                    found[root].add(acc)
                else:
                    todo.add((root, acc))
            if not todo:
                break
            children: dict[str, set[str]] = {}
            for row in self.backend.links_many({acc for _, acc in todo}):
                for end, link in _ends(row):
                    if link.relation in _DOWN.get(kind_of(end), ()):
                        children.setdefault(end, set()).add(link.other)
            reached = {c for cs in children.values() for c in cs}
            held = self.backend.held_many(reached) if reached else set()
            missing |= reached - held
            level = {
                (root, child)
                for root, acc in todo
                for child in children.get(acc, ())
                if child in held
            } - seen
        if missing and not partial:
            raise NotMirrored(f"not held in this store: {', '.join(sorted(missing))}")
        wanted = {acc for accs in found.values() for acc in accs}
        packages = self.backend.raw_many(wanted) if wanted else {}
        built = {
            experiment.accession: experiment
            for experiment in experiments_from_package_xml(
                "".join(text for text, _ in packages.values()),
                client=OfflineEntrezClient(),
                sdl_client=OfflineSdlClient(),
            )
        }
        return {r: [built[a] for a in sorted(found[r]) if a in built] for r in roots}

    def experiments_for(self, accession: str, *, partial: bool = False) -> list[Experiment]:
        """Return the SRA experiments under one accession, sorted, with no network call.

        The store twin of :func:`labdata.experiments_for`: the same ``Experiment`` type,
        built from the stored package XML by the same parser. Their properties, and their
        runs', never call NCBI; ``Run.files`` and ``Run.original_files`` raise instead.

        Raises
        ------
        AccessionError
            If the accession is malformed.
        NotMirrored
            If the accession, or a record under it, is not held (unless ``partial``).
        """
        return self.experiments_for_many([accession], partial=partial)[_norm(accession)]


#: The relation names that lead down from each record type towards SRA experiments.
_DOWN: dict[str, tuple[str, ...]] = {
    "GEO Series": ("SuperSeries of", "sample"),
    "GEO Sample": ("SRA",),
    "BioProject": ("umbrella of", "BioProject of"),
    "SRA study": ("experiment",),
    "SRA sample": ("SRA experiment",),
    "BioSample": ("BioSample of",),
    "SRA run": ("run of",),
}


def _norm(accession: str) -> str:
    accession = accession.strip()
    return accession if accession.startswith("PMID:") else accession.upper()


def _ends(row: tuple[str, str, str, str]) -> tuple[tuple[str, Link], tuple[str, Link]]:
    """Return a link row as read from its ``from`` end and from its ``to`` end."""
    src, dst, relation, source = row
    return (
        (src, Link(relation, dst, source, forward=True)),
        (dst, Link(RELATIONS[relation], src, source, forward=False)),
    )


def open_store(root: str | os.PathLike[str] | None = None) -> Store:
    """Open the store, resolving its root without network or prompt.

    The root is the first of: the ``root`` argument, ``LABDATA_STORE``, then the per-user
    pointer file written by ``labdata store configure``. Opening writes nothing in the store.

    Parameters
    ----------
    root : str or os.PathLike or None
        The store root. ``None`` falls back to the environment, then the pointer file.

    Returns
    -------
    Store
        The opened store.

    Raises
    ------
    StoreNotConfiguredError
        If no root is set anywhere.
    NotAStoreError
        If the root has no ``labdata-store.toml``.
    """
    if root is None:
        root = os.environ.get(STORE_ENV, "").strip() or _from_pointer()
    if root is None:
        raise StoreNotConfiguredError(
            f"store not configured: pass open_store(root=...), set {STORE_ENV}, "
            "or run `labdata store configure <root>`"
        )
    path = Path(root)
    marker = path / MARKER
    if not marker.is_file():
        raise NotAStoreError(f"{path} is not a labdata store: it has no {MARKER}")
    with marker.open("rb") as fh:
        settings = tomllib.load(fh)
    return Store(root=path, settings=settings)


def pointer_path() -> Path:
    """Return the path to the per-user pointer file (which may not exist).

    Returns
    -------
    pathlib.Path
        ``<cache>/store.toml``.
    """
    return liulab_data_cache_dir() / POINTER_FILENAME


def save_pointer(root: str | os.PathLike[str]) -> Path:
    """Write the per-user pointer file, which holds only the store root.

    Parameters
    ----------
    root : str or os.PathLike
        The store root; saved as an absolute path.

    Returns
    -------
    pathlib.Path
        The path the pointer was written to.
    """
    ensure_cache_dir()
    path = pointer_path()
    absolute = Path(root).expanduser().absolute()
    # A JSON string is a valid TOML basic string, escapes included.
    path.write_text(f"[store]\nroot = {json.dumps(str(absolute))}\n", encoding="utf-8")
    return path


def _from_pointer() -> str | None:
    path = pointer_path()
    if not path.is_file():
        return None
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    return str(data.get("store", {}).get("root", "")).strip() or None
