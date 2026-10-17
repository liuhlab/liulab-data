"""Find the store root and check its marker.

The per-user pointer file follows the ``ncbi.toml`` pattern and holds only the root;
store settings live in the store's own ``labdata-store.toml``.
"""

from __future__ import annotations

import os
import tomllib
from collections.abc import Collection, Iterable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, overload

from labdata._cache import ensure_cache_dir, liulab_data_cache_dir
from labdata.exceptions import (
    AccessionError,
    LabdataError,
    NotAStoreError,
    NotFound,
    NotMirrored,
    StoreNotConfiguredError,
)
from labdata.geo.sra_records import Experiment, experiments_from_package_xml
from labdata.store import snapshots
from labdata.store._files import temp_beside, toml_line, write_atomic
from labdata.store._offline import OfflineEntrezClient, OfflineSdlClient
from labdata.store.model import (
    ANNOTATION,
    PAPER,
    RELATIONS,
    Link,
    Partial,
    RawText,
    Record,
    SupplementaryFile,
    forward_name,
    kind_of,
)

if TYPE_CHECKING:
    import pyarrow as pa

    from labdata.store.backend import DuckDbBackend

#: Environment variable naming the store root.
STORE_ENV = "LABDATA_STORE"

#: Basename of the per-user pointer file, beside ``ncbi.toml``.
POINTER_FILENAME = "store.toml"

#: The marker file every store root carries.
MARKER = "labdata-store.toml"

#: The store layout version ``labdata store init`` writes.
FORMAT_VERSION = 1


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
            stopped it (``AccessionError``, ``NotMirrored`` or ``NotFound``).
        """
        order = [_norm(a) for a in accessions]
        out: dict[str, Record | LabdataError] = {}
        valid: list[str] = []
        for accession in order:
            try:
                kind = kind_of(accession)
                if kind == PAPER:
                    raise AccessionError(f"{accession} is a paper, not a record; follow 'PubMed'")
                if kind == ANNOTATION:
                    raise AccessionError(
                        f"{accession} is a GEO named annotation, not a record; "
                        "follow 'Named Annotation'"
                    )
                valid.append(accession)
            except AccessionError as err:
                out[accession] = err
        rows = self.backend.get_many(valid) if valid else {}
        by_end = self._links(rows) if rows else {}
        absent = self._absent_errors(set(valid) - set(rows))
        for accession in valid:
            row = rows.get(accession)
            if row is None:
                out[accession] = absent[accession]
                continue
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
                links=tuple(by_end[accession]),
            )
        return {a: out[a] for a in order}

    def open(self, accession: str) -> Record:
        """Open one record by public accession; ``open_many`` of one.

        Raises
        ------
        AccessionError
            If the accession is malformed or names a paper.
        NotMirrored
            If a snapshot lists it but the store does not hold it.
        NotFound
            If no snapshot lists it. The message gives the snapshot date.
        """
        [result] = self.open_many([accession]).values()
        if isinstance(result, LabdataError):
            raise result
        return result

    def links(
        self, accessions: str | Iterable[str], relation: str | None = None
    ) -> dict[str, list[Link]]:
        """Map each accession to its link rows as read from its end, in one backend call.

        A far end may be an accession the store does not hold.

        Parameters
        ----------
        accessions : str or iterable of str
            Accessions of any record type, or ``PMID:<n>``.
        relation : str or None
            Keep only links of this name as read from the given end, forward or reverse.

        Returns
        -------
        dict
            Each accession, in input order, mapped to its links sorted by relation, far end
            and source.

        Raises
        ------
        AccessionError
            If an accession is malformed.
        ValueError
            If the relation name is unknown.
        """
        accs = _accessions(accessions)
        relations = None if relation is None else [forward_name(relation)]
        found = self._links(set(accs), relations)
        return {
            a: [k for k in found[a] if relation is None or k.relation == relation] for a in accs
        }

    def follow(self, accessions: str | Iterable[str], relation: str) -> dict[str, list[str]]:
        """Map each accession to the sorted accessions at the far end of one relation.

        Reads links only, in one backend call, so a far end may be an accession the store
        does not hold. ``relation`` takes a forward or a reverse name.

        Raises
        ------
        AccessionError
            If an accession is malformed.
        ValueError
            If the relation name is unknown.
        """
        return {
            a: sorted({k.other for k in links})
            for a, links in self.links(accessions, relation).items()
        }

    def _links(
        self, accessions: Collection[str], relations: list[str] | None = None
    ) -> dict[str, list[Link]]:
        """Read the links of ``accessions`` from their own end, with one backend call."""
        out: dict[str, list[Link]] = {a: [] for a in accessions}
        for row in self.backend.links_many(accessions, relations):
            for end, link in _ends(row):
                if end in out:
                    out[end].append(link)
        for links in out.values():
            links.sort(key=lambda k: (k.relation, k.other, k.source, k.forward))
        return out

    def raw(self, accessions: Iterable[str]) -> dict[str, RawText]:
        """Map each accession to its raw source text and ``content_hash``, in one call.

        Raises
        ------
        AccessionError
            If an accession is malformed.
        NotMirrored
            If a snapshot lists an accession the store does not hold. The message lists
            every such accession.
        NotFound
            If no snapshot lists an accession. The message gives the snapshot date.
        """
        wanted = set(_accessions(accessions))
        rows = self.backend.raw_many(wanted) if wanted else {}
        self._raise_absent(wanted - set(rows))
        return {a: RawText(text, digest) for a, (text, digest) in sorted(rows.items())}

    def supplementary_files(self, accessions: Iterable[str]) -> dict[str, list[SupplementaryFile]]:
        """Map each accession to the supplementary files its SOFT lists, in two backend calls.

        A Series lists its own files; its samples' files are under the samples. An
        accession the store holds with no files maps to an empty list.

        Parameters
        ----------
        accessions : iterable of str
            GEO accessions, in the order wanted back.

        Returns
        -------
        dict
            Each accession, in input order, mapped to its files sorted by name.

        Raises
        ------
        AccessionError
            If an accession is malformed.
        NotMirrored
            If a snapshot lists an accession the store does not hold. The message lists
            every such accession.
        NotFound
            If no snapshot lists an accession. The message gives the snapshot date.
        """
        order = _accessions(accessions)
        self._require_held(set(order))
        out: dict[str, list[SupplementaryFile]] = {a: [] for a in order}
        for row in self.backend.supplementary_many(set(order)) if order else []:
            out[row[0]].append(SupplementaryFile(*row))
        return out

    @overload
    def experiments_for_many(
        self, accessions: Iterable[str], *, partial: Literal[False] = False
    ) -> dict[str, list[Experiment]]: ...
    @overload
    def experiments_for_many(
        self, accessions: Iterable[str], *, partial: Literal[True]
    ) -> Partial[dict[str, list[Experiment]]]: ...
    def experiments_for_many(
        self, accessions: Iterable[str], *, partial: bool = False
    ) -> dict[str, list[Experiment]] | Partial[dict[str, list[Experiment]]]:
        """Map each accession to the SRA experiments under it, built from stored packages.

        Walks down one level at a time, with one link lookup and one holdings lookup per
        level for all roots together.

        Parameters
        ----------
        accessions : iterable of str
            The roots of the walk.
        partial : bool
            Return what is held instead of raising ``NotMirrored``.

        Returns
        -------
        dict or Partial
            Each root mapped to its sorted experiments. With ``partial=True``, a ``Partial``
            whose ``held`` is that mapping over held records and whose ``missing`` lists every
            accession the walk reached but the store does not hold.

        Raises
        ------
        AccessionError
            If an accession is malformed.
        NotFound
            If no snapshot lists a root, even with ``partial``.
        NotMirrored
            If a root or a record on the way down is not held, unless ``partial``. The
            message lists every missing accession.
        """
        roots = _accessions(accessions)
        missing = self._require_held(set(roots), not_held_ok=True)
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
                    if _leads_down(kind_of(end), link):
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
        result = {r: [built[a] for a in sorted(found[r]) if a in built] for r in roots}
        return Partial(result, tuple(sorted(missing))) if partial else result

    @overload
    def experiments_for(
        self, accession: str, *, partial: Literal[False] = False
    ) -> list[Experiment]: ...
    @overload
    def experiments_for(
        self, accession: str, *, partial: Literal[True]
    ) -> Partial[list[Experiment]]: ...
    def experiments_for(
        self, accession: str, *, partial: bool = False
    ) -> list[Experiment] | Partial[list[Experiment]]:
        """Return the SRA experiments under one accession, sorted, with no network call.

        The store twin of :func:`labdata.experiments_for`: the same ``Experiment`` type,
        built from the stored package XML by the same parser. Their properties, and their
        runs', never call NCBI; ``Run.files`` and ``Run.original_files`` raise instead.
        With ``partial=True`` it returns a ``Partial`` of that list and the missing
        accessions.

        Raises
        ------
        AccessionError
            If the accession is malformed.
        NotFound
            If no snapshot lists the accession.
        NotMirrored
            If the accession, or a record under it, is not held (unless ``partial``).
        """
        key = _norm(accession)
        if partial:
            walk = self.experiments_for_many([accession], partial=True)
            return Partial(walk.held[key], walk.missing)
        return self.experiments_for_many([accession])[key]

    def _require_held(self, accessions: set[str], *, not_held_ok: bool = False) -> set[str]:
        """Return which accessions the store does not hold, with one holdings lookup.

        Raises ``NotFound`` if a snapshot lists none of them, and ``NotMirrored`` for the
        rest unless ``not_held_ok``.
        """
        missing = accessions - self.backend.held_many(accessions) if accessions else set()
        self._raise_absent(missing, not_held_ok=not_held_ok)
        return missing

    def _raise_absent(self, missing: set[str], *, not_held_ok: bool = False) -> None:
        """Raise ``NotFound`` if no snapshot lists one of ``missing``, else ``NotMirrored``."""
        errors = self._absent_errors(missing).values()
        unknown = sorted(str(e) for e in errors if isinstance(e, NotFound))
        if unknown:
            raise NotFound("; ".join(unknown))
        if missing and not not_held_ok:
            raise NotMirrored(f"not held in this store: {', '.join(sorted(missing))}")

    def table(self, type: str, fields: Sequence[str], where: str | None = None) -> pa.Table:
        """Return one record type as a ``pyarrow.Table``, sorted by accession, in one call.

        The only call that imports pyarrow.

        Parameters
        ----------
        type : str
            A record type, such as ``"GEO Series"`` or ``"SRA experiment"``.
        fields : sequence of str
            Raw field names. Each becomes a list column of every value, in source order.
        where : str or None
            A DuckDB condition over the columns, such as ``"published >= '2020-01-01'"``.
            It is trusted SQL from the caller, pasted into the query as written.

        Returns
        -------
        pyarrow.Table
            ``accession``, ``type``, ``status``, ``submitted``, ``updated``, ``published``,
            ``content_hash`` and ``snapshot_date``, then one column per field.

        Raises
        ------
        ValueError
            If the record type is unknown, or ``where`` holds a ``;``.
        """
        if type not in _SNAPSHOT_OF:
            raise ValueError(f"unknown record type {type!r}; known: {sorted(_SNAPSHOT_OF)}")
        if where is not None and ";" in where:
            raise ValueError(f"where must be one condition, without ';': {where!r}")
        return self.backend.table(type, list(fields), where)

    def _absent_errors(self, accessions: set[str]) -> dict[str, NotMirrored | NotFound]:
        """Classify accessions the store does not hold, with one ``known`` lookup for all."""
        if not accessions:
            return {}
        known = self.backend.known_many(accessions)
        unknown = accessions - set(known)
        dates = self.backend.snapshot_dates() if unknown else {}
        out: dict[str, NotMirrored | NotFound] = {
            a: NotMirrored(f"not held in this store: {a}") for a in known
        }
        for a in unknown:
            source = _SNAPSHOT_OF[kind_of(a)]
            out[a] = NotFound(
                f"not in any snapshot: {a} (the {source} snapshot of {dates.get(source, '?')})"
            )
        return out


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

#: The record types a walk may step to from a BioProject: SRA only, not into the GEO
#: Series that name it.
_BIOPROJECT_DOWN_TO = ("BioProject", "SRA study", "SRA experiment")


def _leads_down(kind: str, link: Link) -> bool:
    """Return whether a walk at a record of ``kind`` steps along ``link``."""
    if link.relation not in _DOWN.get(kind, ()):
        return False
    return kind != "BioProject" or kind_of(link.other) in _BIOPROJECT_DOWN_TO


#: The snapshot that lists each record type's accessions.
_SNAPSHOT_OF: dict[str, str] = {
    "GEO Series": snapshots.GEO_SERIES,
    "GEO Sample": snapshots.GEO_SERIES,
    "GEO Platform": snapshots.GEO_SERIES,
    "SRA study": snapshots.SRA_ACCESSIONS,
    "SRA experiment": snapshots.SRA_ACCESSIONS,
    "SRA run": snapshots.SRA_ACCESSIONS,
    "SRA sample": snapshots.SRA_ACCESSIONS,
    "BioProject": snapshots.BIOPROJECT,
    "BioSample": snapshots.BIOSAMPLE,
}


def _accessions(accessions: str | Iterable[str]) -> list[str]:
    """Normalise one accession or many, checking each is well formed."""
    out = [_norm(a) for a in ([accessions] if isinstance(accessions, str) else accessions)]
    for accession in out:
        kind_of(accession)
    return out


def _norm(accession: str) -> str:
    accession = accession.strip()
    return accession if accession.startswith("PMID:") else accession.upper()


def _ends(row: tuple[str, str, str, str]) -> tuple[tuple[str, Link], tuple[str, Link]]:
    """Return a link row as read from its ``from`` end and from its ``to`` end."""
    src, dst, relation, source = row
    return (
        (src, Link(relation, dst, source, forward=True)),
        (dst, Link(RELATIONS.get(relation, relation), src, source, forward=False)),
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
    return Store(root=path, settings=read_marker(path))


def read_marker(root: Path) -> dict[str, Any]:
    """Return the store's parsed ``labdata-store.toml``.

    Raises
    ------
    NotAStoreError
        If the root has no marker.
    """
    marker = root / MARKER
    if not marker.is_file():
        raise NotAStoreError(f"{root} is not a labdata store: it has no {MARKER}")
    with marker.open("rb") as fh:
        return tomllib.load(fh)


def current_build(root: Path) -> str | None:
    """Return the build id the marker names; ``None`` with no marker or no build yet."""
    if not (root / MARKER).is_file():
        return None
    build = read_marker(root).get("build")
    return str(build) if build else None


def init_store(root: str | os.PathLike[str]) -> Path:
    """Create the store root if missing and write its marker, ``format_version = 1``.

    The marker appears whole or not at all, and nothing else in the root is touched.

    Parameters
    ----------
    root : str or os.PathLike
        The store root.

    Returns
    -------
    pathlib.Path
        The marker written.

    Raises
    ------
    LabdataError
        If the root already has a marker.
    """
    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    marker = path / MARKER
    tmp = temp_beside(marker, "init")
    tmp.write_text(f"format_version = {FORMAT_VERSION}\n", encoding="utf-8")
    try:
        # A hard link fails if the marker exists, so two inits cannot both win.
        os.link(tmp, marker)
    except FileExistsError:
        raise LabdataError(f"{marker} already exists; this store is initialised") from None
    finally:
        tmp.unlink(missing_ok=True)
    return marker


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
    write_atomic(path, f"[store]\n{toml_line('root', str(absolute))}\n")
    return path


def _from_pointer() -> str | None:
    path = pointer_path()
    if not path.is_file():
        return None
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    return str(data.get("store", {}).get("root", "")).strip() or None
