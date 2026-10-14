"""PROTOTYPE, throwaway. A stub of the offline store's Python API (#28).

Nothing here is the real package. The backend is a JSON file behind a small batch-shaped
interface, because the table format is still open (#27). Every backend method takes a
collection of accessions, so a columnar backend can answer each with one scan or one join.
The wrapper never calls the backend once per record.
"""

from __future__ import annotations

import json
import os
import re
import tomllib
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

# ---------------------------------------------------------------- errors


class LabdataError(Exception):
    """Base of the stub's errors (mirrors labdata.exceptions)."""


class AccessionError(LabdataError, ValueError):
    """Malformed accession."""


class StoreNotConfigured(LabdataError):
    """No store root from the argument, LABDATA_STORE or the per-user config file."""


class NotAStore(LabdataError):
    """The root has no labdata-store.toml marker."""


class NotMirrored(LabdataError):
    """A valid accession that exists at its source but is not in this store."""


class NotFound(LabdataError):
    """A valid accession that the store's source index says does not exist."""


# ---------------------------------------------------------------- vocabulary

RECORD_TYPES = (
    "GEO Series", "GEO Sample", "GEO Platform",
    "SRA study", "SRA experiment", "SRA run", "SRA sample",
    "BioProject", "BioSample",
)  # fmt: skip

#: The fixed relation list. Each link is stored once, forward; the inverse name reads it
#: from the other end.
RELATIONS: dict[str, str] = {
    "SuperSeries of": "SubSeries of",
    "sample": "series",  # GSE -> GSM
    "platform": "platform of",  # GSE/GSM -> GPL
    "SRA": "GEO",  # GSM -> SRX, GSE -> SRP
    "BioProject": "BioProject of",  # GSE/SRP -> PRJNA
    "BioSample": "BioSample of",  # GSM/SRS -> SAMN
    "umbrella of": "under umbrella",  # PRJNA -> PRJNA
    "study": "experiment",  # SRX -> SRP
    "SRA sample": "SRA experiment",  # SRX -> SRS
    "run": "run of",  # SRX -> SRR
    "PubMed": "PubMed of",  # record -> PMID
}
INVERSE = {v: k for k, v in RELATIONS.items()}

_KINDS = [
    (r"GSE\d+", "GEO Series"), (r"GSM\d+", "GEO Sample"), (r"GPL\d+", "GEO Platform"),
    (r"[SED]RP\d+", "SRA study"), (r"[SED]RX\d+", "SRA experiment"),
    (r"[SED]RR\d+", "SRA run"), (r"[SED]RS\d+", "SRA sample"),
    (r"PRJ[A-Z]{2}\d+", "BioProject"), (r"SAM[NED]A?\d+", "BioSample"), (r"PMID:\d+", "paper"),
]  # fmt: skip


def kind_of(acc: str) -> str:
    for pat, kind in _KINDS:
        if re.fullmatch(pat, acc):
            return kind
    raise AccessionError(f"not a resolvable accession: {acc!r}")


def _check_relation(relation: str) -> None:
    if relation not in RELATIONS and relation not in INVERSE:
        raise ValueError(f"unknown relation {relation!r}; known: {sorted(RELATIONS)}")


# ---------------------------------------------------------------- results


@dataclass(frozen=True, slots=True)
class Link:
    """One stated link, as seen from the record it was looked up on."""

    relation: str  # the name as read from this end (forward or inverse)
    other: str  # the accession at the far end
    source: str  # SOFT, SRA XML, BioProject XML, Entrez link, PubMed
    stated_in: str  # the record (or PubMed ID) whose text states it
    forward: bool  # True when this end is the link's "from"


@dataclass(frozen=True, slots=True)
class Record:
    accession: str
    type: str
    source: str
    status: str | None
    submitted: str | None
    updated: str | None
    published: str | None
    fields: tuple[tuple[str, str], ...]  # ordered, repeats kept, the source's own names
    raw_text: str
    content_hash: str
    snapshot_date: str
    links: tuple[Link, ...]

    def field(self, name: str) -> list[str]:
        """Every value of one field name, in source order."""
        return [v for k, v in self.fields if k == name]

    def follow(self, relation: str) -> list[str]:
        """Accessions at the far end of one relation (forward or inverse name)."""
        _check_relation(relation)
        return sorted({lk.other for lk in self.links if lk.relation == relation})


@dataclass(frozen=True, slots=True)
class Frame:
    """A columnar result: equal-length tuples by column name. Stand-in for an Arrow table."""

    columns: dict[str, tuple]

    def __len__(self) -> int:
        return len(next(iter(self.columns.values()), ()))

    def rows(self) -> list[tuple]:
        return list(zip(*self.columns.values(), strict=True))

    def head(self, n: int = 5) -> str:
        names = list(self.columns)
        widths = [max([len(c)] + [len(str(v)[:28]) for v in self.columns[c][:n]]) for c in names]
        out = ["  ".join(c.ljust(w) for c, w in zip(names, widths, strict=True))]
        for row in self.rows()[:n]:
            out.append(
                "  ".join(str(v)[:28].ljust(w) for v, w in zip(row, widths, strict=True)).rstrip()
            )
        if len(self) > n:
            out.append(f"... {len(self) - n} more rows")
        return "\n".join(out)


# ---------------------------------------------------------------- backend seam


class Backend(Protocol):
    """Every method takes a batch. A columnar backend serves each with one scan or join."""

    def get_many(self, accs: Iterable[str]) -> dict[str, dict]:
        """Held records among ``accs``; absent ones are simply missing from the result."""

    def links_many(self, accs: Iterable[str], relations: Iterable[str] | None) -> list[dict]:
        """Every link row with an end in ``accs`` (and a forward name in ``relations``)."""

    def known_many(self, accs: Iterable[str]) -> set[str]:
        """Which of ``accs`` the source indexes list as existing."""

    def scan(self, type: str, typed: list[str], fields: list[str]) -> dict[str, list]:
        """Columns for every held record of one type: typed columns, then raw fields."""

    def all_accessions(self) -> list[str]: ...


class JsonBackend:
    """Whole fixture in memory. Swap for the real format once #27 decides it."""

    def __init__(self, path: Path) -> None:
        d = json.loads(path.read_text())
        self._recs = {r["accession"]: r for r in d["records"]}
        self._by_end: dict[str, list[dict]] = {}
        for lk in d["links"]:
            self._by_end.setdefault(lk["from"], []).append(lk)
            self._by_end.setdefault(lk["to"], []).append(lk)
        self.calls: Counter[str] = Counter()

    def get_many(self, accs: Iterable[str]) -> dict[str, dict]:
        self.calls["get_many"] += 1
        return {a: self._recs[a] for a in accs if a in self._recs}

    def links_many(self, accs: Iterable[str], relations: Iterable[str] | None) -> list[dict]:
        self.calls["links_many"] += 1
        rel = set(relations) if relations is not None else None
        seen, out = set(), []
        for a in accs:
            for lk in self._by_end.get(a, ()):
                if (rel is None or lk["relation"] in rel) and id(lk) not in seen:
                    seen.add(id(lk))
                    out.append(lk)
        return out

    def known_many(self, accs: Iterable[str]) -> set[str]:
        # Stand-in for the source indexes (SRA_Accessions.tab, the GEO esummary dump).
        # Here: anything a held record links to is known to exist.
        self.calls["known_many"] += 1
        return {a for a in accs if a in self._by_end}

    def scan(self, type: str, typed: list[str], fields: list[str]) -> dict[str, list]:
        self.calls["scan"] += 1
        cols: dict[str, list] = {"accession": []} | {c: [] for c in typed + fields}
        for acc in sorted(self._recs):
            r = self._recs[acc]
            if r["type"] != type:
                continue
            cols["accession"].append(acc)
            for c in typed:
                cols[c].append(r["content_hash"] if c == "content_hash" else r["typed"][c])
            first: dict[str, str] = {}
            for k, v in r["fields"]:
                first.setdefault(k, v)
            for f in fields:
                cols[f].append(first.get(f))
        return cols

    def all_accessions(self) -> list[str]:
        return sorted(self._recs)


# ---------------------------------------------------------------- store


class Store:
    def __init__(self, root: Path, backend: Backend) -> None:
        self.root = root
        self.backend = backend

    def __repr__(self) -> str:
        return f"Store({self.root.name!r}, {len(self.backend.all_accessions())} records)"

    # -- bulk paths: one backend call per step, whatever the batch size

    def open_many(self, accessions: Iterable[str]) -> dict[str, Record | LabdataError]:
        """Open many records: one get, one link lookup, one index check if any are absent.

        Each failure is returned in place of its record, not raised.
        """
        out: dict[str, Record | LabdataError] = {}
        valid: list[str] = []
        for a in accessions:
            acc = a.strip().upper()
            try:
                if kind_of(acc) == "paper":
                    raise AccessionError(f"{acc} is a paper, not a record; follow 'PubMed'")
                valid.append(acc)
            except AccessionError as e:
                out[acc] = e
        raws = self.backend.get_many(valid)
        by_end: dict[str, list[dict]] = {}
        for lk in self.backend.links_many(list(raws), None):
            for end in {lk["from"], lk["to"]} & raws.keys():
                by_end.setdefault(end, []).append(lk)
        absent = [a for a in valid if a not in raws]
        known = self.backend.known_many(absent) if absent else set()
        for acc in valid:
            if acc in raws:
                out[acc] = _record(acc, raws[acc], by_end.get(acc, []))
            elif acc in known:
                out[acc] = NotMirrored(
                    f"{acc} ({kind_of(acc)}) exists at its source but is not in this store"
                )
            else:
                out[acc] = NotFound(f"{acc} ({kind_of(acc)}) is not in the store's source index")
        return out

    def open(self, accession: str) -> Record:
        """Open one record by public accession, of any record type."""
        [r] = self.open_many([accession]).values()
        if isinstance(r, LabdataError):
            raise r
        return r

    def links(self, accessions: Iterable[str], relation: str | None = None) -> Frame:
        """Every link touching ``accessions``, read from their end, in one backend call.

        ``relation`` takes a forward or an inverse name and filters to links read that way.
        """
        accs = sorted({a.strip().upper() for a in accessions})
        fwd_name = None
        if relation is not None:
            _check_relation(relation)
            fwd_name = relation if relation in RELATIONS else INVERSE[relation]
        rows = []
        want = set(accs)
        for lk in self.backend.links_many(accs, None if fwd_name is None else [fwd_name]):
            for end, other, fwd in ((lk["from"], lk["to"], True), (lk["to"], lk["from"], False)):
                name = lk["relation"] if fwd else RELATIONS[lk["relation"]]
                if end in want and (relation is None or name == relation):
                    rows.append((end, name, other, lk["source"], lk["stated_in"]))
        rows.sort()
        names = ("accession", "relation", "other", "source", "stated_in")
        return Frame({n: tuple(r[i] for r in rows) for i, n in enumerate(names)})

    def follow(self, accessions: Iterable[str], relation: str) -> dict[str, list[str]]:
        """For each accession, the far ends of one relation. One backend call."""
        f = self.links(accessions, relation)
        out: dict[str, set[str]] = {a.strip().upper(): set() for a in accessions}
        for acc, other in zip(f.columns["accession"], f.columns["other"], strict=True):
            out[acc].add(other)
        return {a: sorted(v) for a, v in out.items()}

    def experiments_for_many(
        self, accessions: Iterable[str], *, partial: bool = False
    ) -> dict[str, list[str]]:
        """SRA experiment accessions under each accession.

        Walks down level by level: one link call and one index check per level for the whole
        batch, never per record. Raises NotMirrored when a record on the way down is
        missing, unless ``partial``.
        """
        roots = [a.strip().upper() for a in accessions]
        for a in roots:
            kind_of(a)
        # (root, accession) pairs on the current level
        level = [(r, r) for r in roots]
        out: dict[str, set[str]] = {r: set() for r in roots}
        seen: set[tuple[str, str]] = set()
        missing: set[str] = set()
        while level:
            level = [p for p in level if p not in seen]
            seen.update(level)
            for root, acc in level:
                if kind_of(acc) == "SRA experiment":
                    out[root].add(acc)
            todo = [p for p in level if kind_of(p[1]) != "SRA experiment"]
            if not todo:
                break
            f = self.backend.links_many(sorted({a for _, a in todo}), None)
            nxt: dict[str, set[str]] = {}
            for lk in f:
                for end, other, fwd in (
                    (lk["from"], lk["to"], True),
                    (lk["to"], lk["from"], False),
                ):
                    name = lk["relation"] if fwd else RELATIONS[lk["relation"]]
                    if name in _DOWN.get(kind_of(end), ()):
                        nxt.setdefault(end, set()).add(other)
            children = sorted({c for cs in nxt.values() for c in cs})
            held = set(self.backend.get_many(children)) if children else set()
            missing |= set(children) - held
            level = [(root, c) for root, acc in todo for c in sorted(nxt.get(acc, ())) if c in held]
        if missing and not partial:
            ms = sorted(missing)
            raise NotMirrored(
                f"{len(ms)} records under {', '.join(roots)} are not in this store: "
                f"{', '.join(ms[:4])}{', ...' if len(ms) > 4 else ''}"
            )
        return {r: sorted(v) for r, v in out.items()}

    def experiments_for(self, accession: str, *, partial: bool = False) -> list[Record]:
        """Offline twin of labdata.experiments_for: the SRA experiments under an accession."""
        self.open(accession)  # raises NotMirrored / NotFound for the root itself
        accs = self.experiments_for_many([accession], partial=partial)[accession.strip().upper()]
        recs = self.open_many(accs)
        return [recs[a] for a in accs]  # type: ignore[misc]

    def table(
        self,
        type: str,
        fields: list[str],
        where: Callable[[dict], bool] | None = None,
        typed: tuple[str, ...] = ("status", "published", "content_hash"),
    ) -> Frame:
        """One scan over every record of a type: typed columns plus the first value of each
        raw field asked for. ``where`` filters rows as dicts; a real backend pushes it down.
        """
        cols = self.backend.scan(type, list(typed), fields)
        if where is not None:
            keep = [
                i
                for i in range(len(cols["accession"]))
                if where({c: v[i] for c, v in cols.items()})
            ]
            cols = {c: [v[i] for i in keep] for c, v in cols.items()}
        return Frame({c: tuple(v) for c, v in cols.items()})


#: Which relation names lead "down" from each record type, for experiments_for.
_DOWN: dict[str, tuple[str, ...]] = {
    "GEO Series": ("SuperSeries of", "sample"),
    "GEO Sample": ("SRA",),
    "BioProject": ("umbrella of", "BioProject of"),
    "SRA study": ("experiment",),
    "SRA sample": ("SRA experiment",),
    "BioSample": ("BioSample of",),
    "SRA run": ("run of",),
}


def _record(acc: str, raw: dict, link_rows: list[dict]) -> Record:
    links = []
    for lk in link_rows:
        fwd = lk["from"] == acc
        links.append(Link(
            relation=lk["relation"] if fwd else RELATIONS[lk["relation"]],
            other=lk["to"] if fwd else lk["from"],
            source=lk["source"], stated_in=lk["stated_in"], forward=fwd,
        ))  # fmt: skip
    links.sort(key=lambda x: (x.relation, x.other, x.source, x.stated_in))
    t = raw["typed"]
    return Record(
        accession=acc, type=raw["type"], source=raw["source"],
        status=t["status"], submitted=t["submitted"], updated=t["updated"],
        published=t["published"],
        fields=tuple((k, v) for k, v in raw["fields"]), raw_text=raw["raw_text"],
        content_hash=raw["content_hash"], snapshot_date=raw["snapshot_date"],
        links=tuple(links),
    )  # fmt: skip


# ---------------------------------------------------------------- open_store (#29)

STORE_ENV = "LABDATA_STORE"
MARKER = "labdata-store.toml"


def _user_pointer() -> Path:
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "liulab-data" / "store.toml"


def open_store(root: str | os.PathLike[str] | None = None) -> Store:
    """Open a store. Root: the argument, then $LABDATA_STORE, then the per-user file."""
    if root is None:
        root = os.environ.get(STORE_ENV) or None
    if root is None and _user_pointer().exists():
        root = tomllib.loads(_user_pointer().read_text()).get("root")
    if root is None:
        raise StoreNotConfigured(
            f"store not configured: pass open_store(root=...), set {STORE_ENV}, "
            "or run `labdata store configure`"
        )
    path = Path(root)
    marker = path / MARKER
    if not marker.exists():
        raise NotAStore(f"{path.name!r} is not a labdata store: no {MARKER}")
    settings = tomllib.loads(marker.read_text())
    if settings.get("backend") != "prototype-json":
        raise NotAStore(f"unknown backend {settings.get('backend')!r}")
    return Store(path, JsonBackend(path / "records.json"))
