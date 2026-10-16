"""Turn source text into store records and link rows.

Field names are XML paths with ``@`` for attributes (``RUN@total_spots``), or a SOFT key
without its ``!`` (``Sample_title``); values are the text exactly as served. Each parser
returns rows only; the build owns typed columns from the snapshots, copy checks and dedupe.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

from labdata.exceptions import StoreBuildError

SRA_XML = "SRA XML"
BIOPROJECT_XML = "BioProject XML"
BIOSAMPLE_XML = "BioSample XML"
PUBMED = "PubMed"
SOFT = "SOFT"
ENTREZ_LINK = "Entrez link"
#: A Series' BioProject as the ``geo-series`` snapshot's ``esummary`` gives it.
GEO_SUMMARY = "GEO summary"


@dataclass(frozen=True, slots=True)
class LinkRow:
    """One link row: the four columns of the link tables."""

    from_: str
    to: str
    relation: str
    source: str


@dataclass(slots=True)
class Parsed:
    """A record's text, its fields, and its typed columns where its own XML gives them."""

    accession: str
    type: str
    raw_text: str
    fields: list[tuple[str, str]]
    typed: dict[str, str | None] = field(default_factory=dict)


def flatten(element: ET.Element, path: str = "") -> list[tuple[str, str]]:
    """Return an element's ordered (path, value) pairs: attributes, text, then children."""
    here = f"{path}/{element.tag}" if path else element.tag
    out = [(f"{here}@{name}", value) for name, value in element.attrib.items()]
    if element.text and element.text.strip():
        out.append((here, element.text))
    for child in element:
        out.extend(flatten(child, here))
    return out


def _slices(xml: str, tag: str) -> list[str]:
    """Return every ``<tag …>…</tag>`` or self-closing ``<tag …/>`` slice of ``xml``."""
    pattern = rf"<{tag}\b(?:[^>]*?/>|[^>]*>.*?</{tag}>)"
    return re.findall(pattern, xml, flags=re.S)


def sra_package(package: str) -> tuple[list[Parsed], list[LinkRow]]:
    """Parse one ``EXPERIMENT_PACKAGE`` into its SRA records and the links it states.

    The experiment's raw text is the whole package; the study, sample and run texts are
    their elements within it. The study carries the ``SUBMISSION`` block's center name.
    """
    records: list[Parsed] = []
    links: list[LinkRow] = []
    root = ET.fromstring(package)
    experiment = root.find("EXPERIMENT")
    if experiment is None:
        raise ValueError("EXPERIMENT_PACKAGE without an EXPERIMENT")
    srx = experiment.get("accession", "")
    records.append(Parsed(srx, "SRA experiment", package, flatten(experiment)))
    study_ref = experiment.find("STUDY_REF")
    if study_ref is not None and study_ref.get("accession"):
        links.append(LinkRow(srx, study_ref.get("accession", ""), "study", SRA_XML))
    sample_ref = experiment.find("DESIGN/SAMPLE_DESCRIPTOR")
    if sample_ref is not None and sample_ref.get("accession"):
        links.append(LinkRow(srx, sample_ref.get("accession", ""), "SRA sample", SRA_XML))

    submission = root.find("SUBMISSION")
    center = submission.get("center_name") if submission is not None else None
    for text in _slices(package, "STUDY"):
        study = ET.fromstring(text)
        srp = study.get("accession", "")
        fields = flatten(study)
        if center is not None:
            fields.append(("SUBMISSION@center_name", center))
        records.append(Parsed(srp, "SRA study", text, fields))
        links.extend(
            LinkRow(srp, x.text.strip(), "BioProject", SRA_XML)
            for x in study.iter("EXTERNAL_ID")
            if x.get("namespace") == "BioProject" and x.text
        )
    for text in _slices(package, "SAMPLE"):
        sample = ET.fromstring(text)
        srs = sample.get("accession", "")
        records.append(Parsed(srs, "SRA sample", text, flatten(sample)))
        links.extend(
            LinkRow(srs, x.text.strip(), "BioSample", SRA_XML)
            for x in sample.iter("EXTERNAL_ID")
            if x.get("namespace") == "BioSample" and x.text
        )
    for text in _slices(package, "RUN"):
        run = ET.fromstring(text)
        srr = run.get("accession", "")
        records.append(Parsed(srr, "SRA run", text, flatten(run)))
        links.append(LinkRow(srx, srr, "run", SRA_XML))
    return records, links


def _date(value: str | None) -> str | None:
    return value[:10] if value else None


def bioproject(text: str) -> tuple[Parsed, list[LinkRow]]:
    """Parse one bulk ``bioproject.xml`` ``<Package>`` into its record and links."""
    package = ET.fromstring(text)
    archive = package.find(".//ProjectID/ArchiveID")
    if archive is None:
        raise ValueError("BioProject package without an ArchiveID")
    accession = archive.get("accession", "")
    submission = package.find("Project/Submission")
    typed = {
        "status": package.findtext("Project/Submission/Description/Access"),
        "submitted": _date(submission.get("submitted") if submission is not None else None),
        "updated": _date(submission.get("last_update") if submission is not None else None),
        "published": _date(package.findtext(".//ProjectReleaseDate")),
    }
    links: list[LinkRow] = []
    for link in package.iter("Link"):
        hierarchical = link.find("Hierarchical")
        child, top = link.find("ProjectIDRef"), link.find("Hierarchical/MemberID")
        if (
            hierarchical is not None
            and hierarchical.get("type") == "TopAdmin"
            and child is not None
            and top is not None
        ):
            links.append(
                LinkRow(
                    top.get("accession", ""),
                    child.get("accession", ""),
                    "umbrella of",
                    BIOPROJECT_XML,
                )
            )
    for publication in package.iter("Publication"):
        if publication.get("id") and publication.findtext("DbType") == "ePubmed":
            links.append(
                LinkRow(accession, f"PMID:{publication.get('id')}", "PubMed", BIOPROJECT_XML)
            )
    return Parsed(accession, "BioProject", text, flatten(package), typed), links


def biosample(text: str) -> Parsed:
    """Parse one ``<BioSample>`` element into its record."""
    element = ET.fromstring(text)
    status = element.find("Status")
    typed = {
        "status": status.get("status") if status is not None else None,
        "submitted": _date(element.get("submission_date")),
        "updated": _date(element.get("last_update")),
        "published": _date(element.get("publication_date")),
    }
    return Parsed(element.get("accession", ""), "BioSample", text, flatten(element), typed)


def pubmed_si(accession: str, raw_text: str) -> list[LinkRow]:
    """Return the PubMed links of one saved ``<accession>[si]`` result (one PMID per line)."""
    return [
        LinkRow(accession, f"PMID:{pmid}", "PubMed", PUBMED)
        for pmid in raw_text.split()
        if pmid.isdigit()
    ]


_SOFT_TYPES = {"SERIES": "GEO Series", "SAMPLE": "GEO Sample", "PLATFORM": "GEO Platform"}
_SOFT_PREFIX = {"SERIES": "Series", "SAMPLE": "Sample", "PLATFORM": "Platform"}
_MONTHS = {
    m: i
    for i, m in enumerate(
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1
    )
}
_SOFT_DATE = re.compile(r"([A-Z][a-z]{2}) (\d{1,2}) (\d{4})")
_URL_ACCESSION = re.compile(r"(?:term=|/)([A-Z]+\d+)\s*$")
_TABLE_BEGIN = ("!sample_table_begin", "!platform_table_begin", "!series_table_begin")
_TABLE_END = ("!sample_table_end", "!platform_table_end", "!series_table_end")

#: Each GEO ``!…_relation`` label: the relation it states, whether this record is the
#: link's ``from``, and how the far end is written (``url``, ``accession`` or ``annotation``).
#: A label not listed here is kept as GEO's own relation name, forward, with its value as given.
SOFT_RELATIONS: dict[str, tuple[str, bool, str]] = {
    "SuperSeries of": ("SuperSeries of", True, "accession"),
    "SubSeries of": ("SuperSeries of", False, "accession"),
    "Reanalyzed by": ("Reanalyzed by", True, "accession"),
    "Reanalysis of": ("Reanalyzed by", False, "accession"),
    "BioProject": ("BioProject", True, "url"),
    "BioSample": ("BioSample", True, "url"),
    "SRA": ("SRA", True, "url"),
    "Named Annotation": ("Named Annotation", True, "annotation"),
}


def soft_blocks(text: str) -> list[tuple[str, str, str]]:
    """Split family SOFT into (entity, accession, block text), each block exactly as served."""
    out: list[tuple[str, str, list[str]]] = []
    for line in text.splitlines(keepends=True):
        if line.startswith("^"):
            entity, _, accession = line[1:].partition(" = ")
            out.append((entity, accession.strip(), [line]))
        elif out:
            out[-1][2].append(line)
    return [(entity, accession, "".join(lines)) for entity, accession, lines in out]


def soft_fields(block: str) -> list[tuple[str, str]]:
    """Return a SOFT block's ``!`` lines as (name, value) pairs, data tables left out.

    Table rows, their ``#`` column descriptions and the table markers are dropped.
    """
    fields: list[tuple[str, str]] = []
    in_table = False
    for line in block.splitlines():
        if line.startswith(_TABLE_BEGIN):
            in_table = True
        elif line.startswith(_TABLE_END):
            in_table = False
        elif not in_table and line.startswith("!"):
            name, sep, value = line[1:].partition(" = ")
            fields.append((name, value) if sep else (line[1:].removesuffix(" ="), ""))
    return fields


def _soft_date(value: str | None) -> str | None:
    match = _SOFT_DATE.search(value or "")
    return f"{match[3]}-{_MONTHS[match[1]]:02d}-{int(match[2]):02d}" if match else None


def soft(text: str) -> tuple[list[Parsed], list[LinkRow]]:
    """Parse one family SOFT file into its GEO records and the links they state.

    Each record's raw text is its block. Typed columns come from the ``_status``,
    ``_submission_date`` and ``_last_update_date`` lines.

    Raises
    ------
    StoreBuildError
        If a relation line names no accession.
    """
    records: list[Parsed] = []
    links: list[LinkRow] = []
    for entity, accession, block in soft_blocks(text):
        if entity not in _SOFT_TYPES:
            continue
        fields = soft_fields(block)
        first = dict(reversed(fields))
        pre = _SOFT_PREFIX[entity]
        status = first.get(f"{pre}_status")
        typed = {
            "status": status.split(" on ")[0] if status else None,
            "submitted": _soft_date(first.get(f"{pre}_submission_date")),
            "updated": _soft_date(first.get(f"{pre}_last_update_date")),
            "published": _soft_date(status),
        }
        records.append(Parsed(accession, _SOFT_TYPES[entity], block, fields, typed))
        links.extend(_soft_links(accession, fields))
    return records, links


def _soft_links(accession: str, fields: list[tuple[str, str]]) -> list[LinkRow]:
    links = []
    for name, value in fields:
        if name.endswith("_relation"):
            label, _, rest = value.partition(": ")
            relation, forward, form = SOFT_RELATIONS.get(label, (label, True, "verbatim"))
            if form == "verbatim":
                other = rest.strip()
            elif form == "url":
                match = _URL_ACCESSION.search(rest)
                other = match[1] if match else ""
            else:
                other = rest.split(" ", 1)[0].strip()
            if not other:
                raise StoreBuildError(f"{accession}: no accession in {name} = {value}")
            links.append(
                LinkRow(accession, other, relation, SOFT)
                if forward
                else LinkRow(other, accession, relation, SOFT)
            )
        elif name == "Series_sample_id":
            links.append(LinkRow(accession, value, "sample", SOFT))
        elif name == "Sample_series_id":
            links.append(LinkRow(value, accession, "sample", SOFT))
        elif name in ("Series_platform_id", "Sample_platform_id"):
            links.append(LinkRow(accession, value, "platform", SOFT))
        elif name in ("Series_pubmed_id", "Sample_pubmed_id", "Platform_pubmed_id"):
            links.append(LinkRow(accession, f"PMID:{value}", "PubMed", SOFT))
    return links


def subseries(text: str) -> list[str]:
    """Return the SubSeries a family SOFT file's Series names."""
    return re.findall(r"^!Series_relation = SuperSeries of: (GSE\d+)", text, re.M)


#: The relation an ``elink`` from GEO states, by target database.
_ENTREZ_RELATIONS = {"sra": "SRA", "bioproject": "BioProject"}


def entrez_links(text: str) -> list[LinkRow]:
    """Return the links of one saved ``entrez-links`` row.

    Each line is from accession, target database, target UID and target accession, tab
    separated. A line whose UID resolved to no accession states no link.
    """
    links = []
    for line in text.splitlines():
        source, db, _uid, target = line.split("\t")
        if target:
            links.append(LinkRow(source, target, _ENTREZ_RELATIONS[db], ENTREZ_LINK))
    return links
