"""Turn source text into store records and link rows.

Field names are XML paths with ``@`` for attributes (``RUN@total_spots``), values the text
exactly as served. Each parser returns rows only; the build owns typed columns from the
snapshots, copy checks and dedupe.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

SRA_XML = "SRA XML"
BIOPROJECT_XML = "BioProject XML"
BIOSAMPLE_XML = "BioSample XML"
PUBMED = "PubMed"


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
    """Return every ``<tag …>…</tag>`` (or self-closing ``<tag …/>``) slice of ``xml``, in order."""
    pattern = rf"<{tag}\b(?:[^>]*?/>|[^>]*>.*?</{tag}>)"
    return re.findall(pattern, xml, flags=re.S)


def split_packages(xml: str) -> dict[str, str]:
    """Map each experiment accession in an ``efetch db=sra`` reply to its package text."""
    out = {}
    for package in _slices(xml, "EXPERIMENT_PACKAGE"):
        match = re.search(r'<EXPERIMENT\b[^>]*?\baccession="([^"]+)"', package)
        if match:
            out[match[1]] = package
    return out


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
