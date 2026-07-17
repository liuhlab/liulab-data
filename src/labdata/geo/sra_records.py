r"""The SRA object model: Experiment and Run.

SRA records (``SRX``/``SRR``) live in the Entrez ``sra`` database
(:class:`_SraRecord`), whose summary carries the experiment/run XML. Each class is
a lazy handle on one accessioned record: construction only validates and stores
the accession; properties issue their NCBI request on first access and cache it
(``functools.cached_property``), so building a record is cheap and network traffic
happens only for the fields you read.

Properties that reference other SRA objects (``Run.experiment``,
``Experiment.runs``) return **instances** sharing this record's Entrez client, not
bare accession strings.
"""

from __future__ import annotations

import re
from functools import cached_property
from typing import Any

from labdata.exceptions import EntrezError
from labdata.geo._base import (
    _attr,
    _attrs_all,
    _int_attr,
    _read_lengths,
    _read_structure,
    _Record,
    _run_blocks,
    _tag_text,
)
from labdata.ncbi.entrez import EntrezClient, _docsum_uid
from labdata.ncbi.sdl import RemoteFile


class _SraRecord(_Record):
    """Base for SRA records (``SRX``/``SRR``) backed by the ``sra`` database."""

    @cached_property
    def uid(self) -> str:
        """The Entrez ``sra`` UID for this record's experiment.

        Raises
        ------
        EntrezError
            If NCBI returns no matching ``sra`` record.
        """
        uids = self.client.esearch(db="sra", term=self.accession)
        if not uids:
            raise EntrezError(f"no SRA {self._KIND} found for {self.accession!r}")
        return uids[0]

    @cached_property
    def _summary(self) -> dict[str, Any]:
        """The raw ``sra`` esummary docsum for this record (fetched once)."""
        return self.client.esummary(db="sra", uid=self.uid)

    @property
    def _exp_xml(self) -> str:
        return str(self._summary.get("ExpXml", ""))

    @property
    def _runs_xml(self) -> str:
        return str(self._summary.get("Runs", ""))

    @property
    def organism(self) -> str:
        """The organism (scientific name) recorded in SRA."""
        return _attr(self._exp_xml, "Organism", "ScientificName")

    @property
    def instrument_model(self) -> str:
        """The sequencing instrument model recorded in SRA."""
        return _attr(self._exp_xml, "Platform", "instrument_model")

    @property
    def url(self) -> str:
        """The NCBI SRA web URL for this record."""
        return f"https://www.ncbi.nlm.nih.gov/sra/?term={self.accession}"


def _experiments_from_uids(client: EntrezClient, sra_uids: list[str]) -> list[Experiment]:
    """Build the :class:`Experiment` set for a list of ``sra`` UIDs, de-duplicated.

    One batched ``esummary`` for the whole set; each returned :class:`Experiment`
    is seeded with its UID and summary already fetched, so reading its fields costs
    no further network. Shared by both link-based (:func:`_linked_experiments`) and
    search-based (a study/sample/BioSample accession) resolution.
    """
    found: dict[str, Experiment] = {}
    for docsum in client.esummary_many(db="sra", uids=sra_uids):
        accession = _attr(str(docsum.get("ExpXml", "")), "Experiment", "acc")
        if accession and accession not in found:
            experiment = Experiment(accession, client=client)
            experiment._seed(uid=_docsum_uid(docsum), summary=dict(docsum))
            found[accession] = experiment
    return list(found.values())


def _linked_experiments(client: EntrezClient, uid: str, *, dbfrom: str = "gds") -> list[Experiment]:
    """Resolve the SRA experiments (``SRX``) linked from a ``dbfrom`` UID.

    One ``elink`` (to reach the linked ``sra`` UIDs, traversing GEO SuperSeries and
    BioProject umbrellas transitively) plus the batched ``esummary`` of
    :func:`_experiments_from_uids`.
    """
    sra_uids = client.elink(dbfrom=dbfrom, db="sra", uid=uid)
    return _experiments_from_uids(client, sra_uids)


class Experiment(_SraRecord):
    r"""An SRA experiment (``SRX000000``), resolved lazily through NCBI Entrez.

    Parameters
    ----------
    accession : str
        An SRA experiment accession, e.g. ``"SRX5921017"`` (``^[SED]RX\d+$``,
        covering SRA/ENA/DDBJ).
    client : EntrezClient or None
        Entrez client to use; a default one is built on first network access.

    Raises
    ------
    AccessionError
        If ``accession`` is not a well-formed experiment accession.
    """

    _ACCESSION_RE = re.compile(r"^[SED]RX\d+$")
    _KIND = "Experiment"

    @property
    def title(self) -> str:
        """The experiment title."""
        return _attr(self._exp_xml, "Experiment", "name") or _tag_text(self._exp_xml, "Title")

    @property
    def study(self) -> str:
        """The parent study accession (``SRP…``), or empty string if absent."""
        return _attr(self._exp_xml, "Study", "acc")

    @cached_property
    def runs(self) -> list[Run]:
        """The sequencing runs (``SRR``) of this experiment, as :class:`Run` instances.

        A run resolves to the same ``sra`` record as its experiment, so each
        :class:`Run` is seeded with this experiment's summary (no extra fetch).
        """
        return [
            Run(acc, client=self.client)._seed(uid=self.uid, summary=self._summary)
            for acc in _attrs_all(self._runs_xml, "Run", "acc")
        ]


class Run(_SraRecord):
    r"""An SRA run (``SRR000000``), resolved lazily through NCBI Entrez.

    Parameters
    ----------
    accession : str
        An SRA run accession, e.g. ``"SRR9000001"`` (``^[SED]RR\d+$``).
    client : EntrezClient or None
        Entrez client to use; a default one is built on first network access.

    Raises
    ------
    AccessionError
        If ``accession`` is not a well-formed run accession.
    """

    _ACCESSION_RE = re.compile(r"^[SED]RR\d+$")
    _KIND = "Run"

    @property
    def _run_tag(self) -> str:
        match = re.search(
            rf'<Run\b[^>]*?\bacc="{re.escape(self.accession)}"[^>]*?/?>', self._runs_xml
        )
        return match.group(0) if match else ""

    @property
    def total_spots(self) -> int | None:
        """The number of spots (reads) in this run, or ``None`` if unreported."""
        return _int_attr(self._run_tag, "total_spots")

    @property
    def total_bases(self) -> int | None:
        """The number of bases in this run, or ``None`` if unreported."""
        return _int_attr(self._run_tag, "total_bases")

    @property
    def is_public(self) -> bool:
        """Whether the run's data is publicly accessible."""
        return _attr(self._run_tag, "Run", "is_public") == "true"

    @cached_property
    def _full_xml(self) -> str:
        """The full SRA XML for this run's experiment package (fetched once).

        The esummary ``Runs`` fragment carries spot/base totals but not the
        per-read layout, so the read structure is read from the full record.
        """
        return self.client.efetch(db="sra", ids=[self.uid], rettype="full", retmode="xml")

    @property
    def _statistics_block(self) -> str:
        """Return this run's ``<RUN>…</RUN>`` block within :attr:`_full_xml`."""
        return _run_blocks(self._full_xml).get(self.accession, "")

    @property
    def read_lengths(self) -> list[int]:
        """The average length of each read (mate) sequenced per spot, in read order.

        Reads SRA's per-run ``<Statistics>`` from the full record (one EFetch,
        cached). A paired run yields two lengths, e.g. ``[28, 94]``; a single-end
        run yields one. Empty when SRA reports no read statistics.
        """
        return _read_lengths(self._statistics_block)

    @property
    def n_reads(self) -> int | None:
        """The number of reads (mates) sequenced per spot, or ``None`` if unreported."""
        nreads = _int_attr(self._statistics_block, "nreads")
        if nreads is not None:
            return nreads
        return len(self.read_lengths) or None

    @property
    def read_structure(self) -> str:
        """The run's read structure as ``{L1}+{L2}+…`` (e.g. ``"28+94"``), or ``""``.

        A compact rendering of :attr:`read_lengths`: each read's rounded average
        length joined with ``+`` in read order. Empty when the lengths are unknown.
        """
        return _read_structure(self.read_lengths)

    @cached_property
    def experiment(self) -> Experiment:
        """The parent :class:`Experiment` (``SRX``) of this run.

        Shares this run's ``sra`` record, so the experiment is seeded from the
        same summary (no extra fetch).
        """
        accession = _attr(self._exp_xml, "Experiment", "acc")
        return Experiment(accession, client=self.client)._seed(uid=self.uid, summary=self._summary)

    @cached_property
    def files(self) -> list[RemoteFile]:
        """Every file the SRA Data Locator lists for this run (one SDL request, cached).

        Includes both the normalized SRA archive file(s) and any original/submitted
        files (e.g. a 10X Genomics ``possorted_genome_bam``). This is the data behind
        the Run Browser's "Data access" panel.

        Returns
        -------
        list of RemoteFile
            All files SDL exposes for this run, each with its download locations.

        Raises
        ------
        SdlError
            If the SDL request fails or the run is unknown/non-public.

        Examples
        --------
        >>> [(f.name, f.type) for f in Run("SRR20172067").files]  # doctest: +SKIP
        [('possorted_genome_bam_TC2_d15_1.bam', 'TenX'), ('SRR20172067', 'sra')]
        """
        return self.sdl_client.retrieve(self.accession)

    @property
    def original_files(self) -> list[RemoteFile]:
        """The original/submitted files for this run (the "Original format" listing).

        The submitter's own uploads — e.g. the 10X Genomics BAM behind a run whose
        ``.sra`` only stored a single read — as opposed to the normalized SRA archive.
        Filters :attr:`files` to the non-archive types.

        Returns
        -------
        list of RemoteFile
            Files whose type is not a normalized SRA archive type; empty when the
            submission has no original-format files.

        Examples
        --------
        >>> [f.name for f in Run("SRR20172067").original_files]  # doctest: +SKIP
        ['possorted_genome_bam_TC2_d15_1.bam']
        """
        return [file for file in self.files if not file.is_sra_archive]
