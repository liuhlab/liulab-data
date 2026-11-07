"""DOI links: BioProject ``eDOI`` publications and DOIs in a Series' web link and citation."""

from __future__ import annotations

from labdata.store import sources
from labdata.store.sources import LinkRow

_BIOPROJECT = """<Package><Project><Project>
  <ProjectID><ArchiveID accession="PRJNA1" archive="NCBI" id="1"/></ProjectID>
  <ProjectDescr>
    <Publication id="30115783" status="ePublished"><DbType>ePubmed</DbType></Publication>
    <Publication id="https://doi.org/10.7717/PeerJ.7649" status="ePublished">
      <DbType>eDOI</DbType></Publication>
    <Publication id="10.1016/S0092-8674(00)80012-3" status="ePublished">
      <DbType>eDOI</DbType></Publication>
    <Publication id="17/11/110794" status="ePublished"><DbType>eDOI</DbType></Publication>
    <Publication id="PMC2854126"><DbType>ePMC</DbType></Publication>
  </ProjectDescr>
</Project></Project></Package>"""


_SOFT = """^SERIES = GSE1
!Series_title = A study
!Series_pubmed_id = 38114495
!Series_web_link = https://doi.org/10.1101/2020.04.08.031419
!Series_web_link = https://www.biorxiv.org/content/10.1101/2020.04.08.031419v1.full
!Series_web_link = https://dx.doi.org/10.21203%2Frs.3.rs-2919458%2Fv1?utm=x
!Series_web_link = http://www.ncbi.nlm.nih.gov/geo/info/ENCODE.html
!Series_citation = Smith J et al. Cell 2020; doi: 10.1016/J.CELL.2020.01.001.
!Series_summary = Re-analysed data of doi:10.1038/nature1
^SAMPLE = GSM1
!Sample_title = s1
!Sample_series_id = GSE1
"""


def _paper_links(links: list[LinkRow]) -> list[LinkRow]:
    return [link for link in links if link.relation in ("PubMed", "DOI")]


def test_bioproject_edoi_publications_give_doi_links_beside_pubmed() -> None:
    _, links = sources.bioproject(_BIOPROJECT)
    assert _paper_links(links) == [
        LinkRow("PRJNA1", "PMID:30115783", "PubMed", "BioProject XML"),
        LinkRow("PRJNA1", "DOI:10.7717/peerj.7649", "DOI", "BioProject XML"),
        LinkRow("PRJNA1", "DOI:10.1016/s0092-8674(00)80012-3", "DOI", "BioProject XML"),
    ]


def test_series_web_link_and_citation_dois_give_doi_links_naming_the_field() -> None:
    _, links = sources.soft(_SOFT)
    assert _paper_links(links) == [
        LinkRow("GSE1", "PMID:38114495", "PubMed", "SOFT"),
        LinkRow("GSE1", "DOI:10.1101/2020.04.08.031419", "DOI", "SOFT Series_web_link"),
        LinkRow("GSE1", "DOI:10.1101/2020.04.08.031419", "DOI", "SOFT Series_web_link"),
        LinkRow("GSE1", "DOI:10.21203/rs.3.rs-2919458/v1", "DOI", "SOFT Series_web_link"),
        LinkRow("GSE1", "DOI:10.1016/j.cell.2020.01.001", "DOI", "SOFT Series_citation"),
    ]
