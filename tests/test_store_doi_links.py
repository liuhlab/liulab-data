"""DOI links: BioProject ``eDOI`` publications and DOIs in a Series' web link and citation."""

from __future__ import annotations

import pytest

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


@pytest.mark.parametrize(
    ("text", "doi"),
    [
        (
            "https://academic.oup.com/jimmunol/advance-article/doi/10.1093/jimmun/vkaf212/8264081",
            "10.1093/jimmun/vkaf212",
        ),
        (
            "https://aacrjournals.org/cancerimmunolres/article-abstract/doi/"
            "10.1158/2326-6066.CIR-23-1047/747364/CD36-pro-inflammatory?redirectedFrom=fulltext",
            "10.1158/2326-6066.cir-23-1047",
        ),
        (
            "https://academic.oup.com/nar/article-lookup/doi/10.1093/nar/gkx1234",
            "10.1093/nar/gkx1234",
        ),
        (
            "https://static-content.springer.com/esm/art%3A10.1038%2Fs41375-024-02475-5/"
            "MediaObjects/41375_2024_2475_MOESM1_ESM.docx",
            "10.1038/s41375-024-02475-5",
        ),
        (
            "https://www.cell.com/cms/10.1016/j.molcel.2024.02.028/attachment/"
            "195ef521-e217-4680-a8e5-05bd25219b06/mmc7.pdf",
            "10.1016/j.molcel.2024.02.028",
        ),
        (
            "https://www.pnas.org/doi/suppl/10.1073/pnas.2218632120/suppl_file/"
            "pnas.2218632120.sapp.pdf",
            "10.1073/pnas.2218632120",
        ),
        ("https://www.mdpi.com/article/10.3390/cancers16122262/s1", "10.3390/cancers16122262"),
        (
            "https://www.frontiersin.org/articles/10.3389/fimmu.2018.00042/full",
            "10.3389/fimmu.2018.00042",
        ),
        (
            "https://www.frontiersin.org/articles/10.3389/fphys.2022.836810/",
            "10.3389/fphys.2022.836810",
        ),
        (
            "http://onlinelibrary.wiley.com/doi/10.1111/acel.12611/abstract;jsessionid=72c8b7b2",
            "10.1111/acel.12611",
        ),
        ("https://onlinelibrary.wiley.com/doi/10.1111/pbi.12548/epdf", "10.1111/pbi.12548"),
        (
            "https://link.springer.com/content/pdf/10.1134/S0026893324020055.pdf",
            "10.1134/s0026893324020055",
        ),
    ],
)
def test_dois_drop_what_a_publisher_url_adds_after_the_doi(text: str, doi: str) -> None:
    assert sources.dois(text) == [doi]


@pytest.mark.parametrize(
    "doi",
    [
        "10.1093/nar/gkab587",
        "10.1155/2021/5522723",
        "10.23937/2643-4008/1710012",
        "10.1088/1758-5090/ad5766",
        "10.17504/protocols.io.kxygx9yqdg8j/v1",
        "10.1134/s0026893324020055",
        "10.1038/s41421-019-0106-xdo",
        "10.1002/adfm.76472digital",
    ],
)
def test_dois_keep_real_dois_with_slashes_and_run_on_text_whole(doi: str) -> None:
    assert sources.dois(f"https://doi.org/{doi}") == [doi]
