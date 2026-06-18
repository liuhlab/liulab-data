"""A small, coherent synthetic GEO/SRA graph for wiring FakeEntrezClient.

One Series (GSE131907) with two samples on one platform (GPL16791), linked to one
BioProject and one SRA experiment (SRX5921017) that has two runs. The values are
synthetic but structurally faithful to real esummary/elink/ExpXml responses.
"""

from __future__ import annotations

from tests._fakes import FakeEntrezClient

GSE = "GSE131907"
GSE_UID = "200131907"
GSM1, GSM2 = "GSM3827114", "GSM3827115"
GSM1_UID = "303827114"
GPL = "GPL16791"
GPL_UID = "100016791"
BP_UID = "545296"
SRX = "SRX5921017"
SRR1, SRR2 = "SRR9000001", "SRR9000002"
SRA_UID = "5921017"

_EXP_XML = (
    "<Summary><Title>scRNA-seq of lung adenocarcinoma</Title>"
    '<Platform instrument_model="Illumina HiSeq 2500">ILLUMINA</Platform>'
    '<Statistics total_runs="2" total_spots="1000" total_bases="2000"/></Summary>'
    f'<Experiment acc="{SRX}" name="scRNA-seq of LUNG_N01"/>'
    '<Study acc="SRP200000" name="Lung adenocarcinoma scRNA-seq"/>'
    '<Organism taxid="9606" ScientificName="Homo sapiens"/>'
    '<Sample acc="SRS200000" name=""/>'
)
_RUNS_XML = (
    f'<Run acc="{SRR1}" total_spots="600" total_bases="1200" is_public="true"/>'
    f'<Run acc="{SRR2}" total_spots="400" total_bases="800" is_public="true"/>'
)

_GSE_SUMMARY = {
    "title": "Single-cell landscape of lung adenocarcinoma",
    "summary": "An scRNA-seq atlas ...",
    "taxon": "Homo sapiens",
    "GPL": "16791",
    "PubMedIds": ["32385277"],
    "Samples": [
        {"Accession": GSM1, "Title": "LUNG_N01"},
        {"Accession": GSM2, "Title": "LUNG_N02"},
    ],
}
_GSM_SUMMARY = {
    "title": "LUNG_N01",
    "taxon": "Homo sapiens",
    "GPL": "16791",
    "GSE": "131907",
}
_GPL_SUMMARY = {
    "title": "Illumina HiSeq 2500 (Homo sapiens)",
    "taxon": "Homo sapiens",
    "n_samples": "373734",
}
_SRA_SUMMARY = {"ExpXml": _EXP_XML, "Runs": _RUNS_XML}


def build_client() -> FakeEntrezClient:
    """Return a FakeEntrezClient wired with the synthetic GEO/SRA graph."""
    return FakeEntrezClient(
        esearch={
            f"{GSE}[ACCN] AND gse[ETYP]": [GSE_UID],
            f"{GSM1}[ACCN] AND gsm[ETYP]": [GSM1_UID],
            f"{GPL}[ACCN] AND gpl[ETYP]": [GPL_UID],
            SRX: [SRA_UID],
            SRR1: [SRA_UID],
            SRR2: [SRA_UID],
        },
        esummary={
            ("gds", GSE_UID): _GSE_SUMMARY,
            ("gds", GSM1_UID): _GSM_SUMMARY,
            ("gds", GPL_UID): _GPL_SUMMARY,
            ("sra", SRA_UID): _SRA_SUMMARY,
            ("bioproject", BP_UID): {"Project_Acc": "PRJNA545296"},
        },
        elink={
            ("gds", "sra", GSE_UID): [SRA_UID],
            ("gds", "sra", GSM1_UID): [SRA_UID],
            ("gds", "bioproject", GSE_UID): [BP_UID],
        },
    )
