"""A small, coherent synthetic GEO/SRA graph for wiring FakeEntrezClient.

One Series (GSE131907) with two samples on one platform (GPL16791), linked to one
BioProject (PRJNA545296) and one SRA experiment (SRX5921017) that has two runs.
The values are synthetic but structurally faithful to real esummary/elink/ExpXml
responses.
"""

from __future__ import annotations

from tests._fakes import FakeEntrezClient

GSE = "GSE131907"
GSE_UID = "200131907"
GSM1, GSM2 = "GSM3827114", "GSM3827115"
GSM1_UID = "303827114"
GPL = "GPL16791"
GPL_UID = "100016791"
PRJNA = "PRJNA545296"
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
    "Id": GSE_UID,
    "title": "Single-cell landscape of lung adenocarcinoma",
    "summary": "An scRNA-seq atlas ...",
    "taxon": "Homo sapiens",
    "entryType": "GSE",
    "Accession": GSE,
    "GPL": "16791",
    "PubMedIds": ["32385277"],
    "Samples": [
        {"Accession": GSM1, "Title": "LUNG_N01"},
        {"Accession": GSM2, "Title": "LUNG_N02"},
    ],
}
_BIOPROJECT_SUMMARY = {
    "Project_Acc": PRJNA,
    "Project_Title": "Single cell RNA sequencing of lung adenocarcinoma",
    "Project_Name": "Single cell RNA sequencing of lung adenocarcinoma",
    "Project_Description": "We performed single cell RNA sequencing ...",
    "Project_Data_Type": "Transcriptome or Gene expression",
    "Registration_Date": "2019/05/29 00:00",
    "Organism_Name": "Homo sapiens",
    "Submitter_Organization": "The Catholic University of Korea",
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
_SRA_SUMMARY = {"Id": SRA_UID, "ExpXml": _EXP_XML, "Runs": _RUNS_XML}


def build_client() -> FakeEntrezClient:
    """Return a FakeEntrezClient wired with the synthetic GEO/SRA graph."""
    return FakeEntrezClient(
        esearch={
            f"{GSE}[ACCN] AND gse[ETYP]": [GSE_UID],
            f"{GSM1}[ACCN] AND gsm[ETYP]": [GSM1_UID],
            f"{GPL}[ACCN] AND gpl[ETYP]": [GPL_UID],
            PRJNA: [BP_UID],
            SRX: [SRA_UID],
            SRR1: [SRA_UID],
            SRR2: [SRA_UID],
        },
        esummary={
            ("gds", GSE_UID): _GSE_SUMMARY,
            ("gds", GSM1_UID): _GSM_SUMMARY,
            ("gds", GPL_UID): _GPL_SUMMARY,
            ("sra", SRA_UID): _SRA_SUMMARY,
            ("bioproject", BP_UID): _BIOPROJECT_SUMMARY,
        },
        elink={
            ("gds", "sra", GSE_UID): [SRA_UID],
            ("gds", "sra", GSM1_UID): [SRA_UID],
            ("gds", "bioproject", GSE_UID): [BP_UID],
            ("bioproject", "gds", BP_UID): [GSE_UID],
            ("bioproject", "sra", BP_UID): [SRA_UID],
        },
    )


# --------------------------------------------------------------------------- #
# A fully public Series (GSE229022) for the SRA run-table feature. The runinfo
# rows are trimmed from the real ``efetch(db=sra, rettype=runinfo)`` response.
# --------------------------------------------------------------------------- #

GSE2 = "GSE229022"
GSE2_UID = "200229022"
RUNTABLE_SRA_UIDS = ["31566815", "27255945"]

RUNINFO_CSV = (
    "Run,ReleaseDate,spots,bases,avgLength,size_MB,Experiment,LibraryName,"
    "LibraryStrategy,LibrarySelection,LibrarySource,LibraryLayout,Platform,Model,"
    "SRAStudy,BioProject,Sample,BioSample,TaxID,ScientificName,SampleName,CenterName\n"
    "SRR24084454,2024-03-21,71383145,9065659415,127,3362,SRX19885398,GSM7147956,"
    "RNA-Seq,cDNA,TRANSCRIPTOMIC SINGLE CELL,PAIRED,ILLUMINA,NextSeq 550,"
    "SRP431124,PRJNA952585,SRS17239590,SAMN34081621,6239,Caenorhabditis elegans,"
    "GSM7147956,JANELIA\n"
    "SRR24084455,2024-03-21,68000000,8600000000,126,3200,SRX19885399,GSM7147957,"
    "RNA-Seq,cDNA,TRANSCRIPTOMIC SINGLE CELL,PAIRED,ILLUMINA,NextSeq 550,"
    "SRP431124,PRJNA952585,SRS17239591,SAMN34081622,6239,Caenorhabditis elegans,"
    "GSM7147957,JANELIA\n"
    "SRR27685594,2024-01-01,50000000,6000000000,120,2500,SRX23000000,GSM8000000,"
    "RNA-Seq,cDNA,TRANSCRIPTOMIC SINGLE CELL,PAIRED,ILLUMINA,NextSeq 550,"
    "SRP431124,PRJNA952585,SRS18000000,SAMN35000000,6239,Caenorhabditis elegans,"
    "GSM8000000,JANELIA\n"
)


def build_runtable_client() -> FakeEntrezClient:
    """Return a FakeEntrezClient wired for ``GSE229022.make_sra_run_table``."""
    return FakeEntrezClient(
        esearch={f"{GSE2}[ACCN] AND gse[ETYP]": [GSE2_UID]},
        elink={("gds", "sra", GSE2_UID): RUNTABLE_SRA_UIDS},
        efetch={("sra", "runinfo"): RUNINFO_CSV},
    )
