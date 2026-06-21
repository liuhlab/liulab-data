"""Tests for Series.make_sra_run_table (the SRA Run Selector-style table)."""

import pandas as pd

from labdata.geo import Series
from tests import _geodata as g


def test_run_table_is_run_level() -> None:
    table = Series(g.GSE2, client=g.build_runtable_client()).make_sra_run_table()
    assert isinstance(table, pd.DataFrame)
    assert list(table["Run"]) == ["SRR24084454", "SRR24084455", "SRR27685594"]


def test_run_table_renames_and_orders_columns() -> None:
    table = Series(g.GSE2, client=g.build_runtable_client()).make_sra_run_table()
    # runinfo's avgLength/bases/Model are renamed to Run-Selector names.
    for column in (
        "Run",
        "BioSample",
        "AvgSpotLen",
        "Bases",
        "Experiment",
        "Instrument",
        "LibraryName",
    ):
        assert column in table.columns
    assert "avgLength" not in table.columns
    assert "Model" not in table.columns
    # the key columns lead the frame, with ReadStructure right after Bases
    assert list(table.columns[:5]) == ["Run", "BioSample", "AvgSpotLen", "Bases", "ReadStructure"]


def test_run_table_values() -> None:
    table = Series(g.GSE2, client=g.build_runtable_client()).make_sra_run_table()
    first = table.iloc[0]
    assert first["BioSample"] == "SAMN34081621"
    assert first["Experiment"] == "SRX19885398"
    assert first["Instrument"] == "NextSeq 550"
    assert first["AvgSpotLen"] == 127
    assert first["Bases"] == 9065659415
    assert first["LibraryName"] == "GSM7147956"


def test_run_table_read_structure_column() -> None:
    table = Series(g.GSE2, client=g.build_runtable_client()).make_sra_run_table()
    structures = dict(zip(table["Run"], table["ReadStructure"], strict=True))
    # Per-read <Statistics> renders as {L1}+{L2}; the run lacking stats is empty.
    assert structures["SRR24084454"] == "28+99"
    assert structures["SRR24084455"] == "28+98"
    assert structures["SRR27685594"] == ""


def test_run_table_uses_batched_efetch_per_report() -> None:
    client = g.build_runtable_client()
    Series(g.GSE2, client=client).make_sra_run_table()
    # One batched runinfo fetch + one batched full-XML fetch (for ReadStructure).
    assert client.count("efetch") == 2


def test_run_table_empty_without_sra() -> None:
    # A Series whose elink returns no SRA UIDs yields an empty table, no efetch.
    client = g.build_runtable_client().but(elink={})
    table = Series(g.GSE2, client=client).make_sra_run_table()
    assert table.empty
    assert client.count("efetch") == 0
