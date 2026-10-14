"""Stream the bulk bioproject.xml once and write one TSV row per BioProject.

Columns: accession, submitted, organism, data_type, geo_ids, pmids, title.
``geo_ids`` joins every GEO ID the record cites (CenterID center=GEO and dbXREF db=GEO);
``pmids`` joins the Publication IDs whose DbType is ePubmed.

Usage: python3 bioproject_index.py BIOPROJECT_XML OUT_TSV
"""

import sys
import xml.etree.ElementTree as ET


def clean(text: str | None) -> str:
    """Flatten a text node to one tab-free line."""
    return " ".join((text or "").split())


def main(src: str, out: str) -> None:
    """Write the index."""
    with open(out, "w") as fh:
        fh.write("accession\tsubmitted\torganism\tdata_type\tgeo_ids\tpmids\ttitle\n")
        for _, el in ET.iterparse(src, events=("end",)):
            if el.tag != "Package":
                continue
            arch = el.find(".//ArchiveID")
            if arch is None:
                el.clear()
                continue
            geo = {clean(c.text) for c in el.iter("CenterID") if c.get("center") == "GEO"}
            for x in el.iter("dbXREF"):
                if x.get("db") == "GEO":
                    geo.update(clean(i.text) for i in x.iter("ID"))
            pmids = sorted(
                {
                    p.get("id") or ""
                    for p in el.iter("Publication")
                    if clean(p.findtext("DbType")) == "ePubmed"
                }
                - {""}
            )
            sub = el.find(".//Submission")
            dtype = el.find(".//ProjectDataTypeSet/DataType")
            row = [
                arch.get("accession") or "",
                (sub.get("submitted") if sub is not None else "") or "",
                clean(el.findtext(".//Organism/OrganismName")),
                clean(dtype.text if dtype is not None else ""),
                ",".join(sorted(geo - {""})),
                ",".join(pmids),
                clean(el.findtext(".//ProjectDescr/Title")),
            ]
            fh.write("\t".join(row) + "\n")
            el.clear()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
