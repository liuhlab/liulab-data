"""10x Genomics special-case handling: recover FASTQ from cellranger BAMs.

When a 10x run's SRA-normalized ``.sra`` is unusable (it dropped the technical reads),
the submitter's original cellranger BAM is the correct source. This subpackage converts
that BAM back into remap-ready FASTQ with 10x's ``bamtofastq``, driven as a Snakemake
DAG (:mod:`labdata.tenx.pipeline`) so it mirrors the download pipeline's layout.
"""

from labdata.tenx.bamtofastq import TenxConverter, bamtofastq_run, is_tenx_bam_run

__all__ = [
    "TenxConverter",
    "bamtofastq_run",
    "is_tenx_bam_run",
]
