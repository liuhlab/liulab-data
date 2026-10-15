# liulab-data

Vocabulary for curating public sequencing metadata for the lab.

## Glossary

### Study

The unit at which a set of experiments and samples is understood together: why there are this
many, how they relate, and what biology they teach. It is decided by reading the content and the
paper. No single source container (a GEO SuperSeries or Series, a BioProject) is a study, and
labdata stores none.

_Avoid_: series, project, dataset (each names one source's container, not the study)

### Build

One complete set of queryable tables, made offline from the store's raw files and snapshots and
never changed afterward. The store names one build as current, and readers open that one.

_Avoid_: snapshot (a dated copy of one bulk source, which a build reads)

### Target Series list

The GEO Series a full download must hold: the sequencing Series in a dated `geo-series`
snapshot. A Series is on the list by the snapshot's type, even when its SOFT file gives another.

### Gap

A Series on the target Series list that the store does not hold, recorded with a reason. Coverage
passes when every target Series is either held or a gap with a known reason.
