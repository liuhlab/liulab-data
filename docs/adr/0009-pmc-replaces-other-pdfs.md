---
search:
  exclude: true
---

# PMC replaces a PDF from OpenAlex or a host link

The paper store prefers PMC over OpenAlex and host links. PMC gives the article XML, figures as
files and supplements; OpenAlex and host links give a PDF only. When a paper the store holds from
OpenAlex or a host link later appears in the PMC bucket with XML, the PMC sync fetches the PMC
files and then removes the other PDF: its file and its `files` row. The paper's `xml_tier` and
`pdf_tier` then read as for any PMC paper, and PMC's own PDF stays listed, not downloaded.

The removal runs only after the PMC files are written and the tables saved, so a stop leaves the
paper with both copies, never neither. Each sync checks every paper with PMC XML for another
tier's PDF, so the next sync finishes a half-done swap. A file another `files` row still points at
is kept.

## Considered options

- **Keep both.** The other tier's PDF holds the paper's name, and the store keeps a second copy
  of a paper it already has in a better form.
- **Keep the PDF, add PMC beside it.** This was the first rule. It reads the same article twice
  and leaves `pdf_tier` naming a tier the store no longer needs.
- **Replace it.** One copy per paper, from the best source.

## Consequences

- A paper withdrawn from PMC after the swap keeps its PMC files with a `gone` date; the old PDF
  is not fetched again.
- The daily pass never fetches from OpenAlex or a host link for a paper the store holds, so a
  swapped paper never returns to those tiers.
