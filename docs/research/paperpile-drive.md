---
search:
  exclude: true
---

# Paperpile's Google Drive sync, read from labdata

Question ([#140](https://github.com/liuhlab/liulab-data/issues/140)): how does Paperpile sync
attached PDFs to Google Drive (folder layout, file names, rename and delete)? How can labdata
match a synced PDF back to its PMID or DOI? How would a script read that folder, and how would
labdata hand the maintainer a queue of papers to add?

Sources are Paperpile's help center, the rclone Google Drive backend page and Google's
developer docs, read on 2026-11-07. Nobody's Drive or Paperpile account was opened. Each claim
is tagged **read** (stated by the source) or **inferred** (my conclusion, not stated). Forum
posts by Paperpile staff are marked as forum, since they can be years old.

## Answer

- **Layout.** Paperpile pushes every attached PDF and supplementary file into a `Paperpile`
  folder in the Drive of the account that signed up. By default the files sit in one-letter
  sub-folders (first author's last name) and the main PDF is named `Author year - Title`. The
  pattern is configurable; an optional `All Papers` sub-folder can hold everything.
- **No identifier in the file name.** The pattern placeholders are author, title, journal, year
  and original file name only. There is no DOI, PMID or citation-key placeholder. A file name
  alone cannot be matched to a PMID.
- **Rename and delete.** Drive mirrors the Paperpile library one way. Delete in Paperpile to
  remove a file; a file deleted only in Drive is uploaded again on the next sync. Paperpile asks
  users not to rename or delete in Drive.
- **Matching (recommended).** Read the identifiers from the PDF itself (DOI in the first pages
  or in the PDF metadata), and confirm against title, first author and year from an automatic
  BibTeX export of the library with identifiers switched on. Neither path alone is documented to
  be complete; together they should cover nearly every publisher PDF (inferred). A manual RIS
  export reportedly gives each PDF's path directly (forum, 2017), which suits a one-time match
  of the existing library.
- **Reading.** `rclone` with the lab's own OAuth client, scope `drive.readonly`, rooted at the
  `Paperpile` folder's ID: `rclone lsjson -R --hash` lists names, Drive IDs and MD5s; `rclone
  copy` fetches new files. The weak point is the login, not the listing.
- **Handing over the queue.** Write a PubMed `.nbib` file (MEDLINE format, which labdata can
  get from NCBI efetch) or a plain DOI/PMID list. Paperpile imports `.nbib`, `.ris` and `.bib`
  files and accepts pasted DOIs and PMIDs, with duplicates skipped by default.

## 1. How the Drive sync works

| Fact | Evidence | Source |
| --- | --- | --- |
| Synced files appear in a `Paperpile` folder in the user's Google Drive | read | [Sync with Google Drive][sync] |
| Sync covers all PDFs and supplementary files, automatically, in the background | read | [Sync with Google Drive][sync] |
| Default: folders by first letter of first author's last name; main PDF named `Author year - Title`; supplementary files keep their name with an author-year prefix | read | [Sync with Google Drive][sync], [Customize names][names] |
| Drive files "always reflect your current Paperpile library" | read | [Sync with Google Drive][sync] |
| "Always delete or rename files in Paperpile, not directly in Google Drive" | read | [Sync with Google Drive][sync] |
| A file deleted in Drive is detected and uploaded again on the next sync; to delete for good, delete in Paperpile | read | [Sync with Google Drive][sync] |
| Turning sync off leaves the files in Drive | read | [Sync with Google Drive][sync] |
| Paperpile only has access to the files it uploaded, not the whole Drive; it cannot import PDFs already in Drive | read | [Sync with Google Drive][sync], [FAQ][faq] |
| Sync goes only to the Drive of the account used to sign up | read | [FAQ][faq] |
| A low-Drive-space error stops sync until space is freed and the user clicks Resync | read | [Sync with Google Drive][sync] |
| Sync is one way, Paperpile to Drive; a file dropped into the folder by hand is not imported | inferred from the two rows above; the help page does not cover manual additions | [Sync with Google Drive][sync], [FAQ][faq] |
| Renaming a reference or editing its metadata renames the Drive file to match | inferred from "always reflect your current library"; not stated | [Sync with Google Drive][sync] |
| What happens to existing files when the name pattern changes | not documented | [Customize names][names] |
| A user saw split `W` and `w` folders under `All Papers`; staff fixed it with Resync under Settings > Data and files (2025) | forum | [Duplicate folder][forum-dup] |

### File and folder name patterns

Settings > General > "Set file and folder name pattern" ([Customize names][names], read):

- Presets "Organize files by": Author (default), Folder (mirrors the Paperpile folder tree),
  Journal, Year.
- Advanced placeholders: `[authors]`, `[firstauthor]`, `[lastauthor]`, `[title]`,
  `[titleshort]`, `[journal]`, `[year]`, `[filename]`. A `/` makes a folder level; `:n` caps
  characters or words (`[firstauthor:1]`, `[year:2]`).
- An option puts all files in a sub-folder called `All Papers` ([FAQ][faq], read from the
  dialog's screenshot description).

No placeholder carries a DOI, PMID or Paperpile ID (read: the list above is the full list on
the page). So labdata cannot ask Paperpile to put the identifier in the name.

## 2. Matching a synced PDF to its PMID or DOI

Three sources of identity, with what is known about each:

| Route | What it gives | Evidence | Source |
| --- | --- | --- | --- |
| File name | first author, year, title (default pattern); no identifier | read | [Customize names][names] |
| Automatic BibTeX export | a BibTeX file of a folder, a label or the whole library, kept current on Drive, GitHub or a download link; updated when a reference is added, removed or changed | read | [Automatic BibTeX][autobib] |
| — its identifiers | DOI and PMID appear with the "Include identifiers (doi, pmid)" setting (Settings > BibTeX, 2015 post); PMCID exports too (2021 post) | forum | [DOI missing][forum-doi], [PMCID and BibTeX][forum-pmcid] |
| — its link to the file | not documented | — | [Automatic BibTeX][autobib] |
| — limits | shared folders cannot be exported; the file is read-only and overwritten on each sync; the Drive copy can be moved and renamed | read | [Automatic BibTeX][autobib] |
| RIS export (manual) | the `L1` field holds the PDF's path relative to the synced Drive folder, e.g. `All Papers/<folder>/…`; several paths, comma-joined, when a reference sits in several folders | forum, staff 2017 and users to 2021 | [Export attachments with RIS][forum-ris] |
| JSON export (manual) | Paperpile's internal format with metadata, attachments, folders and labels; no automatic JSON | read | [Export library data][export] |
| — its link to the file | "The JSON includes the links to your files in Google Drive" | forum, staff 2016 | [Export attachments with RIS][forum-ris] |
| The PDF itself | publisher PDFs usually print the DOI on the first page and often carry it in XMP metadata | inferred; no Paperpile source, and preprints or scans may lack it | — |

Recommended match, in order (inferred):

1. Extract a DOI from the PDF (XMP metadata, then text of the first two pages). Look it up in
   the store's paper table; a hit is a match.
2. Otherwise, parse `Author year - Title` from the file name and match it against the BibTeX
   export (with identifiers on) by first author, year and normalized title. The BibTeX entry
   gives the DOI or PMID.
3. Otherwise, log the file as unmatched; never guess.

The RIS and JSON exports give a direct file-to-reference link if the forum posts still hold,
but both are manual, so they suit a one-time match of the existing library, not the daily job.
The automatic export is BibTeX only. One manual RIS export by the maintainer would show whether
`L1` paths still appear; that is a check for the build ticket, not this one.

Match by Drive file ID and MD5 after the first time (inferred): Drive keeps a file's ID when
it is renamed, and rclone reports MD5, so a renamed but unchanged file needs no new match.

## 3. Reading the folder from a script

### rclone

From the [rclone Google Drive page][rclone] (read):

- Scope `drive.readonly`: "Files may be listed and downloaded but not uploaded, renamed or
  deleted." Scopes can be combined, for example `drive.readonly,drive.file`.
- `root_folder_id` makes a folder the remote's root, so the remote sees only `Paperpile/`.
- Drive reports MD5, SHA1 and SHA256 hashes (older files may lack SHA1 and SHA256).
- `--fast-list` uses fewer API calls for a recursive listing.
- Drive allows two files with the same name in one folder; rclone warns and `rclone dedupe`
  exists to fix it.
- "rclone's shared Google Drive client_id is being retired and will stop working during
  2026." A personal OAuth client ID is needed.
- A service account is supported (`service_account_file`); it sees a personal Drive folder only
  if that folder is shared with the account (inferred from how Drive sharing works).

A daily job would run `rclone lsjson -R --hash --fast-list` (inferred), diff the listing
against the last one by Drive ID and MD5, and `rclone copy` only new or changed PDFs into the
store's staging area. labdata already wraps rclone behind `store.gcs._run_rclone`; a Drive
remote fits the same seam (inferred).

### The Drive API directly

The [Drive search guide][drive-search] (read) gives the query
`'FOLDER_ID' in parents and trashed = false` and `modifiedTime > '…'` filters; extra fields
such as checksums must be named in `fields`. The guide does not say `in parents` is recursive,
so a tree walk is one query per folder. rclone already does this walk, retries and paging;
the API adds nothing labdata needs (inferred).

### The login is the weak point

From [Google's OAuth 2.0 overview][oauth] (read):

- An OAuth client whose consent screen is external and in "Testing" status gets refresh tokens
  that expire in 7 days.
- An organization admin can set a session length of 1 to 24 hours, after which calls fail with
  `invalid_grant`.
- An unused refresh token expires after six months.

So a daily unattended job needs either an OAuth client in "Production" status (or an internal
client of a Workspace organization), or a service account the `Paperpile` folder is shared with
(inferred). If the maintainer's Google account sits under a session-length policy, as the GCS
login does, the Drive step must skip cleanly when the token is dead and pick up after the next
login, like the GCS steps.

## 4. Handing the queue to the maintainer

Paperpile's import paths ([Paste reference data][paste], [Import RIS or BibTeX][import],
read):

- Add > Paste accepts DOIs, PMIDs and PMCIDs, arXiv IDs, BibTeX and RIS records, several at
  once, with a "skip duplicates" option. The page does not say one per line or give a limit.
- Add > Upload files accepts `.bib`, `.ris` and PubMed `.nbib` files. "Skip duplicates" is on by
  default. The import can go to a chosen folder or label.
- "Automatically download article PDFs" (Settings > General) is on by default, with an optional
  preprint fallback; for many references at once, select them and choose More > Find PDFs
  online (or press D) ([Find PDFs][findpdf], read). The help pages do not say whether an
  upload triggers the automatic download, so "select all, press D" is the documented path.

Recommended hand-off (inferred):

- labdata writes `queue.nbib`: the MEDLINE records of the queued PMIDs, fetched with NCBI
  efetch (`rettype=medline`), which is the `.nbib` format. Papers with only a DOI go in a second
  file, `queue-doi.txt`, one DOI per line, for the paste dialog.
- The maintainer uploads the file into a Paperpile folder such as `labdata queue`, selects all,
  presses D, and fetches the rest by hand through the library proxy.
- The queue lands where the maintainer looks: a Drive folder outside `Paperpile/` written with
  the `drive.file` scope, or the store's status report. Each queue entry keeps the reason it is
  queued (paywalled, no open copy found).
- The queue is rebuilt from the store each run: a paper drops out once a matched PDF for it
  arrives in Drive, so the file never needs editing by hand.

## Open points for the build spec

- Whether the automatic BibTeX export holds a file path (undocumented), and whether a manual
  RIS export still carries `L1` paths (forum, 2017): one export of each settles both.
- How many synced PDFs carry a findable DOI: measure on the real folder in the build ticket.
- Whether Paperpile renames existing Drive files when the name pattern changes: matters only
  if the maintainer changes the pattern; matching by Drive ID makes it harmless.
- Which login works unattended for the maintainer's Google account: a Production OAuth client
  or a service account shared on `Paperpile/`. This is a decision for the maintainer.

[sync]: https://paperpile.com/h/sync-google-drive/
[names]: https://paperpile.com/h/customize-file-folder-names/
[faq]: https://paperpile.com/h/faqs/
[autobib]: https://paperpile.com/h/automatic-bibtex-export/
[export]: https://paperpile.com/h/export-library-data/
[paste]: https://paperpile.com/h/paste-reference-data/
[import]: https://paperpile.com/h/import-ris-bibtex/
[findpdf]: https://paperpile.com/h/find-download-pdfs/
[forum-dup]: https://forum.paperpile.com/t/duplicate-folder-in-google-drive/11361
[forum-doi]: https://forum.paperpile.com/t/doi-missing-when-exporting-bibtex-library/915
[forum-pmcid]: https://forum.paperpile.com/t/pmcid-and-bibtex/6205
[forum-ris]: https://forum.paperpile.com/t/export-attachments-with-ris/1728
[rclone]: https://rclone.org/drive/
[drive-search]: https://developers.google.com/workspace/drive/api/guides/search-files
[oauth]: https://developers.google.com/identity/protocols/oauth2
