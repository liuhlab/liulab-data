# Yearly maintenance run

The heavy checks and the removal of withdrawn SRA records, wrapped around one monthly run.
The steps, their order and their rough cost live in the "Yearly" section of
`docs/how-to/store-run.md`; follow them from there. The paper store adds nothing yearly.

**Budget:** the refresh keeps its 2 h. Each heavy step's budget is its row in "What a yearly
run adds", with an hour's margin; size the run plan from that table.

1. **Before the refresh**, on the current build: "Open every record", then "Rebuild with no
   network and compare", then "Fetch the whole Series list again" on the refresh's day.
2. **The monthly page.** Run every step of [`monthly.md`](monthly.md), on the snapshot step 1
   fetched.
3. **After the refresh**: "Remove withdrawn SRA records", then "Time the queries on a quiet
   node" on the build that removal made. The removal ends with a copy, so run "Check the copy"
   in `docs/how-to/store-gcs.md` again after it.

Record in the log entry, beside the monthly checks:

- the build ID the removal made, as the entry's build;
- each heavy step's outcome and wall time;
- the SRA removal's counts: candidates, confirmed gone, kept on a fetch error;
- each benchmark query's wall time and memory against its limit.

Done when: the five heavy steps and the monthly checks all have an outcome.
