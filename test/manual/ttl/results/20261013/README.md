# TTL verification results — 2026-10-13

- [summary.json](summary.json): tested checkout, environment, package results, counts, randomized validation sizes, build checksum and limitations.
- [test-events.jsonl](test-events.jsonl): complete run/pass/fail/skip records for TTL and registration tests, including integration subtests. Full parser/planner regression counts are in the summary.
- [fuzz.log](fuzz.log): the final 15-second fuzz run.
- [verification.log](verification.log): successful completion of the verification script.

The verbose source JSONL logs remain in the execution workspace; their SHA-256 checksums are recorded in the summary. The committed event extract omits runtime output and fixture authentication diagnostics.

Reproduce with `bash test/manual/ttl/verify.sh` from a repository with the Go toolchain and native dependencies prepared. Tests use embedded services and require local socket access. Counts include Go parent tests and subtests, not distinct database rows. No multi-CN failure or throughput claim is made.
