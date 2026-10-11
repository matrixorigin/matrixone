# Development campaign evidence

All 744 required, unprofiled executions completed on 2026-10-11. Native MO and
embedded Sirius GPU execution agreed on exact schema, values and duplicate counts
for canonical Q1-Q22 at SF1/SF10 and streams 1/2/4. Each embedded query completed
GPU work with no fallback, healthy terminal status and zero retained input/result
credit. `runs.jsonl` contains safe observations; it contains no headings, SQL,
credentials or result rows. CSV retains every timing repetition, including labeled
excluded warmups and stream controls.

These are **development results**, with the actual source/binary/SDK identities in
`provenance.json`. They use unmerged Sirius #30 at `06ab53c5`, so they cannot satisfy
release acceptance. The MatrixOne validation branch contains the same compiler and
schema-evidence changes as D. The runner here is the original streaming consumer;
the subsequent drain-owner extraction and its error-path tests require a rebuilt
runner for final release validation.

Median full-suite times were 1.607767s (native MO) and 4.199636s (embedded GPU) at
SF1; 25.330881s and 47.163896s at SF10. Per-query medians, sums, suites and Q9 raw
repetitions are retained. The GPU route was slower overall on this hardware.

The later development preflight gates are retained under `gates/`:

- `resources.json`: all 264 required native/GPU executions passed; all six SF1/SF10
  stream profiles had zero warmed allocated-byte growth and reconciled logical
  GPU pools. Four captures use the original producer; two diagnostic captures use
  the refreshed SDK and rebuilt runner. The capture contexts remain distinct.
- `numeric-baselines.json`: fresh Native A/B/current binaries used matched operands
  and 21 samples per case. All ten ratios passed the 1.1 limit; checked-add64 was
  0.981322 and the maximum ratio was 1.007914. No historical-sample waiver applies.
- `lifecycle.json`: ten owning lifecycle cases with barrier/source/log proofs,
  including held-borrow cancellation, bounded admission, error ownership and
  process-death closure for fatal quarantine. The current writer/quota consumer
  checks cancellation before database/sql driver close and passes normal/race.
- `sanitizer.json`: preserves four raw exit statuses of 99. The full GPU memory
  check passed 34,509 assertions in 30 cases with one CUB registration diagnostic;
  the unchanged Native A input path reproduced it. The production ABI leak check
  passed 1,964 assertions in ten cases with one 41,386,248-byte cuDF default pinned
  allocation, matched by the no-GPU-work initialization control. Raw counts,
  hashes, source ownership and six zero-growth profiles support the classification.

These wrappers explicitly set `complete_release_acceptance: false`. Native gate
reports retain their original campaign input context and independently recorded
test/harness source, binary and log identities. They are not a uniform release
manifest and cannot be passed directly to final acceptance. The final bounded
allocation parser replayed the retained SF10 trace exactly under its private
sort-file, address-space and ledger limits. Current Python contract tests passed
74 cases; the consumer cancellation tests passed normal/race.

A merged native release pin, rebuilt final consumers, a uniform complete 744/264
release campaign and required CI/reviewer approval remain gates. The recorded
summary deliberately leaves acceptance incomplete. Q18's original QueryEnd pool
snapshots retain 768 GPU bytes before engine destruction; these older records lack
the subsequently added successor-window post-close proof. Do not claim logical
pool acceptance from these snapshots.
