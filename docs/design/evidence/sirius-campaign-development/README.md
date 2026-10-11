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

Six resource configurations, lifecycle closure, full sanitizer/pinned-pool/CUB
reconciliation, fresh matched Native A/B/current numeric baselines, a merged
release pin and required CI/reviewer approval remain separate gates. The recorded
summary deliberately leaves acceptance incomplete. Q18's original QueryEnd pool
snapshots retain 768 GPU bytes before engine destruction; these older records lack
the subsequently added successor-window post-close proof. Do not claim logical
pool acceptance from these snapshots.
