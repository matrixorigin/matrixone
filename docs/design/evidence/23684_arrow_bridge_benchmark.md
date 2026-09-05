# #23684 Arrow bridge A/B benchmark

This record compares identical Arrow records with ordinary borrow policy and
`ForceMaterialize=true`. It contains both a bridge-level attribution benchmark
and a local end-to-end SQL LOAD benchmark; neither is a cloud-provider or
deployment acceptance benchmark.

Command, run on 2026-09-03 on Darwin/arm64 Apple M4:

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -run '^$' \
  -bench '^BenchmarkArrowBridgeMaterializeAB$' -benchmem \
  -benchtime=500ms -count=3 ./pkg/container/arrowbridge
```

## Results

| Fixture | Mode | ns/op, three runs | Borrowed / copied payload | Interpretation |
| --- | --- | --- | --- | --- |
| numeric + decimal | borrow | 982 / 991 / 990 | 98,304 / 0 bytes | exact fixed layouts retain the Arrow buffers |
| numeric + decimal | materialize | 19,951 / 20,053 / 20,171 | 0 / 98,304 bytes | forced mode performs the attributed payload copy |
| timestamp + short string | borrow | 346,201 / 346,379 / 345,458 | 0 / 73,728 bytes copied | no payload is borrow-eligible; temporal conversion and canonical inline strings dominate |
| timestamp + short string | materialize | 346,088 / 363,055 / 361,882 | 0 / 73,728 bytes copied | modes are intentionally equivalent for an ineligible schema |
| long binary | borrow | 78,241 / 76,979 / 77,033 | 1,048,576 / 0 bytes | long varlen payload is retained, with descriptor work still performed |
| long binary | materialize | 560,541 / 607,918 / 545,651 | 0 / 1,048,576 bytes | forced mode copies the full payload |

The counters prove attribution: eligible fixed/long-varlen cases report the
same logical payload in either mode, while ownership changes from borrowed to
copied. The short-string/temporal case correctly reports no eligible bytes and
therefore no artificial zero-copy win.

## End-to-end LOAD A/B

The same candidate binary was also exercised through the MySQL frontend,
parser/planner, External reader, conversion, transaction commit, storage, and
result acknowledgement. Each sample loaded 100,000 rows; table truncation was
outside the timer. The benchmark asserts the expected borrowed/copy counters so
the two modes cannot silently collapse to the same policy.

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -p=1 -run '^$' \
  -bench '^BenchmarkArrowLoadEndToEndMaterializeAB$' -benchmem \
  -benchtime=3x -count=3 -timeout=2400s ./pkg/tests/arrowload
```

| Mode | ns/op, three runs | rows/s, three runs | B/op | allocs/op |
| --- | --- | --- | --- | --- |
| borrow | 99,915,126 / 110,736,000 / 77,651,930 | 1,000,849 / 903,049 / 1,287,798 | 18,472,338 / 21,284,285 / 16,848,141 | 149,529 / 220,473 / 139,554 |
| materialize | 86,487,403 / 99,493,569 / 94,644,931 | 1,156,238 / 1,005,090 / 1,056,581 | 16,791,154 / 16,896,400 / 16,789,093 | 139,463 / 139,928 / 139,789 |

On the final rebased candidate, the median forced-materialize run was 5.3%
faster and delivered 5.6% more rows/s than the median borrow run. Both modes
showed substantial run-to-run noise, including one borrow sample with elevated
allocation counts, so this result proves the two policies execute and remain
within the local reference gate; it does not prove a repeatable performance win
for either policy. Materialize remains a correctness diagnostic and emergency
fallback because it deliberately gives up eligible zero-copy ownership.

## Remaining deployment evidence

The local A/B does not replace provider-specific object-store measurements,
ordinary Parquet/INSERT controls on customer data, cache/pin pressure testing,
or production CPU/RSS profiles. Deployment owners must run those controls on
the exact release artifact before enabling S3 or distributed execution.
