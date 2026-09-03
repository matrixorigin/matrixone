# #23684 Arrow bridge A/B benchmark

This benchmark compares identical Arrow records with ordinary borrow policy and
`ForceMaterialize=true`. It is a bridge-level attribution test, not a cloud or
end-to-end LOAD release benchmark.

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

## Remaining release evidence

Representative end-to-end datasets must still measure complete LOAD latency,
CPU, allocations, object-store requests/bytes, cache hit and pin behavior,
pipeline copies, memory high-water, cancellation cleanup, and ordinary
Parquet/INSERT controls on the exact candidate build. Deployment owners must
set acceptance thresholds before enabling the feature by default.
