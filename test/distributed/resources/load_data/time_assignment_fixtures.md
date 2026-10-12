TIME assignment fixtures contain synthetic boundary values only.

- `valid`: negative/positive ±838:59:59.000000 endpoints, -1 microsecond, NULL.
- `range`: a valid 7-microsecond prefix followed by both endpoints exceeded by
  1 microsecond, 839 hours and -839 hours. Strict LOAD must roll back the batch;
  permissive LOAD must retain the prefix and clamp the four overflows.

CSV and JSONLINE spell the same values as text. Parquet files were generated
with PyArrow 24.0.0, uncompressed, with `id: int32` and `v: duration[us]`.
The duration physical INT64 contains exact signed microseconds, not HHMMSS.

`extreme.parquet` contains ids 1/2/3 with duration microseconds INT64_MIN,
INT64_MAX and NULL. Row identity must retain the original signs when values
are clamped after assignment to TIME(0).

`text_overflow.jl` and `text_overflow.parquet` contain ids 1/2/3/4 with
`00:00:01.9876549`, `2562047788:00:00`, `-2562047788:00:00` and NULL.
The Parquet fixture uses parquet-go v0.25.1, uncompressed, with an INT32 `id`
and optional dictionary-encoded STRING `v`. These valid TIME spellings exceed
the internal int64 duration representation and must reach the shared native
assignment policy intact. They are not raw duration or HHMMSS integers.

TIME LOAD reader placement: supplied TIME inputs in CSV, JSONLINE and Parquet
are decoded and assigned on the coordinator CN, including typed Parquet TIME.
This avoids an older CN pre-parsing TIME before the assignment policy sees its
original text during rolling upgrades. Local file, row-group and byte-offset
parallelism still respects `max_dop`; writer distribution uses its existing
path. S3 reads of these inputs therefore use one CN rather than cluster-wide
reader capacity. Arrow, foreign scans and omitted TIME default columns retain
their existing placement.
