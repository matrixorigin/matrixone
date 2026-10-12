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
