# Invalid temporal storage and compatibility

- Issue: [matrixorigin/matrixone#28921](https://github.com/matrixorigin/matrixone/issues/28921)
- PR: [matrixorigin/matrixone#29076](https://github.com/matrixorigin/matrixone/pull/29076)
- Status: proposed; approval is required before enabling the feature in a mixed-version deployment

## Problem

`ALLOW_INVALID_DATES` preserves calendar fields such as `2024-02-30`. The
legacy DATE/DATETIME scalar is a day/microsecond offset, so its signed integer
ordering cannot represent the SQL calendar order of both valid and invalid
fields. Persisting the tagged scalar without a discriminator also lets an old
reader silently interpret it as a different date.

The required invariants are:

1. Tuple/index bytes are ordered by `(year, month, day, time)` and round-trip
   the original fields.
2. Legacy object-column bytes remain readable without reinterpretation.
3. A reader that cannot preserve tagged fields fails closed instead of silently
   changing their meaning.
4. The prepared binary protocol emits stored fields directly and does not
   reparse them under a stricter session policy.

## Chosen format

Tuple DATE and DATETIME values use new type codes `0x55` and `0x56` followed by
an unsigned calendar/time order key. The legacy codes remain decodable for old
keys. The unsigned key is length/order compatible with the existing unsigned
tuple encoding, and focused tests cover valid-invalid-valid boundaries and
round trips.

Object ColumnData V1/V2 are unchanged. A column containing a tagged invalid
DATE or DATETIME is written as ColumnData V3. V3 has the V2 vector payload but
is an explicit format fence. New readers accept V1, V2, and V3; a V1/V2-only
binary does not know V3 and must not be allowed to read an object containing
tagged values. Ordinary columns, and temporal columns containing only legacy
valid values, continue to use V2.

The prepared binary result path takes the stored `Datetime` scalar and writes
its year/month/day/hour/minute/second/microsecond fields directly. It does not
format and feed the value back through strict `ParseDatetime`.

## Rollout and rollback

1. Deploy binaries with V3 readers and the new tuple decoder before enabling
   writes of tagged invalid temporal values. Upgrade all CNs, TNs, compaction
   workers, object readers, and protocol-serving nodes before enabling the
   session behavior.
2. The feature gate is `ALLOW_INVALID_DATES` persistence. During rollout it
   must remain disabled until the reader-capability inventory is complete. A
   node that cannot read ColumnData V3 is not an eligible writer or reader for
   tables that can contain tagged values.
3. Rollback first disables new tagged-value writes and drains/isolates old
   binaries. Downgrading while V3 objects or new ordered tuple/index keys are
   reachable is rejected by deployment policy; it is not a supported
   best-effort operation.
4. To return to a pre-change binary, operators must either keep the new reader
   in place or create a verified rewrite/backup in the legacy format after all
   tagged values have been normalized or removed and affected indexes have
   been rebuilt. The rewrite must be validated before the old binary is
   admitted. There is no in-place reinterpretation fallback.

This makes mixed-version behavior enforceable at the storage boundary: V3 is
registered and validated by new readers, while old readers fail on the unknown
column version. The deployment gate prevents old readers from being exposed to
new tuple/index key formats. A compatibility test must verify V1/V2 reads,
V3 round trips, and rejection by a V1/V2 compatibility reader before release.

## Alternatives considered

- Normalize invalid dates on write: loses the original calendar fields and
  breaks `ALLOW_INVALID_DATES` semantics.
- Keep the negative raw scalar and only change comparators: fixes SQL
  comparison but not byte-range/index ordering or rollback compatibility.
- Add a sidecar field to every temporal value: widens all existing vectors and
  complicates every codec; a versioned boundary is smaller and fail-closed.

## Required approval and evidence

This RFC is the exact design revision to review for PR #29076. The approval
decision and rollout/rollback sign-off must be recorded on the PR before the
feature is enabled. Validation evidence is mapped to tuple order/round-trip
UTs, ColumnData V3 round-trip and legacy-version tests, prepared binary result
UTs, remote CGo package tests, and the remote mixed-version/upgrade check.
