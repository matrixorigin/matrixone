# MatrixOne MySQL Compatibility Exceptions

This document records cases where MatrixOne behavior differs from MySQL, including cases where MO provides more reasonable behavior than MySQL.

---

## ⚙️ Function / Expression Behavior Differences

### ID-F002: Seeded `RAND(seed)` is not supported and has no reproducibility contract

**Category:** Function / Compatibility Limitation

**MySQL Behavior:** MySQL accepts `RAND(seed)` and uses the seed to initialize its pseudo-random sequence.

**MO Behavior:** MatrixOne supports the unseeded `RAND()` form. `RAND(seed)` is rejected as an invalid function argument and is not implemented as a compatibility overload.

**Incompatibility:** The MySQL seeded `RAND(seed)` syntax is unsupported in MO.

**Limitation and project decision:** MatrixOne does not promise to match MySQL's pseudo-random-number sequence, nor to preserve its own random-number algorithm across releases. A seed also cannot establish a stable row-to-value mapping when execution plans or parallel schedules differ. Therefore, MO does not provide a deterministic or cross-version reproducibility contract for `RAND`, and will not add `RAND(seed)` merely for MySQL syntax compatibility or replace its generator to match MySQL.

**Impact and guidance:** Existing `RAND()` behavior is unchanged. Workloads that require reproducible random values should generate and persist them outside this function, rather than relying on SQL random-number sequences.

**Reference:** [#28577](https://github.com/matrixorigin/matrixone/issues/28577) — project decision: not planned.

---

### ID-F001: `TIMEDIFF()` with mixed TIME and DATETIME types

**Category:** Function

**MySQL Behavior:** `TIMEDIFF(time_expr, datetime_expr)` returns `NULL` when the two arguments have different types (one is TIME, the other is DATETIME). MySQL strictly requires both arguments to be of the same type.

**MO Behavior:** MO implicitly converts the TIME value to DATETIME using the current date, then calculates the difference.

**Example:**
```sql
SELECT TIMEDIFF('15:30:45', '2000-01-01 15:30:45') AS mixed_format;
-- MySQL: NULL
-- MO: 228072:00:00.000000 (assuming current date is 2026-01-27)
```

**Incompatibility:** MO returns a calculated time difference, while MySQL returns NULL.

**Reason:** MO provides more user-friendly behavior by performing implicit type conversion. When a TIME value is compared with a DATETIME value, MO treats the TIME as the current date with that time. This is arguably more intuitive than MySQL's strict NULL return, as users likely expect a meaningful result rather than NULL.

**Reference:** [MySQL 8.0 Date and Time Functions - TIMEDIFF](https://docs.oracle.com/cd/E17952_01/mysql-8.0-en/date-and-time-functions.html)

---

## 📋 Related Issues

| Issue ID | Description | Status |
|----------|-------------|--------|
| [#23464](https://github.com/matrixorigin/matrixone/issues/23464) | TIMEDIFF() and SUBTIME() results incompatible with MySQL | TIMEDIFF: MO behavior is more reasonable |
| [#28577](https://github.com/matrixorigin/matrixone/issues/28577) | `RAND(seed)` is unavailable for deterministic random sequences | Not planned: seeded random compatibility and reproducibility contract are out of scope |

---

## 📝 Notes

This document clarifies the following situations:
1. Cases where MO provides more user-friendly behavior than MySQL's strict interpretation
2. Edge cases requiring explicit documentation
3. Explicitly unsupported MySQL-compatible syntax or behavior where MO intentionally provides no compatibility contract
