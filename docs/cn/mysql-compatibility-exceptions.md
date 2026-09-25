# MatrixOne MySQL Compatibility Exceptions

This document records cases where MatrixOne behavior differs from MySQL, including cases where MO provides more reasonable behavior than MySQL.

---

## 🧭 Recursive CTE Behavior Differences

### ID-CTE-001: `cte_max_recursion_depth` excludes empty convergence rounds

**Category:** Recursive CTE / system variable

**MySQL Behavior:** In the official MySQL 8.0.45 image, a limit of `2` rejects the recursive CTE below with error 3636 after three recursive iterations: the third attempt is empty and only detects convergence. A limit of `0` also rejects an anchor-only CTE whose recursive member returns no rows, and rejects a duplicate-only `UNION DISTINCT` round.

**MO Behavior:** The limit counts recursive rounds that add rows to the next frontier after duplicate elimination. The anchor is not counted, and empty or duplicate-only convergence rounds do not consume depth. Thus, depth `2` returns `1, 2, 3` for the query below, and depth `0` permits an anchor followed by an empty or duplicate-only recursive round.

**Incompatibility:** The same configured limit can succeed in MatrixOne and fail in MySQL 8.0.45 when the final recursive attempt produces no new rows.

**Reason:** This is the explicit contract established by [issue #29138](https://github.com/matrixorigin/matrixone/issues/29138): `cte_max_recursion_depth` limits productive recursive result levels, not the empty iteration required to detect convergence. Regression coverage is in `test/distributed/cases/recursive_cte/recursive_cte_depth.sql` and the `MergeCTE` unit tests.

**Example:**
```sql
SET SESSION cte_max_recursion_depth = 2;
WITH RECURSIVE r(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM r WHERE n < 3
)
SELECT n FROM r ORDER BY n;
-- MySQL 8.0.45: ERROR 3636 after 3 iterations
-- MatrixOne: 1, 2, 3
```

**Reference:** [MySQL 8.0 recursive CTE and recursion-limit documentation](https://dev.mysql.com/doc/refman/8.0/en/with.html); behavior verified against MySQL 8.0.45.

---

## ⚙️ Function / Expression Behavior Differences

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
| [#29138](https://github.com/matrixorigin/matrixone/issues/29138) | `cte_max_recursion_depth` and empty convergence rounds | MatrixOne intentionally counts only productive recursive levels |
| [#23464](https://github.com/matrixorigin/matrixone/issues/23464) | TIMEDIFF() and SUBTIME() results incompatible with MySQL | TIMEDIFF: MO behavior is more reasonable |

---

## 📝 Notes

This document clarifies the following situations:
1. Cases where MO provides more user-friendly behavior than MySQL's strict interpretation
2. Edge cases requiring explicit documentation
