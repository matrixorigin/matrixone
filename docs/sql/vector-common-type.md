# Vector common type

This page states the type that an operator chooses when it combines vector values of
different vector types. The BVT case `test/distributed/cases/vector/vector_common_type.sql`
checks every statement here over every pair of the eight vector types; the unit test
`TestVectorCommonTypeContract` (`pkg/sql/plan/function/vector_type_test.go`) checks the same
matrix at bind time. The rule is implemented by `commonVectorType`
(`pkg/sql/plan/function/vector_type.go`).

The eight vector types are `vecf32`, `vecf64`, `vecbf16`, `vecf16`, `vecint8`, `vecuint8`,
`vecf8` and `vecf4`.

## Operators

| Operator | Arguments the rule applies to | Result |
|---|---|---|
| `CASE WHEN ... THEN ... ELSE ... END` | the `THEN` and `ELSE` values | the common type |
| `IF(c, a, b)` | `a`, `b` | the common type |
| `IFNULL(a, b)` | `a`, `b` | the common type |
| `COALESCE(a, b, ...)` | every argument | the common type |
| `NULLIF(a, b)` | `a`, `b` (compared with `=`) | the type of `a` |
| `GREATEST(a, b, ...)`, `LEAST(a, b, ...)` | every argument | the common type; `vecf32` when the common type is `vecf8` or `vecf4` |
| `=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`, `BETWEEN`, `IN` | both operands | `BOOL`; the operands are compared as the common type |
| `UNION`, `UNION ALL` | the column at one position in every branch | the common type |

The rule is the same with `sql_mode = 'MATRIXONE_NATIVE'`.

Arithmetic (`+`, `-`, `*`, `/`) and the distance functions follow their own rules, stated
in [vector-distance-functions.md](vector-distance-functions.md).

## The common type

| Vector arguments | Common type |
|---|---|
| all of one type `T` | `T` |
| more than one type, one of them `vecf64` | `vecf64` |
| more than one type, none of them `vecf64` | `vecf32` |

The order of the arguments does not change the result. Every vector type converts to
`vecf32` and to `vecf64` without changing a value: `vecbf16`, `vecf16`, `vecint8` and
`vecuint8` elements are exact in `float32`, and `vecf8`/`vecf4` cells dequantize to
`float32` elements.

```sql
create table t (id int, a vecbf16(3), b vecint8(3), c vecf64(3));
insert into t values (1, '[1,2,3]', null, '[4,5,6]');
select coalesce(b, a) from t;          -- [1, 2, 3], vecf32(3)
select coalesce(b, a, c) from t;       -- [1, 2, 3], vecf64(3)
select a = c from t;                   -- 0, compared as vecf64
select a from t union all select b from t;   -- one vecf32(3) column
```

## Strings and NULL

A string argument and an untyped `NULL` take the common type of the vector arguments.

```sql
select ifnull(b, '[0,0,0]') from t;    -- [0, 0, 0], vecint8(3)
select case when id = 1 then a else '[9,9,9]' end from t;   -- [1, 2, 3], vecbf16(3)
```

## Rejections

- Vector arguments of different dimensions are rejected when the statement is bound. The
  message names the argument types, not the dimensions:
  `case when id = 1 then v4 else cast('[1,2,3]' as vecf64(3)) end` over a `vecf32(4)` column
  `v4` fails with `invalid argument operator case, bad value [BOOL VECF32 VECF64]`. The same
  holds for two arguments of one vector type in `CASE`, `IF`, `IFNULL`, `COALESCE`, `GREATEST`,
  `LEAST` and `UNION`.
- A comparison of two arguments of one vector type and different dimensions is not rejected;
  it compares the elements in order and returns `0` for `=`.
- A vector argument together with a number, JSON, `BINARY`, `VARBINARY` or any other
  argument that has no cast to the common type is rejected:
  `invalid argument operator case, bad value [BOOL VECF32 BIGINT]`.

## Casts used by the rule

`CAST(v AS vecf64(N))` of a `vecf8` or `vecf4` value dequantizes the cell; it returns the
same elements as `CAST(v AS vecf32(N))`. A cast between `vecf8`/`vecf4` and `vecbf16`,
`vecf16`, `vecint8` or `vecuint8` is rejected:
`invalid argument operator cast, bad value [VECF8 VECBF16]`.
