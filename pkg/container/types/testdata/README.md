# Type-switch families

`TestTypeSwitchFamilies` (`../typeswitch_test.go`) checks, in `pkg/` (`_test.go` files
excluded), every `switch` over `types.T`, every `||` chain of `==` comparisons (and `&&`
chain of `!=` comparisons) that names two or more members of a family, and every
`[]types.T{...}` / `map[types.T]...{...}` literal that names two or more members. In a chain,
`IsFloat()`, `IsLowPrecisionFloat()`, `IsArray()` and `IsArrayRelate()` name the members
they cover. Such a switch, chain or list must name every member of the family, or every
member of one subfamily and no other family member:

| Family | Members | Subfamily |
|--------|---------|-----------|
| float | every `T_float*` and `T_bf*` constant (`T_float32`, `T_float64`, `T_bf16`, `T_float16`, `T_float8`, `T_float4`, ...) | low-precision: the members other than `T_float32` and `T_float64` |
| vector | every `T_array_*` constant (`T_array_float32`, `T_array_float64`, `T_array_bf16`, ..., `T_array_float8`, `T_array_float4`) | block-scaled: `T_array_float8`, `T_array_float4` |
| decimal | every type whose traits row has the decimal flag (`T.IsDecimal`): `T_decimal64`, `T_decimal128`, `T_decimal256` | — |
| temporal | every type whose traits row has the date-related flag (`T.IsDateRelate`): `T_date`, `T_time`, `T_datetime`, `T_timestamp`, `T_year` | — |

The decimal and temporal families come from the flags in `../traits_table.go`, so a type
joins them by its row there. In a chain, `IsDecimal()` and `IsDateRelate()` name their
family's members, like `IsFloat()` does.

Members are read from the `T` constants in `types.go`, so a type added there is checked at
every switch that names a member of its family, and a low-precision type added later at
every low-precision-only switch. In a switch that lists the members, give the last one its
own case and keep `default` for an error, so a later member is not read as another type.

## When the test fails

The failure names the switch and the members it misses, for example

```
pkg/sql/foo/bar.go:120 handleX: vector switch misses T_array_float8, T_array_float4
```

Either handle the missing types in the switch, or, when leaving them out is intended,
say why on the switch line or in the comment block right above it:

```go
// typeswitch:partial vecf8/vecf4 are block-scaled cells, handled by castToBlockScaled
switch typ.Oid {
```

A tag without a reason fails the test. Do not add the switch to the baseline.

A tag excuses every family by default. To excuse only some, name them in parentheses, so
the switch is still checked for the others:

```go
// typeswitch:partial(decimal) decimal256 targets are cast by castToDecimal256 before this dispatch
switch toType.Oid {
```

## The baseline

`typeswitch_baseline.txt` lists the untagged partial switches that predate the test, one
line per file, function and family, with a count and the members those switches miss:

```
pkg/sql/plan/function/func_cast.go	castToDecimal256	float	1	T_bf16,T_float16,T_float4,T_float8
pkg/sql/plan/utils.go	checkNoNeedCast	float chain	4	T_bf16,T_float16
```

The family column carries `chain` or `list` for those constructs; their entries were
added when the check was extended to them, after each one was reviewed.

The test fails when a key has more untagged partial switches than its count, or when one of
them misses a member its line does not list. The second rule is what catches a new type: a
type added to a family is reported in every switch that names part of the family, listed
or not, until each switch handles it or is tagged. The list may
only shrink: after fixing or tagging listed switches, regenerate it from the repository
root

```
GOWORK=off go test ./pkg/container/types -run TestTypeSwitchFamilies -count=1 -args -update-typeswitch
```

and check that the diff only removes lines (or lowers counts):

```
git diff pkg/container/types/testdata/typeswitch_baseline.txt
```

The flag writes whatever the scan finds, including new partial switches; a baseline diff
that adds a line or raises a count hides a switch that misses a type, and should be
replaced by a fix or a `typeswitch:partial` tag. Renaming a function that holds a listed
switch moves its key; tag the switch rather than adding the new key.
