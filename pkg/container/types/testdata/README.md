# Type-switch families

`TestTypeSwitchFamilies` (`../typeswitch_test.go`) checks every `switch` over `types.T`
in `pkg/` (`_test.go` files excluded). A switch that names any member of a family must name
every member of the family, or every member of one subfamily and no other family member:

| Family | Members | Subfamily |
|--------|---------|-----------|
| float | every `T_float*` and `T_bf*` constant (`T_float32`, `T_float64`, `T_bf16`, `T_float16`, `T_float8`, `T_float4`, ...) | low-precision: the members other than `T_float32` and `T_float64` |
| vector | every `T_array_*` constant (`T_array_float32`, `T_array_float64`, `T_array_bf16`, ..., `T_array_float8`, `T_array_float4`) | block-scaled: `T_array_float8`, `T_array_float4` |

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

## The baseline

`typeswitch_baseline.txt` lists the untagged partial switches that predate the test, one
line per file, function and family with a count:

```
pkg/sql/plan/function/func_cast.go	castToDecimal256	float	1
```

The test fails when a key has more untagged partial switches than its count. The list may
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
