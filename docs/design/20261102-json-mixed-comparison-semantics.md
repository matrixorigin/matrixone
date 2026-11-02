# JSON mixed-kind comparison and round-trip semantics

- Status: Specification of the shipped contracts in PR [#31980](https://github.com/matrixorigin/matrixone/pull/31980)
- Owning issue: [#31121](https://github.com/matrixorigin/matrixone/issues/31121)
- Prior contracts this document builds on:
  [json_sql_physical_order.md](json_sql_physical_order.md)
  (SQL ordering and equality, PR #28086) and
  [prepared_json_comparison.md](prepared_json_comparison.md)
  (prepared parameters, PR #27540)
- Compatibility oracle: [MySQL 8.0 JSON comparison
  ordering](https://dev.mysql.com/doc/refman/8.0/en/json.html#json-comparison)
  (type precedence and exact numeric comparison), verified against MySQL
  8.0.44/8.0.45 for each pinned row cited below

## The rule in two sentences

Every comparison with a JSON operand evaluates inside the JSON's own ranked
domain and never re-casts or re-parses the JSON side: two values first order by
JSON type - JSON null < number < string < object < array < false < true < date
< time < datetime < bit < blob - and only equal JSON types compare by their
value (numbers compared exactly, strings by decoded bytes). The SQL operand
joins that domain through a fixed pair rule (character text becomes a JSON
string scalar; SQL TRUE/FALSE becomes the JSON boolean category; a numeric
peer compares through the exact numeric domain; a binary byte string is
rejected), SQL NULL keeps SQL three-valued propagation while JSON literal
null stays a comparable value ranked lowest, and a prepared parameter answers
exactly like the equivalent typed literal in every operator, operand order,
and EXECUTE channel.

The pair rule is the admission table below. MySQL comparison documentation
owns the ordering, so this document pins only the MatrixOne pair behavior,
the NULL surfaces, and the channel-parity obligation.

## Admission table for `JSON <op> <SQL operand>`

`<op>` covers `=`, `<>`, `!=`, `<=>`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`,
and the legs of `BETWEEN`/`NOT BETWEEN`, in both operand orders.

| SQL operand pair | Rule |
| --- | --- |
| CHAR, VARCHAR, TEXT | The character value becomes a JSON string scalar (its raw text, never parsed as a JSON document, so `'active'` matches the JSON string `"active"` and `'{"a":1}'` compares unequal to the JSON object `{"a":1}`); both sides compare through the ranked order. Reserved-word spellings and hex literals take the same path. |
| BINARY, VARBINARY, BLOB | Rejected in every spelling (literal, typed column, hex literal, direct prepared parameter) with the invalid-argument operator error; the pair has no comparison domain, mirroring the pre-existing ordering rejection. |
| BOOL | SQL TRUE/FALSE compare as the JSON boolean category, the highest scalar category: a JSON boolean compares by its value, a JSON number keeps MatrixOne's established nonzero-is-true fold, and JSON strings, objects, arrays and JSON literal null rank (unequal) below the peer instead of parsing as boolean text or erroring through a cast. Receipts recorded at
`compareJSONScalarWithBoolean` in `pkg/sql/plan/function/func_compare.go`
(captured verbatim from a MySQL 8.0.44 instance as
`mysql-r21-bool-table.txt` and `mysql-r21-bool-category-cells.txt`). |
| INT8..INT64, UINT8..UINT64, DECIMAL64/128/256 | The JSON number side compares through the exact numeric domain of PR #31719; a Float/DOUBLE-typed peer coerces the pair to DOUBLE exactly the way an ordinary numeric comparison does. Non-numeric JSON documents rank by category (a string never parses as a number). An explicit `CAST(JSON AS <numeric>)` keeps the public cast contract including range errors and the JSON-null fold. |
| other typed peers (temporal, ENUM/SET/BIT, ...) | Outside this contract: no pair rule is defined and none was changed. Such pairs keep the comparison resolution they already had (the pre-existing comparison admission decides bindings exactly as before this change set), and the JSON scalar domains above are gated to the three predicates `isJSONComparisonStringType`, `isJSONComparisonNumericType` and `IsJSONBooleanComparison` (plus the binary rejection). |

`IN`/`NOT IN` inherit their element equality semantics from this table after
binder lowering.

## NULL surfaces stay distinct

- SQL NULL on either side propagates SQL unknown for ordinary operators and
  follows the null-safe rule for `<=>`; it never compares equal to a JSON
  value.
- JSON literal null is a value in the JSON domain, ranked below every other
  category: `CAST('null' AS JSON) = TRUE` is false, `< TRUE` is true, and it
  no longer folds into SQL unknown.
- A missing JSON path (for example an unmatched `JSON_EXTRACT`) already
  produces SQL NULL and therefore keeps SQL unknown semantics; it is never
  exchanged for JSON literal null.

## BETWEEN and NOT BETWEEN

A mixed JSON/BOOL or JSON/numeric triple binds one uncast overload and
evaluates `lo <= x AND x <= hi` with each leg in its own domain from the table
above. The legs combine under SQL AND tri-state: a definitively false leg
answers FALSE even when the other leg is unknown, and only undecided rows
stay NULL, so `x NOT BETWEEN lo AND hi` agrees with both `NOT (x BETWEEN lo
AND hi)` and the lowered `x < lo OR x > hi` on every row. SQL NULL in the
subject or either bound is unknown for the leg(s) that consume it; JSON
literal null ranks below every bound. These comparisons stay typed residual
predicates: they are not hash keys, shuffle joins, runtime-filter operands,
or zone-map/index range candidates.

## Prepared parameters and channel parity

PARAMETER transport is unaffected by runtime values: MatrixOne caches one
plan and binds parameters as bytes with a deferred SQL type, per
[prepared_json_comparison.md](prepared_json_comparison.md). Only a
direct dynamic parameter beside a JSON operand is wrapped by the hidden
`__mo_json_ordering_param`/`__mo_json_comparison_param` adapter, which
carries the parameter's runtime SQL category and exact type as metadata; the
EXECUTE channel applies the pair rule for the category the typed literal
would have used:

- SQL NULL parameter - SQL unknown for ordinary operators, `<=>` null-safe;
- BOOL parameter or Boolean kind - the BOOL row of the table;
- character parameter - JSON string scalar;
- numeric parameter - the exact numeric domain, DOUBLE coercion for
  float-typed peers;
- BINARY/VARBINARY/BLOB parameter - the same rejection as its typed peers.

The invariant is parity: the equivalent typed SQL literal is the oracle for
every operator listed above, in both operand orders, direct or lowered forms,
and both EXECUTE bindings (typed COM_STMT_EXECUTE parameters and untyped
text-user-variable/driver parameters). Ordinary SQL literals, typed columns
and unrelated parameters never enter this adapter path.

Other comparison types do not need this channel. A typed column comparison
fixes its domain at bind time, so the general prepared path types the
parameter through the ordinary execute-time rebind; the pair rules for
temporal, numeric-text and integer filter comparisons are owned by their own
prior contracts and suites (see `visit_plan_rule.go` rebind specializations,
`normalizeNonConstantTemporalComparisonArgs`, and the `test/distributed/cases/prepare/`
battery). Only JSON comparisons rank the SQL operand's runtime kind against a
runtime-dependent JSON document, which is exactly why the metadata adapter -
and its channel-parity obligation - exists.

## JSON text round-trip

- An integral-valued JSON DOUBLE keeps a fractional or exponent marker in
  every text it serializes into (display, JSON_PRETTY, casts, materialized
  text), so `1.0`, `3.0e2` and `-0.0` re-parse as DOUBLE rather than as
  INTEGER, at every nesting depth.
- A `COALESCE` result with character type keeps the selected JSON branch as
  complete document text (quotes preserved) so INSERT ... SELECT round trips
  and do not silently unwrap JSON strings.

## DUMP/LOAD predecessor verification

Predecessor-era persisted expression trees (defaults, generated and CHECK
expressions) that pre-date this contract are verified through the era's own
binding and evaluation semantics via the guarded legacy-context in
`pkg/sql/plan/function/legacy_json_scalar_semantics.go`. That era context is
reachable only by the dump verifier; ordinary SQL and new DDL always use the
table above.

## Implementation and test map

- Ordering/ranking: `pkg/container/bytejson` `CompareByteJsonTrusted`.
- Pair kernels: `pkg/sql/plan/function/json_string_comparison.go`,
  `json_numeric_comparison.go`, `func_compare.go`,
  `operator_between.go` (BETWEEN ternary), `func_binary.go` (COALESCE),
  `pkg/container/bytejson/bytejson.go` (text serialization).
- Binder/admission: `pkg/sql/plan/base_binder.go` (adapter insertion),
  `pkg/sql/plan/function/list_operator.go` (overload admission),
  `pkg/sql/plan/join_order.go`, `applyIndices.go`, `stats.go` (residual
  predicate gates).
- Proofs: `pkg/sql/plan/json_string_comparison_test.go` and the
  `json_string_null_comparison`, `json_binary_string_comparison`,
  `json_bool_ordering_comparison`, `json_numeric_comparison`,
  `json_*_between_comparison`, `json_prepared_*` suites; BVT
  `test/distributed/cases/function/func_json_scalar_semantics_31121.test`
  (every user-visible row), with the killed rows of prior eras updated in
  `func_json_extract.result`.
