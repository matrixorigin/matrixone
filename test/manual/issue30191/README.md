# Window BOOLEAN result metadata probe

This probe covers the three window expressions from the review of PR #30191:

- `MAX(n IS NULL) OVER ()`
- `MIN(n IS NULL) OVER ()`
- `FIRST_VALUE(n IS NULL) OVER (ORDER BY id)`

It also selects a stored `BOOLEAN` beside a window result, then repeats a
computed-window query through both Connector/J client-side and server-side
prepared statements. With `tinyInt1isBit=true`, computed BOOLEAN results must
be advertised as numeric `TINYINT` metadata and return `Number` values, while
the forwarded stored BOOLEAN remains `BIT`/`java.lang.Boolean`.

Run it against an isolated MatrixOne proxy (the default is port 6001):

```sh
probe_classes=$(mktemp -d "$TMPDIR/window-boolean-jdbc.XXXXXX")
trap 'rm -rf "$probe_classes"' EXIT
javac -cp "$CONNECTOR_JAR" -d "$probe_classes" \
  test/manual/issue30191/WindowBooleanMetadataProbe.java
java -cp "$probe_classes:$CONNECTOR_JAR" WindowBooleanMetadataProbe
```

Set `DB_URL`, `DB_USER`, `DB_PASSWORD`, and `CONNECTOR_JAR` to override the
defaults. The probe uses bounded connection/query waits and drops its temporary
table before exiting.

## JOIN projection routing

`JoinBooleanMetadataProbe.java` covers DISTINCT/GROUP BY on both JOIN inputs,
stored BOOLEAN on either side, reordered outputs, outer DISTINCT, and
CROSS/INNER/LEFT/RIGHT JOIN. It checks prepare-time and executed metadata,
actual Java value classes, NULL extension, and exact row multisets. All 64 SQL
shapes run in Statement, client-prepared, and verified server-prepared mode
(192 cases); server-prepared fallback is disabled.

The same probe also checks 21 compositions: a grouped
aggregate with a nonzero group-column offset, repeated DISTINCT outputs,
MINUS and INTERSECT with a computed right input, and repeated recursive CTE
outputs. LAG/LEAD computed, stored and NULL defaults, plus grouped SAMPLE and stored
SAMPLE controls are included. VALUES producers are exercised directly and through
derived tables, DISTINCT, reordered/repeated outputs, UNION and a value-preserving
window. SAMPLE covers omitted grouping prefixes in both computed/stored directions
and multiple omitted keys. These add 62 positive cases (254 total), checking exact
values as well as prepare/execute metadata and Java classes. Bare VALUES runs in
Statement and client-prepared mode; the existing PREPARE grammar rejects that form,
which is asserted separately as a 1064/42000 control. Derived VALUES and every other
composition must pass actual server preparation, with no fallback.

Six additional forwarded-SAMPLE shapes use a UNION ALL input with stored BOOL
and predicate siblings, including reordered/repeated outputs and multiple sampled
columns. They retain the sampled `id+1` BIGINT output and check that the prefixed
forwarded slots preserve BIT/Boolean versus TINYINT/Number, NULLs and both copies
of every input row. All six run in the three execution modes (18 more positive
cases, 272 total), with prepare-time metadata checked in both prepared modes.

Compile and run it with Connector/J 8.4.0 and `tinyInt1isBit=true`:

```sh
probe_classes=$(mktemp -d "${TMPDIR:-/tmp}/join-boolean-jdbc.XXXXXX")
javac -cp "$CONNECTOR_JAR" -d "$probe_classes" \
  test/manual/issue30191/JoinBooleanMetadataProbe.java
java -cp "$probe_classes:$CONNECTOR_JAR" JoinBooleanMetadataProbe
```

The same `DB_URL`, `DB_USER`, and `DB_PASSWORD` overrides apply. This probe
creates a uniquely named database and drops only that database on exit.

## MARK JOIN predicate results

`MarkBooleanMetadataProbe.java` covers `IN (SELECT ...)` and `= ANY (SELECT ...)`,
including DISTINCT, stored BOOLEAN siblings in either order, derived/CTE
projections, multiple markers, value-preserving windows, and an enclosing JOIN.
Controls include literal IN, NOT IN, EXISTS, IS NULL, and native BOOLEAN. It
checks prepare/execute metadata, actual Java value classes and exact row
multisets, including NULL results. The RHS is tested without NULL, with NULL,
and empty: 16 SQL shapes × 3 RHS states × 3 execution modes = 144 cases.

Use the same Connector/J 8.4.0 and connection overrides as above:

```sh
probe_classes=$(mktemp -d "${TMPDIR:-/tmp}/mark-boolean-jdbc.XXXXXX")
javac -cp "$CONNECTOR_JAR" -d "$probe_classes" \
  test/manual/issue30191/MarkBooleanMetadataProbe.java
java -cp "$probe_classes:$CONNECTOR_JAR" MarkBooleanMetadataProbe
```

Server-prepared fallback is disabled and the implementation class is checked.
The probe creates a uniquely named database and drops only that database.
