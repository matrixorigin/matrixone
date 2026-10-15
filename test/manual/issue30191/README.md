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
