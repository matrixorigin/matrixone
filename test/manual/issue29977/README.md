# COALESCE server-prepared result metadata

Run with Connector/J 8.4.0 against an isolated MatrixOne instance. The probe
requires no tables or DDL. It asserts that the driver uses a real
`ServerPreparedStatement`, disables statement caching and client fallback,
and bounds connection/query waits.

Set `DB_URL`, `DB_USER`, `DB_PASSWORD` and `CONNECTOR_JAR`. The defaults are
`jdbc:mysql://127.0.0.1:29977/`, `root` and `111`.

```sh
probe_classes=$(mktemp -d "$TMPDIR/coalesce-jdbc.XXXXXX")
trap 'rm -rf "$probe_classes"' EXIT
javac -cp "$CONNECTOR_JAR" -d "$probe_classes" test/manual/issue29977/PrepareCoalesceMetadataProbe.java
java -cp "$probe_classes:$CONNECTOR_JAR" PrepareCoalesceMetadataProbe
```

Fresh COALESCE handles in both operand orders must expose DECIMAL metadata
before binding and before execution. Reused handles exercise DECIMAL → NULL →
numeric string → DECIMAL, and nonnumeric string → DECIMAL. Values and execution
metadata are checked, including the existing numeric-prefix conversion of a
nonnumeric JDBC string to zero. An explicit DECIMAL CAST is the control; real
string peers and all-unknown markers retain string metadata. Precision/scale
parity with MySQL is outside #29977's scope.

Statements and result sets close immediately on success or assertion failure.
The probe runs twice on the same connection using fresh handles and checks
connection health. Expected: two `PASS run=...` lines, exit status 0. Before the
fix, it fails on the first pre-execution metadata assertion (VARCHAR).
