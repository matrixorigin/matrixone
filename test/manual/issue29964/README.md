# First NULL JDBC server-prepared execution

Run `NullFirstPreparedProbe.java` against an isolated MatrixOne instance with
Connector/J 8.4.0. It requires permission to create/drop its dedicated database
`qa_null_first_29964`; that name must be unused before the run. The probe asserts
that Connector/J created an actual `ServerPreparedStatement`, forces server
preparation, disables statement caching/client fallback, and bounds network and
query waits. Any unexpected SQL error or result fails the process.

Set `DB_URL`, `DB_USER`, and `DB_PASSWORD` in the environment. For example,
`DB_URL=jdbc:mysql://127.0.0.1:6001/?useSSL=false`. With `CONNECTOR_JAR` pointing
to the connector JAR:

```sh
probe_classes=$(mktemp -d "$TMPDIR/null-first-jdbc.XXXXXX")
trap 'rm -rf "$probe_classes"' EXIT
javac -cp "$CONNECTOR_JAR" -d "$probe_classes" test/manual/issue29964/NullFirstPreparedProbe.java
java -cp "$probe_classes:$CONNECTOR_JAR" NullFirstPreparedProbe
```

The probe checks fresh `SELECT ?` for `setNull(Types.NULL/DECIMAL/INTEGER)`,
NULL → decimal → NULL on one handle, string/integer type changes,
`clearParameters` rejection and NULL rebinding, and explicit DECIMAL CAST.
A nullable INT/DECIMAL insert with an auto-increment primary key and secondary
index checks first/reused all-NULL and mixed values, exact predicates,
transaction isolation and rollback from a second connection. It checks database
teardown and repeats once on the same instance.

The expected output is two `PASS run=...` lines and exit status 0. On the original
buggy source, the first `SELECT ?` throws the missing protocol source type error.
This JDBC probe is necessary because SQL `PREPARE` and Go's MySQL driver do not
produce Connector/J's first all-NULL execute packet without a type vector.
