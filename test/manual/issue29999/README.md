Run `CalendarMetadataProbe` with Connector/J 8.4.0 against an isolated MatrixOne
instance. It checks ordinary queries and both client and real server prepared
statements, including metadata before binding, execution, NULL reuse and
`getObject()` returning `Long`. Statement caching and server fallback are disabled.

Set `DB_URL`, `DB_USER`, `DB_PASSWORD` and `CONNECTOR_JAR`; the defaults are
`jdbc:mysql://127.0.0.1:29999/`, `dump` and `111`.

```sh
probe_classes=$(mktemp -d "$TMPDIR/calendar-jdbc.XXXXXX")
trap 'rm -rf "$probe_classes"' EXIT
javac -cp "$CONNECTOR_JAR" -d "$probe_classes" test/manual/issue29999/CalendarMetadataProbe.java
java -cp "$probe_classes:$CONNECTOR_JAR" CalendarMetadataProbe
```

Expected: `PASS serverPrepared=false` and `PASS serverPrepared=true`, exit 0.
CTAS signed INT storage and wider inserts are covered by
`TestCalendarResultWireAndCTAS` and `temporal_typed_date_ctas_28851.test`.
