# Two-CN cursor-drain probe

Run either probe against an isolated LOG/TN/CN1/CN2/Proxy cluster built from
the PR head. Both use server-side prepared statements and fetch size 13, and
verify all 5,000 rows are delivered in order while CN2 drains. After the final
FETCH they reuse the same connection. The Java probe exercises Connector/J;
the raw-wire Go probe explicitly runs with both legacy and deprecated EOF.

For the local multi-CN Docker configuration, set `cn1.toml` and `cn2.toml`
`[cn].sql-address` to each container's **current** IP and SQL port before
starting/restarting the CNs. The Proxy records backend addresses as container
IPs; advertising `mo-cn1:16001` and `mo-cn2:16002` instead makes the migration
query-service lookup fail before the cursor guard is exercised. Obtain the IPs
with `docker inspect mo-cn1 mo-cn2` and keep this adjustment in generated,
ignored local configuration only.

Compile and run with a Connector/J 8.x JAR:

```sh
javac -cp mysql-connector-java-8.0.27.jar Issue29182CursorDrainProbe.java
java -cp .:mysql-connector-java-8.0.27.jar Issue29182CursorDrainProbe

go run ./raw_cursor_probe.go
```

The Java probe's optional arguments are Proxy JDBC URL, CN1 JDBC URL, CN1 UUID,
and CN2 UUID. The Go probe accepts the same endpoints as `-proxy`, `-cn1`,
`-cn1-id`, and `-cn2-id` flags. Defaults match
`etc/docker-multi-cn-local-disk`. Check the Proxy log for
`OkExpectedNotSafeToStartTransfer` on the test connection while its cursor is
open, followed by one `transfer to a new CN server` from CN2 to CN1 after
cursor exhaustion. The program checks ordered delivery and same-connection
reuse, restores both CNs to Working, and drops its uniquely named test table.

The focused Go regression `TestFinalFetchForwardingBeforeBackendReplacement`
additionally exercises the pending-transfer pipe with legacy and deprecated
EOF framing, including a 16 MiB row whose five-byte continuation resembles
EOF. The raw-wire probe exercises both capability modes in the live topology;
Connector/J covers the production client path.
