# Two-CN cursor-drain probe

Run `Issue29182CursorDrainProbe.java` against an isolated LOG/TN/CN1/CN2/Proxy
cluster built from the PR head. It uses Connector/J server-side prepared
statements and fetch size 13, and verifies all 5,000 rows are delivered in
order while CN2 drains. After the final FETCH it reuses the same connection.

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
```

The optional arguments are Proxy JDBC URL, CN1 JDBC URL, CN1 UUID, and CN2
UUID. Defaults match `etc/docker-multi-cn-local-disk`. Check the Proxy log for
`OkExpectedNotSafeToStartTransfer` on the test connection while its cursor is
open, followed by one `transfer to a new CN server` from CN2 to CN1 after
cursor exhaustion. The program checks ordered delivery and same-connection
reuse, restores both CNs to Working, and drops its uniquely named test table.

The focused Go regression `TestFinalFetchForwardingBeforeBackendReplacement`
additionally exercises legacy and deprecated EOF framing, including a 16 MiB
row whose five-byte continuation resembles EOF. Connector/J itself covers the
live two-CN migration topology, not both EOF capability modes.
