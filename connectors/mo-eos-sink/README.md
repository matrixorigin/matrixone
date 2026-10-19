# MatrixOne atomic event sink for Kafka Connect

This module implements the narrow `increment` profile in
[`docs/cn/kafka_sink_eos_profile.md`](../../docs/cn/kafka_sink_eos_profile.md).
It is a Kafka Connect 3.9 `SinkConnector` and `SinkTask`; it uses the MySQL
protocol to commit the event ledger, scoped counter effect, ledger budget, and
partition checkpoint in one MatrixOne transaction. The database checkpoint is
the recovery authority after a worker restart or ambiguous COMMIT response.

Build and test with Java 17+ and Maven:

```sh
mvn -B verify
```

Install `target/mo-eos-sink-0.1.0.jar` in its own Connect `plugin.path`
directory. Initialize the target database with [`sql/schema.sql`](sql/schema.sql)
and adjust the example scope/topic/partition rows before starting the connector.
The configuration and NiFi ingress route are documented in the linked guide.

The opt-in `IntegrationProbe` in `src/test/java` runs against a real Kafka
broker and MatrixOne instance. It requires an empty profile database already
initialized by `schema.sql` and a fresh broker with no `mo_eos_events` or
`mo_eos_batch` topics. It drives the real connector task callbacks and verifies
DB state after duplicate replay, restart, batch rollback, epoch fencing,
retention overrun before assignment and before `put`, and an aborted Kafka
transaction that leaves a delivered-offset hole.
The probe is one component of the fault matrix; it does not replace worker
process, network proxy, or NiFi qualification runs. `CommitAckProbe` pairs with
[`test/drop_commit_ok_proxy.py`](test/drop_commit_ok_proxy.py) to forward a real
MO `COMMIT` and drop only its response, then checks ledger recovery, replay,
and a zero-delta event against an isolated profile database.

```sh
MO_TEST_PASSWORD=... java -cp 'target/classes:target/test-classes:DEPENDENCY_JARS/*' \
  io.matrixone.connect.eos.IntegrationProbe \
  127.0.0.1:9092 'jdbc:mysql://127.0.0.1:6001/eos?useSSL=false'
```

For an isolated local broker, the repository's
[`etc/launch-kafka-local/compose.yaml`](../../etc/launch-kafka-local/compose.yaml)
can be used with `KAFKA_PORT` set to a free local port. The example profile
does not create or drop its tables automatically; a failed task therefore
never erases ledger or checkpoint evidence.
