# Kafka Connect recovery qualification

`optools/kafka_connect_ci.bash e2e-local` runs one real Kafka 3.9.0 / Kafka
Connect 3.9.0 / Debezium JDBC 3.1.2 sink path against a locally built MatrixOne. It
requires Docker Compose, Go, Python 3, Make, and the MatrixOne native build
dependencies. Images are pinned by digest in `compose.connect.yaml`.

The run checks a baseline insert, kills the owned Connect worker, writes an
insert, update, and keyed tombstone while it is down, then starts the same
worker and checks the exact destination rows plus the Connect REST offset,
Kafka consumer-group committed offset, and Kafka log-end offset. It also stops
the connector, rewinds the fixture consumer offset through Kafka Connect's REST
API, verifies replay leaves the target unchanged, and checks an empty-work
worker restart. The report records the
MatrixOne binary source revision, harness source revision, binary digest,
Connect/plugin versions, exact rows, and offsets under
`test/kafkaconnect/reports/` by default. Set
`MO_KAFKA_CONNECT_REPORT_DIR` to select another report directory.
The driver limits the full run to 10 minutes and each readiness or drain wait
to 90 seconds by default; `--timeout` and `--phase-timeout` can adjust those
limits. It atomically updates `report.json` after each stage so an interrupted
run retains its current stage and completed phase receipts. Artifact collection
or service-cleanup failures change the overall report to `failed` and make the
runner exit nonzero.

This harness qualifies only this Kafka Connect JDBC sink recovery path. It does
not certify Airbyte, NiFi generic CDC, large-volume recovery, or cross-system
exactly-once delivery.
