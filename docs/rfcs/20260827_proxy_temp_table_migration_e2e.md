# Proxy temporary-table migration topology validation

- PR: https://github.com/matrixorigin/matrixone/pull/27623
- Issue: https://github.com/matrixorigin/matrixone/issues/27602
- Exact head: `eed54db29b02a70a777fefd086871d3cda09d548`
- Date: 2026-08-27
- Topology: one Log service, one TN, two CNs, and one Proxy from
  `etc/launch-with-proxy/launch.toml`
- Client: MySQL Connector/J with `useServerPrepStmts=true`

The service binary was built from the exact head, then the isolated topology
was launched with:

```text
mo-service -with-proxy -launch etc/launch-with-proxy/launch.toml
```

The acceptance suite was executed through the Proxy using the repository's
local development credentials and explicit Connector/J server prepares:

```text
MO_PROXY_TEMP_TABLE_E2E_URL=<two-CN-Proxy-Connector/J-URL> \
MO_PROXY_TEMP_TABLE_E2E_CN1=dd1dccb4-4d3c-41f8-b482-5251dc7a41bf \
MO_PROXY_TEMP_TABLE_E2E_CN2=dd1dccb5-4d3c-41f8-b482-5251dc7a41bf \
./mvnw -q -Dtest=ProxyTempTableMigrationE2ETest test
```

Terminal result:

```text
Test set: io.matrixone.jstfu.ProxyTempTableMigrationE2ETest
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 25.40 s
```

The suite exercised the required migration matrix: indexed temporary-table
rows and index metadata after handoff, SQL `PREPARE`, binary
`COM_STMT_PREPARE`/`COM_STMT_EXECUTE`, a stable target beyond Proxy's retry
interval, and commit/rollback admission before migration.
