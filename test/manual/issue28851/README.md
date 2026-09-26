# Temporal compatibility from the 4.2 release

Use an isolated cluster and Connector/J 8.4.0. Compile `TemporalUpgradeProbe.java`
with the connector JAR on the classpath. Set `MO_JDBC_URL`, `MO_USER` and
`MO_PASSWORD` in the environment; do not put credentials in the source.

1. Start an official 4.2 release binary on fresh storage, with the release's
   `DISK` fileservice backend. Run `TemporalUpgradeProbe seed` once. This creates
   `qa_temporal_upgrade_28851`, persisted defaults and a view.
2. Stop the release cleanly. Keep its storage and configuration, and start the
   candidate binary. Run `TemporalUpgradeProbe` without arguments.
3. Restart the candidate and run the probe again. Successful runs remove their
   transient rows/table, so the restart check reuses the original release data.
4. Drop `qa_temporal_upgrade_28851` when finished, or remove the isolated cluster.

The candidate probe also requires both view metadata catalog tables, which are
absent in released 4.2 and required for startup admission after restart.
The probe asserts the old EXTRACT VARCHAR/UINT32 and string ADDTIME/SUBTIME
DATETIME defaults still execute, while a SQL-rebound view exposes the new BIGINT
EXTRACT result through both text and actual server-prepared JDBC. The same
prepared handles exercise INSERT/UPDATE/CAST with text and binary payloads,
strict/non-strict modes, NULL, empty, whitespace and valid rebinding controls.
The same probe checks exact arithmetic bounds, inactive diagnostics, EXTRACT
mode changes and interval parameter rebinding. `TemporalUpgradeProbe contract`
runs those checks alone on an existing candidate. Temporal FSP is read from
the MySQL field decimals: Connector/J returns zero from JDBC `getScale()` for
non-numeric types.

Empty string payloads retain the release's NULL policy. SQL hex literals have
separate numeric provenance (`x''` assigns zero); they are not equivalent to
JDBC `setBytes(new byte[0])`.

The ordinary BVT extends `func_temporal_consistency_28851.test`; registry ABI
tests cover every released result-changing overload and planner protocol tests
exercise 4.2 capability values 9/10, placement, destination checks and persisted
admission. This manual test is a stop/replace/start upgrade, not a live mixed-CN
rolling-upgrade test. Automated Upgrade CI currently skips its upgrade jobs.
