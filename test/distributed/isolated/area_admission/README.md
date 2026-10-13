# HashJoin Area admission regression

Run only on a disposable, dedicated single-CN local service. This test intentionally
exhausts a query budget. The unfixed server suspends subsequent allocation
admission; it must not be used against a shared instance. The tester also performs
its normal database cleanup, so use an isolated tester configuration.

Set the following public startup setting in the CN configuration before launch:

```toml
[cn.frontend]
processLimitationSize = 8388608
```

Configure the official `mo-tester` to connect to this service, then run comparison
mode (not result generation):

Use `jdbc.paremeter.autoReconnect: "false"` in its `mo.yml` so a rejected new
connection is reported immediately rather than hidden behind JDBC retries.

```sh
./run.sh -p /absolute/matrixone/test/distributed/isolated/area_admission/hashjoin_area.sql -m run
```

The fixture uses the supported `optimizer_hints` session variable to retain a
single-CPU HashJoin with `src` on the probe side. The checked physical plan must
contain `hash join` over `src` and a `hash build` over `rhs`. The filtered build
side is empty. `EXPLAIN PHYPLAN` executes the query, so the exact query's plan is
captured before adding the second, large batch. The first 8,192 probe strings are 64 bytes, allocating an Area;
the next 8,192 strings are 2,048 bytes and force result-batch Area growth beyond
the 8 MiB budget. Both widths exceed the 23-byte inline threshold. Do not replace
the cap with `join_spill_mem`: that is not this fixture's admission limit.

Expected repaired behavior is the ordinary budget-rejection error, followed by
`COUNT(*) = 16384` on both the existing connection and a new session. On the
unfixed implementation, the rejected query additionally reports an allocation
account invariant failure (`owner=1`, `site=103`, `used=524288`, one live
allocation); new-session admission is suspended. This is a red/green defect
oracle, not a success-only large-LIMIT example.

This directory is outside the default BVT cases because its expected error
requires the explicit 8 MiB startup configuration. Run it separately in QA.
`cases/dml/select/merge_top_large_limit.sql` covers wide-varlen success paths;
it does not establish budget-denial cleanup or a specific compiler rewrite.
