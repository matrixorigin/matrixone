# TTL verification

See the [English design and verification matrix](../../../docs/design/20261013-table-ttl.en.md), [original Chinese design](../../../docs/design/20261013-table-ttl.md), and [committed verification results](results/20261013/README.md).

With the repository Go toolchain and native dependencies prepared, run:

```bash
TTL_OUTPUT_DIR=/tmp/matrixone-ttl-validation bash test/manual/ttl/verify.sh
```

The script emits full unit/registration/integration JSONL logs, a fuzz log and the `mo-service-ttl` binary. `TTL_FUZZ_TIME` defaults to `15s`. Real-storage tests start embedded services and need local socket access. The committed results include compact TTL test events; verbose runtime logs and the service binary are kept in the execution output directory.
