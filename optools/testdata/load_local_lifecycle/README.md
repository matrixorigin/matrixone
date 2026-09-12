# LOCAL upload lifecycle public-protocol witness

Run only against a disposable, test-owned `etc/launch-multi-cn` instance with
CN ports 16001 and 16002. This script creates and drops `pr26109_probe`; never
point it at a shared or production database. It does not start/stop services.
Wait for SQL readiness and `ISCP-Task Start` in the service log first.

With PyMySQL installed (validated with 1.2.0):

```sh
python3 optools/testdata/load_local_lifecycle/validate.py
python3 optools/testdata/load_local_lifecycle/validate.py
```

Defaults are the repository's test root credentials; override `MO_TEST_USER`
and `MO_TEST_PASSWORD` if necessary. Every run verifies database teardown before
returning. Two consecutive runs prove same-instance reuse. The fixture needs
only two committed rows and one uncommitted row, not a performance dataset.

The raw PyMySQL packet calls are intentional: an ordinary LOCAL client always
sends EOF and cannot prove termination when EOF never arrives. Cases cover
successful LOCAL on either CN and subsequent connection reuse; `KILL QUERY`
during idle read, incomplete header, incomplete payload and ongoing packet
upload; UDF import cancellation with no catalog row published; aborted-load
rollback; and per-CN read-your-writes, peer isolation and
rollback. Server disconnect is expected for an interrupted upload before EOF.

This is public-protocol evidence, not proof of AP_MULTI selection by itself.
For the ingress scheduling boundary use the committed compile/placement UTs;
a separate white-box service build calling `plan.SetForceScanOnMultiCN(true)`
can run this same witness with tiny data. Label that result as injection, and
never deliver the injection or report its performance as the normal binary.
The future compute-group lifecycle/authorization matrix is not implemented by
these legacy scheduler tests.
