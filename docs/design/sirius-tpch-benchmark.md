# Single-CN Sirius TPC-H benchmark

This profile is for a one-shot TPC-H run with one MatrixOne CN, one TN, and a
separately started Sirius sidecar. It is not a production deployment profile:
the TN GC manager must remain disabled for the whole run, and CN restart/crash
replay is not supported by the process-local lease authority.

The sidecar is local by ownership, not necessarily by network namespace. One
sidecar is paired with exactly one CN through one Flight address, one
`MO_SIDECAR_READ_URL`, and a unique sidecar read-client certificate whose SPKI
the CN authorizes on every read lease. Do not share a sidecar between CNs. A
multi-CN deployment needs one independently configured sidecar and certificate
identity per CN; this benchmark profile intentionally uses only one pair.

## MatrixOne configuration

Set the TN option in the TN configuration:

```toml
[tn.GCCfg]
disable-gc = true
```

Set the CN Sirius section in the CN configuration. The certificate roles are
directional:

```toml
[cn.sirius]
enabled = true
benchmark-no-gc = true
flight-address = "sidecar:32010"
flight-server-name = "sidecar"
flight-client-cert-path = "/certs/mo-flight-client.crt"
flight-client-key-path = "/certs/mo-flight-client.key"
flight-server-ca-path = "/certs/sidecar-flight-ca.crt"
resolver-address = "0.0.0.0:32011"
resolver-server-cert-path = "/certs/mo-resolver-server.crt"
resolver-server-key-path = "/certs/mo-resolver-server.key"
resolver-client-ca-path = "/certs/sidecar-read-client-ca.crt"
resolver-client-cert-path = "/certs/sidecar-read-client.crt"
data-dir = "/shared/matrixone-objects"
```

The top-level `mo-service` launcher verifies that the paired TN has
`disable-gc = true` before it starts the CN benchmark adapter. The CN and
sidecar must see `data-dir` at the same path (or an equivalent shared object
store mount). Start MatrixOne with a `-launch` manifest so the launcher can
derive this proof from the same `tnservices` files it starts. A standalone
`-cfg` CN and the embedded cluster API cannot establish which TN serves their
snapshots, so they reject this benchmark mode even if their CN configuration
contains a sibling `[tn.GCCfg]` section.

## Standalone sidecar

Start the sidecar independently with its Flight endpoint enabled. Use the
sidecar's Flight server certificate and client-CA settings, and configure its
read client identity to call the CN resolver:

```text
MO_SIDECAR_FLIGHT_HOST=0.0.0.0
MO_SIDECAR_FLIGHT_PORT=32010
MO_SIDECAR_FLIGHT_CERT=/certs/sidecar-flight-server.crt
MO_SIDECAR_FLIGHT_KEY=/certs/sidecar-flight-server.key
MO_SIDECAR_FLIGHT_CLIENT_CA=/certs/mo-flight-client-ca.crt
MO_SIDECAR_READ_URL=https://cn:32011/internal/v1/sidecar/read/resolve
MO_SIDECAR_READ_CA=/certs/mo-resolver-server-ca.crt
MO_SIDECAR_READ_CLIENT_CERT=/certs/sidecar-read-client.crt
MO_SIDECAR_READ_CLIENT_KEY=/certs/sidecar-read-client.key
```

Do not start the legacy bundled MatrixOne/HTTP sidecar entrypoint for this
profile. The CN must be started separately and must be reachable at the
resolver address.

## Query sequence

Load TPC-H natively, flush all tables, and use a fresh read-only/autocommit
session. Run each query with the explicit hint:

```sql
/*+ SIDECAR GPU */
SELECT ...;
```

Start with Q1, then run Q2–Q22. Compare each result with native MatrixOne
output. Queries that observe unflushed in-memory rows, visible tombstones,
non-TAE tables, or writes in the current transaction are intentionally rejected
by Sirius rather than silently offloaded.
