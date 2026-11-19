# Disconnect acceptance record

Executed 2026-10-25 on Darwin/arm64 with Go 1.27.1, CGO enabled,
`GOEXPERIMENT=simd`, and PyMySQL. Production source revision:
`5e88f3fb2c54a6b6450404c85d3616364958b95a`; `go version -m` recorded
`vcs.modified=false`. The changes accompanying this record touch tests and
documentation only; the production binary matches that PR revision.

The single `mo-service` executable hosts LOG, TN, two CNs and the actual Proxy
service. Binary SHA-256:
`b8746b57735b3ce0317efd1b98c22e6564447420cd07d4085f2c932feaa11b13`.
Native `cgo/libmo.dylib` SHA-256:
`36a104229e8527a9e182c0d8aa0949e3bc9aa5f42ff33992379fddc008aecbc6`.

## Build and topology

From the repository root (`repo=$PWD`, `out` is an isolated temporary directory):

```sh
export CGO_CFLAGS="-I$repo/cgo -I$repo/thirdparties/install/include"
export CGO_LDFLAGS="-L$repo/thirdparties/install/lib"
export GOWORK=off GOEXPERIMENT=simd
go build -mod=readonly \
  -ldflags="-extldflags '-L$repo/cgo -lmo -L$repo/thirdparties/install/lib -Wl,-rpath,$repo/cgo -Wl,-rpath,$repo/thirdparties/install/lib'" \
  -o "$out/mo-service" ./cmd/mo-service
```

Build exit status: **0**. The configs copy `etc/launch-with-proxy` with absolute
launch-manifest paths, temporary data/socket paths, 64MB memory caches and
256MB disk caches. Proxy configuration includes:

```toml
[proxy]
uuid = "rd1dccb4-4d3c-41f8-b482-5251dc7a41bf"
listen-address = "0.0.0.0:6001"
conn-cache-enabled = true
```

CN frontend ports are 16001 and 16002. The service command, from the temporary
directory, was:

```sh
GOMEMLIMIT=6GiB GOMAXPROCS=6 "$out/mo-service" -launch "$out/launch.toml" -with-proxy
```

Service logs confirm `proxy-service` with the configured UUID and backend
connections to the CNs. SQL readiness checked all three frontend ports and ISCP
startup before running the script.

## Public-interface results

Two sequential invocations of the updated script, each with exit status **0**:

```sh
python3 "$repo/test/manual/issue30276_30278/session_authorization.py" \
  --proxy-port 6001 --cn-ports 16001 16002
```

Script SHA-256: `5f895f8723edb4d38073c9c2247745fda6c1bc86e9d55565fae1d5571b455c21`.

Each invocation returned `"status": "PASS"` for the existing authorization,
role/user transition, remote cancellation, prepared-statement, failure and
self-kill checks, plus the two disconnect probes. Each probe first observed its
marked `SELECT SLEEP(60)` in processlist, force-closed the client without
`COM_QUIT`, and required the **whole backend row** to disappear within ten
seconds measured from force-close. It did not merely wait for the SQL text to
clear. The root control did not change identity.

| Run | Case | Connection | CN UUID suffix | Row disappearance (seconds) |
| --- | --- | --- | --- | --- |
| 1 | changed_user | 414 | dd1dccb5 | 0.002080417 |
| 1 | unchanged_root | 415 | dd1dccb5 | 0.113975125 |
| 2 | changed_user | 422 | dd1dccb4 | 0.000739583 |
| 2 | unchanged_root | 423 | dd1dccb4 | 0.000999041 |

The two runs selected opposite CNs for the cross-node authorization cases.
Afterward, administrator queries confirmed the temporary user, role and database
were absent. The owned service was terminated and reaped (exit 0).

Full build identity, config hashes, effective configs, launch command, service
log, script commands/stdout and individual exit statuses are retained in:
`/Users/linjunhong/.loom/tmp/mo-pr30928-acceptance-_pnjxe91`.
This local artifact path is a retention pointer; the commands, identities and
results above are the reviewable evidence retained in Git.

An initial diagnostic launch omitted `-with-proxy` and therefore exercised the
built-in TCP forwarder, where the ten-second cleanup assertion failed. Its logs
are retained separately as `diagnostic-without-proxy-*`; it is not counted as
Proxy acceptance evidence. No assertion or production code was weakened to
obtain the two passing Proxy runs.

## Retained script stdout

```json
{"status": "PASS", "remote_cn": 16001, "actor_node": "dd1dccb5-4d3c-41f8-b482-5251dc7a41bf", "disconnects": [{"case": "changed_user", "conn_id": 414, "node_id": "dd1dccb5-4d3c-41f8-b482-5251dc7a41bf", "disappeared": true, "elapsed_seconds": 0.002080417238175869}, {"case": "unchanged_root", "conn_id": 415, "node_id": "dd1dccb5-4d3c-41f8-b482-5251dc7a41bf", "disappeared": true, "elapsed_seconds": 0.11397512536495924}], "checks": ["visibility/show/function/view/prepared", "remote query+connection denial without effect", "role downgrade+upgrade (single and multi-statement, prepared)", "COM_CHANGE_USER denial+own kill", "failed user+role switches", "self kill", "changed-user and unchanged-root disconnect cancellation"]}
{"status": "PASS", "remote_cn": 16002, "actor_node": "dd1dccb4-4d3c-41f8-b482-5251dc7a41bf", "disconnects": [{"case": "changed_user", "conn_id": 422, "node_id": "dd1dccb4-4d3c-41f8-b482-5251dc7a41bf", "disappeared": true, "elapsed_seconds": 0.0007395832799375057}, {"case": "unchanged_root", "conn_id": 423, "node_id": "dd1dccb4-4d3c-41f8-b482-5251dc7a41bf", "disappeared": true, "elapsed_seconds": 0.0009990409016609192}], "checks": ["visibility/show/function/view/prepared", "remote query+connection denial without effect", "role downgrade+upgrade (single and multi-statement, prepared)", "COM_CHANGE_USER denial+own kill", "failed user+role switches", "self kill", "changed-user and unchanged-root disconnect cancellation"]}
```
