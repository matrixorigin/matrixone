# Session authorization regression (#30276, #30278)

Run against an isolated instance with two CNs behind an upgraded proxy. All CNs
and the proxy must contain this change. Use `etc/launch-with-proxy/launch.toml`
as the topology template, with isolated data/socket paths. Start it with
`mo-service -launch /absolute/path/to/launch.toml -with-proxy` (the flag is
required to select the Proxy service instead of the built-in TCP forwarder), and set
`conn-cache-enabled = true` in its `[proxy]` section. Install PyMySQL, then run:

```sh
python3 session_authorization.py --proxy-port 6001 --cn-ports 16001 16002
```

The script creates and removes only `session_auth_regression`,
`session_auth_reader`, and `session_auth_low`; these names must be reserved for
this test. The root password defaults to `111` (`--password` overrides it).
It asserts visibility and cancellation outcomes, current role/user behavior,
response synchronization, and bounded disconnect cleanup. It selects a target
on a different CN from the ordinary proxy actor using administrator-observed
node IDs. Readiness uses observable processlist phases with bounded polling.

The final probes force-close clients without `COM_QUIT` while `SELECT SLEEP(60)`
is visible: one after `COM_CHANGE_USER` to the ordinary user, and one unchanged
root control. Each complete backend row must disappear within ten seconds of
the client close. PASS JSON includes the connection/node identities and measured
cleanup times. Retain the command, stdout/stderr and exit status together with
the binary source revision/hash and effective topology/configuration (including
connection caching); the script cannot attest to the server's build identity or
cache setting itself.

See [the recorded acceptance run](disconnect_acceptance.md) for matching-build
evidence and the observed cleanup times.
