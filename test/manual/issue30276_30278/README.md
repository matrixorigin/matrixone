# Session authorization regression (#30276, #30278)

Run against an isolated instance with two CNs behind an upgraded proxy. All CNs
and the proxy must contain this change. Install PyMySQL, then run:

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
