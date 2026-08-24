# Local MongoDB connector fixture

`optools/mongodb_ci.bash` is the only supported entry point. It creates a unique Compose project, port, keyfile, credentials, MongoDB volume, MO data directory and report directory; waits for a writable ReplicaSet primary; seeds a read-only MongoDB user; and cleans everything on exit.

The connector uses a direct connection to the published localhost port for this single-node fixture. Production ReplicaSets should use normal discovery and a dedicated read-only role with only `find`, `listCollections`, `listIndexes` and `collStats` on the mapped databases/collections. Do not grant write or cluster-administration roles to a MatrixOne source identity.

The built-in environment resolver is intended for local/controlled deployments. System-account references must start with `secret://env/MO_MONGODB_`; tenant account `N` is restricted to `secret://env/MO_MONGODB_ACCOUNT_N_`, preventing one tenant from naming another tenant's or an unrelated process secret. Production secret-manager implementations receive the account ID and must enforce an equivalent namespace and authorization check.
