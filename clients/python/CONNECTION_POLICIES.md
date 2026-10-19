# Connection and timeout contracts

`Client.connect()` creates an engine and requires an authenticated SELECT 1 before
reporting success. Initial probe failures dispose the failed engine and clear its
state. `connected()` reports successful initialization, not a live health probe;
use a query for live health. Reconnect disposes the old engine. Failed disposal
also clears the client's connected state, while preserving its error.

`ssl_mode='required'` uses a certificate/hostname-verifying SSLContext. `ssl_ca`
selects a CA file; without it, system roots are used. Peers without TLS are refused
before authentication on older and current PyMySQL. `disabled` explicitly disables
TLS. `preferred` follows the driver's opportunistic behavior unless a CA is
supplied, in which case certificate verification is requested. A client certificate
requires a CA or required mode. This policy change may reject previously accepted
untrusted endpoints; configure a trusted test/organization CA rather than disabling
verification on a required connection.

Constructor `connection_timeout` is inherited unless connect explicitly overrides
it. The synchronous engine also passes query_timeout as read/write inactivity
limits and the configured charset. Async connect applies its connection deadline
and passes its driver connect timeout. `AsyncClient.execute()` applies query_timeout
to the full call (pool wait, execution, fetch and transaction completion).
Cancellation propagates and SQLAlchemy releases/invalidates the interrupted
connection. This is a client deadline, not a guaranteed server-side KILL operation.
Independently managed Session calls retain their own transaction/deadline scope.

A timed-out write can have an unknown commit outcome. Reconcile before retry;
TimeoutError or cancellation is not proof of rollback. No automatic retry is added.

Connection diagnostics use an interactive `mysql -p` prompt, redact any configured
password echoed in the driver error, and suppress the original exception's display
chain. The SDK depends on `SQLAlchemy[asyncio]` so supported async runtimes are
installed instead of silently leaving async imports unavailable.

Transport regression tests require an isolated TLS CN with a test certificate
having only IP:127.0.0.1 in its SAN, a plaintext CN, an existing disposable test
database, and MO_SDK_ONLINE=1, MO_TLS_CA, MO_TLS_PORT and MO_PLAIN_PORT. Tests use
synthetic credentials and do not commit test private keys/certificates.
