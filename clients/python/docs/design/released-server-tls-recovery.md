# Recover released-server TLS negotiation before authentication

Owner: [SDK #32671](https://github.com/matrixorigin/matrixone/pull/32671).
Native owner: [4.2 fixes #32723](https://github.com/matrixorigin/matrixone/pull/32723).
Revision: `released-server-tls-recovery-r1`.

## Contract and evidence

Official 4.2.8 can read the MySQL SSLRequest and TLS ClientHello into its packet
buffer, then hand only the underlying socket to TLS. The unread TLS prefix is
lost. An independent valid-protocol probe deterministically reproduces this;
the native fix preserves the buffered prefix. A full SDK sweep also recorded
two TLS EOF connection failures affecting six setup cases. Those EOF failures
have not been conclusively attributed to the buffered-prefix defect.

The SDK must preserve TLS policy and certificate verification. A failed TLS
handshake cannot become a passing functional test. There is, however, a safe
recovery boundary: before the first authentication payload, no credentials,
session initialization or SQL have been sent. Re-establishing that physical
connection does not repeat an authenticated operation.

Add at most one fresh connection attempt after an actual `ssl.SSLEOFError`
during this boundary, only for exactly qualified release handshake banners.
The second attempt must retain all original connection/TLS options and fit in
the remaining physical-connect timeout. Each TLS handshake uses the remaining
budget, without extending a shorter configured socket timeout. Authentication,
driver initialization and later SQL retain their existing timeout owners.
No recovery is permitted after an authentication payload starts, for certificate
errors, timeouts, authentication rejection, unknown/custom/source banners or a
second failed attempt. There is no sleep, backoff, TLS downgrade or SQL retry.

This changes a connection-recovery/security boundary, so design review must
precede implementation even though its runtime scope is small.

## Responsibility and implementation

Keep one exact-banner authority in the existing release compatibility module.
Qualify the newly published `v4.2.8-hotfix-20261114` independently; its frontend
TLS source is unchanged from 4.2.8. Its release asset must match the published
SHA-256 and actual handshake/revision before selecting its SDK contract.

The SDK-owned synchronous PyMySQL creator is the sole retry owner. A driver
subclass records the authentication phase around the existing driver method:
the first successful 32-byte CLIENT_SSL request is the protocol SSLRequest;
the first subsequent authentication packet is marked **before** its write.
Only an SSL EOF after SSLRequest and before authentication is tagged with the
observed release. The driver still owns packet formatting, TLS wrapping,
authentication plugins and cleanup. No duplicate implementation of those
protocols is introduced. Existing required-TLS rejection remains before auth.

The creator can inspect the tagged original SSL exception retained by
PyMySQL's OperationalError. It reconnects once if budget remains, otherwise
propagates the real failure with fixed actionable advice and attempt count.
Retain native errno and underlying SSL exception. Diagnostics contain no
credentials or SQL. Different targets reached through a pool are classified
on each physical handshake, without a client-wide capability cache.

Integrate this creator at the existing LOCAL-file do_connect owner; retain
explicit True/False/automatic LOCAL behavior and its initialization guard.
Adopted engines and async driver creators remain under their existing owners.
This fix does not add a new async TLS API or mutate a caller's SSLContext.

## Alternatives

* Native buffered-prefix fix is the complete protocol repair and remains in
  #32723. It cannot alter an already-published immutable binary.
* Delaying ClientHello or changing packet spacing has no application-level
  acknowledgement to synchronize with, so cannot provide a sound workaround.
* Disabling TLS, relaxing verification or retrying an entire Client operation
  changes security or can duplicate effects; reject these approaches.
* Unbounded/general connection retry can hide certificate, permission or
  availability problems. Keep this recovery to one proven pre-auth TLS EOF.

## Ownership and bounds

Each failed attempt is closed by PyMySQL's existing BaseException cleanup;
the creator never publishes it to a pool. A successful attempt transfers once
to SQLAlchemy. LOCAL guard installation retains its existing abort-on-failure
owner. Cancellation, timeout and unrelated errors propagate without retry.
At most two sequential physical attempts exist, with no new worker, queue,
timer, global state or retained failure history. The deadline and phase flags
belong to that creator/connection. No authenticated resource is duplicated.

## Validation and release boundaries

Use the existing connection tests and disposable services. A deterministic
local protocol peer closes once after SSLRequest, then performs actual TLS
and native-password authentication on the fresh connection. Prove identical
certificate policy and exactly one authentication on success, at most two
accepted sockets, deadline/no-retry exhaustion, and closure of failed sockets.
Controls cover certificate/hostname errors, post-auth EOF, unknown identities,
wrong error type, required TLS on plaintext peer and explicit LOCAL options.

Then qualify actual 4.2.8, the new published 4.2 hotfix and current main:
unchanged positive connection/query/transaction assertions, complete online
and analytics, the existing offline driver matrix, installed core and packaging.
Keep direct native coalescing controls visible: SDK recovery does not claim to
repair the old server protocol. A persistent failure remains a real failure
with actionable advice, not an expected-error functional pass.

The release contract runner must recognize the exact hotfix identity rather
than silently label it 4.2.8. Preserve the existing two CI jobs and environments;
no runner, shard or test assertion is removed. Unknown releases need their own
qualification. Revisit this recovery only when supported affected releases are
retired or contrary evidence changes its pre-authentication safety proof.
