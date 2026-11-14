# Released-server TLS repair and SDK diagnostics

Owner: [SDK #32671](https://github.com/matrixorigin/matrixone/pull/32671).
Native defect: [#29928](https://github.com/matrixorigin/matrixone/issues/29928).
Revision: `released-server-tls-recovery-r2`.

## Decision

Repair lost TLS input in the server on the actual `v4.2.8-hotfix` release
branch. The older 4.2-dev companion does not place that repair on this release
branch. Retain certificate/hostname verification and all existing positive TLS
assertions. SDK diagnostics identify failed negotiation on qualified release
profiles without silently retrying authentication, changing TLS settings or
claiming that a connection failure is functional compatibility.

This supersedes r1's proposed one-EOF reconnect. Real tests against the published
hotfix also produce TLS handshake timeouts within the caller's three-second
budget. Reconnection cannot recover an already-consumed budget, and a caller's
native engine has its own creator. A successful synthetic EOF recovery does
not prove repair of the underlying protocol. The rejected implementation and
failed tests remain outside tracked source; no automatic retry is delivered.

## Protocol invariant and first owner

The frontend MySQL reader can consume SSLRequest and ClientHello in one read.
Every byte beyond SSLRequest must reach TLS exactly once before further socket
reads. Preserve the buffered prefix in frontend.Conn and use that connection
at both TLS handoff sites. Keep net.Conn deadlines, close and Unix socket
liveness available. Upstream #29996 already implements this repair; backport
its complete four-file change, including buffered/unbuffered/coalesced and
peer-close tests, rather than changing client packet spacing or adding sleeps.

Each connection retains its existing socket owner. The copied prefix is bounded
by the frontend read buffer, drained once and released. The wrapper forwards
writes, close, addresses, deadlines and SyscallConn. There is no new worker,
queue, protocol round trip, wire format, persistent state or retry owner.

## SDK responsibility

Use PyMySQL's existing `_secure` state to distinguish negotiation from an
established TLS/authentication exchange. For known release banners only, an
actual SSL EOF or timeout while TLS is selected and `_secure` is still false
gets fixed actionable advice. Preserve the real errno/SSL exception and existing
password redaction. Do not relabel certificate, permission, TCP-connect,
post-TLS/authentication or unrelated errors as the buffered-prefix defect.
Advice must be conditional: the same symptom does not prove that exact cause.

The SDK-owned creator integrates this diagnostic with its existing LOCAL-file
owner. Adopted engines and async creators retain their owners. Do not introduce
a second authentication state machine, packet-length classifier, deadline,
worker, cache or retry. The successful path uses the driver unchanged.

## Version and packaging boundaries

Qualify published 4.2.8 and `4.2.8-hotfix-20261114` by their exact banner,
revision and asset hash. The maintained source branch identifies itself as
`v4.2.8-hotfix`; its release-family SQL compatibility profile is separate from
4.2-dev and main. Source-candidate results must pin the real git revision and
be reported as candidate evidence, never as tests of an immutable published
image. Test configuration cannot select runtime feature behavior.

The published ZIP also omitted the dictionaries required by its tokenizer.
Use the same release source's dictionary assets for validation and fix bundle
packaging to include the standard sibling `dict` directory. Keep the original
ZIP and binary hashes; do not describe an installed missing dependency as an
unchanged complete bundle. No dataset, parser, query or expected value is
reduced to make the Chinese search test pass.

Native MySQL and SDK-adopted reflection are separate consumers. Preserve the
exact native uppercase-type control and all SDK-positive metadata/value/replay
oracles. Do not claim a raw third-party dialect is repaired by SDK adoption.

## Validation

For the native hotfix, run focused TLS/disconnect tests, the complete owning
frontend package, relevant race cases, scoped static checks and an independent
coalesced SSLRequest/ClientHello probe against the actual release-line build.
Run the existing positive SDK TLS/certificate/hostname/plaintext-peer controls
without altered assertions. Verify the release archive contains its required
dictionary assets.

For the SDK, test diagnostic phase/identity/error controls and LOCAL integration,
then full online/analytics on main and the fixed release-line candidate, plus
the existing offline matrix, installed core, package and CI contract checks.
Keep the original released-server failed runs as evidence. No extra CI runner,
shard, skipped case, broader timeout or expected-error functional pass is added.
