# Released-server compatibility design review r1

Decision: **accepted for implementation**, before runtime changes. Reviewed by
Codex (`/root`); this is a root self-review, not independent external approval.

Reviewed revision: `dfaf152d54cd944a18b7411b5099de4082761346`.
Design SHA-256:
`ca46ccf296741b76efde0f772368d41d021fd847e7870f6c19f4c3b41c438c44`.
Design: [released-server-compatibility.md](released-server-compatibility.md).

The gate applies to the changed SDK/server compatibility and error contract.
The actual-checkout handshake avoids client-wide version drift and requires no
network request, new capability cache or credential-bearing diagnostic.
Equivalent numeric whitespace has independent release proof; other operations
retain semantics or fail explicitly. Unsafe DDL rejection precedes dispatch,
including CREATE TABLE with an inline unsupported index. Native-failure hints
preserve real errno/cause and let the existing owner determine rollback/effect
status. Plain MySQL engines and unknown builds are unaffected.

Required implementation controls: lex quoted/comment/executable-comment text
correctly; do not diagnose from caller data embedded in an exception; do not
claim an unsupported preflight has a native errno; do not classify permission,
transport or unrelated syntax as a known limitation. SQL-mode queries must
remain on the selected checkout and outside recursive paths. A handler must
never access/reconnect a closed connection while preserving a primary error.

Resource owners, admission, cleanup, transaction modes and write cardinality
are unchanged. No automatic DROP, schema widening, ASYNC substitution or replay
is permitted. Release validation must test exact diagnostics, no mutation,
independent native controls and explicit alternatives; main/candidate keep full
positive oracles. Final source/API documentation and PR policy must replace the
prior kernel-first release requirement. All mapped validation remains required
before delivery. Unresolved design blockers: **none**.
