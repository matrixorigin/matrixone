# Transaction-neutral release checks: design review r3

Decision: **accepted for implementation**, before replacing the listener's
SQL-mode lookup. Codex `/root` self-review, not independent external approval.

Reviewed revision: `73e39d2cb13f92e954b57efeb561b2a48455261d`.
Amendment SHA-256:
`620c61bd3574d3e2e622da0577ded0c174cfdc6c2fd67db0db9e7f24867fe2b9`.

Native before/after probes disprove the old assumption that read-only mode
queries are transaction-neutral. Removing that query closes the first-owner
defect without committing or rolling back caller data. Agreement across a fixed
set of lexical interpretations is a safe admission criterion: disagreement
cannot authorize a rewrite/refusal. Mode count is bounded and does not persist
state. Existing parameter-rendering and explicit driver execution contracts
remain separate. Required proof includes unchanged original Stage consumers,
ambiguity controls and complete native-release suites. Main/candidate retain
their native path; no new resource, race or design blocker remains.
