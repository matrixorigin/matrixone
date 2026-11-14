# Released SET reflection amendment review

Decision: **accepted for implementation**, before the SET adapter change.
Codex `/root` self-review; not independent external approval.

Reviewed revision: `f91368a5f17256b01c69ff5238c64d7feae1c677`.
Design SHA-256:
`fb66c9c16daf720772bed697ba9b9efd6ce634266b03b8110a898984a007378f`.

The r1 release execution decisions remain unchanged. The amendment closes an
independently reproduced case-loss defect without changing source values or
creating a metadata cache. SET members come from the existing native catalog
representation with bound names on the executing connection. Other SET options
and result/bind processors must remain intact. Missing metadata and query
failures propagate; no fabricated domains or implicit case normalization is
allowed. Query cost is limited to release SET reflection. Native VARCHAR views
remain VARCHAR and are documented/tested separately from preserved source SET
domains. Required validation adds case-sensitive members, replay and actual
typed values on the release, with main/unknown paths unchanged.

No new resource/worker, persistent format or design blocker is introduced.
