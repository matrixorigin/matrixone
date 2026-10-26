# Distinct integration design decision — 2026-10-26

Decision: **APPROVED for the default-disabled, restricted isolated experiment**.
There are no design blockers in the reviewed revision. This is an independent
model design review, not maintainer approval, implementation approval or
production qualification. It does not dismiss the formal review on #30816.

Reviewed document: [DESIGN.md at the frozen revision](https://github.com/VioletQwQ-0/matrixone/blob/97c156b2208d2b024e4c059510eee92109c24758/tools/data_cleaning_birth/DESIGN.md).

- Exact commit: `97c156b2208d2b024e4c059510eee92109c24758`.
- Document SHA256: `ecc6bc4d6dcfe4ac06d232f0d7d342c326c5d06e1d4bbd3355d6b5b5e783ddfe`.
- Independent reviewer: Codex agent `01a17636-3af5-7ea3-af44-54b32c4f8374`,
  started without parent context, instructed not to delegate or modify code.
- Classification: feature/R3; the complete #30816 + #30958 series crosses
  planner, CN/TN, catalog, object reads, persistent history and recovery.
- Phase order: this approval precedes the new integration implementation review
  and delivery. It is not evidence of an approval before the existing prototype.

The reviewer accepted: separate immutable birth fill and current SQL Default;
SchemaExtra as the durable authority with independently owned projections;
complete relation/schema/prefix proofs for memory and objects; existing DDL
transaction ownership and standard COPY exit; ordinary-build shared changes
requiring separate validation; bounded history/proof/replay costs; and explicit
isolation, permission and old-binary limitations. No second storage authority,
publication state machine or deferred materialization executor was needed.

The approval requires the documented admission/exit invariant to hold for both
NULL and false histories. A later transition must preserve the append invariant
or use COPY; an unsupported explicit algorithm must reject. Implementation review
must verify this rather than silently narrow the design to false descriptors.

Non-blocking rollout risks remain assigned: authors must measure ordinary and
tagged hot-path costs and preserve source-bound evidence; maintainers and QA must
accept/qualify ordinary historical missing-column compatibility, mixed versions,
recovery tools and production deployment. A cooperative launcher cannot protect
against arbitrary old executables. Failure to maintain isolation means disabled.

The design review did not accept performance results: historical measured
results are assessed separately, and an integrated head has no new performance
claim until measured. No build, test, Ubuntu action or public review was performed
by the design reviewer. Any material deviation requires a new design decision.
