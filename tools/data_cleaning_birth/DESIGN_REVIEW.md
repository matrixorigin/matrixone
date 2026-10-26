# Distinct integration design decision — 2026-10-26

Decision: **APPROVED for the default-disabled, restricted isolated experiment**.
There are no design blockers in the reviewed revision. This is an independent
model design review, not maintainer approval, implementation approval or
production qualification. It does not dismiss the formal review on #30816.

Reviewed document: [DESIGN.md at the frozen revision](https://github.com/VioletQwQ-0/matrixone/blob/75927cb378956258e049b3e0316b59a5496aeb10/tools/data_cleaning_birth/DESIGN.md).

- Exact commit: `75927cb378956258e049b3e0316b59a5496aeb10`.
- Document SHA256: `2a2f7cd2604ebc4df226a08893dbc329bc6e9d3d80d0fa78f1a8a8eb0612c443`.
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

R2 explicitly distinguishes the existing NULL-only experimental domain from
false descriptors. Unsupported later DDL on the NULL-only domain rejects before
mutation, preserving `next=b+v`; no general NULL-domain COPY exit is introduced.
False descriptors extend history for qualifying ADD, and other legal default-
algorithm transitions use the existing transactional COPY exit. Unsupported
explicit INPLACE rejects. Ordinary builds retain legal COPY behavior.

The same independent reviewer re-read the frozen R2 Git object and approved this
clarification without design blockers. R1 at `97c156b2208d2b024e4c059510eee92109c24758`
is retained in history; its broad admission/exit wording is superseded by this
explicit R2 decision, not by an unreviewed implementation change.

Non-blocking rollout risks remain assigned: authors must measure ordinary and
tagged hot-path costs and preserve source-bound evidence; maintainers and QA must
accept/qualify ordinary historical missing-column compatibility, mixed versions,
recovery tools and production deployment. A cooperative launcher cannot protect
against arbitrary old executables. Failure to maintain isolation means disabled.

The design review did not accept performance results: historical measured
results are assessed separately, and an integrated head has no new performance
claim until measured. No build, test, Ubuntu action or public review was performed
by the design reviewer. Any material deviation requires a new design decision.
