# Design review: release Arrow upload compatibility r1

Decision: **accepted for implementation**. This is a design-only review, before
implementation changes, conducted by Codex (`/root`). It is not an independent
external review or a claim that implementation validation has passed.

Reviewed revision: `89052f62563dc4c546d7be32b989b3eb26ca38ed`.  
Design SHA-256: `39cbae04d3c3e16b7bcae25e8bba8d10b38dae4d7d43f1650872f1787eb24d4f`.  
Design: [legacy-arrow-upload.md](legacy-arrow-upload.md).

The gate applies because file-format negotiation changes an existing compatibility
contract and crosses capability, staging and async ownership boundaries.

- The exact existing format-rejection classifier owns fallback admission; versions,
  permissions, missing files and unknown errors cannot authorize fallback.
- One file and the existing staging/CPU/unlink owners close all success/error/cancel
  paths. No retry, cache, second file, worker or unbounded queue is introduced.
- Probe checkout closes before CPU admission; CPU staging closes before final DB
  admission. The wait graph has no DB/CPU cycle. Async acquired connections abort
  on failure and close through existing tracked cleanup. Engine/generation checks
  reject late results across reconnect.
- Final-checkout requalification rejects Arrow-to-unsupported drift before DDL.
  Existing CREATE/LOAD transaction ownership, commit uncertainty and cleanup error
  precedence remain authoritative.
- The design deliberately accepts one additional read-only checkout and different
  input-error precedence; neither changes target data before validation. Native
  main continues using IPC; only qualified legacy rejection selects Parquet.
- Existing byte and batch controls remain, including footer failure. Footer memory
  scales with row groups/columns, and caller input/RSS is not globally capped.
- Feasibility covers exact difficult scalar values; it does not replace the old
  positive transaction, late-failure, isolation, cancellation and cleanup oracles.
  The complete acceptance map remains a delivery requirement.

Unresolved design blockers: **none**. Implementation deviations require a new
reviewed revision. Implementation delivery is blocked until its mapped validation
and final self-review pass.
