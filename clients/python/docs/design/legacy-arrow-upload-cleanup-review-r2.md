# Probe cleanup admission design review

Decision: **accepted for implementation**, reviewed by Codex `/root` before the
production change. This is root self-review, not independent external approval.

Reviewed revision: `5a43151ae3`.  
Design SHA-256: `ed68d1cfcf3ff13e8fc0893764b03004668e6ad5e919e1962c7c0c9b40e26bc5`.

The counterexample is reproduced through the public upload method. Abort of a
driver cannot guarantee completion of an arbitrary pool reset callback. Keeping
the existing operation charged closes that gap at its first admission owner.

One future per operation and two existing permits bound retained probe closes.
Registration precedes the cancellation point. Producer and close completion both
route through the same idempotent cleanup owner; only successful unlink releases
the permit. Pending close cannot occupy a CPU worker or a new queue. Successful
close and the existing blocked-reader/unlink-retry state machines are preserved.
The additive status field describes why charged capacity remains unavailable.

The design introduces no independent cleanup lane, capacity reset, retry or
exception suppression. The regression must retain both pending closes, stop the
third checkout, and prove release/reuse after cancellation and close completion.
Implementation delivery still requires the mapped focused and real-service
validation plus final self-review. Unresolved design blockers: **none**.
