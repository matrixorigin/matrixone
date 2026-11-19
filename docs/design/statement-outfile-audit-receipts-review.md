# Distinct design-review decision for PR #30916 — revision 2

Decision: **PASS / APPROVE (design only)**, 2026-10-24. This revision supersedes revision 1's local recover/convert/re-panic mechanism; all other approved design decisions remain in force.

Reviewer: `/root/design`; designated `gpt-6.1-sol`, `xhigh`, explicitly selected in task dispatch. No independent runtime-model introspection endpoint is exposed to this agent. This targeted review performed source/artifact reads and design-artifact writes only; it did not modify implementation/tests, run tests/builds, push, or submit an external review.

Reviewed artifact: `design.md`, revision 2, intended for `docs/design/statement-outfile-audit-receipts.md`.

**Reviewed design SHA256: `2276895a1052794440975620d42087e7c3ca7956a1469eb974affc0182c244c9`.**

Source snapshot: current repair head `7927962f94cfa3c297a027d1c98c3cccfc88a55f`, frozen base `70e48cbef1235a8ef4b6c887cb37b0947bace714`. Original feature/design source identities remain recorded in revision 1.

Trigger and new evidence: configured molint emitted an introduced `checkrecover` diagnostic at `pkg/frontend/status_stmt.go:121:17` in the current source, retained in `sca.log`; the vettool's zero exit code does not erase that diagnostic. `pkg/common/moerr/error_handling.md` restricts recovery to well-defined boundaries. The existing `executeStmtWithWorkspace` recovery is the established owner. A second local recovery boundary is unnecessary. This material new static/policy evidence warrants reopening the mechanism; it does not invalidate the original producer-completion/abort-before-join/lower-bound invariants.

Approved correction:

1. Install stack-local `exportCompleted=false` immediately before the existing OUTFILE cleanup defer.
2. Set it true only after `exportAllDataFromBatches(ep)` returns nil.
3. Deferred cleanup uses named primary `err` when present; otherwise `context.Canceled` when the completion marker is false. Call `ep.finish(cause)` before reading confirmed totals.
4. Receipt completion requires `exportCompleted && err == nil` after finish.
5. Add no recover, panic conversion or explicit re-panic. Panic propagation retains the exact original payload automatically; the existing outer recovery continues to own error conversion. Goexit follows the same cleanup path without needing detection.

Soundness checked against the actual source:

| Trigger | Guard/cause | Result |
|---|---|---|
| Normal producer and ordered drain succeed | true / nil | existing successful finish; complete only if finish succeeds |
| Existing GetColumns/open/runner/conversion/drain error | false / original error | abort-before-join; original concrete error retained; incomplete lower bound |
| Panic before completed drain | false / cancellation sentinel if no prior error | cancellation releases abandoned publication before join; incomplete; original panic propagates untouched |
| Goexit before completed drain | false / cancellation sentinel if no prior error | same bounded cleanup; Goexit continues |
| Late context/sink-close failure after drain | true / finish detects failure | terminal error, incomplete confirmed lower bound |
| Future normal nil return bypasses drain | false / cancellation sentinel | fails closed; cannot claim complete |

No normal existing success path returns before the marker location. The marker adds no shared ownership, goroutine, queue, allocation or persistence state. Existing confirmed-split accounting and cleanup contracts remain unchanged. The design now records both the policy and the accepted alternative explicitly.

Required implementation/validation: commit versioned revision-2 design and this distinct approval before implementing the correction. Existing deterministic panic/no-worker/blocked-publication/prior-split/original-identity tests, normal-success neighbor, and typed ordinary error cases remain suitable and must pass for the corrected semantic inputs. Rerun relevant static checks and require no introduced checkrecover diagnostic; preserve any baseline diagnostics separately. Rerun focused frontend normal/race and owning evidence affected by the changed lifecycle; public success/ordinary/error evidence may be reused only with an explicit semantic-validity argument. No optional Goexit-specific test is required because there is no termination-type branch, and source reasoning closes that arc.

Blocking design findings: **none**. Delivery approval remains pending the implementation and execution-evidence reconciliation. Revision-1 source PASS at the current head is superseded for the known introduced static diagnostic until correction; this design approval does not assert the old source is ready.

History preserved: `design-v1.md` SHA256 `6c41942200b3e1788b0cc411a4289e36b78040ffb3dde7f306728c50bffea00e`; original `design-decision-v1.md` SHA256 `8e778cd14730746a5b6a6bfd44e7aafc4b403972a24919e4b9c457f6d2a7782f`; original versioned design-first commit `d42578cbbd` remains in git history. Revision 2 changes only the cleanup mechanism and necessary policy/lifecycle/validation statements.
