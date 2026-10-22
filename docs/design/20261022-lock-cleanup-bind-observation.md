# Observe lock retirement without allocating during transaction cleanup

Issue: #29447. Base: 75ae9a79d626622e6ccec0bcb28412f5fcf84c7f.
Revision 1: design review PASS before production implementation.

## Evidence and invariant

The historical post-#30269 Linux coverage failure rebinds mo_tables (table 2)
from CN0 to CN2 during CN2's Unlock cleanup. CN2 closes immediately afterward.
The allocator tolerates two 5s keep-bind intervals and scans every 5s; the first
DROP on CN1 exhausts its 10s retry budget before that second owner is reclaimed.
A three-LockService deterministic regression reproduces the ownership side
effect on current main: after closing and invalidating the owner, Unlock by
another service makes that cleaning service the valid replacement owner.

Cleanup may prove that its recorded binding retired, or retry the original
owner while it remains valid. It must not allocate a replacement binding,
register or reactivate its caller, or publish an invalid physical table.
Actual lock acquisition remains the sole reason to allocate a new owner.

## Decision and compatibility

Extend GetBindRequest with LookupOnly (default false), and GetBindResponse with
a matching acknowledgement (default false). Existing acquisition behavior is
unchanged. A new allocator handles lookup before caller admission: under its
existing mutex it snapshots a valid current binding, or returns a canonical
invalid binding with the requested table/group/sharding and no owner. It does
not create maps, register services, update liveness or assign ownership.

Only remote Unlock error recovery requests lookup mode. It retains existing
RPC deadlines, allocator-epoch acceptance, exact-generation fencing and cleanup
ownership. An acknowledged invalid lookup proves the old binding retired;
retire the exact old cached route without publishing a replacement. A valid
replacement can use existing publication checks. An unchanged valid owner
continues existing bounded/cancellation-aware Unlock retry behavior.

The acknowledgement distinguishes absence evidence from malformed or legacy
responses. An older allocator ignores the new request field and returns its
legacy allocating response; consume only a valid response using the existing
rules. This preserves mixed-version behavior without inventing retirement
proof, but the ownership-side-effect fix requires an upgraded allocator as well
as CN. An old CN ignores both added protobuf fields and retains old behavior.
New allocator restarts remain protected by existing epoch acceptance; a stale
allocator response cannot retire a newer cached generation.

## Alternatives, ownership and cost

Waiting for every catalog binding before DROP hides the production condition;
restore the original scale-down regression. Longer SQL retry budgets or shorter
failure-detection grace change availability/safety tradeoffs and retain the
cleanup side effect. Suppressing all cleanup errors or trusting membership
absence can abandon a live owner's physical locks. A separate observation RPC
would duplicate routing/version handling; an additive acknowledged mode keeps
one existing bounded RPC and introduces no new resource owner.

No new goroutine, retry, cache, lock order or long-lived allocation. The server
uses one existing allocator map lock and an O(1) lookup. Existing request,
response, RPC deadline and transaction teardown owners remain unchanged.

## Required validation and design decision

- Deterministic pre-fix failure and post-fix success through real Unlock and
  allocator RPCs, followed by an independent survivor acquisition.
- Lookup preserves valid owner/version and liveness, observes retired/missing
  binds without allocating, works for a retired requester, and does not create
  an unknown group. Ordinary acquisition still allocates.
- Client mode/acknowledgement, invalid/mismatched responses, legacy fallback,
  and cancellation remain explicit. Invalid evidence never creates a table.
- Owning LockService package and focused adaptive race stress; the original
  isolated scale-down SQL regression under atomic coverage, without added
  catalog readiness or client retries. Changed-package static checks and the
  dependent CN/SQL integration build/test close the consumer boundary.

Review: contract, old/new peers, allocator epochs, invalid route publication,
unchanged-owner retry, cancellation and bounded ownership are mapped above.
Decision PASS, no open design blocker. Protocol addition warrants this design
record; no user-visible SQL or exported LockService interface is changed.
