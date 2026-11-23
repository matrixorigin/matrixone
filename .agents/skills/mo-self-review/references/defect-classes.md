# Transition defect classes

These classes come from 30 mo-bot findings over 15 review rounds of one fileservice
PR (2026-11). Each class gives the shape of the bug, the probe that exposes it, and
the fix that closed it.

## A. An ordered consumer pins producers that finish out of order
- **Shape:** the consumer copies units in order, and each unit's hold is released
  only when the consumer reaches it. A later unit ends but stays charged behind an
  earlier stalled one.
- **Probe:** two units; gate the first and let the second finish. Charged bytes must
  equal one unit, and a bystander that fits once the ended unit is released must not
  wait for the stalled one. Run both orders.
- **Fix:** release each unit when it ends; hold it only while copying from it, and read
  an ended unit's result from where it was published.

## B. A waiter waits on an aggregate signal instead of its own unit
- **Shape:** a join waits for "all admitted" while its own unit is already available,
  or a unit that hasn't started blocks it.
- **Probe:** saturate the connections or the budget so that the second unit can't
  start. A request for the first unit must return. Control: a direct read.
- **Fix:** proceed when the request's own unit is available; when it can't start, do
  the work directly.

## C. Admission and budget accounting
- **Shapes:** deferred bytes not charged; bytes of ended work never reclaimed; a
  recomputed charge driven negative; per-unit rounding ignored; a measured zero
  overridden by a floor.
- **Probe:** limits of a few units. Observe the charge after every add, merge, end and
  reclaim: ≥ 0, ≤ the limit, back to 0, and new work admitted after an end.
- **Fix:** one charge per byte owned, moved or released on every transition, with a
  measured bound and no floor over it.

## D. Unbounded fan-out before a slot is acquired
- **Shape:** one goroutine per unit (thousands) waits for a slot or a predecessor.
- **Probe:** the supported max input with the connections saturated; count goroutines
  and concurrent opens.
- **Fix:** one ordered dispatcher; start a goroutine only for a registered unit.

## E. A new exit or join point bypasses a policy flag
- **Shape:** a flag honored by the original path but not by a new join point or lookup.
- **Probe:** each flag × entry point while work for the same target is active or
  queued. The request must not wait for it or read from it.
- **Fix:** apply the sibling's guard at each new point.

## F. A lifecycle event does not reach work in a pending state
- **Shape:** delete cancels registered work, but work still waiting for admission or
  its first connection keeps going, or never releases its worker.
- **Probe:** fire delete or close in each state; opens after the event must be 0, and
  workers released.
- **Fix:** check the event's generation at every admission, and wake waiters from it.

## G. A blocking call before the cancellable guard
- **Shape:** connection setup runs before the idle timer or cancellation is armed.
- **Probe:** block in the setup and pass several scaled timeouts; the timer must fire.
  Control: the same stall in the body is cancelled.
- **Fix:** arm the guard before the first blocking call.

## H. A successful exit skips shared postconditions
- **Shape:** an early `return nil` skips the step that updates the earlier tiers.
- **Probe:** a caller tier that always misses; require one update for every way the
  request can be served.
- **Fix:** route every success exit through one finisher; audit the returns.

## I. Check-then-act across two structures (TOCTOU)
- **Shape:** the request misses the cache; the producer publishes and unregisters; the
  registry lookup then finds nothing; the work is done again.
- **Probe:** gate the request between the two lookups, let the producer publish and
  unregister, then release; the backend count must not change.
- **Fix:** when the producer publishes before it unregisters, look in the registry
  first and the cache second.

## J. Callback and publication ordering across workers
- **Shape:** publication in worker-completion order rather than submission order;
  skipped work holding places in the order.
- **Probe:** hold the first write and finish the second; its callback must not run
  first. Skipped work must not grow the chain.
- **Fix:** keep submission order; advance past skipped entries without parking.

## K. A faulty cache tier fails the request instead of falling through
- **Shape:** a tier's file is short, missing or unreadable; a request whose contract
  promises the data returns EOF or an I/O error although the source holds it, and the
  bad entry stays.
- **Probe:** a truncated, a missing and an unopenable entry. Expect the bytes from the
  next tier the request may use, and the bad entry not served again. Derive the
  backend reads and any refill from the configured tiers and the request's policy: a
  later cache tier may supply the bytes, a skip of cache writes refills nothing, and a
  cache-only read reports a miss. Control: decoder and caller errors are still
  returned.
- **Fix:** record the tier reader's error; on a tier failure drop the entry and fall
  through.

## L. A wait before registration is invisible to lifecycle events
- **Shape:** a unit waits for a slot before it registers, so delete has nothing to
  cancel.
- **Probe:** fill the slots, start the unit, delete. The unit must end and release its
  worker while every slot stays held, with 0 opens. Control: the same event while it
  waits for a connection.
- **Fix:** the wait checks the unit's delete mark, and the event wakes waiters.

## M. A callback waits for capacity it holds
- **Shape:** a callback submits work, deletes or closes while every slot is held by
  work kept for callbacks, or while its own unit is still running.
- **Probe:** capacity 1; the callback submits each kind of work and calls delete and
  close, each under a deadline.
- **Fix:** count capacity kept for callbacks and drop work only they could admit; run
  callbacks after the unit has ended.
