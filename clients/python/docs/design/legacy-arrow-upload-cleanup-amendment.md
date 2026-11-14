# Arrow probe cleanup admission amendment

Revision: `PR32671-ARROW-CLEANUP-2026-11-13-r2`.
Extends [the approved upload design](legacy-arrow-upload.md); r1 remains unchanged.

The public-upload regression
`test_pending_probe_close_keeps_upload_admission_bounded` fails before this fix:
two probe closes remain pending, but both staging operations and their permits
have already been released. A pool reset callback can remain blocked after the
physical driver has been aborted. Tracking tasks in a set does not bound them.

The existing staging operation owns the complete upload admission, including
its probe checkout's close. Register at most one probe-close future on that
operation before awaiting it. `finish()` may enqueue its existing unlink only
after the producer and the registered close have actually finished. Their
completion callbacks re-enter the same idempotent `finish()` owner. The existing
unlink completion remains the sole permit release owner.

Successful probes finish closing before staging begins. Error, timeout and
cancellation retain the charged operation while a close is pending, even before
a file exists. Return the primary error without adding another pending
`wait_cleanup` task for that still-running connection cleanup. Report this state
through an additive `connection_cleanup_pending` operation status field. Once
close completes, normal cleanup reclaims the operation and wakes admission.

The limit remains two staged operations, two CPU workers and one unlink worker.
Each operation can retain one probe-close future; there is no new worker, queue,
cache, retry, file or independent finalizer admission. Driver abort still precedes
asynchronous cleanup. Disconnect closes admission; reconnect retains the existing
generation guard and rejects pending database cleanup. Foreign close callbacks
cannot be forcibly completed, so their charged capacity must not be reset.

The deterministic regression must hold both closes after the public calls return
their timeout, verify a third upload waits before checkout/input/DDL, then cancel
that waiter and release the closes. Pending operations/finalizers must drain and
admission must be usable again. Preserve successful-close, primary-error,
generation, worker, real one-slot stream and all existing positive write oracles.
Run focused SQLAlchemy 1.4/2.0/2.1 and core-without-Arrow checks, then real native
Arrow and legacy Parquet analytics. Earlier unrelated online evidence is reusable
only with a recorded consumer/change map.
