// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockservice

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

const (
	unknownCommitResolveRetryInterval = 100 * time.Millisecond
	resolverWorkers                   = 4
	resolverOwnerSendTimeout          = time.Second
	resolverRollbackTimeout           = 2 * resolverOwnerSendTimeout
)

type resolverContextKey struct{}

// resolverUnlockDeferred identifies an attempt that did not send. The owner
// lets the dispatcher park its transaction until that owner's slot is ready.
type resolverUnlockDeferred struct {
	owner string
}

func (resolverUnlockDeferred) Error() string { return "lock cleanup was deferred before sending" }

// Keep a fence after the client deadline to cover bounded clock skew between
// the CN that created the Commit and the TN that admits it. The TN rejects an
// expired Commit before allocator.Valid, so this is a safety margin rather
// than a retry window.
const unknownCommitFenceMinimumGrace = time.Second

// Bound one resolver-owned remote unlock attempt. Normal transaction unlocks
// continue to retry until they complete. An unknown result stays registered
// until the resolver completes the owner-side handoff or the source service is
// no longer available for orphan recovery.
var unknownCommitUnlockTimeout = defaultRPCTimeout

var _ UnknownCommitResolver = (*service)(nil)

// unknownCommitResolver owns transferred rollback and unknown-commit cleanup.
// A dispatcher gives distinct ready transactions to bounded workers. Owner
// slots limit concurrent network cleanup separately from transaction admission.
type unknownCommitResolver struct {
	service *service
	wakeC   chan struct{}

	mu struct {
		sync.Mutex
		pending map[string]unknownCommitTxn
		owners  map[string]*resolverOwner
		refresh bool
		fencing bool
		running bool
	}
}

type resolverOwner struct {
	active   bool
	retryAt  time.Time
	assigned string
	waiting  []string
}

func (o *resolverOwner) unavailable(now time.Time) bool {
	return o.active || now.Before(o.retryAt)
}

type unknownCommitTxn struct {
	rollback        bool
	id              []byte
	deadline        time.Time
	sequence        uint64
	onResolved      func()
	inFlight        bool
	retryAt         time.Time
	waitingForOwner string
}

// The transaction client authorizes one terminal operation per generation.
// This check keeps a resolver completion tied to the queued registration.
func (t unknownCommitTxn) sameResolution(other unknownCommitTxn) bool {
	return t.sequence == other.sequence && t.deadline.Equal(other.deadline)
}

func newUnknownCommitResolver(s *service) *unknownCommitResolver {
	r := &unknownCommitResolver{
		service: s,
		wakeC:   make(chan struct{}, 1),
	}
	r.mu.pending = make(map[string]unknownCommitTxn)
	r.mu.owners = make(map[string]*resolverOwner)
	return r
}

// ResolveCommitUnknown transfers lock cleanup to lockservice. It returns as
// soon as cleanup is scheduled. onResolved is called after terminal cleanup so
// the txn client can release admission independently of the frontend request.
func (s *service) ResolveCommitUnknown(
	txnID []byte,
	commitDeadline time.Time,
	commitSequence uint64,
	onResolved func(),
) error {
	if !s.beginResolverAdmission() {
		return moerr.NewInternalErrorNoCtx("lock service is closing")
	}
	defer s.endResolverAdmission()

	if s.unknownCommitResolver == nil {
		return moerr.NewInternalErrorNoCtx("unknown commit resolver is not initialized")
	}
	if commitDeadline.IsZero() {
		return moerr.NewInternalErrorNoCtx("unknown commit has no deadline")
	}
	if commitSequence == 0 {
		// Compatibility with a caller that does not yet provide a source
		// sequence. Its TN request is also legacy (sequence zero), so the
		// allocator will fail closed for legacy admission until the deadline.
		commitSequence = s.NextCommitSequence()
	}
	return s.unknownCommitResolver.enqueue(
		txnID,
		commitDeadline,
		commitSequence,
		onResolved,
	)
}

func (s *service) ResolveRollbackCleanup(txnID []byte, onResolved func()) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := s.unlockTransaction(ctx, txnID, timestamp.Timestamp{}, localRollbackCleanup)
	cancel()
	if err == nil {
		return false, nil
	}
	if !s.beginResolverAdmission() {
		return false, ErrLockTableBindChanged
	}
	defer s.endResolverAdmission()
	r := s.unknownCommitResolver
	if r == nil {
		return false, ErrLockTableBindChanged
	}
	return true, r.enqueue(txnID, time.Time{}, 0, onResolved)
}

// Remote cleanup can wait on a proxy mutex or a transport writer. Transfer
// that work before synchronously entering either on a frontend rollback.
func (s *service) canCloseRollbackLocallyLocked(txn *activeTxn) bool {
	if txn.remoteInFlight != 0 || len(txn.remoteCutoffs) != 0 || len(txn.blockedWaiters) != 0 {
		return false
	}
	local := true
	txn.iterLockTableBindsLocked(func(_ uint32, _ uint64, bind pb.LockTable) {
		local = local && bind.ServiceID == s.serviceID
	})
	return local
}

func (r *unknownCommitResolver) enqueue(
	txnID []byte,
	commitDeadline time.Time,
	commitSequence uint64,
	onResolved func(),
) error {
	key := string(txnID)
	id := append([]byte(nil), txnID...)

	r.mu.Lock()
	old, ok := r.mu.pending[key]
	if !ok || old.deadline.Before(commitDeadline) || old.sequence < commitSequence {
		if ok && old.onResolved != nil {
			onResolved = old.onResolved
		}
		r.mu.pending[key] = unknownCommitTxn{
			rollback:   commitDeadline.IsZero(),
			id:         id,
			deadline:   commitDeadline,
			sequence:   commitSequence,
			onResolved: onResolved,
			inFlight:   old.inFlight,
		}
	} else if old.onResolved == nil && onResolved != nil {
		old.onResolved = onResolved
		r.mu.pending[key] = old
	}
	r.mu.refresh = true
	if r.mu.running {
		r.mu.Unlock()
		r.wake()
		return nil
	}
	r.mu.running = true
	r.mu.Unlock()

	if err := r.service.stopper.RunTask(r.run); err != nil {
		r.mu.Lock()
		r.mu.running = false
		r.mu.Unlock()
		return err
	}
	return nil
}

func (r *unknownCommitResolver) run(ctx context.Context) {
	ctx = context.WithValue(ctx, resolverContextKey{}, r)
	jobs := make(chan unknownCommitTxn, resolverWorkers)
	done := make(chan struct{}, resolverWorkers)
	var workers sync.WaitGroup
	for range resolverWorkers {
		workers.Go(func() {
			for txn := range jobs {
				r.finishAttempt(txn, r.resolve(ctx, txn))
				done <- struct{}{}
			}
		})
	}
	defer func() {
		close(jobs)
		workers.Wait()
	}()
	ticker := time.NewTicker(unknownCommitResolveRetryInterval)
	defer ticker.Stop()

	var pending []unknownCommitTxn
	running, refresh := 0, true
	preferOwner := false
	for ctx.Err() == nil {
		if running < resolverWorkers && len(pending) == 0 {
			r.mu.Lock()
			refresh = refresh || r.mu.refresh
			r.mu.refresh = false
			r.mu.Unlock()
			if refresh {
				pending = r.pendingActiveTxns()
				refresh = false
			}
		}
		for running < resolverWorkers && ctx.Err() == nil {
			if preferOwner || len(pending) == 0 {
				if txn, ok := r.nextOwnerAttempt(); ok {
					running++
					jobs <- txn
					preferOwner = false
					continue
				}
			}
			if len(pending) == 0 {
				break
			}
			txn := pending[0]
			pending[0] = unknownCommitTxn{}
			pending = pending[1:]
			if r.startAttempt(txn) {
				running++
				jobs <- txn
				preferOwner = true
			}
		}
		if running == 0 && !r.hasPending() {
			return
		}
		if running < resolverWorkers && len(pending) == 0 && refresh {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-done:
			running--
		case <-r.wakeC:
		case <-ticker.C:
			refresh = true
		}
	}
}

func (r *unknownCommitResolver) startAttempt(txn unknownCommitTxn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := string(txn.id)
	current, ok := r.mu.pending[key]
	if !ok || current.inFlight || current.waitingForOwner != "" || !current.sameResolution(txn) {
		return false
	}
	current.inFlight = true
	r.mu.pending[key] = current
	return true
}

func (r *unknownCommitResolver) finishAttempt(txn unknownCommitTxn, err error) {
	key := string(txn.id)
	r.mu.Lock()
	if txn.waitingForOwner != "" {
		owner := r.mu.owners[txn.waitingForOwner]
		owner.assigned = ""
		r.forgetIdleOwnerLocked(txn.waitingForOwner, owner, time.Now())
	}
	current, ok := r.mu.pending[key]
	if !ok {
		r.mu.Unlock()
		return
	}
	if current.sameResolution(txn) {
		if err == nil {
			delete(r.mu.pending, key)
			r.mu.Unlock()
			dispatchUnknownCommitResolved(current.onResolved)
			return
		}
		deferred, _ := err.(resolverUnlockDeferred)
		current.waitingForOwner = deferred.owner
		if deferred.owner == "" {
			current.retryAt = time.Now().Add(unknownCommitResolveRetryInterval)
		} else {
			owner := r.ownerLocked(deferred.owner)
			owner.waiting = append(owner.waiting, key)
		}
	} else {
		// A previously enqueued replacement may have been skipped while
		// the earlier registration was still in flight.
		r.mu.refresh = true
	}
	current.inFlight = false
	r.mu.pending[key] = current
	r.mu.Unlock()
	r.wake()
}

func (r *unknownCommitResolver) resolve(ctx context.Context, txn unknownCommitTxn) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if txn.rollback {
		unlockCtx, cancel := context.WithTimeout(ctx, resolverRollbackTimeout)
		defer cancel()
		return r.service.unlockTransaction(unlockCtx, txn.id, timestamp.Timestamp{}, resolverCleanup)
	}
	// A single allocator outage must not occupy all workers before they can
	// try independent rollback routes.
	r.mu.Lock()
	if r.mu.fencing {
		r.mu.Unlock()
		return ErrLockTimeout
	}
	r.mu.fencing = true
	r.mu.Unlock()
	committing, fenceTS, ok := r.service.canUnlockUnknownCommits(
		ctx,
		[][]byte{txn.id},
		txn.deadline,
		txn.sequence,
	)
	r.mu.Lock()
	r.mu.fencing = false
	r.mu.Unlock()
	if !ok {
		return ErrLockTimeout
	}
	if _, ok := committing[string(txn.id)]; ok {
		return ErrLockTimeout
	}
	unlockCtx, cancel := context.WithTimeout(ctx, min(unknownCommitUnlockTimeout, resolverRollbackTimeout))
	defer cancel()
	return r.service.unlockUnknownCommit(unlockCtx, txn.id, fenceTS)
}

func (r *unknownCommitResolver) ownerLocked(id string) *resolverOwner {
	owner := r.mu.owners[id]
	if owner == nil {
		owner = &resolverOwner{}
		r.mu.owners[id] = owner
	}
	return owner
}

func (r *unknownCommitResolver) tryOwner(id, txnKey string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	owner := r.ownerLocked(id)
	if owner.unavailable(time.Now()) || (owner.assigned != "" && owner.assigned != txnKey) {
		return false
	}
	owner.active = true
	return true
}

func (r *unknownCommitResolver) forgetIdleOwnerLocked(id string, owner *resolverOwner, now time.Time) {
	if !owner.unavailable(now) && owner.assigned == "" && len(owner.waiting) == 0 {
		delete(r.mu.owners, id)
	}
}

func (r *unknownCommitResolver) finishOwner(id string, unavailable bool) {
	r.mu.Lock()
	owner := r.mu.owners[id]
	owner.active = false
	now := time.Now()
	if unavailable {
		owner.retryAt = now.Add(unknownCommitResolveRetryInterval)
	}
	r.forgetIdleOwnerLocked(id, owner, now)
	ready := !unavailable && owner.assigned == "" && len(owner.waiting) > 0
	r.mu.Unlock()
	if ready {
		r.wake()
	}
}

// Only one parked transaction per owner is handed to a worker at a time.
// A worker still takes the actual network slot at its remote unlock, since a
// multi-table transaction can visit another owner first.
func (r *unknownCommitResolver) nextOwnerAttempt() (unknownCommitTxn, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for id, owner := range r.mu.owners {
		if owner.assigned != "" || owner.unavailable(now) {
			continue
		}
		for len(owner.waiting) > 0 {
			key := owner.waiting[0]
			owner.waiting[0] = ""
			owner.waiting = owner.waiting[1:]
			txn, ok := r.mu.pending[key]
			if !ok || txn.waitingForOwner != id || txn.inFlight {
				continue
			}
			owner.assigned = key
			txn.inFlight = true
			r.mu.pending[key] = txn
			return txn, true
		}
		owner.waiting = nil
		r.forgetIdleOwnerLocked(id, owner, now)
	}
	return unknownCommitTxn{}, false
}

func (r *unknownCommitResolver) pendingActiveTxns() []unknownCommitTxn {
	r.mu.Lock()

	now := time.Now()
	var values []unknownCommitTxn
	var resolved []func()
	for txnKey, txn := range r.mu.pending {
		if txn.inFlight {
			continue
		}
		if !r.service.activeTxnHolder.hasActiveTxn(txn.id) {
			delete(r.mu.pending, txnKey)
			if txn.onResolved != nil {
				resolved = append(resolved, txn.onResolved)
			}
			continue
		}
		if txn.waitingForOwner == "" && !now.Before(txn.retryAt) {
			values = append(values, txn)
		}
	}
	r.mu.Unlock()

	for _, fn := range resolved {
		dispatchUnknownCommitResolved(fn)
	}
	return values
}

// dispatchUnknownCommitResolved runs external completion code outside the
// resolver's service-stopper task. The pending entry is removed under r.mu
// before dispatch, which is the exactly-once ownership transfer. In
// particular, a callback may re-enter service.Close: Close can cancel and join
// the resolver task without waiting for the callback that initiated it.
func dispatchUnknownCommitResolved(callback func()) {
	if callback == nil {
		return
	}
	go callback()
}

func (r *unknownCommitResolver) isPending(txnID []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, ok := r.mu.pending[string(txnID)]
	return ok
}

func (r *unknownCommitResolver) hasPending() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.mu.pending) > 0 {
		return true
	}
	r.mu.running = false
	return false
}

func (r *unknownCommitResolver) wake() {
	select {
	case r.wakeC <- struct{}{}:
	default:
	}
}

// takeResolvedCallbacks transfers every pending completion callback to the
// service Close owner after the resolver task and active transaction holder
// have stopped. The Close owner invokes external code only after sync.Once has
// completed, so a callback may safely re-enter Close.
func (r *unknownCommitResolver) takeResolvedCallbacks() []func() {
	r.mu.Lock()
	callbacks := make([]func(), 0, len(r.mu.pending))
	for key, txn := range r.mu.pending {
		delete(r.mu.pending, key)
		if txn.onResolved != nil {
			callbacks = append(callbacks, txn.onResolved)
		}
	}
	clear(r.mu.owners)
	r.mu.running = false
	r.mu.Unlock()
	return callbacks
}

func (s *service) canUnlockUnknownCommits(
	ctx context.Context,
	txnIDs [][]byte,
	commitDeadline time.Time,
	commitSequence uint64,
) (map[string]struct{}, timestamp.Timestamp, bool) {
	ctx, cancel := context.WithTimeout(ctx, defaultRPCTimeout)
	defer cancel()

	resp, err := s.notifyCannotCommit(ctx, []pb.OrphanTxn{{
		Service:          s.serviceID,
		Txn:              txnIDs,
		Persist:          true,
		ExpireAtUnixNano: s.unknownCommitFenceExpiry(commitDeadline).UnixNano(),
		CommitSequence:   commitSequence,
	}})
	if err != nil || resp.FenceTS.IsEmpty() {
		return nil, timestamp.Timestamp{}, false
	}

	committing := make(map[string]struct{}, len(resp.CommittingTxn))
	for _, txnID := range resp.CommittingTxn {
		committing[string(txnID)] = struct{}{}
	}
	return committing, resp.FenceTS, true
}

func (s *service) unknownCommitFenceExpiry(commitDeadline time.Time) time.Time {
	grace := unknownCommitFenceMinimumGrace
	if s.clock != nil && s.clock.MaxOffset() > grace/2 {
		grace = s.clock.MaxOffset() * 2
	}
	return commitDeadline.Add(grace)
}
