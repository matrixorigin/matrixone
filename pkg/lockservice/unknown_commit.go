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

const unknownCommitResolveRetryInterval = 100 * time.Millisecond

// Completion callbacks only release txn-client admission and are required to
// return promptly. Keep a hard bound even when an external implementation
// violates that contract: resolver work never creates an unbounded number of
// detached goroutines.
const maxUnknownCommitCallbacks = 1024

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

// unknownCommitResolver owns lock cleanup after CN loses a Commit response.
// A single task deduplicates txn IDs and retries until the allocator can prove
// that a fenced unlock is safe.
type unknownCommitResolver struct {
	service   *service
	wakeC     chan struct{}
	callbacks unknownCommitCallbacks

	mu struct {
		sync.Mutex
		pending map[string]unknownCommitTxn
		running bool
	}
}

type unknownCommitTxn struct {
	id         []byte
	deadline   time.Time
	sequence   uint64
	onResolved *unknownCommitCallback
}

type unknownCommitCallbacks struct {
	mu struct {
		sync.Mutex
		sealed bool
	}
	slots chan struct{}
}

type unknownCommitCallback struct {
	fn      func()
	slots   chan struct{}
	release sync.Once
}

func newUnknownCommitCallbacks(limit int) unknownCommitCallbacks {
	return unknownCommitCallbacks{slots: make(chan struct{}, limit)}
}

func (c *unknownCommitCallbacks) admit(
	callback func(),
) (*unknownCommitCallback, error) {
	if callback == nil {
		return nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.sealed {
		return nil, moerr.NewInternalErrorNoCtx(
			"unknown commit callback dispatcher is stopping")
	}
	select {
	case c.slots <- struct{}{}:
		return &unknownCommitCallback{fn: callback, slots: c.slots}, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx(
			"unknown commit callback capacity exhausted")
	}
}

func (c *unknownCommitCallbacks) seal() {
	c.mu.Lock()
	c.mu.sealed = true
	c.mu.Unlock()
}

func (c *unknownCommitCallback) drop() {
	if c == nil {
		return
	}
	c.release.Do(func() { <-c.slots })
}

// dispatch transfers execution to external callback code before invoking it.
// Close therefore never waits for the callback that re-entered Close. The
// reservation is retained until return, bounding a contract-violating blocked
// callback without adding a goroutine to the resolver stopper.
func (c *unknownCommitCallback) dispatch() {
	if c == nil {
		return
	}
	go func() {
		defer c.drop()
		c.fn()
	}()
}

func newUnknownCommitResolver(s *service) *unknownCommitResolver {
	r := &unknownCommitResolver{
		service:   s,
		wakeC:     make(chan struct{}, 1),
		callbacks: newUnknownCommitCallbacks(maxUnknownCommitCallbacks),
	}
	r.mu.pending = make(map[string]unknownCommitTxn)
	return r
}

// ResolveCommitUnknown transfers lock cleanup to lockservice. It returns as
// soon as cleanup is scheduled. onResolved is called after terminal cleanup so
// the txn client can release admission independently of the frontend request.
// Callback ownership transfers only when this method returns nil.
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
	callback := old.onResolved
	var callbackErr error
	var admitted *unknownCommitCallback
	if callback == nil && onResolved != nil {
		admitted, callbackErr = r.callbacks.admit(onResolved)
		callback = admitted
	}
	if !ok || old.deadline.Before(commitDeadline) || old.sequence < commitSequence {
		r.mu.pending[key] = unknownCommitTxn{
			id:         id,
			deadline:   commitDeadline,
			sequence:   commitSequence,
			onResolved: callback,
		}
	} else if old.onResolved == nil && callback != nil {
		old.onResolved = callback
		r.mu.pending[key] = old
	}
	if r.mu.running {
		r.mu.Unlock()
		r.wake()
		return callbackErr
	}
	r.mu.running = true
	r.mu.Unlock()

	if err := r.service.stopper.RunTask(r.run); err != nil {
		r.mu.Lock()
		r.mu.running = false
		if admitted != nil {
			txn := r.mu.pending[key]
			if txn.onResolved == admitted {
				txn.onResolved = nil
				r.mu.pending[key] = txn
			}
		}
		r.mu.Unlock()
		admitted.drop()
		return err
	}
	return callbackErr
}

func (r *unknownCommitResolver) run(ctx context.Context) {
	for {
		r.resolvePending(ctx)
		if !r.hasPending() {
			return
		}

		timer := time.NewTimer(unknownCommitResolveRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-r.wakeC:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

func (r *unknownCommitResolver) resolvePending(ctx context.Context) {
	txns := r.pendingActiveTxns()
	if len(txns) == 0 || ctx.Err() != nil {
		return
	}

	for _, txn := range txns {
		committing, fenceTS, ok := r.service.canUnlockUnknownCommits(
			ctx,
			[][]byte{txn.id},
			txn.deadline,
			txn.sequence,
		)
		if !ok {
			continue
		}
		if _, ok := committing[string(txn.id)]; ok {
			continue
		}
		unlockCtx, cancel := context.WithTimeout(ctx, unknownCommitUnlockTimeout)
		err := r.service.unlockUnknownCommit(unlockCtx, txn.id, fenceTS)
		cancel()
		if err != nil && ctx.Err() != nil {
			return
		}
		if err == nil {
			r.remove(txn.id)
		}
	}
}

func (r *unknownCommitResolver) pendingActiveTxns() []unknownCommitTxn {
	r.mu.Lock()

	values := make([]unknownCommitTxn, 0, len(r.mu.pending))
	var resolved []*unknownCommitCallback
	for txnKey, txn := range r.mu.pending {
		if !r.service.activeTxnHolder.hasActiveTxn(txn.id) {
			delete(r.mu.pending, txnKey)
			if txn.onResolved != nil {
				resolved = append(resolved, txn.onResolved)
			}
			continue
		}
		values = append(values, txn)
	}
	r.mu.Unlock()

	for _, fn := range resolved {
		fn.dispatch()
	}
	return values
}

func (r *unknownCommitResolver) remove(txnID []byte) {
	r.mu.Lock()
	txn, ok := r.mu.pending[string(txnID)]
	if ok {
		delete(r.mu.pending, string(txnID))
	}
	r.mu.Unlock()
	if ok && txn.onResolved != nil {
		txn.onResolved.dispatch()
	}
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
// have stopped. Dispatch transfers execution out of the Close owner before
// invocation, so a callback may safely re-enter Close while teardown finishes.
func (r *unknownCommitResolver) takeResolvedCallbacks() []*unknownCommitCallback {
	r.mu.Lock()
	callbacks := make([]*unknownCommitCallback, 0, len(r.mu.pending))
	for key, txn := range r.mu.pending {
		delete(r.mu.pending, key)
		if txn.onResolved != nil {
			callbacks = append(callbacks, txn.onResolved)
		}
	}
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
