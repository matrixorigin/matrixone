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
	service *service
	wakeC   chan struct{}

	mu struct {
		sync.Mutex
		pending map[string]unknownCommitTxn
		running bool
	}
}

type unknownCommitTxn struct {
	id       []byte
	deadline time.Time
	sequence uint64
}

func newUnknownCommitResolver(s *service) *unknownCommitResolver {
	r := &unknownCommitResolver{
		service: s,
		wakeC:   make(chan struct{}, 1),
	}
	r.mu.pending = make(map[string]unknownCommitTxn)
	return r
}

// ResolveCommitUnknown transfers lock cleanup to lockservice. It returns as
// soon as cleanup is scheduled so the caller does not retain a frontend slot
// while TN resolves the Commit.
func (s *service) ResolveCommitUnknown(
	txnID []byte,
	commitDeadline time.Time,
	commitSequence uint64,
) error {
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
	return s.unknownCommitResolver.enqueue(txnID, commitDeadline, commitSequence)
}

func (r *unknownCommitResolver) enqueue(
	txnID []byte,
	commitDeadline time.Time,
	commitSequence uint64,
) error {
	key := string(txnID)
	id := append([]byte(nil), txnID...)

	r.mu.Lock()
	if old, ok := r.mu.pending[key]; !ok || old.deadline.Before(commitDeadline) ||
		old.sequence < commitSequence {
		r.mu.pending[key] = unknownCommitTxn{
			id:       id,
			deadline: commitDeadline,
			sequence: commitSequence,
		}
	}
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
	defer r.mu.Unlock()

	values := make([]unknownCommitTxn, 0, len(r.mu.pending))
	for txnKey, txn := range r.mu.pending {
		if !r.service.activeTxnHolder.hasActiveTxn(txn.id) {
			delete(r.mu.pending, txnKey)
			continue
		}
		values = append(values, txn)
	}
	return values
}

func (r *unknownCommitResolver) remove(txnID []byte) {
	r.mu.Lock()
	delete(r.mu.pending, string(txnID))
	r.mu.Unlock()
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
