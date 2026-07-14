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

// Bound one resolver-owned remote unlock attempt. Normal transaction unlocks
// continue to retry until they complete; an unknown result is already fenced,
// so a failed remote cleanup can safely be finished by orphan recovery.
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
		pending map[string][]byte
		running bool
	}
}

func newUnknownCommitResolver(s *service) *unknownCommitResolver {
	r := &unknownCommitResolver{
		service: s,
		wakeC:   make(chan struct{}, 1),
	}
	r.mu.pending = make(map[string][]byte)
	return r
}

// ResolveCommitUnknown transfers lock cleanup to lockservice. It returns as
// soon as cleanup is scheduled so the caller does not retain a frontend slot
// while TN resolves the Commit.
func (s *service) ResolveCommitUnknown(txnID []byte) error {
	if s.unknownCommitResolver == nil {
		return moerr.NewInternalErrorNoCtx("unknown commit resolver is not initialized")
	}
	return s.unknownCommitResolver.enqueue(txnID)
}

func (r *unknownCommitResolver) enqueue(txnID []byte) error {
	key := string(txnID)
	id := append([]byte(nil), txnID...)

	r.mu.Lock()
	r.mu.pending[key] = id
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
	txnIDs := r.pendingActiveTxnIDs()
	if len(txnIDs) == 0 || ctx.Err() != nil {
		return
	}

	committing, fenceTS, ok := r.service.canUnlockUnknownCommits(ctx, txnIDs)
	if !ok {
		return
	}
	for _, txnID := range txnIDs {
		if _, ok := committing[string(txnID)]; ok {
			continue
		}
		unlockCtx, cancel := context.WithTimeout(ctx, unknownCommitUnlockTimeout)
		err := r.service.unlockUnknownCommit(unlockCtx, txnID, fenceTS)
		cancel()
		if err != nil && ctx.Err() != nil {
			return
		}
		// unlockUnknownCommit removes the local holder before attempting a
		// remote unlock. If the bounded attempt fails, the persistent fence
		// makes remote orphan recovery safe, so it must not block this batch.
		r.remove(txnID)
	}
}

func (r *unknownCommitResolver) pendingActiveTxnIDs() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()

	values := make([][]byte, 0, len(r.mu.pending))
	for txnKey, txnID := range r.mu.pending {
		if !r.service.activeTxnHolder.hasActiveTxn(txnID) {
			delete(r.mu.pending, txnKey)
			continue
		}
		values = append(values, txnID)
	}
	return values
}

func (r *unknownCommitResolver) remove(txnID []byte) {
	r.mu.Lock()
	delete(r.mu.pending, string(txnID))
	r.mu.Unlock()
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
) (map[string]struct{}, timestamp.Timestamp, bool) {
	ctx, cancel := context.WithTimeout(ctx, defaultRPCTimeout)
	defer cancel()

	resp, err := s.notifyCannotCommit(ctx, []pb.OrphanTxn{{
		Service: s.serviceID,
		Txn:     txnIDs,
		Persist: true,
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
