// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type membershipState struct {
	mu        sync.Mutex
	ready     chan struct{}
	col       int
	typ       types.Type
	work      uint64
	threshold uint64
	disabled  bool
	index     *MembershipIndex
	err       error
}

// GetMembershipIndex credits completed evaluator work and builds at most once
// after 32 times the actual build rows. Call only with an unprocessed probe row.
// Callers hold their existing JoinMap reference throughout observation/build/use.
// Only the successful builder receives newly published native bytes to account;
// waiters, reuse and every fallback/failure receive zero.
func (jm *JoinMap) GetMembershipIndex(ctx context.Context, col int, typ types.Type, completedWork uint64) (*MembershipIndex, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if col < 0 || !SupportsMembershipType(typ) {
		return nil, 0, nil
	}
	state := &jm.membership
	state.mu.Lock()
	if state.threshold == 0 && !state.disabled && state.ready == nil {
		state.col, state.typ = col, typ
		// HashBuild publishes this actual count before SendJoinMap. Do not scan
		// RHS metadata (let alone keys) merely to admit the first probe.
		if jm.rowCnt <= 0 || membershipSlotCount(uint64(jm.rowCnt), jm.mpool.Cap()) == 0 {
			state.disabled = true
		} else {
			// Slot admission bounds rows to 64 MiB / 16, so 32*rows cannot overflow.
			state.threshold = 32 * uint64(jm.rowCnt)
		}
	}
	if state.col != col || state.typ != typ || state.disabled {
		state.mu.Unlock()
		return nil, 0, nil
	}
	if state.ready == nil {
		state.work += min(completedWork, state.threshold-state.work)
		if state.work < state.threshold {
			state.mu.Unlock()
			return nil, 0, nil
		}
		state.ready = make(chan struct{})
		state.mu.Unlock()

		index, err := NewMembershipIndex(ctx, jm.batches, col, typ, jm.rowCnt, jm.mpool)
		state.mu.Lock()
		state.index, state.err = index, err
		if err == context.Canceled || err == context.DeadlineExceeded {
			// Independently live siblings retain the evaluator, not this
			// builder's cancelled pipeline context.
			state.err = nil
		}
		close(state.ready)
		state.mu.Unlock()
		if index != nil {
			return index, index.Size(), err
		}
		return nil, 0, err
	}
	ready := state.ready
	state.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	case <-ready:
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		// Closing ready publishes the immutable index and terminal error.
		return state.index, 0, state.err
	}
}

func (state *membershipState) free() {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.index != nil {
		state.index.Free()
		state.index = nil
	}
	state.work, state.threshold = 0, 0
	state.disabled = true
}

func (state *membershipState) size() int64 {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.index == nil {
		return 0
	}
	return state.index.Size()
}
