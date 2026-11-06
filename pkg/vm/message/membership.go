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
	mu    sync.Mutex
	ready chan struct{}
	col   int
	typ   types.Type
	index *MembershipIndex
	err   error
}

// GetMembershipIndex builds at most once per JoinMap. Callers must hold their
// existing JoinMap reference throughout build, wait, and probe. No goroutine or
// second RHS owner is created. A nil index denotes the evaluator fallback.
func (jm *JoinMap) GetMembershipIndex(ctx context.Context, col int, typ types.Type) (*MembershipIndex, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	state := &jm.membership
	state.mu.Lock()
	if state.ready == nil {
		state.ready = make(chan struct{})
		state.col, state.typ = col, typ
		state.mu.Unlock()

		index, err := NewMembershipIndex(ctx, jm.batches, col, typ, jm.mpool)
		state.mu.Lock()
		state.index, state.err = index, err
		if err == context.Canceled || err == context.DeadlineExceeded {
			// Pipeline contexts can be cancelled independently. Return cancellation
			// to this builder, but leave live siblings the batch-only fallback.
			state.err = nil
		}
		close(state.ready)
		state.mu.Unlock()
		return index, err
	}
	ready := state.ready
	sameKey := state.col == col && state.typ == typ
	state.mu.Unlock()
	if !sameKey {
		return nil, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ready:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Closing ready publishes the immutable index and terminal error.
		return state.index, state.err
	}
}

func (state *membershipState) free() {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.index != nil {
		state.index.Free()
		state.index = nil
	}
}

func (state *membershipState) size() int64 {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.index == nil {
		return 0
	}
	return state.index.Size()
}
