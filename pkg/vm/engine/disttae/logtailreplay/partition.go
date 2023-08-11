// Copyright 2022 Matrix Origin
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

package logtailreplay

import (
	"bytes"
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// a partition corresponds to a dn
type Partition struct {
	lock  chan struct{}
	state atomic.Pointer[PartitionState]
	TS    timestamp.Timestamp // last updated timestamp

	// assuming checkpoints will be consumed once
	checkpointConsumed atomic.Bool
}

func NewPartition() *Partition {
	lock := make(chan struct{}, 1)
	lock <- struct{}{}
	ret := &Partition{
		lock: lock,
	}
	ret.state.Store(NewPartitionState(false))
	return ret
}

type RowID types.Rowid

func (r RowID) Less(than RowID) bool {
	return bytes.Compare(r[:], than[:]) < 0
}

func (p *Partition) Snapshot() *PartitionState {
	return p.state.Load()
}

func (*Partition) CheckPoint(ctx context.Context, ts timestamp.Timestamp) error {
	panic("unimplemented")
}

func (p *Partition) MutateState() (*PartitionState, func()) {
	curState := p.state.Load()
	state := curState.Copy()
	return state, func() {
		if !p.state.CompareAndSwap(curState, state) {
			panic("concurrent mutation")
		}
	}
}

func (p *Partition) Lock() <-chan struct{} {
	return p.lock
}

func (p *Partition) Unlock() {
	p.lock <- struct{}{}
}

func (p *Partition) ConsumeCheckpoints(
	ctx context.Context,
	fn func(
		checkpoint string,
		state *PartitionState,
	) error,
) (
	err error,
) {

	if p.checkpointConsumed.Load() {
		return nil
	}

	select {
	case <-p.Lock():
		defer p.Unlock()
	case <-ctx.Done():
		return ctx.Err()
	}

	curState := p.state.Load()
	if len(curState.checkpoints) == 0 {
		return nil
	}

	state := curState.Copy()

	if err := state.consumeCheckpoints(fn); err != nil {
		return err
	}

	if !p.state.CompareAndSwap(curState, state) {
		panic("concurrent mutation")
	}

	p.checkpointConsumed.Store(true)

	return
}
