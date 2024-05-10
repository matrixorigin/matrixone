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
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// a partition corresponds to a dn
type Partition struct {
	//lock is used to protect pointer of PartitionState from concurrent mutation
	lock  chan struct{}
	state atomic.Pointer[PartitionState]

	// assuming checkpoints will be consumed once
	checkpointConsumed atomic.Bool

	//current partitionState can serve snapshot read only if start <= ts <= end
	mu struct {
		sync.Mutex
		start types.TS
		end   types.TS
	}

	TableInfo   TableInfo
	TableInfoOK bool
}

type TableInfo struct {
	ID            uint64
	Name          string
	PrimarySeqnum int
}

func (p *Partition) CanServe(ts types.TS) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return ts.GreaterEq(&p.mu.start) && ts.LessEq(&p.mu.end)
}

func NewPartition() *Partition {
	lock := make(chan struct{}, 1)
	lock <- struct{}{}
	ret := &Partition{
		lock: lock,
	}
	ret.mu.start = types.MaxTs()
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

func (p *Partition) Lock(ctx context.Context) error {
	select {
	case <-p.lock:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Partition) Unlock() {
	p.lock <- struct{}{}
}

func (p *Partition) checkValid() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mu.start.LessEq(&p.mu.end)
}

func (p *Partition) UpdateStart(ts types.TS) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.start != types.MaxTs() {
		p.mu.start = ts
	}
}

// [start, end]
func (p *Partition) UpdateDuration(start types.TS, end types.TS) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.start = start
	p.mu.end = end
}

func (p *Partition) GetDuration() (types.TS, types.TS) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mu.start, p.mu.end
}

func (p *Partition) ConsumeSnapCkps(
	_ context.Context,
	ckps []*checkpoint.CheckpointEntry,
	fn func(
		ckp *checkpoint.CheckpointEntry,
		state *PartitionState,
	) error,
) (
	err error,
) {
	//Notice that checkpoints must contain only one or zero global checkpoint
	//followed by zero or multi continuous incremental checkpoints.
	state := p.state.Load()
	start := types.MaxTs()
	end := types.TS{}
	for _, ckp := range ckps {
		if err = fn(ckp, state); err != nil {
			return
		}
		if ckp.GetType() == checkpoint.ET_Global {
			start = ckp.GetEnd()
		}
		if ckp.GetType() == checkpoint.ET_Incremental {
			ckpstart := ckp.GetStart()
			if ckpstart.Less(&start) {
				start = ckpstart
			}
			ckpend := ckp.GetEnd()
			if ckpend.Greater(&end) {
				end = ckpend
			}
		}
	}
	if end.IsEmpty() {
		//only one global checkpoint.
		end = start
	}
	p.UpdateDuration(start, end)
	if !p.checkValid() {
		panic("invalid checkpoint")
	}

	return nil
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
	curState := p.state.Load()
	if len(curState.checkpoints) == 0 {
		p.UpdateDuration(types.TS{}, types.MaxTs())
		return nil
	}

	lockErr := p.Lock(ctx)
	if lockErr != nil {
		return lockErr
	}
	defer p.Unlock()

	curState = p.state.Load()
	if len(curState.checkpoints) == 0 {
		logutil.Infof("xxxx impossible path")
		p.UpdateDuration(types.TS{}, types.MaxTs())
		return nil
	}

	state := curState.Copy()

	if err := state.consumeCheckpoints(fn); err != nil {
		return err
	}

	p.UpdateDuration(state.start, types.MaxTs())

	if !p.state.CompareAndSwap(curState, state) {
		panic("concurrent mutation")
	}

	p.checkpointConsumed.Store(true)

	return
}

func (p *Partition) Truncate(ctx context.Context, ids [2]uint64, ts types.TS) error {
	err := p.Lock(ctx)
	if err != nil {
		return err
	}
	defer p.Unlock()
	curState := p.state.Load()

	state := curState.Copy()

	state.truncate(ids, ts)

	//TODO::update partition's start and end

	if !p.state.CompareAndSwap(curState, state) {
		panic("concurrent mutation")
	}

	return nil
}
