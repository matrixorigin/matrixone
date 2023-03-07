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

package disttae

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func NewPartition() *Partition {
	lock := make(chan struct{}, 1)
	lock <- struct{}{}
	ret := &Partition{
		lock: lock,
	}
	ret.state.Store(NewPartitionState())
	return ret
}

type RowID types.Rowid

func (r RowID) Less(than RowID) bool {
	return bytes.Compare(r[:], than[:]) < 0
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

func rowIDToBlockID(rowID RowID) uint64 {
	id, _ := catalog.DecodeRowid(types.Rowid(rowID))
	return id
}

func blockIDFromRowID(rowID types.Rowid) uint64 {
	id, _ := catalog.DecodeRowid(rowID)
	return id
}

func (p Partitions) Snapshot() []*PartitionState {
	ret := make([]*PartitionState, 0, len(p))
	for _, partition := range p {
		ret = append(ret, partition.state.Load())
	}
	return ret
}
