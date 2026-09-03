// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggexec

import (
	"bytes"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var _ ExactCountDistinctSpillState = (*countColumnExec)(nil)

type countDistinctArgumentDrain struct {
	exec        *countColumnExec
	replacement *AllocationAccount
	keys        uint64
	bytes       uint64
	done        bool
}

func initBoundedDistinctWorkState(
	state *aggState,
	mp *mpool.MPool,
	length int32,
	capacity int32,
	allocation *AllocationAccount,
) error {
	if state == nil || mp == nil || length < 0 || capacity <= 0 ||
		length > capacity || capacity > AggBatchSize {
		return mpool.ErrAllocationAccountInvalid
	}
	counts, err := allocation.makeArgumentCounts(mp, int(capacity))
	if err != nil {
		return err
	}
	arenaBytes := 64 * 1024
	if capacity < 1024 {
		arenaBytes = 16 * 1024
	}
	buffer, err := allocation.allocArgumentArena(mp, arenaBytes)
	if err != nil {
		mpool.FreeSlice(mp, counts)
		return err
	}
	state.length = length
	state.capacity = capacity
	state.allocation = allocation
	state.argCnt = counts
	state.argbuf = buffer
	state.argSkl = arenaskl.NewSkiplist(arenaskl.NewArena(buffer), bytes.Compare)
	state.boundedArgumentGrowth = true
	return nil
}

func (exec *countColumnExec) SupportsExactCountDistinctSpill() bool {
	return exec != nil && exec.IsDistinct() && exec.aggInfo.saveArg
}

func (exec *countColumnExec) HasDistinctArguments() (bool, error) {
	if !exec.SupportsExactCountDistinctSpill() {
		return false, moerr.NewInternalErrorNoCtx(
			"aggregate does not support exact distinct argument spill")
	}
	for i := range exec.state {
		state := &exec.state[i]
		if state.argSkl == nil || state.length < 0 || state.capacity <= 0 {
			return false, moerr.NewInternalErrorNoCtx(
				"invalid exact distinct argument state")
		}
		for _, count := range state.argCnt[:state.length] {
			if count != 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

func (exec *countColumnExec) DistinctArgumentStats() (
	keys uint64,
	retainedBytes uint64,
	err error,
) {
	if !exec.SupportsExactCountDistinctSpill() {
		return 0, 0, moerr.NewInternalErrorNoCtx(
			"aggregate does not support exact distinct argument spill")
	}
	for i := range exec.state {
		state := &exec.state[i]
		if state.argSkl == nil || state.length < 0 || state.capacity <= 0 {
			return 0, 0, moerr.NewInternalErrorNoCtx(
				"invalid exact distinct argument state")
		}
		for _, count := range state.argCnt[:state.length] {
			if keys > math.MaxUint64-uint64(count) {
				return 0, 0, moerr.NewInternalErrorNoCtx(
					"exact distinct key count overflow")
			}
			keys += uint64(count)
		}
		resident := uint64(cap(state.argbuf)) + uint64(cap(state.argScratch))
		countBytes := uint64(cap(state.argCnt)) * uint64(4)
		if resident > math.MaxUint64-countBytes ||
			retainedBytes > math.MaxUint64-resident-countBytes {
			return 0, 0, moerr.NewInternalErrorNoCtx(
				"exact distinct retained byte count overflow")
		}
		retainedBytes += resident + countBytes
	}
	return keys, retainedBytes, nil
}

func (exec *countColumnExec) BeginArgumentDrain(
	replacement *AllocationAccount,
) (DistinctArgumentDrain, error) {
	if !exec.SupportsExactCountDistinctSpill() {
		return nil, moerr.NewInternalErrorNoCtx(
			"aggregate does not support exact distinct argument spill")
	}
	drain := &countDistinctArgumentDrain{
		exec:        exec,
		replacement: replacement,
	}
	var err error
	drain.keys, drain.bytes, err = exec.DistinctArgumentStats()
	if err != nil {
		drain.Abort()
		return nil, err
	}
	return drain, nil
}

func (d *countDistinctArgumentDrain) ForEach(
	fn func(group int, payload []byte) error,
) error {
	if d == nil || d.done || d.exec == nil || fn == nil {
		return moerr.NewInternalErrorNoCtx("invalid exact distinct drain")
	}
	for chunk := range d.exec.state {
		state := &d.exec.state[chunk]
		for row := 0; row < int(state.length); row++ {
			group := chunk*AggBatchSize + row
			if err := state.iter(uint16(row), func(key []byte) error {
				return fn(group, aggPayloadFromKey(&d.exec.aggInfo, key))
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *countDistinctArgumentDrain) KeyCount() uint64 {
	if d == nil {
		return 0
	}
	return d.keys
}

func (d *countDistinctArgumentDrain) RetainedBytes() uint64 {
	if d == nil {
		return 0
	}
	return d.bytes
}

func (d *countDistinctArgumentDrain) Commit() error {
	if d == nil || d.done || d.exec == nil {
		return moerr.NewInternalErrorNoCtx("invalid exact distinct drain commit")
	}
	for i := range d.exec.state {
		state := &d.exec.state[i]
		var replacement aggState
		if err := initBoundedDistinctWorkState(
			&replacement,
			d.exec.mp,
			state.length,
			state.capacity,
			d.replacement,
		); err != nil {
			return err
		}
		state.free(d.exec.mp)
		*state = replacement
	}
	d.replacement = nil
	d.done = true
	d.exec = nil
	return nil
}

func (d *countDistinctArgumentDrain) Abort() {
	if d == nil || d.done {
		return
	}
	d.replacement = nil
	d.done = true
	d.exec = nil
}

func (exec *countColumnExec) InsertDistinctArgument(
	group int,
	payload []byte,
) error {
	if !exec.SupportsExactCountDistinctSpill() || group < 0 {
		return moerr.NewInternalErrorNoCtx(
			"invalid exact distinct argument insertion")
	}
	if group >= exec.GetNumGroups() {
		return moerr.NewInternalErrorNoCtxf(
			"exact distinct argument group %d exceeds %d",
			group,
			exec.GetNumGroups(),
		)
	}
	x, y := exec.getXY(uint64(group))
	exec.state[x].boundedArgumentGrowth = true
	return exec.state[x].fillArg(exec.mp, y, payload, true)
}

func (exec *countColumnExec) RehomeDistinctArgumentState(
	allocation *AllocationAccount,
) error {
	if !exec.SupportsExactCountDistinctSpill() {
		return moerr.NewInternalErrorNoCtx(
			"aggregate does not support exact distinct argument rehome")
	}
	keys, _, err := exec.DistinctArgumentStats()
	if err != nil {
		return err
	}
	if keys != 0 {
		return moerr.NewInternalErrorNoCtx(
			"cannot rehome non-empty exact distinct argument state")
	}
	for i := range exec.state {
		state := &exec.state[i]
		var replacement aggState
		if err := initBoundedDistinctWorkState(
			&replacement, exec.mp, state.length, state.capacity, allocation,
		); err != nil {
			return err
		}
		state.free(exec.mp)
		*state = replacement
	}
	return nil
}

func (exec *countColumnExec) AddDistinctCountContribution(
	group int,
	count uint64,
	allocation *AllocationAccount,
) error {
	if !exec.SupportsExactCountDistinctSpill() ||
		group < 0 || group >= exec.GetNumGroups() || count > math.MaxInt64 {
		return moerr.NewInternalErrorNoCtx(
			"invalid exact distinct count contribution")
	}
	x, y := exec.getXY(uint64(group))
	for len(exec.distinctContributions) <= x {
		exec.distinctContributions = append(exec.distinctContributions, nil)
	}
	if exec.distinctContributions[x] == nil {
		contributions, err := allocation.newVector(types.T_int64.ToType())
		if err != nil {
			return err
		}
		// Contributions are installed only after the owning Group work set is
		// complete. Reserve the published rows, not the spare aggregate chunk
		// capacity: generic spill leaves are deliberately much smaller than
		// AggBatchSize under a hard statement account.
		if err := contributions.PreExtend(int(exec.state[x].length), exec.mp); err != nil {
			contributions.Free(exec.mp)
			return err
		}
		contributions.SetLength(int(exec.state[x].length))
		exec.distinctContributions[x] = contributions
	}
	values := vector.MustFixedColNoTypeCheck[int64](exec.distinctContributions[x])
	if int64(count) > math.MaxInt64-values[y] {
		return moerr.NewInternalErrorNoCtx("count distinct result overflow")
	}
	values[y] += int64(count)
	return nil
}
