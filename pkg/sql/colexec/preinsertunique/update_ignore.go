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

package preinsertunique

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// updateIgnoreKeyState is the execution-local overlay on the keys at the start
// of one alias stage. A present, non-live entry represents a released key.
// Both the hash table and its owner vectors use the query memory pool.
type updateIgnoreKeyState struct {
	keys    *hashmap.StrHashMap
	iter    hashmap.Iterator
	owners  *vector.Vector
	live    *vector.Vector
	keyType types.Type
	hasType bool
}

func (preInsertUnique *PreInsertUnique) prepareUpdateIgnore(proc *process.Process) error {
	ctx := preInsertUnique.PreInsertCtx
	if preInsertUnique.allocationAccount == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	count := len(ctx.UpdateNewKeys)
	if count == 0 || len(ctx.UpdateOldKeys) != count || len(ctx.UpdateInitialConflicts) != count ||
		ctx.UpdateRowId < 0 || len(ctx.UpdateOutputs) == 0 {
		return moerr.NewInvalidInput(proc.Ctx, "invalid UPDATE IGNORE arbitration context")
	}
	if len(preInsertUnique.ctr.updateIgnoreKeys) != 0 {
		return moerr.NewInternalError(proc.Ctx, "UPDATE IGNORE arbitration already prepared")
	}
	preInsertUnique.ctr.updateIgnoreKeys = make([]updateIgnoreKeyState, count)
	for i := range preInsertUnique.ctr.updateIgnoreKeys {
		state := &preInsertUnique.ctr.updateIgnoreKeys[i]
		var err error
		state.keys, err = hashmap.NewStrHashMapWithAllocations(false, proc.Mp(), preInsertUnique.hashAllocation, preInsertUnique.iteratorAllocation)
		if err != nil {
			preInsertUnique.freeUpdateIgnore(proc)
			return err
		}
		state.iter = state.keys.NewIterator()
		state.owners, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_Rowid.ToType(), preInsertUnique.vectorAllocation)
		if err != nil {
			preInsertUnique.freeUpdateIgnore(proc)
			return err
		}
		state.live, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_bool.ToType(), preInsertUnique.vectorAllocation)
		if err != nil {
			preInsertUnique.freeUpdateIgnore(proc)
			return err
		}
	}
	return nil
}

func (preInsertUnique *PreInsertUnique) freeUpdateIgnore(proc *process.Process) {
	for i := range preInsertUnique.ctr.updateIgnoreKeys {
		state := &preInsertUnique.ctr.updateIgnoreKeys[i]
		if state.iter != nil {
			hashmap.IteratorClearOwner(state.iter)
		}
		if state.keys != nil {
			state.keys.Free()
		}
		if state.owners != nil {
			state.owners.Free(proc.Mp())
		}
		if state.live != nil {
			state.live.Free(proc.Mp())
		}
	}
	preInsertUnique.ctr.updateIgnoreKeys = nil
}

func updateIgnoreVectorRow(vec *vector.Vector, row int) int {
	if vec.IsConst() {
		return 0
	}
	return row
}

func (state *updateIgnoreKeyState) find(vec *vector.Vector, row int) (int, bool, error) {
	if vec.IsNull(uint64(updateIgnoreVectorRow(vec, row))) {
		return 0, false, nil
	}
	groups, nonnull, err := state.iter.Find(row, 1, []*vector.Vector{vec})
	if err != nil {
		return 0, false, err
	}
	if nonnull[0] == 0 || groups[0] == 0 {
		return 0, false, nil
	}
	group := int(groups[0] - 1)
	if group < 0 || group >= state.owners.Length() || group >= state.live.Length() {
		return 0, false, moerr.NewInternalErrorNoCtx("invalid UPDATE IGNORE key owner")
	}
	return group, true, nil
}

func (state *updateIgnoreKeyState) set(proc *process.Process, vec *vector.Vector, row int, owner types.Rowid, live bool) error {
	if vec.IsNull(uint64(updateIgnoreVectorRow(vec, row))) {
		return nil
	}
	groups, nonnull, err := state.iter.Insert(row, 1, []*vector.Vector{vec})
	if err != nil {
		return err
	}
	if nonnull[0] == 0 || groups[0] == 0 {
		return nil
	}
	group := int(groups[0] - 1)
	if group == state.owners.Length() {
		if err = vector.AppendFixed(state.owners, types.Rowid{}, false, proc.Mp()); err != nil {
			return err
		}
		if err = vector.AppendFixed(state.live, false, false, proc.Mp()); err != nil {
			return err
		}
	}
	if group < 0 || group >= state.owners.Length() || group >= state.live.Length() {
		return moerr.NewInternalError(proc.Ctx, "invalid UPDATE IGNORE key owner")
	}
	if err = vector.SetFixedAtNoTypeCheck(state.owners, group, owner); err != nil {
		return err
	}
	return vector.SetFixedAtNoTypeCheck(state.live, group, live)
}

// The planner supplies one snapshot conflict bit per key. The overlay records
// only changes made by earlier accepted candidates of this alias. A candidate
// checks every key first and publishes releases/reservations only after all
// keys pass, so a rejected row cannot exclude an otherwise valid later row.
func (preInsertUnique *PreInsertUnique) callUpdateIgnore(proc *process.Process, result vm.CallResult) (vm.CallResult, error) {
	input := result.Batch
	ctx := preInsertUnique.PreInsertCtx
	if len(preInsertUnique.ctr.updateIgnoreKeys) != len(ctx.UpdateNewKeys) {
		return vm.CancelResult, moerr.NewInternalError(proc.Ctx, "UPDATE IGNORE arbitration is not prepared")
	}
	checkPos := func(pos int32) bool { return pos >= 0 && int(pos) < len(input.Vecs) && input.Vecs[pos] != nil }
	if !checkPos(ctx.UpdateRowId) || input.Vecs[ctx.UpdateRowId].GetType().Oid != types.T_Rowid {
		return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "invalid UPDATE IGNORE row identity")
	}
	for _, pos := range ctx.UpdateOutputs {
		if !checkPos(pos) {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "invalid UPDATE IGNORE output column")
		}
	}
	for i := range preInsertUnique.ctr.updateIgnoreKeys {
		oldPos, newPos, conflictPos := ctx.UpdateOldKeys[i], ctx.UpdateNewKeys[i], ctx.UpdateInitialConflicts[i]
		if !checkPos(oldPos) || !checkPos(newPos) || !checkPos(conflictPos) ||
			!input.Vecs[oldPos].GetType().Eq(*input.Vecs[newPos].GetType()) ||
			input.Vecs[conflictPos].GetType().Oid != types.T_bool {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "invalid UPDATE IGNORE key columns")
		}
		state := &preInsertUnique.ctr.updateIgnoreKeys[i]
		if !state.hasType {
			state.keyType, state.hasType = *input.Vecs[newPos].GetType(), true
		}
		if !state.keyType.Eq(*input.Vecs[newPos].GetType()) {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "UPDATE IGNORE key type changed between batches")
		}
	}
	sels := vector.GetSels()
	defer vector.PutSels(sels)
	sels = sels[:0]
	ids := input.Vecs[ctx.UpdateRowId]
	for row := 0; row < input.RowCount(); row++ {
		if row%256 == 0 {
			if err := proc.Ctx.Err(); err != nil {
				return vm.CancelResult, err
			}
		}
		idRow := updateIgnoreVectorRow(ids, row)
		if ids.IsNull(uint64(idRow)) {
			return vm.CancelResult, moerr.NewInternalError(proc.Ctx, "NULL UPDATE IGNORE row identity")
		}
		owner := vector.GetFixedAtNoTypeCheck[types.Rowid](ids, idRow)
		accepted := true
		for i := range preInsertUnique.ctr.updateIgnoreKeys {
			state := &preInsertUnique.ctr.updateIgnoreKeys[i]
			key := input.Vecs[ctx.UpdateNewKeys[i]]
			if key.IsNull(uint64(updateIgnoreVectorRow(key, row))) {
				continue
			}
			group, found, err := state.find(key, row)
			if err != nil {
				return vm.CancelResult, err
			}
			if found {
				if vector.GetFixedAtNoTypeCheck[bool](state.live, group) &&
					vector.GetFixedAtNoTypeCheck[types.Rowid](state.owners, group) != owner {
					accepted = false
					break
				}
				continue
			}
			conflict := input.Vecs[ctx.UpdateInitialConflicts[i]]
			conflictRow := updateIgnoreVectorRow(conflict, row)
			if !conflict.IsNull(uint64(conflictRow)) && vector.GetFixedAtNoTypeCheck[bool](conflict, conflictRow) {
				accepted = false
				break
			}
		}
		if !accepted {
			continue
		}
		for i := range preInsertUnique.ctr.updateIgnoreKeys {
			state := &preInsertUnique.ctr.updateIgnoreKeys[i]
			if err := state.set(proc, input.Vecs[ctx.UpdateOldKeys[i]], row, owner, false); err != nil {
				return vm.CancelResult, err
			}
			if err := state.set(proc, input.Vecs[ctx.UpdateNewKeys[i]], row, owner, true); err != nil {
				return vm.CancelResult, err
			}
		}
		sels = append(sels, int64(row))
	}
	if len(sels) == 0 {
		result.Batch = batch.EmptyBatch
		return result, nil
	}
	if preInsertUnique.ctr.buf == nil {
		preInsertUnique.ctr.buf = batch.NewWithSize(len(ctx.UpdateOutputs))
		for i, pos := range ctx.UpdateOutputs {
			output, err := vector.NewOffHeapVecWithTypeAndAllocation(*input.Vecs[pos].GetType(), preInsertUnique.vectorAllocation)
			if err != nil {
				return vm.CancelResult, err
			}
			preInsertUnique.ctr.buf.Vecs[i] = output
		}
	} else {
		preInsertUnique.ctr.buf.CleanOnlyData()
	}
	for i, pos := range ctx.UpdateOutputs {
		if err := preInsertUnique.ctr.buf.Vecs[i].Union(input.Vecs[pos], sels, proc.Mp()); err != nil {
			return vm.CancelResult, err
		}
	}
	preInsertUnique.ctr.buf.SetRowCount(len(sels))
	result.Batch = preInsertUnique.ctr.buf
	return result, nil
}
