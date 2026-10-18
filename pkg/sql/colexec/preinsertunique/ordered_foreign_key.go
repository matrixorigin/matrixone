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

package preinsertunique

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Ordinary foreign keys keep their vectorized join path. Only checks whose
// truth depends on final candidate IDs or accepted self rows reach this arbiter.
func (preInsertUnique *PreInsertUnique) orderedForeignKeys() []*plan.OrderedForeignKey {
	if spec := preInsertUnique.PreInsertCtx.OrderedAutoIncrement; spec != nil {
		return spec.ForeignKeys
	}
	return preInsertUnique.PreInsertCtx.OrderedForeignKeys
}

func (preInsertUnique *PreInsertUnique) checkOrderedForeignKeys(proc *process.Process, input *batch.Batch, row int, generated bool, value uint64) (bool, error) {
	checks := preInsertUnique.orderedForeignKeys()
	if len(checks) == 0 {
		return true, nil
	}
	if err := proc.Ctx.Err(); err != nil {
		return false, err
	}
	if err := preInsertUnique.ensureOrderedWorkspace(proc); err != nil {
		return false, err
	}
	if preInsertUnique.ctr.ordered.foreignKeyResults == nil {
		preInsertUnique.ctr.ordered.foreignKeyResults = make([]orderedForeignKeyResult, len(checks))
	}
	for i, check := range checks {
		state := &preInsertUnique.ctr.ordered.foreignKeyResults[i]
		if state.lookup != nil {
			state.lookup.needsLock = false
		}
		valid, err := preInsertUnique.fillOrderedForeignKeyTuple(proc, state, check.ChildColumns, input, row, generated, value)
		if err != nil {
			return false, err
		}
		if !valid {
			continue
		} // MATCH SIMPLE
		if len(check.SelfParentColumns) != 0 {
			matches, err := preInsertUnique.currentRowSatisfiesForeignKey(proc, state, check, input, row, generated, value)
			if err != nil {
				return false, err
			}
			if matches {
				continue
			}
		}
		if err := preInsertUnique.prepareOrderedFKLookup(proc, check, state); err != nil {
			return false, err
		}
		l := state.lookup
		if err := evalOrderedFKKeys(proc, l.probeExecutors, state.tuple, l.probeValues); err != nil {
			return false, err
		}
		if state.parents != nil {
			ids, zs, err := state.iterator.Find(0, 1, l.probeValues)
			if err != nil {
				return false, err
			}
			if zs[0] != 0 && ids[0] != 0 {
				continue
			}
		}
		reusable := len(check.SelfParentColumns) == 0 && generated && len(check.ChildColumns) == 1 && check.ChildColumns[0].ColPos == preInsertUnique.PreInsertCtx.AutoIncrementColumn
		if reusable && state.valid && state.value == value {
			if !state.exists {
				return false, nil
			}
			l.needsLock = true
			continue
		}
		var found bool
		if check.ParentAccess.Kind == plan.OrderedForeignKeyAccess_RESIDENT {
			found, err = preInsertUnique.residentOrderedFKParent(proc, check, state)
		} else {
			found, err = preInsertUnique.pointOrderedFKParent(proc, check, state)
		}
		if reusable && err == nil {
			state.value, state.valid, state.exists = value, true, found
		}
		if err != nil || !found {
			return false, err
		}
	}
	// Do not lock or publish a partially accepted row. Refresh invalidates this
	// attempt; LockRows returns the existing retry error to its transaction owner.
	return true, preInsertUnique.lockOrderedFKWitnesses(proc, checks)
}

func (preInsertUnique *PreInsertUnique) lockOrderedFKWitnesses(proc *process.Process, checks []*plan.OrderedForeignKey) error {
	targets := make([]*plan.LockTarget, 0, len(checks))
	owners := make(map[*plan.LockTarget]*orderedFKLookup, len(checks))
	baseIDs := make(map[uint64]struct{}, len(checks))
	for i, check := range checks {
		l := preInsertUnique.ctr.ordered.foreignKeyResults[i].lookup
		if l == nil || !l.needsLock {
			continue
		}
		target := check.ParentAccess.LockTarget
		targets = append(targets, target)
		owners[target] = l
		baseIDs[check.TableDef.TblId] = struct{}{}
	}
	plan2.SortForeignKeyLockTargets(targets, baseIDs)
	for _, target := range targets {
		l := owners[target]
		rel := l.base
		if l.access != nil {
			rel = l.access
		}
		b := &batch.Batch{Vecs: []*vector.Vector{l.witness}}
		b.SetRowCount(1)
		if err := lockop.LockRows(proc.Base.SessionInfo.StorageEngine, proc, rel, target.TableId,
			b, 0, types.MustTypeFromPlan(target.PrimaryColTyp), target.Mode, lockpb.Sharding_None, 0); err != nil {
			return err
		}
	}
	return nil
}

func (preInsertUnique *PreInsertUnique) currentRowSatisfiesForeignKey(proc *process.Process, state *orderedForeignKeyResult, check *plan.OrderedForeignKey, input *batch.Batch, row int, generated bool, value uint64) (bool, error) {
	parentState := orderedForeignKeyResult{tuple: state.selfTuple}
	valid, err := preInsertUnique.fillOrderedForeignKeyTuple(proc, &parentState, check.SelfParentColumns, input, row, generated, value)
	state.selfTuple = parentState.tuple // Keep partial allocations owned on failure too.
	if err != nil || !valid {
		return false, err
	}
	if state.selfExecutor == nil {
		positions := make(map[int32]int32, len(check.ParentColumns))
		for i, pos := range check.ParentColumns {
			if _, exists := positions[pos]; !exists {
				positions[pos] = int32(i)
			}
		}
		predicate, err := remapOrderedFKExpr(check.Predicate, positions, false)
		if err != nil {
			return false, err
		}
		state.selfExecutor, err = colexec.NewExpressionExecutor(proc, predicate)
		if err != nil {
			return false, err
		}
	}
	parent := batch.Batch{Vecs: state.selfTuple}
	child := batch.Batch{Vecs: state.tuple}
	parent.SetRowCount(1)
	child.SetRowCount(1)
	result, err := state.selfExecutor.Eval(proc, []*batch.Batch{&parent, &child}, nil)
	if err != nil {
		return false, err
	}
	return !result.IsNull(0) && vector.GetFixedAtNoTypeCheck[bool](result, 0), nil
}

// Publish referenced parent keys only after every constraint has passed.
// Current-row self matching is local and never publishes a speculative key.
func (preInsertUnique *PreInsertUnique) recordOrderedForeignKeyParents(proc *process.Process, input *batch.Batch, row int, generated bool, value uint64) error {
	foreignKeys := preInsertUnique.orderedForeignKeys()
	if len(foreignKeys) == 0 {
		return nil
	}
	if preInsertUnique.ctr.ordered.foreignKeyResults == nil {
		preInsertUnique.ctr.ordered.foreignKeyResults = make([]orderedForeignKeyResult, len(foreignKeys))
	}
	for i, check := range foreignKeys {
		if len(check.SelfParentColumns) == 0 {
			continue
		}
		state := &preInsertUnique.ctr.ordered.foreignKeyResults[i]
		parentState := orderedForeignKeyResult{tuple: state.selfTuple}
		valid, err := preInsertUnique.fillOrderedForeignKeyTuple(proc, &parentState, check.SelfParentColumns, input, row, generated, value)
		state.selfTuple = parentState.tuple
		if err != nil {
			return err
		}
		if !valid {
			continue
		}
		if err := preInsertUnique.prepareOrderedFKLookup(proc, check, state); err != nil {
			return err
		}
		l := state.lookup
		if err := evalOrderedFKKeys(proc, l.selfExecutors, state.selfTuple, l.selfValues); err != nil {
			return err
		}
		if state.parents == nil {
			state.parents, err = hashmap.NewStrHashMapWithAllocation(false, proc.Mp(), preInsertUnique.hashAllocation)
			if err != nil {
				return err
			}
			state.iterator = state.parents.NewIterator()
		}
		if _, err := state.iterator.DetectDup(l.selfValues, 0); err != nil {
			return err
		}
	}
	return nil
}

func (preInsertUnique *PreInsertUnique) fillOrderedForeignKeyTuple(proc *process.Process, state *orderedForeignKeyResult, columns []*plan.ColRef, input *batch.Batch, row int, generated bool, value uint64) (bool, error) {
	if state.tuple == nil {
		state.tuple = make([]*vector.Vector, len(columns))
	}
	for i, col := range columns {
		if col.ColPos < 0 || int(col.ColPos) >= len(input.Vecs) {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid self-referencing foreign-key column")
		}
		src := input.Vecs[col.ColPos]
		if state.tuple[i] == nil {
			state.tuple[i] = vector.NewVec(*src.GetType())
		}
		dst := state.tuple[i]
		dst.CleanOnlyData()
		if generated && col.ColPos == preInsertUnique.PreInsertCtx.AutoIncrementColumn {
			if err := appendAutoIncrementValue(proc, dst, *src.GetType(), value); err != nil {
				return false, err
			}
		} else {
			index := vectorRowIndex(src, row)
			if src.IsNull(uint64(index)) {
				return false, nil
			}
			if err := dst.UnionOne(src, int64(index), proc.Mp()); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}
