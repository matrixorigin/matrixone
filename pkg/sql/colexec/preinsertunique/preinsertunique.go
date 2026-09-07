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

package preinsertunique

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	indexColPos int32 = iota
	pkColPos
	rowIdColPos
)

const opName = "pre_insert_unique"

func (preInsertUnique *PreInsertUnique) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": pre processing insert unique key")
}

func (preInsertUnique *PreInsertUnique) OpType() vm.OpType {
	return vm.PreInsertUnique
}

func (preInsertUnique *PreInsertUnique) Prepare(proc *process.Process) error {
	if preInsertUnique.OpAnalyzer == nil {
		preInsertUnique.OpAnalyzer = process.NewAnalyzer(preInsertUnique.GetIdx(), preInsertUnique.IsFirst, preInsertUnique.IsLast, "pre_insert_unique")
	} else {
		preInsertUnique.OpAnalyzer.Reset()
	}

	if preInsertUnique.PreInsertCtx == nil {
		return moerr.NewInvalidInput(proc.Ctx, "missing pre-insert unique context")
	}
	insertIgnore := preInsertUnique.PreInsertCtx.GetInsertIgnoreMultiDedup()
	odkuArbitration := preInsertUnique.PreInsertCtx.GetOdkuTargetArbitration()
	if !insertIgnore && !odkuArbitration {
		return nil
	}
	if insertIgnore && odkuArbitration {
		return moerr.NewInvalidInput(proc.Ctx, "conflicting ordered unique-key arbitration modes")
	}
	if len(preInsertUnique.PreInsertCtx.KeyColumns) == 0 ||
		preInsertUnique.PreInsertCtx.OutputColumns <= 0 {
		return moerr.NewInvalidInput(proc.Ctx, "invalid ordered unique-key arbitration context")
	}
	if insertIgnore && len(preInsertUnique.PreInsertCtx.KeyColumns) != len(preInsertUnique.PreInsertCtx.ConflictColumns) {
		return moerr.NewInvalidInput(proc.Ctx, "invalid INSERT IGNORE multi-key dedup context")
	}
	if preInsertUnique.PreInsertCtx.AutoIncrementReorder {
		ctx := preInsertUnique.PreInsertCtx
		if !insertIgnore || ctx.AutoIncrementKeyIndex < 0 ||
			int(ctx.AutoIncrementKeyIndex) >= len(ctx.KeyColumns) ||
			ctx.AutoIncrementColumn < 0 || ctx.AutoIncrementGeneratedColumn < 0 {
			return moerr.NewInvalidInput(proc.Ctx, "invalid INSERT IGNORE auto-increment reorder context")
		}
		if ctx.AutoIncrementOutputColumn < 0 || ctx.AutoIncrementOutputColumn >= ctx.OutputColumns {
			return moerr.NewInvalidInput(proc.Ctx, "invalid INSERT IGNORE auto-increment output column")
		}
	}
	if odkuArbitration && len(preInsertUnique.PreInsertCtx.KeyColumns) != len(preInsertUnique.PreInsertCtx.TargetColumns) {
		return moerr.NewInvalidInput(proc.Ctx, "invalid ODKU target arbitration context")
	}
	if odkuArbitration && preInsertUnique.allocationAccount == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if len(preInsertUnique.ctr.acceptedMaps) == 0 {
		keyCount := len(preInsertUnique.PreInsertCtx.KeyColumns)
		preInsertUnique.ctr.acceptedMaps = make([]*hashmap.StrHashMap, keyCount)
		preInsertUnique.ctr.acceptedIters = make([]hashmap.Iterator, keyCount)
		preInsertUnique.ctr.acceptedKeyVecs = make([][]*vector.Vector, keyCount)
		for i := range keyCount {
			accepted, err := hashmap.NewStrHashMapWithAllocation(
				false, proc.Mp(), preInsertUnique.hashAllocation)
			if err != nil {
				preInsertUnique.freeAcceptedState(proc)
				return err
			}
			preInsertUnique.ctr.acceptedMaps[i] = accepted
			preInsertUnique.ctr.acceptedIters[i] = accepted.NewIterator()
			preInsertUnique.ctr.acceptedKeyVecs[i] = make([]*vector.Vector, 1)
		}
		if odkuArbitration {
			preInsertUnique.ctr.acceptedRows = make([]*vector.Vector, keyCount)
			for i := range keyCount {
				acceptedRows, err := vector.NewOffHeapVecWithTypeAndAllocation(
					types.T_int64.ToType(), preInsertUnique.retainedAllocation)
				if err != nil {
					preInsertUnique.freeAcceptedState(proc)
					return err
				}
				preInsertUnique.ctr.acceptedRows[i] = acceptedRows
			}
		}
	}
	return nil
}

func (preInsertUnique *PreInsertUnique) initBuf(bat *batch.Batch, uniqueColumnPos []int32, pkPos int, isUpdate bool) {
	if preInsertUnique.ctr.buf != nil {
		preInsertUnique.ctr.buf.CleanOnlyData()
		return
	}

	if isUpdate {
		preInsertUnique.ctr.buf = batch.NewWithSize(3)
		preInsertUnique.ctr.buf.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName, catalog.Row_ID}
		preInsertUnique.ctr.buf.Vecs[2] = vector.NewVec(types.T_Rowid.ToType())
	} else {
		preInsertUnique.ctr.buf = batch.NewWithSize(2)
		preInsertUnique.ctr.buf.Attrs = []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName}
	}

	if len(uniqueColumnPos) == 1 {
		ukType := preInsertUnique.PreInsertCtx.UkType
		keyType := types.T(ukType.Id).ToType()
		keyType.Width = ukType.Width
		keyType.Scale = ukType.Scale
		preInsertUnique.ctr.buf.Vecs[0] = vector.NewVec(keyType)
	} else {
		preInsertUnique.ctr.buf.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	}
	preInsertUnique.ctr.buf.Vecs[1] = vector.NewVec(*bat.Vecs[pkPos].GetType())
}

func (preInsertUnique *PreInsertUnique) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := preInsertUnique.OpAnalyzer

	result, err := vm.ChildrenCall(preInsertUnique.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}

	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	if preInsertUnique.PreInsertCtx.GetInsertIgnoreMultiDedup() {
		return preInsertUnique.callInsertIgnoreMultiDedup(proc, result)
	}
	if preInsertUnique.PreInsertCtx.GetOdkuTargetArbitration() {
		return preInsertUnique.callODKUTargetArbitration(proc, result)
	}
	inputBat := result.Batch
	var bitMap *nulls.Nulls

	uniqueColumnPos := preInsertUnique.PreInsertCtx.Columns
	pkPos := int(preInsertUnique.PreInsertCtx.PkColumn)
	isUpdate := inputBat.Vecs[len(inputBat.Vecs)-1].GetType().Oid == types.T_Rowid
	preInsertUnique.initBuf(inputBat, uniqueColumnPos, pkPos, isUpdate)

	colCount := len(uniqueColumnPos)

	if colCount == 1 {
		pos := uniqueColumnPos[indexColPos]
		bitMap, err = util.CompactSingleIndexCol(inputBat.Vecs[pos], preInsertUnique.ctr.buf.Vecs[indexColPos], proc)
		if err != nil {
			return result, err
		}
	} else {
		vs := make([]*vector.Vector, colCount)
		for vIdx, pIdx := range uniqueColumnPos {
			vs[vIdx] = inputBat.Vecs[pIdx]
		}
		bitMap, err = util.SerialWithCompacted(vs, preInsertUnique.ctr.buf.Vecs[indexColPos], proc, &preInsertUnique.packers, util.DefaultPackerSize)
		if err != nil {
			return result, err
		}
	}
	preInsertUnique.ctr.buf.SetRowCount(preInsertUnique.ctr.buf.Vecs[0].Length())

	if err = util.CompactPrimaryCol(inputBat.Vecs[pkPos], preInsertUnique.ctr.buf.Vecs[pkColPos], bitMap, proc); err != nil {
		return result, err
	}

	if isUpdate {
		rowIdInBat := len(inputBat.Vecs) - 1
		if bitMap.IsEmpty() {
			err = preInsertUnique.ctr.buf.Vecs[rowIdColPos].UnionBatch(
				inputBat.Vecs[rowIdInBat], 0, inputBat.Vecs[rowIdInBat].Length(), nil, proc.Mp())
		} else {
			err = util.CompactRowIdCol(
				inputBat.Vecs[rowIdInBat], preInsertUnique.ctr.buf.Vecs[rowIdColPos], bitMap, proc)
		}
		if err != nil {
			return result, err
		}
	}
	result.Batch = preInsertUnique.ctr.buf
	return result, nil
}

// callODKUTargetArbitration assigns one stable target primary key to each
// logical ODKU action. Constraint order is significant: for each input row the
// first pre-statement or statement-local conflict wins. Only a row with no
// conflict is an INSERT and atomically publishes all of its non-NULL keys.
// UPDATE actions deliberately publish none of their candidate input keys,
// because ODKU rejects assignments to UNIQUE columns and therefore those keys
// never become part of the stored row.
func (preInsertUnique *PreInsertUnique) callODKUTargetArbitration(
	proc *process.Process,
	result vm.CallResult,
) (vm.CallResult, error) {
	inputBat := result.Batch
	ctx := preInsertUnique.PreInsertCtx
	outputColumns := int(ctx.OutputColumns)
	pkColumn := int(ctx.PkColumn)
	if outputColumns > len(inputBat.Vecs) || pkColumn < 0 || pkColumn >= len(inputBat.Vecs) {
		return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "invalid ODKU target arbitration output")
	}
	for i := range ctx.KeyColumns {
		keyPos, targetPos := ctx.KeyColumns[i], ctx.TargetColumns[i]
		if keyPos < 0 || int(keyPos) >= len(inputBat.Vecs) ||
			targetPos < 0 || int(targetPos) >= len(inputBat.Vecs) {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "invalid ODKU target arbitration column")
		}
		if !inputBat.Vecs[targetPos].GetType().Eq(*inputBat.Vecs[pkColumn].GetType()) {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "ODKU target primary-key type mismatch")
		}
		preInsertUnique.ctr.acceptedKeyVecs[i][0] = inputBat.Vecs[keyPos]
	}
	if preInsertUnique.ctr.acceptedTarget == nil {
		var err error
		preInsertUnique.ctr.acceptedTarget, err = vector.NewOffHeapVecWithTypeAndAllocation(
			*inputBat.Vecs[pkColumn].GetType(), preInsertUnique.retainedAllocation)
		if err != nil {
			return vm.CancelResult, err
		}
	} else if !preInsertUnique.ctr.acceptedTarget.GetType().Eq(*inputBat.Vecs[pkColumn].GetType()) {
		return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx,
			"ODKU target primary-key type changed between input batches")
	}

	if preInsertUnique.ctr.buf == nil {
		preInsertUnique.ctr.buf = batch.NewWithSize(outputColumns + 1)
		if len(inputBat.Attrs) >= outputColumns {
			attrs := append([]string(nil), inputBat.Attrs[:outputColumns]...)
			preInsertUnique.ctr.buf.SetAttributes(append(attrs, "__mo_odku_target_pk"))
		}
		for i, vec := range inputBat.Vecs[:outputColumns] {
			preInsertUnique.ctr.buf.Vecs[i] = vector.NewVec(*vec.GetType())
		}
		preInsertUnique.ctr.buf.Vecs[outputColumns] = vector.NewVec(*inputBat.Vecs[pkColumn].GetType())
	} else {
		preInsertUnique.ctr.buf.CleanOnlyData()
	}
	for i := 0; i < outputColumns; i++ {
		if err := preInsertUnique.ctr.buf.Vecs[i].UnionBatch(
			inputBat.Vecs[i], 0, inputBat.RowCount(), nil, proc.Mp()); err != nil {
			return vm.CancelResult, err
		}
	}
	targetOutput := preInsertUnique.ctr.buf.Vecs[outputColumns]

	for row := 0; row < inputBat.RowCount(); row++ {
		conflictKey := -1
		conflictGroup := uint64(0)
		conflictTargetColumn := int32(-1)
		for keyIdx, targetPos := range ctx.TargetColumns {
			targetVec := inputBat.Vecs[targetPos]
			if !targetVec.IsNull(uint64(row)) {
				conflictTargetColumn = targetPos
				break
			}
			keyVec := preInsertUnique.ctr.acceptedKeyVecs[keyIdx][0]
			if keyVec.IsNull(uint64(row)) {
				continue
			}
			vals, zvals, err := preInsertUnique.ctr.acceptedIters[keyIdx].Find(
				row, 1, preInsertUnique.ctr.acceptedKeyVecs[keyIdx])
			if err != nil {
				return vm.CancelResult, err
			}
			if zvals[0] != 0 && vals[0] != 0 {
				conflictKey, conflictGroup = keyIdx, vals[0]
				break
			}
		}

		switch {
		case conflictTargetColumn >= 0:
			if err := targetOutput.UnionOne(
				inputBat.Vecs[conflictTargetColumn], int64(row), proc.Mp()); err != nil {
				return vm.CancelResult, err
			}
		case conflictKey >= 0:
			groupIdx := int(conflictGroup - 1)
			if groupIdx >= preInsertUnique.ctr.acceptedRows[conflictKey].Length() {
				return vm.CancelResult, moerr.NewInternalError(proc.Ctx,
					"ODKU accepted-key target ordinal is missing")
			}
			if err := targetOutput.UnionOne(
				preInsertUnique.ctr.acceptedTarget,
				vector.GetFixedAtNoTypeCheck[int64](
					preInsertUnique.ctr.acceptedRows[conflictKey], groupIdx), proc.Mp()); err != nil {
				return vm.CancelResult, err
			}
		default:
			if inputBat.Vecs[pkColumn].IsNull(uint64(row)) {
				return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "NULL ODKU insertion target primary key")
			}
			if err := targetOutput.UnionOne(inputBat.Vecs[pkColumn], int64(row), proc.Mp()); err != nil {
				return vm.CancelResult, err
			}
			targetRow := int64(preInsertUnique.ctr.acceptedTarget.Length())
			if err := preInsertUnique.ctr.acceptedTarget.UnionOne(
				inputBat.Vecs[pkColumn], int64(row), proc.Mp()); err != nil {
				return vm.CancelResult, err
			}
			for keyIdx := range ctx.KeyColumns {
				keyVec := preInsertUnique.ctr.acceptedKeyVecs[keyIdx][0]
				if keyVec.IsNull(uint64(row)) {
					continue
				}
				isNew, err := preInsertUnique.ctr.acceptedIters[keyIdx].DetectDup(
					preInsertUnique.ctr.acceptedKeyVecs[keyIdx], row)
				if err != nil {
					return vm.CancelResult, err
				}
				if !isNew {
					return vm.CancelResult, moerr.NewInternalError(proc.Ctx,
						"ODKU accepted-key state changed during row commit")
				}
				if err := vector.AppendFixed(
					preInsertUnique.ctr.acceptedRows[keyIdx], targetRow, false, proc.Mp()); err != nil {
					return vm.CancelResult, err
				}
			}
		}
	}
	preInsertUnique.ctr.buf.SetRowCount(inputBat.RowCount())
	result.Batch = preInsertUnique.ctr.buf
	return result, nil
}

func (preInsertUnique *PreInsertUnique) callInsertIgnoreMultiDedup(
	proc *process.Process,
	result vm.CallResult,
) (vm.CallResult, error) {
	inputBat := result.Batch
	ctx := preInsertUnique.PreInsertCtx
	keyColumns := ctx.KeyColumns
	conflictColumns := ctx.ConflictColumns
	autoIncrementReorder := ctx.AutoIncrementReorder
	outputColumns := int(ctx.OutputColumns)
	if len(keyColumns) == 0 || len(keyColumns) != len(conflictColumns) ||
		outputColumns <= 0 || outputColumns > len(inputBat.Vecs) {
		return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "invalid INSERT IGNORE multi-key dedup output width")
	}
	var autoIncrementVec, autoIncrementGeneratedVec *vector.Vector
	var assignedCandidates map[int]*autoIncrementCandidate
	autoOutputColumn := -1
	if autoIncrementReorder {
		autoColumn := int(ctx.AutoIncrementColumn)
		autoOutputColumn = int(ctx.AutoIncrementOutputColumn)
		markerColumn := int(ctx.AutoIncrementGeneratedColumn)
		if autoColumn < 0 || markerColumn < 0 || markerColumn >= len(inputBat.Vecs) ||
			autoOutputColumn < 0 || autoOutputColumn >= outputColumns {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "invalid INSERT IGNORE auto-increment reorder column")
		}
		if autoColumn >= len(inputBat.Vecs) {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "invalid INSERT IGNORE auto-increment input column")
		}
		autoIncrementVec = inputBat.Vecs[autoColumn]
		autoIncrementGeneratedVec = inputBat.Vecs[markerColumn]
		if autoIncrementGeneratedVec.GetType().Oid != types.T_bool {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "INSERT IGNORE auto-increment provenance is not boolean")
		}
		if preInsertUnique.ctr.acceptedAutoIncrementValues == nil {
			preInsertUnique.ctr.acceptedAutoIncrementValues = make(map[uint64]struct{})
		}
		assignedCandidates = make(map[int]*autoIncrementCandidate)
	}
	for i := range keyColumns {
		if keyColumns[i] < 0 || int(keyColumns[i]) >= len(inputBat.Vecs) ||
			conflictColumns[i] < 0 || int(conflictColumns[i]) >= len(inputBat.Vecs) {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "invalid INSERT IGNORE multi-key dedup column")
		}
		if !inputBat.Vecs[conflictColumns[i]].GetType().IsBoolean() {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "INSERT IGNORE conflict marker is not boolean")
		}
		preInsertUnique.ctr.acceptedKeyVecs[i][0] = inputBat.Vecs[keyColumns[i]]
	}

	sels := vector.GetSels()
	defer vector.PutSels(sels)
	sels = sels[:0]
	for row := 0; row < inputBat.RowCount(); row++ {
		generated := false
		var autoValue uint64
		var explicitAdvancesSequence bool
		if autoIncrementReorder {
			markerRow := vectorRowIndex(autoIncrementGeneratedVec, row)
			generated = !autoIncrementGeneratedVec.IsNull(uint64(markerRow)) &&
				vector.GetFixedAtNoTypeCheck[bool](autoIncrementGeneratedVec, markerRow)
			var err error
			if generated {
				autoValue, err = autoIncrementCandidateValue(autoIncrementVec, row)
			} else {
				autoValue, explicitAdvancesSequence, err = autoIncrementValue(autoIncrementVec, row)
			}
			if err != nil {
				return vm.CancelResult, err
			}
		}
		accepted := true
		primaryKeyConflict := false
		for keyIdx, conflictPos := range conflictColumns {
			conflictVec := inputBat.Vecs[conflictPos]
			conflictRow := vectorRowIndex(conflictVec, row)
			if !conflictVec.GetNulls().Contains(uint64(conflictRow)) &&
				vector.GetFixedAtNoTypeCheck[bool](conflictVec, conflictRow) {
				if autoIncrementReorder && keyIdx == int(ctx.AutoIncrementKeyIndex) {
					primaryKeyConflict = true
				}
				accepted = false
				break
			}
			keyVec := preInsertUnique.ctr.acceptedKeyVecs[keyIdx][0]
			keyRow := vectorRowIndex(keyVec, row)
			if keyVec.GetNulls().Contains(uint64(keyRow)) {
				continue
			}
			if autoIncrementReorder && keyIdx == int(ctx.AutoIncrementKeyIndex) {
				if _, exists := preInsertUnique.ctr.acceptedAutoIncrementValues[autoValue]; exists {
					// The input candidate may differ from the final value of a
					// previously accepted row after ordered reuse.  Enforce the
					// primary-key postcondition against final values.
					accepted = false
					break
				}
				// acceptedIters contains input candidates for the old path. It
				// cannot be consulted for this primary key because ordered
				// assignment may replace a candidate with an earlier final key.
				continue
			}
			vals, zvals, err := preInsertUnique.ctr.acceptedIters[keyIdx].Find(
				row, 1, preInsertUnique.ctr.acceptedKeyVecs[keyIdx])
			if err != nil {
				return vm.CancelResult, err
			}
			if zvals[0] != 0 && vals[0] != 0 {
				accepted = false
				break
			}
		}
		if !accepted {
			if autoIncrementReorder && generated && !primaryKeyConflict {
				// A row rejected by a secondary unique key did not publish a
				// primary key. Its allocator candidate remains available to the
				// next accepted generated row, preserving input order across
				// PRE_INSERT_UK batches. Candidates rejected by the primary-key
				// join, or already represented by a final key, remain unusable.
				if _, alreadyFinal := preInsertUnique.ctr.acceptedAutoIncrementValues[autoValue]; !alreadyFinal {
					if err := preInsertUnique.appendAutoIncrementCandidate(proc, autoIncrementVec, row); err != nil {
						return vm.CancelResult, err
					}
				}
			}
			continue
		}

		// Commit every key only after the complete row has passed.  This is the
		// ownership boundary that prevents a row rejected by one constraint from
		// reserving another key for later rows.
		for keyIdx := range keyColumns {
			keyVec := preInsertUnique.ctr.acceptedKeyVecs[keyIdx][0]
			keyRow := vectorRowIndex(keyVec, row)
			if keyVec.GetNulls().Contains(uint64(keyRow)) {
				continue
			}
			if autoIncrementReorder && keyIdx == int(ctx.AutoIncrementKeyIndex) {
				// The primary-key state is the final-value map for ordered
				// assignment; acceptedIters retains only input candidates.
				continue
			}
			isNew, err := preInsertUnique.ctr.acceptedIters[keyIdx].DetectDup(
				preInsertUnique.ctr.acceptedKeyVecs[keyIdx], row)
			if err != nil {
				return vm.CancelResult, err
			}
			if !isNew {
				return vm.CancelResult, moerr.NewInternalError(proc.Ctx,
					"INSERT IGNORE multi-key dedup accepted-set changed during row commit")
			}
		}
		if autoIncrementReorder {
			if generated {
				// Assign at the acceptance boundary, before a later input row is
				// considered. This preserves input-order ownership even when the
				// candidate was retained from a preceding input batch.
				if err := preInsertUnique.appendAutoIncrementCandidate(proc, autoIncrementVec, row); err != nil {
					return vm.CancelResult, err
				}
				candidate, ok, err := preInsertUnique.popAutoIncrementCandidate()
				if err != nil {
					return vm.CancelResult, err
				}
				if !ok {
					return vm.CancelResult, moerr.NewInternalError(proc.Ctx,
						"INSERT IGNORE accepted generated row has no reusable auto-increment candidate")
				}
				assignedCandidates[row] = candidate
				preInsertUnique.ctr.acceptedAutoIncrementValues[candidate.value] = struct{}{}
			} else {
				if explicitAdvancesSequence {
					if err := preInsertUnique.ctr.autoIncrementCandidates.discardThrough(autoValue); err != nil {
						return vm.CancelResult, err
					}
				}
				preInsertUnique.ctr.acceptedAutoIncrementValues[autoValue] = struct{}{}
			}
		}
		sels = append(sels, int64(row))
	}

	if len(sels) == 0 {
		if err := preInsertUnique.compactAutoIncrementCandidates(proc); err != nil {
			return vm.CancelResult, err
		}
		result.Batch = batch.EmptyBatch
		return result, nil
	}
	if preInsertUnique.ctr.buf == nil {
		preInsertUnique.ctr.buf = batch.NewWithSize(outputColumns)
		if len(inputBat.Attrs) >= outputColumns {
			preInsertUnique.ctr.buf.SetAttributes(inputBat.Attrs[:outputColumns])
		}
		for i, vec := range inputBat.Vecs[:outputColumns] {
			preInsertUnique.ctr.buf.Vecs[i] = vector.NewVec(*vec.GetType())
		}
	} else {
		preInsertUnique.ctr.buf.CleanOnlyData()
	}
	if err := preInsertUnique.ctr.buf.Union(inputBat, sels, proc.Mp()); err != nil {
		return vm.CancelResult, err
	}
	if autoIncrementReorder {
		if len(assignedCandidates) > 0 {
			autoIncrementOutput := vector.NewVec(*autoIncrementVec.GetType())
			for _, selected := range sels {
				row := int(selected)
				candidate, ok := assignedCandidates[row]
				if ok {
					if err := appendAutoIncrementValue(
						proc, autoIncrementOutput, candidate.typ, candidate.value); err != nil {
						autoIncrementOutput.Free(proc.Mp())
						return vm.CancelResult, err
					}
				} else if err := autoIncrementOutput.UnionOne(
					autoIncrementVec, int64(vectorRowIndex(autoIncrementVec, row)), proc.Mp()); err != nil {
					autoIncrementOutput.Free(proc.Mp())
					return vm.CancelResult, err
				}
			}
			preInsertUnique.ctr.buf.Vecs[autoOutputColumn].Free(proc.Mp())
			preInsertUnique.ctr.buf.Vecs[autoOutputColumn] = autoIncrementOutput
			for _, candidate := range assignedCandidates {
				proc.SetStatementLastInsertIDIfEarlier(candidate.value)
			}
		}
		if err := preInsertUnique.compactAutoIncrementCandidates(proc); err != nil {
			return vm.CancelResult, err
		}
	}
	result.Batch = preInsertUnique.ctr.buf
	return result, nil
}

type autoIncrementCandidate struct {
	typ   types.Type
	value uint64
}

func vectorRowIndex(vec *vector.Vector, row int) int {
	if vec != nil && vec.IsConst() {
		return 0
	}
	return row
}

func autoIncrementValue(vec *vector.Vector, row int) (uint64, bool, error) {
	if vec == nil {
		return 0, false, moerr.NewInvalidInputNoCtx("missing auto-increment value vector")
	}
	row = vectorRowIndex(vec, row)
	if vec.IsNull(uint64(row)) {
		return 0, false, moerr.NewInvalidInputNoCtx("NULL auto-increment value cannot be arbitrated")
	}
	switch vec.GetType().Oid {
	case types.T_int8:
		value := vector.GetFixedAtNoTypeCheck[int8](vec, row)
		return uint64(value), value > 0, nil
	case types.T_int16:
		value := vector.GetFixedAtNoTypeCheck[int16](vec, row)
		return uint64(value), value > 0, nil
	case types.T_int32:
		value := vector.GetFixedAtNoTypeCheck[int32](vec, row)
		return uint64(value), value > 0, nil
	case types.T_int64:
		value := vector.GetFixedAtNoTypeCheck[int64](vec, row)
		return uint64(value), value > 0, nil
	case types.T_uint8:
		value := uint64(vector.GetFixedAtNoTypeCheck[uint8](vec, row))
		return value, value > 0, nil
	case types.T_uint16:
		value := uint64(vector.GetFixedAtNoTypeCheck[uint16](vec, row))
		return value, value > 0, nil
	case types.T_uint32:
		value := uint64(vector.GetFixedAtNoTypeCheck[uint32](vec, row))
		return value, value > 0, nil
	case types.T_uint64:
		value := vector.GetFixedAtNoTypeCheck[uint64](vec, row)
		return value, value > 0, nil
	default:
		return 0, false, moerr.NewInvalidInputNoCtxf("unsupported auto-increment value type %s", vec.GetType().Oid.String())
	}
}

func autoIncrementCandidateValue(vec *vector.Vector, row int) (uint64, error) {
	if vec == nil {
		return 0, moerr.NewInvalidInputNoCtx("missing auto-increment value vector")
	}
	row = vectorRowIndex(vec, row)
	if vec.IsNull(uint64(row)) {
		return 0, moerr.NewInvalidInputNoCtx("NULL auto-increment value cannot be a generated candidate")
	}
	switch vec.GetType().Oid {
	case types.T_int8:
		value := vector.GetFixedAtNoTypeCheck[int8](vec, row)
		if value < 0 {
			return 0, moerr.NewInvalidInputNoCtx("negative auto-increment candidate")
		}
		return uint64(value), nil
	case types.T_int16:
		value := vector.GetFixedAtNoTypeCheck[int16](vec, row)
		if value < 0 {
			return 0, moerr.NewInvalidInputNoCtx("negative auto-increment candidate")
		}
		return uint64(value), nil
	case types.T_int32:
		value := vector.GetFixedAtNoTypeCheck[int32](vec, row)
		if value < 0 {
			return 0, moerr.NewInvalidInputNoCtx("negative auto-increment candidate")
		}
		return uint64(value), nil
	case types.T_int64:
		value := vector.GetFixedAtNoTypeCheck[int64](vec, row)
		if value < 0 {
			return 0, moerr.NewInvalidInputNoCtx("negative auto-increment candidate")
		}
		return uint64(value), nil
	case types.T_uint8:
		return uint64(vector.GetFixedAtNoTypeCheck[uint8](vec, row)), nil
	case types.T_uint16:
		return uint64(vector.GetFixedAtNoTypeCheck[uint16](vec, row)), nil
	case types.T_uint32:
		return uint64(vector.GetFixedAtNoTypeCheck[uint32](vec, row)), nil
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](vec, row), nil
	default:
		return 0, moerr.NewInvalidInputNoCtxf("unsupported auto-increment value type %s", vec.GetType().Oid.String())
	}
}

type autoIncrementCandidateRun struct {
	start uint64
	step  uint64
	count uint64
}

type autoIncrementCandidateStream struct {
	typ         types.Type
	initialized bool
	runs        []autoIncrementCandidateRun
	runIndex    int
	runOffset   uint64
	// lowerBound is the highest explicit AUTO_INCREMENT value accepted by this
	// statement.  Recycled candidates below it must not be assigned to a later
	// generated row, including when a later PRE_INSERT_UK batch arrives after
	// the explicit row.
	lowerBound    uint64
	hasLowerBound bool
}

func (stream *autoIncrementCandidateStream) reset() {
	*stream = autoIncrementCandidateStream{}
}

func (stream *autoIncrementCandidateStream) append(typ types.Type, value uint64) error {
	if stream.hasLowerBound && value <= stream.lowerBound {
		return nil
	}
	if !stream.initialized {
		stream.typ = typ
		stream.initialized = true
	} else if !stream.typ.Eq(typ) {
		return moerr.NewInvalidInputNoCtx("auto-increment candidate type changed within INSERT IGNORE")
	}

	// Do not extend a run that has already been fully consumed. The next
	// candidate must remain visible to popAutoIncrementCandidate, even when
	// the previous accepted row consumed the stream's tail.
	if stream.runIndex < len(stream.runs) {
		last := &stream.runs[len(stream.runs)-1]
		if last.count == 1 && value > last.start {
			last.step = value - last.start
			last.count = 2
			return nil
		}
		if last.count > 1 && last.step != 0 &&
			value >= last.start && last.step <= (value-last.start)/last.count &&
			last.start+last.step*last.count == value {
			last.count++
			return nil
		}
	}
	stream.runs = append(stream.runs, autoIncrementCandidateRun{start: value, count: 1})
	return nil
}

// discardThrough invalidates retained generated candidates at or below an
// accepted explicit key.  It also records the bound so a later input batch
// cannot reintroduce an already-invalidated candidate.  The stream normally
// receives monotonic allocator output, but filtering every remaining run keeps
// this contract correct if a batch is split or an allocator implementation
// supplies a non-monotonic run.
func (stream *autoIncrementCandidateStream) discardThrough(value uint64) error {
	if !stream.hasLowerBound || value > stream.lowerBound {
		stream.lowerBound = value
		stream.hasLowerBound = true
	}
	if stream.runIndex >= len(stream.runs) {
		stream.runs = nil
		stream.runIndex = 0
		stream.runOffset = 0
		return nil
	}

	remaining := stream.runs[stream.runIndex:]
	if stream.runOffset != 0 {
		first := remaining[0]
		start, err := stream.valueAt(first, stream.runOffset)
		if err != nil {
			return err
		}
		first.start = start
		first.count -= stream.runOffset
		remaining[0] = first
	}
	filtered := remaining[:0]
	for _, run := range remaining {
		if run.count == 0 {
			continue
		}
		if run.step == 0 {
			if run.start > value {
				filtered = append(filtered, run)
			}
			continue
		}
		if run.start > value {
			filtered = append(filtered, run)
			continue
		}
		skip := (value-run.start)/run.step + 1
		if skip >= run.count {
			continue
		}
		start, err := stream.valueAt(run, skip)
		if err != nil {
			return err
		}
		filtered = append(filtered, autoIncrementCandidateRun{
			start: start,
			step:  run.step,
			count: run.count - skip,
		})
	}
	stream.runs = append([]autoIncrementCandidateRun(nil), filtered...)
	stream.runIndex = 0
	stream.runOffset = 0
	return nil
}

func (stream *autoIncrementCandidateStream) valueAt(run autoIncrementCandidateRun, offset uint64) (uint64, error) {
	if offset >= run.count {
		return 0, moerr.NewInvalidInputNoCtx("auto-increment candidate stream offset is out of range")
	}
	if run.step != 0 && offset > (^uint64(0)-run.start)/run.step {
		return 0, moerr.NewInvalidInputNoCtx("auto-increment candidate stream overflows")
	}
	return run.start + run.step*offset, nil
}

func (preInsertUnique *PreInsertUnique) appendAutoIncrementCandidate(
	proc *process.Process,
	source *vector.Vector,
	row int,
) error {
	value, err := autoIncrementCandidateValue(source, row)
	if err != nil {
		return err
	}
	return preInsertUnique.ctr.autoIncrementCandidates.append(*source.GetType(), value)
}

func (preInsertUnique *PreInsertUnique) popAutoIncrementCandidate() (*autoIncrementCandidate, bool, error) {
	queue := &preInsertUnique.ctr.autoIncrementCandidates
	for queue.runIndex < len(queue.runs) {
		run := queue.runs[queue.runIndex]
		value, err := queue.valueAt(run, queue.runOffset)
		if err != nil {
			return nil, false, err
		}
		queue.runOffset++
		if queue.runOffset == run.count {
			queue.runIndex++
			queue.runOffset = 0
		}
		if _, blocked := preInsertUnique.ctr.acceptedAutoIncrementValues[value]; blocked {
			continue
		}
		return &autoIncrementCandidate{typ: queue.typ, value: value}, true, nil
	}
	return nil, false, nil
}

func (preInsertUnique *PreInsertUnique) compactAutoIncrementCandidates(proc *process.Process) error {
	queue := &preInsertUnique.ctr.autoIncrementCandidates
	if queue.runIndex == 0 && queue.runOffset == 0 {
		return nil
	}
	if queue.runIndex >= len(queue.runs) {
		queue.reset()
		return nil
	}
	if queue.runIndex < 1024 || queue.runIndex < len(queue.runs)/2 {
		return nil
	}
	remaining := queue.runs[queue.runIndex:]
	if queue.runOffset != 0 {
		first := remaining[0]
		start, err := queue.valueAt(first, queue.runOffset)
		if err != nil {
			return err
		}
		first.start = start
		first.count -= queue.runOffset
		remaining[0] = first
	}
	queue.runs = append([]autoIncrementCandidateRun(nil), remaining...)
	queue.runIndex = 0
	queue.runOffset = 0
	return nil
}

func appendAutoIncrementValue(
	proc *process.Process,
	vec *vector.Vector,
	typ types.Type,
	value uint64,
) error {
	if vec == nil || !vec.GetType().Eq(typ) {
		return moerr.NewInvalidInput(proc.Ctx, "auto-increment output type does not match candidate type")
	}
	switch typ.Oid {
	case types.T_int8:
		if value > 1<<7-1 {
			return moerr.NewInvalidInput(proc.Ctx, "auto-increment candidate overflows int8")
		}
		return vector.AppendFixed(vec, int8(value), false, proc.Mp())
	case types.T_int16:
		if value > 1<<15-1 {
			return moerr.NewInvalidInput(proc.Ctx, "auto-increment candidate overflows int16")
		}
		return vector.AppendFixed(vec, int16(value), false, proc.Mp())
	case types.T_int32:
		if value > 1<<31-1 {
			return moerr.NewInvalidInput(proc.Ctx, "auto-increment candidate overflows int32")
		}
		return vector.AppendFixed(vec, int32(value), false, proc.Mp())
	case types.T_int64:
		if value > ^uint64(0)>>1 {
			return moerr.NewInvalidInput(proc.Ctx, "auto-increment candidate overflows int64")
		}
		return vector.AppendFixed(vec, int64(value), false, proc.Mp())
	case types.T_uint8:
		if value > 1<<8-1 {
			return moerr.NewInvalidInput(proc.Ctx, "auto-increment candidate overflows uint8")
		}
		return vector.AppendFixed(vec, uint8(value), false, proc.Mp())
	case types.T_uint16:
		if value > 1<<16-1 {
			return moerr.NewInvalidInput(proc.Ctx, "auto-increment candidate overflows uint16")
		}
		return vector.AppendFixed(vec, uint16(value), false, proc.Mp())
	case types.T_uint32:
		if value > 1<<32-1 {
			return moerr.NewInvalidInput(proc.Ctx, "auto-increment candidate overflows uint32")
		}
		return vector.AppendFixed(vec, uint32(value), false, proc.Mp())
	case types.T_uint64:
		return vector.AppendFixed(vec, value, false, proc.Mp())
	default:
		return moerr.NewInvalidInputf(proc.Ctx, "unsupported auto-increment output type %s", typ.Oid.String())
	}
}
