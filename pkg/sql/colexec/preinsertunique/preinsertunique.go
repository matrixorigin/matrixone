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
	keyColumns := preInsertUnique.PreInsertCtx.KeyColumns
	conflictColumns := preInsertUnique.PreInsertCtx.ConflictColumns
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
		accepted := true
		for keyIdx, conflictPos := range conflictColumns {
			conflictVec := inputBat.Vecs[conflictPos]
			if !conflictVec.GetNulls().Contains(uint64(row)) &&
				vector.GetFixedAtNoTypeCheck[bool](conflictVec, row) {
				accepted = false
				break
			}
			keyVec := preInsertUnique.ctr.acceptedKeyVecs[keyIdx][0]
			if keyVec.GetNulls().Contains(uint64(row)) {
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
			continue
		}

		// Commit every key only after the complete row has passed.  This is the
		// ownership boundary that prevents a row rejected by one constraint from
		// reserving another key for later rows.
		for keyIdx := range keyColumns {
			keyVec := preInsertUnique.ctr.acceptedKeyVecs[keyIdx][0]
			if keyVec.GetNulls().Contains(uint64(row)) {
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
		sels = append(sels, int64(row))
	}

	if len(sels) == 0 {
		result.Batch = batch.EmptyBatch
		return result, nil
	}
	outputColumns := int(preInsertUnique.PreInsertCtx.OutputColumns)
	if outputColumns > len(inputBat.Vecs) {
		return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "invalid INSERT IGNORE multi-key dedup output width")
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
	result.Batch = preInsertUnique.ctr.buf
	return result, nil
}
