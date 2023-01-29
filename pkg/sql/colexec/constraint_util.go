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

package colexec

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func FilterAndDelByRowId(proc *process.Process, bat *batch.Batch, idxList []int32, rels []engine.Relation) (uint64, error) {
	var affectedRows uint64
	for i, idx := range idxList {
		delBatch := filterRowIdForDel(proc, bat, int(idx))
		affectedRows = affectedRows + uint64(delBatch.Length())
		if delBatch.Length() > 0 {
			err := rels[i].Delete(proc.Ctx, delBatch, catalog.Row_ID)
			if err != nil {
				delBatch.Clean(proc.Mp())
				return 0, err
			}
		}
		delBatch.Clean(proc.Mp())
	}
	return affectedRows, nil
}

func FilterAndUpdateByRowId(
	eg engine.Engine,
	proc *process.Process,
	bat *batch.Batch,
	idxList [][]int32,
	rels []engine.Relation,
	ref []*plan.ObjectRef,
	tableDefs []*plan.TableDef,
	updateCols []map[string]int32,
	parentIdxs []map[string]int32,
) (uint64, error) {
	var affectedRows uint64
	var delBatch *batch.Batch
	var updateBatch *batch.Batch
	var err error
	defer func() {
		if delBatch != nil {
			delBatch.Clean(proc.Mp())
		}
		if updateBatch != nil {
			updateBatch.Clean(proc.Mp())
		}
	}()

	for i, setIdxList := range idxList {
		// get attrs, hasAutoCol
		tableDef := tableDefs[i]
		updateCol := updateCols[i]
		var parentIdx map[string]int32 // nil means don't need check parent constraint
		if len(parentIdxs) > 0 {
			parentIdx = parentIdxs[i]
		}
		attrs := make([]string, len(tableDef.Cols)-1)

		// prepare some data for update batch
		hasAutoCol := false
		pkPos := -1
		compositePkey := ""
		if tableDef.CompositePkey != nil {
			compositePkey = tableDef.CompositePkey.Name
		}
		clusterBy := ""
		if tableDef.ClusterBy != nil {
			clusterBy = tableDef.ClusterBy.Name
		}
		updateNameToPos := make(map[string]int)
		pos := 0
		for j, col := range tableDef.Cols {
			if col.Typ.AutoIncr {
				if _, ok := updateCol[col.Name]; ok {
					hasAutoCol = true
				}
			}
			if compositePkey == "" && col.Name != catalog.Row_ID && col.Primary {
				pkPos = j
			}
			if col.Name != catalog.Row_ID {
				attrs[pos] = col.Name
				updateNameToPos[col.Name] = pos
				pos++
			}
		}
		if compositePkey != "" {
			pkPos = pos
		}

		delBatch, updateBatch, err = filterRowIdForUpdate(proc, bat, setIdxList, attrs, parentIdx)
		if err != nil {
			return 0, err
		}
		if delBatch == nil && updateBatch == nil {
			continue
		}
		affectedRows = affectedRows + uint64(delBatch.Length())
		if delBatch.Length() > 0 {
			// delete old rows
			err = rels[i].Delete(proc.Ctx, delBatch, catalog.Row_ID)
			if err != nil {
				return 0, err
			}

			// check new rows not null
			err := batchDataNotNullCheck(updateBatch, tableDef, proc.Ctx)
			if err != nil {
				return 0, err
			}

			// fill auto incr column
			if hasAutoCol {
				if err = UpdateInsertBatch(eg, proc.Ctx, proc, tableDef.Cols, updateBatch, uint64(ref[i].Obj), ref[i].SchemaName, tableDef.Name); err != nil {
					return 0, err
				}
			}

			//  append hidden columns
			if compositePkey != "" {
				util.FillCompositeClusterByBatch(updateBatch, compositePkey, proc)
			}
			if clusterBy != "" {
				util.FillCompositeClusterByBatch(updateBatch, clusterBy, proc)
			}

			// write unique key table
			writeUniqueTable(eg, proc, updateBatch, tableDef, ref[i].SchemaName, updateNameToPos, pkPos)

			// write origin table
			err = rels[i].Write(proc.Ctx, updateBatch)
			if err != nil {
				return 0, err
			}
		}
	}
	return affectedRows, nil
}

func writeUniqueTable(eg engine.Engine, proc *process.Process, updateBatch *batch.Batch,
	tableDef *plan.TableDef, dbName string, updateNameToPos map[string]int, pkPos int) error {
	var ukBatch *batch.Batch

	defer func() {
		if ukBatch != nil {
			ukBatch.Clean(proc.Mp())
		}
	}()

	for _, def := range tableDef.Defs {
		if idxDef, ok := def.Def.(*plan.TableDef_DefType_UIdx); ok {
			// how to get relation?
			for idx, tblName := range idxDef.UIdx.TableNames {
				db, err := eg.Database(proc.Ctx, dbName, proc.TxnOperator)
				if err != nil {
					return err
				}
				rel, err := db.Relation(proc.Ctx, tblName)
				if err != nil {
					return err
				}
				partsLength := len(idxDef.UIdx.Fields[idx].Parts)
				uniqueColumnPos := make([]int, partsLength)
				for p, column := range idxDef.UIdx.Fields[idx].Parts {
					uniqueColumnPos[p] = updateNameToPos[column]
				}

				colCount := len(uniqueColumnPos)
				if pkPos == -1 {
					//have no pk
					ukBatch = batch.New(true, []string{catalog.IndexTableIndexColName})
				} else {
					ukBatch = batch.New(true, []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName})
				}

				var vec *vector.Vector
				var bitMap *nulls.Nulls
				if colCount == 1 {
					idx := uniqueColumnPos[0]
					vec, bitMap = util.CompactSingleIndexCol(updateBatch.Vecs[idx], proc)
				} else {
					vs := make([]*vector.Vector, colCount)
					for vIdx, pIdx := range uniqueColumnPos {
						vs[vIdx] = updateBatch.Vecs[pIdx]
					}
					vec, bitMap = util.SerialWithCompacted(vs, proc)
				}
				ukBatch.SetVector(0, vec)
				ukBatch.SetZs(vec.Length(), proc.Mp())

				if pkPos != -1 {
					// have pk, append pk vector
					vec = util.CompactPrimaryCol(updateBatch.Vecs[pkPos], bitMap, proc)
					ukBatch.SetVector(1, vec)
				}
				err = rel.Write(proc.Ctx, ukBatch)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func filterRowIdForDel(proc *process.Process, bat *batch.Batch, idx int) *batch.Batch {
	retVec := vector.New(types.T_Rowid.ToType())
	rowIdMap := make(map[types.Rowid]struct{})
	for i, r := range vector.MustTCols[types.Rowid](bat.Vecs[idx]) {
		if !bat.Vecs[idx].Nsp.Contains(uint64(i)) {
			rowIdMap[r] = struct{}{}
		}
	}
	rowIdList := make([]types.Rowid, len(rowIdMap))
	i := 0
	for rowId := range rowIdMap {
		rowIdList[i] = rowId
		i++
	}
	vector.AppendFixed(retVec, rowIdList, proc.Mp())
	retBatch := batch.New(true, []string{catalog.Row_ID})
	retBatch.SetZs(retVec.Length(), proc.Mp())
	retBatch.SetVector(0, retVec)
	return retBatch
}

func filterRowIdForUpdate(proc *process.Process, bat *batch.Batch, idxList []int32, attrs []string, parentIdx map[string]int32) (*batch.Batch, *batch.Batch, error) {
	rowIdMap := make(map[types.Rowid]struct{})
	var rowSkip []bool
	foundRowId := false
	for i, idx := range idxList {
		if bat.Vecs[idx].Typ.Oid == types.T_Rowid {
			for j, r := range vector.MustTCols[types.Rowid](bat.Vecs[idx]) {
				if _, exist := rowIdMap[r]; exist {
					rowSkip = append(rowSkip, true)
				} else if bat.Vecs[idx].Nsp.Contains(uint64(j)) {
					rowSkip = append(rowSkip, true)
				} else {
					rowIdMap[r] = struct{}{}
					rowSkip = append(rowSkip, false)
				}
			}
			foundRowId = true
			idxList = append(idxList[:i], idxList[i+1:]...)
			break
		}
	}
	if !foundRowId {
		return nil, nil, moerr.NewInternalError(proc.Ctx, "need rowid vector for update")
	}
	batLen := len(rowIdMap)
	if batLen == 0 {
		return nil, nil, nil
	}

	// get delete batch
	delVec := vector.New(types.T_Rowid.ToType())
	rowIdList := make([]types.Rowid, len(rowIdMap))
	i := 0
	for rowId := range rowIdMap {
		rowIdList[i] = rowId
		i++
	}
	mp := proc.Mp()
	vector.AppendFixed(delVec, rowIdList, mp)
	delBatch := batch.New(true, []string{catalog.Row_ID})
	delBatch.SetVector(0, delVec)
	delBatch.SetZs(batLen, mp)

	// get update batch
	updateBatch, err := GetUpdateBatch(proc, bat, idxList, batLen, attrs, rowSkip, parentIdx)
	if err != nil {
		delBatch.Clean(proc.Mp())
		return nil, nil, err
	}

	return delBatch, updateBatch, nil
}

func GetUpdateBatch(proc *process.Process, bat *batch.Batch, idxList []int32, batLen int, attrs []string, rowSkip []bool, parentIdx map[string]int32) (*batch.Batch, error) {
	updateBatch := batch.New(true, attrs)
	var toVec *vector.Vector
	var err error

	for i, idx := range idxList {
		fromVec := bat.Vecs[idx]
		colName := attrs[i]
		if parentIdx != nil {
			// if update values is not null, but parent is null, throw error
			if pIdx, exists := parentIdx[colName]; exists {
				parentVec := bat.Vecs[pIdx]
				if fromVec.IsConst() {
					if !fromVec.IsScalarNull() {
						for j := 0; j < batLen; j++ {
							if !rowSkip[j] && parentVec.Nsp.Contains(uint64(j)) {
								return nil, moerr.NewInternalError(proc.Ctx, "Cannot add or update a child row: a foreign key constraint fails")
							}
						}
					}
				} else {
					for j := 0; j < fromVec.Length(); j++ {
						if !rowSkip[j] && !fromVec.Nsp.Contains(uint64(j)) && parentVec.Nsp.Contains(uint64(j)) {
							return nil, moerr.NewInternalError(proc.Ctx, "Cannot add or update a child row: a foreign key constraint fails")
						}
					}
				}
			}
		}

		if fromVec.IsConst() {
			toVec = vector.New(bat.Vecs[idx].Typ)
			if fromVec.IsScalarNull() {
				defVal := vector.GetInitConstVal(bat.Vecs[idx].Typ)
				for j := 0; j < batLen; j++ {
					err := toVec.Append(defVal, true, proc.Mp())
					if err != nil {
						updateBatch.Clean(proc.Mp())
						return nil, err
					}
				}
			} else {
				err = vector.CopyConst(toVec, fromVec, batLen, proc.Mp())
				if err != nil {
					updateBatch.Clean(proc.Mp())
					return nil, err
				}
			}
		} else {
			toVec = vector.New(bat.Vecs[idx].Typ)
			if rowSkip == nil {
				for j := 0; j < fromVec.Length(); j++ {
					vector.UnionOne(toVec, fromVec, int64(j), proc.Mp())
				}
			} else {
				for j := 0; j < fromVec.Length(); j++ {
					if !rowSkip[j] {
						vector.UnionOne(toVec, fromVec, int64(j), proc.Mp())
					}
				}
			}
		}
		updateBatch.SetVector(int32(i), toVec)
	}
	updateBatch.SetZs(batLen, proc.Mp())
	return updateBatch, nil
}

func batchDataNotNullCheck(tmpBat *batch.Batch, tableDef *plan.TableDef, ctx context.Context) error {
	compNameMap := make(map[string]struct{})
	if tableDef.CompositePkey != nil {
		names := util.SplitCompositePrimaryKeyColumnName(tableDef.CompositePkey.Name)
		for _, name := range names {
			compNameMap[name] = struct{}{}
		}
	}

	for j := range tmpBat.Vecs {
		nsp := tmpBat.Vecs[j].Nsp
		if (tableDef.Cols[j].Primary && !tableDef.Cols[j].Typ.AutoIncr) ||
			(tableDef.Cols[j].Default != nil && !tableDef.Cols[j].Default.NullAbility) {
			if nulls.Any(nsp) {
				return moerr.NewConstraintViolation(ctx, fmt.Sprintf("Column '%s' cannot be null", tmpBat.Attrs[j]))
			}
		}
		if _, ok := compNameMap[tmpBat.Attrs[j]]; ok {
			if nulls.Any(nsp) {
				return moerr.NewConstraintViolation(ctx, fmt.Sprintf("Column '%s' cannot be null", tmpBat.Attrs[j]))
			}
		}
	}
	return nil
}

// func filterRowIdForInsert(proc *process.Process, bat *batch.Batch, idxList []int32, attrs []string) (*batch.Batch, *batch.Batch, error) {
// 	rowIdMap := make(map[types.Rowid]struct{})
// 	var rowSkip []bool
// 	var err error
// 	foundRowId := false
// 	insertBatLen := 0
// 	for i, idx := range idxList {
// 		if bat.Vecs[idx].Typ.Oid == types.T_Rowid {
// 			for j, r := range vector.MustTCols[types.Rowid](bat.Vecs[idx]) {
// 				if bat.Vecs[idx].Nsp.Contains(uint64(j)) {
// 					rowSkip = append(rowSkip, false)
// 					insertBatLen++
// 				} else {
// 					rowSkip = append(rowSkip, true)
// 					rowIdMap[r] = struct{}{}
// 				}
// 			}
// 			foundRowId = true
// 			idxList = append(idxList[:i], idxList[i+1:]...)
// 			break
// 		}
// 	}
// 	if !foundRowId {
// 		return nil, nil, moerr.NewInternalError(proc.Ctx, "need rowid vector for update")
// 	}
// 	var delBatch *batch.Batch
// 	var updateBatch *batch.Batch

// 	batLen := len(rowIdMap)
// 	if batLen > 0 {
// 		// get delete batch
// 		delVec := vector.New(types.T_Rowid.ToType())
// 		rowIdList := make([]types.Rowid, len(rowIdMap))
// 		i := 0
// 		for rowId := range rowIdMap {
// 			rowIdList[i] = rowId
// 			i++
// 		}
// 		mp := proc.Mp()
// 		vector.AppendFixed(delVec, rowIdList, mp)
// 		delBatch = batch.New(true, []string{catalog.Row_ID})
// 		delBatch.SetVector(0, delVec)
// 		delBatch.SetZs(batLen, mp)
// 	}

// 	// get update batch
// 	if insertBatLen == 0 {
// 		return delBatch, nil, nil
// 	}
// 	updateBatch, err = GetUpdateBatch(proc, bat, idxList, insertBatLen, attrs, rowSkip)
// 	if err != nil {
// 		delBatch.Clean(proc.Mp())
// 		return nil, nil, err
// 	}
// 	return delBatch, updateBatch, nil
// }
