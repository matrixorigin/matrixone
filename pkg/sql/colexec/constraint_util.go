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

type TableInfo struct {
	HasAutoCol         bool
	pkPos              int
	updateNameToPos    map[string]int
	hasCompositePkey   bool     // Whether the table contains composite primary key
	compositePkeyParts []string // Part name of composite primary key
	clusterBy          string
	Attrs              []string
	IdxList            []int32
}

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
	uniqueRels [][]engine.Relation,
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
		// get Attrs, HasAutoCol
		tableDef := tableDefs[i]
		updateCol := updateCols[i]
		uniqueRel := uniqueRels[i]
		var parentIdx map[string]int32 // nil means don't need check parent constraint
		if len(parentIdxs) > 0 {
			parentIdx = parentIdxs[i]
		}
		info := GetInfoForInsertAndUpdate(tableDef, updateCol)

		delBatch, updateBatch, err = filterRowIdForUpdate(proc, bat, setIdxList, info.Attrs, parentIdx)
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

			// fill auto incr column
			if info.HasAutoCol {
				if err = UpdateInsertBatch(eg, proc.Ctx, proc, tableDef.Cols, updateBatch, uint64(ref[i].Obj), ref[i].SchemaName, tableDef.Name); err != nil {
					return 0, err
				}
			}

			// check new rows not null
			err := BatchDataNotNullCheck(updateBatch, tableDef, proc.Ctx)
			if err != nil {
				return 0, err
			}

			//  append hidden columns
			//if info.compositePkey != "" {
			//	util.FillCompositeClusterByBatch(updateBatch, info.compositePkey, proc)
			//}

			// append hidden columns
			if info.hasCompositePkey {
				err = util.FillCompositeKeyBatch(updateBatch, catalog.CPrimaryKeyColName, info.compositePkeyParts, proc)
				if err != nil {
					return 0, err
				}
			}

			if info.clusterBy != "" && util.JudgeIsCompositeClusterByColumn(info.clusterBy) {
				err = util.FillCompositeClusterByBatch(updateBatch, info.clusterBy, proc)
				if err != nil {
					return 0, err
				}
			}

			// write unique key table
			WriteUniqueTable(nil, proc, updateBatch, tableDef, info.updateNameToPos, info.pkPos, uniqueRel)

			// write origin table
			err = rels[i].Write(proc.Ctx, updateBatch)
			if err != nil {
				return 0, err
			}
		}
	}
	return affectedRows, nil
}

func WriteUniqueTable(s3Writers []*S3Writer, proc *process.Process, updateBatch *batch.Batch,
	tableDef *plan.TableDef, updateNameToPos map[string]int, pkPos int, rels []engine.Relation) error {
	if tableDef.Indexes == nil {
		return nil
	}

	var ukBatch *batch.Batch
	defer func() {
		if ukBatch != nil {
			ukBatch.Clean(proc.Mp())
		}
	}()

	uIdx := 0
	for _, indexDef := range tableDef.Indexes {
		if !indexDef.Unique {
			continue
		}

		partsLength := len(indexDef.Parts)
		uniqueColumnPos := make([]int, partsLength)
		for p, column := range indexDef.Parts {
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

		if s3Writers == nil {
			rel := rels[uIdx]
			err := rel.Write(proc.Ctx, ukBatch)
			if err != nil {
				return err
			}
			uIdx++
		} else {
			uIdx++
			err := s3Writers[uIdx].WriteS3Batch(ukBatch, proc)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func filterRowIdForDel(proc *process.Process, bat *batch.Batch, idx int) *batch.Batch {
	retVec := vector.NewVec(types.T_Rowid.ToType())
	rowIdMap := make(map[types.Rowid]struct{})
	for i, r := range vector.MustFixedCol[types.Rowid](bat.Vecs[idx]) {
		if !bat.Vecs[idx].GetNulls().Contains(uint64(i)) {
			rowIdMap[r] = struct{}{}
		}
	}
	rowIdList := make([]types.Rowid, len(rowIdMap))
	i := 0
	for rowId := range rowIdMap {
		rowIdList[i] = rowId
		i++
	}
	vector.AppendFixedList(retVec, rowIdList, nil, proc.Mp())
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
		if bat.Vecs[idx].GetType().Oid == types.T_Rowid {
			for j, r := range vector.MustFixedCol[types.Rowid](bat.Vecs[idx]) {
				if _, exist := rowIdMap[r]; exist {
					rowSkip = append(rowSkip, true)
				} else if bat.Vecs[idx].GetNulls().Contains(uint64(j)) {
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
	delVec := vector.NewVec(types.T_Rowid.ToType())
	rowIdList := make([]types.Rowid, len(rowIdMap))
	i := 0
	for rowId := range rowIdMap {
		rowIdList[i] = rowId
		i++
	}
	mp := proc.Mp()
	vector.AppendFixedList(delVec, rowIdList, nil, mp)
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
	updateBatch.AddCnt(1)
	var toVec *vector.Vector
	var err error

	for i, idx := range idxList {
		fromVec := bat.Vecs[idx]
		colName := attrs[i]

		// if update values is not null, but parent is null, throw error
		if parentIdx != nil {
			if pIdx, exists := parentIdx[colName]; exists {
				parentVec := bat.Vecs[pIdx]
				if fromVec.IsConst() {
					if !fromVec.IsConstNull() {
						if rowSkip == nil {
							for j := 0; j < batLen; j++ {
								if parentVec.GetNulls().Contains(uint64(j)) {
									return nil, moerr.NewInternalError(proc.Ctx, "Cannot add or update a child row: a foreign key constraint fails")
								}
							}
						} else {
							for j := 0; j < batLen; j++ {
								if !rowSkip[j] && parentVec.GetNulls().Contains(uint64(j)) {
									return nil, moerr.NewInternalError(proc.Ctx, "Cannot add or update a child row: a foreign key constraint fails")
								}
							}
						}
					}
				} else {
					if rowSkip == nil {
						for j := 0; j < fromVec.Length(); j++ {
							if !fromVec.GetNulls().Contains(uint64(j)) && parentVec.GetNulls().Contains(uint64(j)) {
								return nil, moerr.NewInternalError(proc.Ctx, "Cannot add or update a child row: a foreign key constraint fails")
							}
						}
					} else {
						for j := 0; j < fromVec.Length(); j++ {
							if !rowSkip[j] && !fromVec.GetNulls().Contains(uint64(j)) && parentVec.GetNulls().Contains(uint64(j)) {
								return nil, moerr.NewInternalError(proc.Ctx, "Cannot add or update a child row: a foreign key constraint fails")
							}
						}
					}
				}
			}
		}

		if fromVec.IsConst() {
			toVec = proc.GetVector(*bat.Vecs[idx].GetType())
			if fromVec.IsConstNull() {
				for j := 0; j < batLen; j++ {
					err := vector.AppendFixed(toVec, 0, true, proc.Mp())
					if err != nil {
						updateBatch.Clean(proc.Mp())
						return nil, err
					}
				}
			} else {
				err = toVec.UnionMulti(fromVec, 0, batLen, proc.Mp())
				if err != nil {
					updateBatch.Clean(proc.Mp())
					return nil, err
				}
			}
		} else {
			toVec = proc.GetVector(*fromVec.GetType())
			if rowSkip == nil {
				// XXX should we free the fromVec here ?
				if err = vector.GetUnionFunction(*fromVec.GetType(), proc.Mp())(toVec, fromVec); err != nil {
					return nil, err
				}
			} else {
				for j := 0; j < fromVec.Length(); j++ {
					if !rowSkip[j] {
						err = toVec.UnionOne(fromVec, int64(j), proc.Mp())
						if err != nil {
							return nil, err
						}
					}
				}
			}
		}
		updateBatch.SetVector(int32(i), toVec)
	}
	updateBatch.SetZs(batLen, proc.Mp())
	return updateBatch, nil
}

func GetInfoForInsertAndUpdate(tableDef *plan.TableDef, updateCol map[string]int32) *TableInfo {
	info := &TableInfo{
		HasAutoCol:         false,
		pkPos:              -1,
		updateNameToPos:    make(map[string]int),
		hasCompositePkey:   false,
		compositePkeyParts: make([]string, 0),
		clusterBy:          "",
		Attrs:              make([]string, 0, len(tableDef.Cols)),
		IdxList:            make([]int32, 0, len(tableDef.Cols)),
	}

	// Check whether the composite primary key column is included
	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		info.hasCompositePkey = true
		info.compositePkeyParts = tableDef.Pkey.Names
	}

	if tableDef.ClusterBy != nil {
		info.clusterBy = tableDef.ClusterBy.Name
	}
	pos := 0
	for j, col := range tableDef.Cols {
		if col.Typ.AutoIncr {
			if updateCol == nil { // update statement
				info.HasAutoCol = true
			} else if _, ok := updateCol[col.Name]; ok { // insert statement
				info.HasAutoCol = true
			}
		}
		if !info.hasCompositePkey && col.Name != catalog.Row_ID && col.Primary {
			info.pkPos = j
		}
		if col.Name != catalog.Row_ID {
			info.Attrs = append(info.Attrs, col.Name)
			info.IdxList = append(info.IdxList, int32(pos))
			info.updateNameToPos[col.Name] = pos
			pos++
		}
	}
	if info.hasCompositePkey {
		info.pkPos = pos
	}

	return info
}

func BatchDataNotNullCheck(tmpBat *batch.Batch, tableDef *plan.TableDef, ctx context.Context) error {
	compNameMap := make(map[string]struct{})

	// judge whether the table contains composite primary key
	if tableDef.Pkey != nil && len(tableDef.Pkey.Names) > 1 {
		for _, name := range tableDef.Pkey.Names {
			compNameMap[name] = struct{}{}
		}
	}

	for j := range tmpBat.Vecs {
		nsp := tmpBat.Vecs[j].GetNulls()
		if tableDef.Cols[j].Default != nil && !tableDef.Cols[j].Default.NullAbility && nulls.Any(nsp) {
			return moerr.NewConstraintViolation(ctx, fmt.Sprintf("Column '%s' cannot be null", tmpBat.Attrs[j]))
		}
		if _, ok := compNameMap[tmpBat.Attrs[j]]; ok {
			if nulls.Any(nsp) {
				return moerr.NewConstraintViolation(ctx, fmt.Sprintf("Column '%s' cannot be null", tmpBat.Attrs[j]))
			}
		}
	}
	return nil
}
