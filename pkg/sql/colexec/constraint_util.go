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

func FilterAndUpdateUniqueKeyByRowId(proc *process.Process, bat *batch.Batch, idxList [][]int32, rels []engine.Relation, pkList []int32) (uint64, error) {
	var affectedRows uint64
	for i, setIdxList := range idxList {
		if pkList[i] != -1 {
			setIdxList = append(setIdxList, pkList[i])
		}
		attrs := make([]string, len(setIdxList)-1)
		delBatch, updateBatch, err := filterRowIdForUpdate(proc, bat, setIdxList, attrs)
		if err != nil {
			return 0, err
		}
		if delBatch == nil && updateBatch == nil {
			continue
		}
		affectedRows = affectedRows + uint64(delBatch.Length())
		if delBatch.Length() > 0 {
			// delete old unique key
			err := rels[i].Delete(proc.Ctx, delBatch, catalog.Row_ID)
			if err != nil {
				delBatch.Clean(proc.Mp())
				updateBatch.Clean(proc.Mp())
				return 0, err
			}

			err = doInsertUniqueTable(proc, rels[i], updateBatch, pkList[i])
			if err != nil {
				delBatch.Clean(proc.Mp())
				updateBatch.Clean(proc.Mp())
				return 0, err
			}

		}
		delBatch.Clean(proc.Mp())
		updateBatch.Clean(proc.Mp())
	}
	return affectedRows, nil
}

func InsertUniqueKeyToTable(proc *process.Process, bat *batch.Batch, idxList [][]int32, rels []engine.Relation, pkList []int32) (uint64, error) {
	var affectedRows uint64
	for i, getIdxList := range idxList {
		if pkList[i] != -1 {
			getIdxList = append(getIdxList, pkList[i])
		}

		attrs := make([]string, len(pkList))
		batLen := bat.Vecs[0].Length()
		affectedRows = affectedRows + uint64(batLen)
		updateBatch, err := GetUpdateBatch(proc, bat, getIdxList, batLen, attrs, nil)
		if err != nil {
			return 0, err
		}

		err = doInsertUniqueTable(proc, rels[i], updateBatch, pkList[i])
		if err != nil {
			updateBatch.Clean(proc.Mp())
			return 0, err
		}
	}
	return affectedRows, nil
}

func doInsertUniqueTable(proc *process.Process, rel engine.Relation, updateBatch *batch.Batch, pkPos int32) error {
	//updateBatch 、 pk的情况，重新构造一个要Insert的batch，
	var ukBatch *batch.Batch
	var bitMap *nulls.Nulls
	var vec *vector.Vector
	colCount := len(updateBatch.Vecs)
	if pkPos == -1 {
		//have no pk
		ukBatch = batch.New(true, []string{catalog.IndexTableIndexColName})
		if colCount == 1 {
			vec, _ = util.CompactSingleIndexCol(updateBatch.Vecs[0], proc)
			ukBatch.SetVector(0, vec)
		} else {
			vec, _ = util.SerialWithCompacted(updateBatch.Vecs, proc)
		}
		ukBatch.SetVector(0, vec)
		ukBatch.SetZs(vec.Length(), proc.Mp())
	} else {
		ukBatch = batch.New(true, []string{catalog.IndexTableIndexColName, catalog.IndexTablePrimaryColName})
		// the last idx is pk
		if colCount == 2 {
			vec, bitMap = util.CompactSingleIndexCol(updateBatch.Vecs[0], proc)
		} else {
			vs := make([]*vector.Vector, 0)
			for j := 0; j < colCount-1; j++ {
				vs = append(vs, updateBatch.Vecs[j])
			}
			vec, bitMap = util.SerialWithCompacted(vs, proc)
		}
		ukBatch.SetVector(0, vec)
		ukBatch.SetZs(vec.Length(), proc.Mp())
		// append pk vector
		vec = util.CompactPrimaryCol(updateBatch.Vecs[len(updateBatch.Vecs)-1], bitMap, proc)
		ukBatch.SetVector(1, vec)
	}

	// insert new unique key
	return rel.Write(proc.Ctx, ukBatch)
}

func FilterAndUpdateByRowId(proc *process.Process, bat *batch.Batch, idxList [][]int32, rels []engine.Relation, attrsList [][]string,
	hasAutoCol []bool, ref []*plan.ObjectRef, tableDef []*plan.TableDef, eg engine.Engine) (uint64, error) {
	var affectedRows uint64
	for i, setIdxList := range idxList {
		delBatch, updateBatch, err := filterRowIdForUpdate(proc, bat, setIdxList, attrsList[i])
		if err != nil {
			return 0, err
		}
		if delBatch == nil && updateBatch == nil {
			continue
		}
		affectedRows = affectedRows + uint64(delBatch.Length())
		if delBatch.Length() > 0 {
			if hasAutoCol != nil && hasAutoCol[i] {
				if err := UpdateInsertBatch(eg, proc.Ctx, proc, tableDef[i].Cols, updateBatch, uint64(ref[i].Obj), ref[i].SchemaName, tableDef[i].Name); err != nil {
					delBatch.Clean(proc.Mp())
					updateBatch.Clean(proc.Mp())
					return 0, err
				}
			}

			err = rels[i].Delete(proc.Ctx, delBatch, catalog.Row_ID)
			if err != nil {
				delBatch.Clean(proc.Mp())
				updateBatch.Clean(proc.Mp())
				return 0, err
			}

			err = rels[i].Write(proc.Ctx, updateBatch)
			if err != nil {
				delBatch.Clean(proc.Mp())
				updateBatch.Clean(proc.Mp())
				return 0, err
			}
		}
		delBatch.Clean(proc.Mp())
		updateBatch.Clean(proc.Mp())
	}
	return affectedRows, nil
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

func filterRowIdForUpdate(proc *process.Process, bat *batch.Batch, idxList []int32, attrs []string) (*batch.Batch, *batch.Batch, error) {
	rowIdMap := make(map[types.Rowid]struct{})
	var rowSkip []bool
	foundRowId := false
	for i, idx := range idxList {
		if bat.Vecs[idx].Typ.Oid == types.T_Rowid {
			for i, r := range vector.MustTCols[types.Rowid](bat.Vecs[idx]) {
				if _, exist := rowIdMap[r]; exist {
					rowSkip = append(rowSkip, true)
				} else if bat.Vecs[idx].Nsp.Contains(uint64(i)) {
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
	updateBatch, err := GetUpdateBatch(proc, bat, idxList, batLen, attrs, rowSkip)
	if err != nil {
		delBatch.Clean(proc.Mp())
		return nil, nil, err
	}

	return delBatch, updateBatch, nil
}

func GetUpdateBatch(proc *process.Process, bat *batch.Batch, idxList []int32, batLen int, attrs []string, rowSkip []bool) (*batch.Batch, error) {
	updateBatch := batch.New(true, attrs)
	var toVec *vector.Vector
	for i, idx := range idxList {
		fromVec := bat.Vecs[idx]
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
				err := vector.CopyConst(toVec, fromVec, batLen, proc.Mp())
				if err != nil {
					updateBatch.Clean(proc.Mp())
					return nil, err
				}
			}
		} else {
			toVec = vector.New(bat.Vecs[idx].Typ)
			if rowSkip != nil {
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
