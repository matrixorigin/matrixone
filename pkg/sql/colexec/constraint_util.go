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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func FilterRowIdForDel(proc *process.Process, bat *batch.Batch, idx int) *batch.Batch {
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

func FilterRowIdForUpdate(proc *process.Process, bat *batch.Batch, idxList []int32) (*batch.Batch, *batch.Batch, error) {
	rowIdMap := make(map[types.Rowid]struct{})
	var rowSkip []bool
	rowIdIdx := -1
	for _, idx := range idxList {
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
			idxList = append(idxList[:rowIdIdx], idxList[int(rowIdIdx)+1:]...)
			break
		}
	}
	if rowIdIdx == -1 {
		return nil, nil, moerr.NewInternalError(proc.Ctx, "need rowid vector for update")
	}

	// get delete batch
	delVec := vector.New(types.T_Rowid.ToType())
	rowIdList := make([]types.Rowid, len(rowIdMap))
	batLen := len(rowIdMap)
	i := 0
	for rowId := range rowIdMap {
		rowIdList[i] = rowId
		i++
	}
	mp := proc.Mp()
	vector.AppendFixed(delVec, rowIdList, mp)
	delBatch := batch.New(true, []string{catalog.Row_ID})
	delBatch.SetZs(batLen, mp)
	delBatch.SetVector(0, delVec)

	// get update batch
	updateBatch := batch.NewWithSize(len(rowIdMap))
	updateBatch.SetZs(batLen, mp)
	for i, idx := range idxList {
		toVec := vector.New(bat.Vecs[idx].Typ)
		fromVec := bat.Vecs[idx]
		for j := 0; j < fromVec.Length(); j++ {
			if !rowSkip[j] {
				vector.UnionOne(toVec, fromVec, int64(j), mp)
			}
		}
		updateBatch.SetVector(int32(i), toVec)
	}

	return delBatch, updateBatch, nil
}
