// Copyright 2021 Matrix Origin
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

package deletion

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("delete rows")
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

// the bool return value means whether it completed its work or not
func Call(_ int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	p := arg.(*Argument)
	bat := proc.Reg.InputBatch

	// last batch of block
	if bat == nil {
		return true, nil
	}

	// empty batch
	if len(bat.Zs) == 0 {
		return false, nil
	}

	defer bat.Clean(proc.Mp())
	var affectedRows uint64
	delCtx := p.DeleteCtx

	// check OnRestrict, if is not all null, throw error
	for _, idx := range delCtx.OnRestrictIdx {
		if bat.Vecs[idx].Length() != bat.Vecs[idx].Nsp.Np.Count() {
			return false, moerr.NewInternalError(proc.Ctx, "Cannot delete or update a parent row: a foreign key constraint fails")
		}
	}

	// delete unique index
	for i, idx := range delCtx.DelIdxIdx {
		delBatch := colexec.FilterRowIdForDel(proc, bat, int(idx))
		if delBatch.Length() > 0 {
			err := delCtx.DelIdxSource[i].Delete(proc.Ctx, delBatch, catalog.Row_ID)
			if err != nil {
				return false, err
			}
		}
	}

	// delete child table(which ref on delete cascade)
	for i, idx := range delCtx.OnCascadeIdx {
		delBatch := colexec.FilterRowIdForDel(proc, bat, int(idx))
		if delBatch.Length() > 0 {
			err := delCtx.OnCascadeSource[i].Delete(proc.Ctx, delBatch, catalog.Row_ID)
			if err != nil {
				return false, err
			}
		}
	}

	// update child table(which ref on delete set null)
	for i, idxList := range delCtx.OnSetIdx {
		delBatch, updateBatch, err := colexec.FilterRowIdForUpdate(proc, bat, idxList)
		if err != nil {
			return false, err
		}
		if delBatch.Length() > 0 {
			err = delCtx.OnSetSource[i].Delete(proc.Ctx, delBatch, catalog.Row_ID)
			if err != nil {
				return false, err
			}
			err = delCtx.OnSetSource[i].Write(proc.Ctx, updateBatch)
			if err != nil {
				return false, err
			}
		}
	}

	// delete origin table
	for i := 0; i < len(delCtx.DelSource); i++ {
		delBatch := colexec.FilterRowIdForDel(proc, bat, i)
		affectedRows = affectedRows + uint64(delBatch.Length())
		if delBatch.Length() > 0 {
			err := delCtx.DelSource[i].Delete(proc.Ctx, delBatch, catalog.Row_ID)
			if err != nil {
				return false, err
			}
		}
	}

	// for i := range p.DeleteCtxs {
	// 	filterColIndex := p.DeleteCtxs[i].ColIndex

	// 	var cnt uint64
	// 	tmpBat := &batch.Batch{}
	// 	tmpBat.Vecs = []*vector.Vector{bat.Vecs[filterColIndex]}
	// 	tmpBat, cnt = update.FilterBatch(tmpBat, batLen, proc)

	// 	length := tmpBat.GetVector(0).Length()
	// 	if length > 0 {
	// 		tmpBat.SetZs(length, proc.Mp())
	// 		err := p.DeleteCtxs[i].TableSource.Delete(proc.Ctx, tmpBat, p.DeleteCtxs[i].UseDeleteKey)
	// 		if err != nil {
	// 			tmpBat.Clean(proc.Mp())
	// 			return false, err
	// 		}
	// 		affectedRows += cnt
	// 	}

	// 	tmpBat.Clean(proc.Mp())
	// 	if !p.DeleteCtxs[i].IsIndexTableDelete {
	// 		atomic.AddUint64(&p.AffectedRows, affectedRows)
	// 	}
	// }

	return false, nil
}
