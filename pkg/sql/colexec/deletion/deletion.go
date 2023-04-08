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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("delete rows")
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	if ap.DeleteType == RemoteDelete {
		ap.ctr = new(container)
		ap.ctr.mp = pipeline.ArrayMap{
			Mp: make(map[string]*pipeline.Array),
		}
		attrs := []string{catalog.BlockMeta_ID, catalog.BlockMeta_Offsets}
		blkDeleteBat := batch.New(true, attrs)
		blkDeleteBat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		blkDeleteBat.Vecs[1] = vector.NewVec(types.T_text.ToType())
		ap.ctr.blkDeleteBat = blkDeleteBat
	}
	return nil
}

// the bool return value means whether it completed its work or not
func Call(_ int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	p := arg.(*Argument)
	bat := proc.Reg.InputBatch

	// last batch of block
	if bat == nil {
		if p.DeleteType == LocalDelete {
			attrs := []string{catalog.LocalDeleteRows}
			localDeleteBat := batch.New(true, attrs)
			localDeleteBat.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
			vector.AppendFixed(localDeleteBat.GetVector(0), p.AffectedRows, false, proc.GetMPool())
			proc.SetInputBatch(localDeleteBat)
		}
		return true, nil
	}

	// empty batch
	if len(bat.Zs) == 0 {
		return false, nil
	}

	defer bat.Clean(proc.Mp())
	if p.DeleteType == RemoteDelete {
		// the logic will be used for next version's delete for cn bloc delete.
		//  for now it's not useful

		// // in the previous instruction, the
		// // group will block all batch until
		// // it gets all batches, so we will
		// // get a batch which may contain multi
		// // blocks' rowId.
		// // just support single table detele for now.
		// deleteVec := vector.MustFixedCol[types.Rowid](bat.GetVector(0))
		// p.ctr.blkDeleteBat.Clean(proc.GetMPool())
		// for _, rowId := range deleteVec {
		// 	blkid := rowId.GetBlockid()
		// 	p.ctr.PutDeteteOffset((&blkid).String(), int32(rowId.GetRowOffset()))
		// }
		// proc.SetInputBatch(p.ctr.blkDeleteBat)
		ibat := batch.New(true, bat.Attrs)
		for j := range bat.Vecs {
			ibat.SetVector(int32(j), vector.NewVec(*bat.GetVector(int32(j)).GetType()))
		}
		if _, err := ibat.Append(proc.Ctx, proc.GetMPool(), bat); err != nil {
			return false, err
		}
		proc.SetInputBatch(ibat)
		return false, nil
	}
	var affectedRows uint64
	var err error
	delCtx := p.DeleteCtx

	// check OnRestrict, if is not all null, throw error
	for _, idx := range delCtx.OnRestrictIdx {
		if bat.Vecs[idx].Length() != bat.Vecs[idx].GetNulls().Np.Count() {
			return false, moerr.NewInternalError(proc.Ctx, "Cannot delete or update a parent row: a foreign key constraint fails")
		}
	}

	// delete unique index
	_, err = colexec.FilterAndDelByRowId(proc, bat, delCtx.IdxIdx, delCtx.IdxSource)
	if err != nil {
		return false, err
	}

	// delete child table(which ref on delete cascade)
	_, err = colexec.FilterAndDelByRowId(proc, bat, delCtx.OnCascadeIdx, delCtx.OnCascadeSource)
	if err != nil {
		return false, err
	}

	// update child table(which ref on delete set null)
	_, err = colexec.FilterAndUpdateByRowId(p.Engine, proc, bat, delCtx.OnSetIdx, delCtx.OnSetSource,
		delCtx.OnSetRef, delCtx.OnSetTableDef, delCtx.OnSetUpdateCol, nil, delCtx.OnSetUniqueSource)
	if err != nil {
		return false, err
	}

	// delete origin table
	idxList := make([]int32, len(delCtx.DelSource))
	for i := 0; i < len(delCtx.DelSource); i++ {
		idxList[i] = int32(i)
	}
	affectedRows, err = colexec.FilterAndDelByRowId(proc, bat, idxList, delCtx.DelSource)
	if err != nil {
		return false, err
	}

	atomic.AddUint64(&p.AffectedRows, affectedRows)
	return false, nil
}
