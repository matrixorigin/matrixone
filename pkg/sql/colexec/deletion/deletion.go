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
	idxList := make([]int32, len(delCtx.DelIdx))
	for i := 0; i < len(delCtx.DelIdx); i++ {
		// for now, we have row_id & pk. but only use row_id for delete
		idxList[i] = delCtx.DelIdx[i][0]
	}
	affectedRows, err = colexec.FilterAndDelByRowId(proc, bat, idxList, delCtx.DelSource)
	if err != nil {
		return false, err
	}

	atomic.AddUint64(&p.AffectedRows, affectedRows)
	return false, nil
}
