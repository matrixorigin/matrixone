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

package update

import (
	"bytes"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// var nullRowid [16]byte

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("update rows")
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

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
	updateCtx := p.UpdateCtx

	// check parent, if have any null, throw error
	// can not check here.  because 'update c1 set a = null where a =1' is ok. that's not constraint fail
	// for _, idx := range updateCtx.ParentIdx {
	// 	if nulls.Any(bat.Vecs[idx].GetNulls()) {
	// 		return false, moerr.NewInternalError(proc.Ctx, "Cannot add or update a child row: a foreign key constraint fails")
	// 	}
	// }

	// check child on restrict, if is not all null, throw error
	for _, idx := range updateCtx.OnRestrictIdx {
		if bat.Vecs[idx].Length() != bat.Vecs[idx].GetNulls().Count() {
			return false, moerr.NewInternalError(proc.Ctx, "Cannot delete or update a parent row: a foreign key constraint fails")
		}
	}

	// delete old unique index
	_, err = colexec.FilterAndDelByRowId(proc, bat, updateCtx.IdxIdx, updateCtx.IdxSource)
	if err != nil {
		return false, err
	}

	// update child table(which ref on delete cascade)
	_, err = colexec.FilterAndUpdateByRowId(p.Engine, proc, bat, updateCtx.OnCascadeIdx, updateCtx.OnCascadeSource,
		updateCtx.OnCascadeRef, updateCtx.OnCascadeTableDef, updateCtx.OnCascadeUpdateCol, nil, updateCtx.OnCascadeUniqueSource)
	if err != nil {
		return false, err
	}

	// update child table(which ref on delete set null)
	_, err = colexec.FilterAndUpdateByRowId(p.Engine, proc, bat, updateCtx.OnSetIdx, updateCtx.OnSetSource,
		updateCtx.OnSetRef, updateCtx.OnSetTableDef, updateCtx.OnSetUpdateCol, nil, updateCtx.OnSetUniqueSource)
	if err != nil {
		return false, err
	}

	// update origin table
	affectedRows, err = colexec.FilterAndUpdateByRowId(p.Engine, proc, bat, updateCtx.Idxs, updateCtx.Source,
		updateCtx.Ref, updateCtx.TableDefs, updateCtx.UpdateCol, updateCtx.ParentIdx, updateCtx.UniqueSource)
	if err != nil {
		return false, err
	}
	atomic.AddUint64(&p.AffectedRows, affectedRows)
	return false, nil
}
