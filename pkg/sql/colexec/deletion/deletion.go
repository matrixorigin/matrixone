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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	bat := proc.InputBatch()

	// last batch of block
	if bat == nil {
		return true, nil
	}

	// empty batch
	if len(bat.Zs) == 0 {
		return false, nil
	}

	defer proc.PutBatch(bat)
	var affectedRows uint64
	var err error
	delCtx := p.DeleteCtx

	if len(delCtx.PartitionTableIDs) > 0 {
		delBatches := colexec.GroupByPartition(proc, bat, delCtx.RowIdIdx, delCtx.PartitionIndexInBatch, len(delCtx.PartitionTableIDs))
		for i, delBatch := range delBatches {
			tempRows := uint64(delBatch.Length())
			if tempRows > 0 {
				affectedRows += tempRows
				err = delCtx.PartitionSources[i].Delete(proc.Ctx, delBatch, catalog.Row_ID)
				if err != nil {
					delBatch.Clean(proc.Mp())
					return false, err
				}
				delBatch.Clean(proc.Mp())
			}
		}
	} else {
		delBatch := colexec.FilterRowIdForDel(proc, bat, delCtx.RowIdIdx)
		affectedRows = uint64(delBatch.Length())
		if affectedRows > 0 {
			err = delCtx.Source.Delete(proc.Ctx, delBatch, catalog.Row_ID)
			if err != nil {
				delBatch.Clean(proc.GetMPool())
				return false, err
			}
		}
	}

	newBat := batch.NewWithSize(len(bat.Vecs))
	for j := range bat.Vecs {
		newBat.SetVector(int32(j), vector.NewVec(*bat.GetVector(int32(j)).GetType()))
	}
	if _, err := newBat.Append(proc.Ctx, proc.GetMPool(), bat); err != nil {
		newBat.Clean(proc.GetMPool())
		return false, err
	}
	proc.SetInputBatch(newBat)

	if delCtx.AddAffectedRows {
		atomic.AddUint64(&p.affectedRows, affectedRows)
	}
	return false, nil
}
