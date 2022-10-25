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
	"context"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/update"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("delete rows")
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

// the bool return value means whether it completed its work or not
func Call(_ int, proc *process.Process, arg any) (bool, error) {
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
	batLen := batch.Length(bat)
	var affectedRows uint64

	ctx := context.TODO()

	for i := range p.DeleteCtxs {
		filterColIndex := p.DeleteCtxs[i].ColIndex
		if p.DeleteCtxs[i].IsHideKey {
			var cnt uint64
			tmpBat := &batch.Batch{}
			tmpBat.Vecs = []*vector.Vector{bat.Vecs[filterColIndex]}
			tmpBat, cnt = update.FilterBatch(tmpBat, batLen, proc)
			// maxIndex := int32(len(bat.Vecs))
			// if i < len(p.DeleteCtxs)-1 {
			// 	maxIndex = p.DeleteCtxs[i+1].ColIndex
			// }
			// for j := filterColIndex + 1; j < maxIndex; j++ {
			// 	tmpBat.Vecs = append(tmpBat.Vecs, bat.Vecs[j])
			// }
			tmpBat.SetZs(tmpBat.GetVector(0).Length(), proc.Mp())
			err := p.DeleteCtxs[i].TableSource.Delete(ctx, tmpBat, p.DeleteCtxs[i].UseDeleteKey)
			if err != nil {
				tmpBat.Clean(proc.Mp())
				return false, err
			}
			affectedRows += cnt

			tmpBat.Clean(proc.Mp())
		} else {
			tmpBat := &batch.Batch{}
			tmpBat.Vecs = []*vector.Vector{bat.Vecs[filterColIndex]}
			// maxIndex := int32(len(bat.Vecs))
			// if i < len(p.DeleteCtxs)-1 {
			// 	maxIndex = p.DeleteCtxs[i+1].ColIndex
			// }
			// for j := filterColIndex + 1; j < maxIndex; j++ {
			// 	tmpBat.Vecs = append(tmpBat.Vecs, bat.Vecs[j])
			// }
			tmpBat.SetZs(tmpBat.GetVector(0).Length(), proc.Mp())
			err := p.DeleteCtxs[i].TableSource.Delete(ctx, tmpBat, p.DeleteCtxs[i].UseDeleteKey)
			if err != nil {
				tmpBat.Clean(proc.Mp())
				return false, err
			}
			affectedRows += uint64(batLen)

			tmpBat.Clean(proc.Mp())
		}
		for infoNum, info := range p.DeleteCtxs[i].IndexInfos {
			rel := p.DeleteCtxs[i].IndexTables[infoNum]
			oldBatch, rowNum := util.BuildUniqueKeyBatch(bat.Vecs[filterColIndex+1:filterColIndex+1+int32(len(p.DeleteCtxs[i].IndexAttrs))], p.DeleteCtxs[i].IndexAttrs, info.Cols, proc)
			if rowNum != 0 {
				err := rel.Delete(ctx, oldBatch, info.ColNames[0])
				if err != nil {
					return false, err
				}
			}
			oldBatch.Clean(proc.Mp())
		}
	}
	atomic.AddUint64(&p.AffectedRows, affectedRows)

	return false, nil
}
