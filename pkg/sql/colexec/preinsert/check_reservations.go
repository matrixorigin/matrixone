// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package preinsert

import (
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func (preInsert *PreInsert) selectCheckRows(proc *proc, input *batch.Batch, sels []int64) ([]int64, error) {
	pos := int(preInsert.CheckEligibility.ColPos)
	if pos < 0 || pos >= len(input.Vecs) || input.Vecs[pos] == nil || !input.Vecs[pos].GetType().IsBoolean() {
		return nil, moerr.NewInvalidInput(proc.Ctx, "invalid PRE_INSERT CHECK eligibility column")
	}
	flag := input.Vecs[pos]
	for row := 0; row < input.RowCount(); row++ {
		flagRow := row
		if flag.IsConst() {
			flagRow = 0
		}
		if !flag.IsNull(uint64(flagRow)) && vector.GetFixedAtNoTypeCheck[bool](flag, flagRow) {
			sels = append(sels, int64(row))
		}
	}
	return sels, nil
}

// A selected output cannot borrow and shrink a child vector: that vector may
// also be consumed by another branch. Once selected, the buffer owns its vectors.
func (preInsert *PreInsert) constructCheckSelectedBuf(proc *proc, input *batch.Batch, sels []int64) error {
	if preInsert.ctr.buf == nil {
		preInsert.ctr.buf = batch.NewOffHeapWithSize(len(preInsert.Attrs))
		preInsert.ctr.buf.Attrs = slices.Clone(preInsert.Attrs)
	}
	preInsert.ctr.buf.SetRowCount(0)
	for i := range preInsert.Attrs {
		if !preInsert.ctr.canFreeVecIdx[i] {
			preInsert.ctr.buf.Vecs[i] = nil
		}
		preInsert.ctr.canFreeVecIdx[i] = true
		output := preInsert.ctr.buf.Vecs[i]
		if output == nil {
			output = vector.NewOffHeapVecWithType(*input.Vecs[i].GetType())
			preInsert.ctr.buf.Vecs[i] = output
		} else {
			output.CleanOnlyData()
		}
		if err := output.Union(input.Vecs[i], sels, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}
