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

package batch

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// TryDictionarySelection publishes selected columns over retained immutable
// backing. Auxiliary row-buffer/marker formats retain their ordinary path.
func (bat *Batch) TryDictionarySelection(rows []int64, mp *mpool.MPool, account *vector.AllocationAccountSelection) (*Batch, bool, error) {
	if bat == nil || bat.Last() || len(bat.ExtraBuf) > 0 || len(bat.Vecs) == 0 || len(rows) == 0 {
		return nil, false, nil
	}
	output := NewOffHeapWithSize(len(bat.Vecs))
	if err := output.SetAllocationAccount(account); err != nil {
		output.Clean(mp)
		return nil, false, err
	}
	admitted, err := vector.TrySelectDictionaryVectors(bat.Vecs, output.Vecs, rows, bat.RowCount(), mp, account)
	if err != nil || !admitted {
		output.Clean(mp)
		return nil, false, err
	}
	output.Attrs = append(output.Attrs, bat.Attrs...)
	output.Recursive, output.ShuffleIDX = bat.Recursive, bat.ShuffleIDX
	output.SetRowCount(len(rows))
	return output, true, nil
}
