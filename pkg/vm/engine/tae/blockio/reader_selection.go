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

package blockio

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// publishReaderSelection compacts the selected rows of each read column in
// place. Columns the caller marked unused are cleared instead.
func publishReaderSelection(output *batch.Batch, positions []int, rows []int64, request *engine.ReaderFilterRequest, mp *mpool.MPool) error {
	width := len(output.Vecs)
	for _, pos := range positions {
		vec := output.Vecs[pos]
		if len(rows) == 0 || request.ColumnUse(pos, width) == engine.ReaderColumnUnused {
			vec.CleanOnlyData()
			continue
		}
		if err := vec.Shrink(rows, false, mp); err != nil {
			return err
		}
	}
	output.SetRowCount(len(rows))
	return nil
}
