// Copyright 2026 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// boolSelectionPool backs the selections tests build from bool literals. It is
// separate from the test process's pool so allocation assertions on that pool
// are unaffected.
var boolSelectionPool = mpool.MustNewZeroNoFixed()

// boolSelection returns the selection bitmap of mask: row i is selected when
// mask[i] is true. A nil mask means every row, as for Eval.
func boolSelection(mask []bool) *bitmap.Bitmap {
	if mask == nil {
		return nil
	}
	sel := new(bitmap.Bitmap)
	if err := sel.InitWithBools(mask, nil, boolSelectionPool); err != nil {
		panic(err)
	}
	return sel
}
