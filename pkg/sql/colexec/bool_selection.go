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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// InitBoolSelection sets sel to the rows of the first rows rows where the
// boolean vector vec is TRUE: a NULL is not TRUE, and a constant selects every
// row or none. It builds the bitmap a word at a time and reuses sel's words.
func InitBoolSelection(sel *bitmap.Bitmap, vec *vector.Vector, rows int, mp *mpool.MPool) error {
	if !vec.GetType().IsBoolean() {
		return moerr.NewInvalidInputNoCtx("filter condition is not boolean")
	}
	if vec.IsConst() {
		if err := sel.InitWithSize(int64(rows), mp); err != nil {
			return err
		}
		if rows > 0 && !vec.IsConstNull() && vector.GetFixedAtNoTypeCheck[bool](vec, 0) {
			sel.AddRange(0, uint64(rows))
		}
		return nil
	}
	if vec.Length() < rows {
		return moerr.NewInternalErrorNoCtxf("filter condition has %d rows for %d rows", vec.Length(), rows)
	}
	var nulls *bitmap.Bitmap
	if nsp := vec.GetNulls(); !nsp.IsEmpty() {
		nulls = nsp.GetBitmap()
	}
	return sel.InitWithBools(vector.MustFixedColNoTypeCheck[bool](vec)[:rows], nulls, mp)
}
