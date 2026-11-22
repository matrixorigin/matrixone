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

package vector

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// copyWithBorrowedArea replaces the empty varlen destination with a copy of
// source whose descriptors are owned and whose area is a borrowed alias of a
// private lent buffer: the shape a lender (for example a cache reader) gives a
// varlen column. The lent buffer is Go memory, so nothing has to release it.
func copyWithBorrowedArea(dst, source *Vector, mp *mpool.MPool) error {
	if dst == nil || source == nil || mp == nil || dst == source ||
		dst.length != 0 || dst.IsConst() || !dst.typ.IsVarlen() || dst.typ.Oid != source.typ.Oid {
		return moerr.NewInvalidInputNoCtx("borrowed area copy requires an empty compatible varlen destination")
	}
	private := NewVec(dst.typ)
	private.offHeap = dst.offHeap
	private.sorted = dst.sorted
	if err := private.UnionBatch(source, 0, source.length, nil, mp); err != nil {
		private.Free(mp)
		return err
	}
	if len(private.area) > 0 {
		lent := make([]byte, len(private.area))
		copy(lent, private.area)
		mp.Free(private.area)
		private.area = nil
		if err := private.InstallBorrowedArea(lent); err != nil {
			private.Free(mp)
			return err
		}
	}
	dst.Free(mp)
	*dst = *private
	return nil
}

// readonlyView returns a full-row view of v that aliases v's borrowed parts
// and copies the parts v owns.
func readonlyView(v *Vector, mp *mpool.MPool) (*Vector, error) {
	return v.WindowCopyingOwnedParts(0, v.Length(), mp)
}
