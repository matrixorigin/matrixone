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
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// UnmarshalPrefixInVector prepares immutable expression bytes for prefix
// pruning. The result is sorted, non-null and prefix-free: no retained prefix
// starts with another. This preserves the prefix union and makes both binary
// membership and first/last bounds safe. Payload bytes, including empty and
// zero-leading prefixes, are opaque; SQL NULL belongs to the null bitmap.
//
// Canonical input is borrowed. Sorting/compaction uses one private heap copy,
// retained by the returned vector without introducing an mpool owner. An error
// means optional pruning must fall back to the residual expression.
func UnmarshalPrefixInVector(data []byte) (*Vector, error) {
	vec := NewVec(types.T_any.ToType())
	if err := vec.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	if !vec.typ.Oid.IsMySQLString() || (vec.class != FLAT && vec.class != CONSTANT) {
		return nil, moerr.NewInvalidInputNoCtx("invalid prefix IN vector type or class")
	}
	if vec.Length() == 0 {
		return NewVec(vec.typ), nil
	}
	if vec.IsConstNull() || vec.GetNulls().Any() {
		return nil, moerr.NewInvalidInputNoCtx("nullable prefix IN vector")
	}
	if vec.IsConst() {
		vec.SetLength(1)
		vec.SetSorted(true)
		return vec, nil
	}

	col, area := MustVarlenaRawData(vec)
	canonical := true
	prev := col[0].GetByteSlice(area)
	for i := 1; i < len(col); i++ {
		curr := col[i].GetByteSlice(area)
		n := min(len(prev), len(curr))
		// Equality means overlap (including duplicates); greater means unsorted.
		if bytes.Compare(prev[:n], curr[:n]) >= 0 {
			canonical = false
			break
		}
		prev = curr
	}
	if canonical {
		vec.SetSorted(true)
		return vec, nil
	}

	owned := NewVec(types.T_any.ToType())
	// The exact same immutable bytes passed the checked decoder above.
	if err := owned.UnmarshalBinaryTrusted(bytes.Clone(data)); err != nil {
		return nil, err
	}
	owned.InplaceSort()
	col, area = MustVarlenaRawData(owned)
	n := 1
	for i := 1; i < len(col); i++ {
		if !bytes.HasPrefix(col[i].GetByteSlice(area), col[n-1].GetByteSlice(area)) {
			col[n] = col[i]
			n++
		}
	}
	owned.SetLength(n)
	owned.SetSorted(true)
	return owned, nil
}
