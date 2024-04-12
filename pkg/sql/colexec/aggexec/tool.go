// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// vectorAppendWildly is a more efficient version of vector.AppendFixed.
// It ignores the const and null flags check, and uses a wilder way to append (avoiding the overhead of appending one by one).
func vectorAppendWildly[T numeric | types.Decimal64 | types.Decimal128](v *vector.Vector, mp *mpool.MPool, value T) error {
	oldLen := v.Length()
	if oldLen == v.Capacity() {
		if err := v.PreExtend(10, mp); err != nil {
			return err
		}
	}
	v.SetLength(oldLen + 1)

	var vs []T
	vector.ToSlice(v, &vs)
	vs[oldLen] = value
	return nil
}

var _ = vectorAppendBytesWildly

func vectorAppendBytesWildly(v *vector.Vector, mp *mpool.MPool, value []byte) error {
	var va types.Varlena
	if err := vector.BuildVarlenaFromByteSlice(v, &va, &value, mp); err != nil {
		return err
	}

	oldLen := v.Length()
	if oldLen == v.Capacity() {
		if err := v.PreExtend(10, mp); err != nil {
			return err
		}
	}
	v.SetLength(oldLen + 1)

	var vs []types.Varlena
	vector.ToSlice(v, &vs)
	vs[oldLen] = va
	return nil
}

func FromD64ToD128(v types.Decimal64) types.Decimal128 {
	k := types.Decimal128{
		B0_63:   uint64(v),
		B64_127: 0,
	}
	if v.Sign() {
		k.B64_127 = ^k.B64_127
	}
	return k
}
