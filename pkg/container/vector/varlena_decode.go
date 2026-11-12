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

package vector

import "github.com/matrixorigin/matrixone/pkg/container/types"

// TryUnmarshalBinaryVarlena decodes a no-copy varlena vector for optional
// pruning. It returns nil for malformed or incompatible representations. Both
// specialized and general pruning consumers must treat nil as unknown, not as
// an empty membership set, and must not decode the same payload unchecked.
func TryUnmarshalBinaryVarlena(data []byte) (vec *Vector) {
	// UnmarshalBinary assumes a complete trusted buffer and can panic when a
	// serialized header or body is truncated. Keep recovery at this boundary.
	defer func() {
		if recover() != nil {
			vec = nil
		}
	}()
	// Do not let UnmarshalBinary reslice a truncated view into spare capacity.
	data = data[:len(data):len(data)]
	v := NewVec(types.T_any.ToType())
	if err := v.UnmarshalBinary(data); err != nil || !v.GetType().IsVarlen() || v.Length() < 0 {
		return nil
	}
	rows := v.Length()
	if v.IsConst() && rows > 0 {
		rows = 1
	}
	if len(v.GetData())/types.VarlenaSize != rows || len(v.GetData())%types.VarlenaSize != 0 {
		return nil
	}
	if rows > 0 {
		values, area := MustVarlenaRawData(v)
		for i := range values {
			if !values[i].IsSmall() {
				offset, length := values[i].OffsetLen()
				if uint64(offset)+uint64(length) > uint64(len(area)) {
					return nil
				}
			}
		}
	}
	return v
}
