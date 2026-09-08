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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// BenchmarkMarshalBinaryOwnedVarlena is the non-Arrow control for the shared
// vector serialization path. The vector is built through ordinary owned
// appends, and only MarshalBinaryWithBuffer is measured.
func BenchmarkMarshalBinaryOwnedVarlena(b *testing.B) {
	const rows = 8192
	tests := []struct {
		name   string
		value  []byte
		nullAt int
	}{
		{name: "inline_15_owned", value: bytes.Repeat([]byte{'i'}, 15), nullAt: -1},
		{name: "long_49_owned", value: bytes.Repeat([]byte{'l'}, 49), nullAt: -1},
		{name: "inline_15_one_null", value: bytes.Repeat([]byte{'i'}, 15), nullAt: rows / 2},
		{name: "long_49_one_null", value: bytes.Repeat([]byte{'l'}, 49), nullAt: rows / 2},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			vec := NewVec(types.T_varchar.ToType())
			for row := 0; row < rows; row++ {
				if err := AppendBytes(vec, test.value, row == test.nullAt, mp); err != nil {
					vec.Free(mp)
					b.Fatal(err)
				}
			}
			defer vec.Free(mp)

			var buf bytes.Buffer
			buf.Grow(vec.Size() + 64)
			b.ReportAllocs()
			b.SetBytes(int64(vec.Size()))
			b.ResetTimer()
			for range b.N {
				buf.Reset()
				if err := vec.MarshalBinaryWithBuffer(&buf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
