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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func BenchmarkVectorUnmarshalBinary(b *testing.B) {
	const rows = 8192
	mp := mpool.MustNewZero()

	cases := []struct {
		name  string
		typ   types.Type
		build func(*Vector) error
	}{
		{
			name: "fixed_int64",
			typ:  types.T_int64.ToType(),
			build: func(vec *Vector) error {
				return AppendFixedList(vec, make([]int64, rows), nil, mp)
			},
		},
		{
			name: "inline_varchar",
			typ:  types.T_varchar.ToType(),
			build: func(vec *Vector) error {
				values := make([][]byte, rows)
				for i := range values {
					values[i] = []byte("value")
				}
				return AppendBytesList(vec, values, nil, mp)
			},
		},
		{
			name: "null_heavy_int64",
			typ:  types.T_int64.ToType(),
			build: func(vec *Vector) error {
				nulls := make([]bool, rows)
				for i := range nulls {
					nulls[i] = i%2 == 0
				}
				return AppendFixedList(vec, make([]int64, rows), nulls, mp)
			},
		},
		{
			name: "array_float32",
			typ:  types.T_array_float32.ToType(),
			build: func(vec *Vector) error {
				for range rows {
					if err := AppendArray(vec, []float32{1, 2, 3, 4}, false, mp); err != nil {
						return err
					}
				}
				return nil
			},
		},
	}

	for _, test := range cases {
		source := NewVec(test.typ)
		if err := test.build(source); err != nil {
			b.Fatal(err)
		}
		data, err := source.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		source.Free(mp)

		b.Run(test.name+"/checked", func(b *testing.B) {
			var target Vector
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := target.UnmarshalBinary(data); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(test.name+"/trusted", func(b *testing.B) {
			var target Vector
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := target.UnmarshalBinaryTrusted(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
