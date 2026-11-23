// Copyright 2021 Matrix Origin
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
	"github.com/stretchr/testify/require"
)

// BenchmarkAppendValueRows measures the public append boundary used by CSV
// materialization and casts. Each operation reuses physical capacity for one
// object block; metadata controls retain the same value and row-count oracles.
// Performance acceptance compares matched binaries, without a timing threshold
// in ordinary unit tests.
func BenchmarkAppendValueRows(b *testing.B) {
	const rows = 8192
	for _, test := range []struct {
		name       string
		oid        types.T
		value      []byte
		isNull     bool
		seed       bool
		prepare    bool
		literal    bool
		binaryText bool
	}{
		{name: "ordinary_int64", oid: types.T_int64},
		{name: "null_int64", oid: types.T_int64, isNull: true},
		{name: "ordinary_inline_varchar", oid: types.T_varchar, value: []byte("ordinary column")},
		{name: "ordinary_area_varchar", oid: types.T_varchar, value: bytes.Repeat([]byte{'x'}, types.VarlenaInlineSize+1)},
		{name: "null_varchar", oid: types.T_varchar, isNull: true},
		{name: "prepared_prefix_int64", oid: types.T_int64, seed: true, prepare: true},
		{name: "literal_prefix_int64", oid: types.T_int64, seed: true, literal: true},
		{name: "binary_prefix_varchar", oid: types.T_varchar, value: []byte("ordinary column"), seed: true, binaryText: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			vec := NewVec(test.oid.ToType())
			b.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				vec.Free(mp)
				require.Zero(b, mp.CurrNB())
			})
			require.NoError(b, vec.PreExtend(rows, mp))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				vec.ResetWithSameType()
				start := 0
				if test.seed {
					if test.oid == types.T_int64 {
						if err := AppendFixed(vec, int64(42), false, mp); err != nil {
							b.Fatal(err)
						}
					} else if err := AppendBytes(vec, test.value, false, mp); err != nil {
						b.Fatal(err)
					}
					start = 1
					if test.prepare {
						vec.SetPrepareParamKind(PrepareParamInteger)
					}
					if test.literal {
						if err := vec.SetStringSource(types.StringSourceLiteral); err != nil {
							b.Fatal(err)
						}
					}
					if test.binaryText {
						vec.SetIsBinaryString(true)
					}
				}
				if test.oid == types.T_int64 {
					for row := start; row < rows; row++ {
						if err := AppendFixed(vec, int64(42), test.isNull, mp); err != nil {
							b.Fatal(err)
						}
					}
				} else {
					for row := start; row < rows; row++ {
						if err := AppendBytes(vec, test.value, test.isNull, mp); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
			b.StopTimer()
			require.Equal(b, rows, vec.Length())
			require.Equal(b, test.isNull, vec.IsNull(rows-1))
			if !test.isNull {
				if test.oid == types.T_int64 {
					require.Equal(b, int64(42), GetFixedAtNoTypeCheck[int64](vec, rows-1))
				} else {
					require.Equal(b, test.value, vec.GetBytesAt(rows-1))
				}
			}
			require.Equal(b, PrepareParamNone, vec.GetPrepareParamKindAt(rows-1))
			require.Equal(b, types.StringSourceExpression, vec.GetStringSourceAt(rows-1))
			if test.prepare {
				require.Equal(b, PrepareParamInteger, vec.GetPrepareParamKindAt(0))
			}
			if test.literal {
				require.Equal(b, types.StringSourceLiteral, vec.GetStringSourceAt(0))
			}
			if test.binaryText {
				require.True(b, vec.GetIsBinaryStringAt(0))
				require.False(b, vec.GetIsBinaryStringAt(rows-1))
			}
		})
	}
}
