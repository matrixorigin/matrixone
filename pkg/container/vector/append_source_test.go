// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestAppendWithStringSource(t *testing.T) {
	for _, oid := range []types.T{types.T_int32, types.T_varchar} {
		t.Run(oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec := NewVec(oid.ToType())
			t.Cleanup(func() { vec.Free(mp); require.Zero(t, mp.CurrNB()) })
			appendValue := func(source types.StringSource, isNull bool) {
				if oid == types.T_int32 {
					require.NoError(t, AppendFixedWithStringSource(vec, int32(42), isNull, source, mp))
				} else {
					require.NoError(t, AppendBytesWithStringSource(vec, []byte("value"), isNull, source, mp))
				}
			}
			for _, source := range []types.StringSource{
				types.StringSourceExpression, types.StringSourceLiteral, types.StringSourceUserVariable,
				types.StringSourceSQLPrepare, types.StringSourceCOMStmt,
			} {
				// Reuse the vector with a different source after clearing all rows.
				vec.CleanOnlyData()
				appendValue(source, true)
				appendValue(source, false)
				require.Nil(t, vec.GetStringSources())
				require.Equal(t, source, vec.GetStringSource())
				require.Equal(t, source, vec.GetStringSourceAt(0))
				require.Equal(t, source, vec.GetStringSourceAt(1))
				require.True(t, vec.IsNull(0))
				require.False(t, vec.IsNull(1))
				if oid == types.T_int32 {
					require.Equal(t, int32(42), GetFixedAtNoTypeCheck[int32](vec, 1))
				} else {
					require.Equal(t, "value", vec.GetStringAt(1))
				}
			}
			// Append NULL with a distinct source, then a regular expression row.
			appendValue(types.StringSourceLiteral, true)
			require.Equal(t, types.StringSourceExpression, vec.GetStringSource())
			require.Equal(t, types.StringSourceCOMStmt, vec.GetStringSourceAt(1))
			require.Equal(t, types.StringSourceLiteral, vec.GetStringSourceAt(2))
			require.True(t, vec.IsNull(2))
			if oid == types.T_int32 {
				require.NoError(t, AppendFixed(vec, int32(7), false, mp))
			} else {
				require.NoError(t, AppendBytes(vec, []byte("ordinary"), false, mp))
			}
			require.Equal(t, types.StringSourceExpression, vec.GetStringSourceAt(3))
			appendValue(types.StringSourceSQLPrepare, false)
			require.Equal(t, types.StringSourceSQLPrepare, vec.GetStringSourceAt(4))
			if oid == types.T_varchar {
				value := bytes.Repeat([]byte{'x'}, types.VarlenaInlineSize+1)
				require.NoError(t, AppendBytesWithStringSource(vec, value, false, types.StringSourceLiteral, mp))
				require.Equal(t, value, vec.GetBytesAt(5))
				require.Equal(t, types.StringSourceLiteral, vec.GetStringSourceAt(5))
			}
		})
	}
}

func TestAppendWithStringSourceMixedAllocation(t *testing.T) {
	for _, n := range []int{100, 1000, 10000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec := NewVec(types.T_int32.ToType())
			t.Cleanup(func() { vec.Free(mp); require.Zero(t, mp.CurrNB()) })
			require.NoError(t, vec.PreExtend(n, mp))
			stats := mp.Stats()
			before := stats.NumAllocBytes.Load()
			for i := 0; i < n; i++ {
				source := types.StringSourceLiteral
				if i >= n/2 {
					source = types.StringSourceCOMStmt
				}
				require.NoError(t, AppendFixedWithStringSource(vec, int32(i), false, source, mp))
			}
			// Sidecars grow geometrically; no per-row prefix normalization is needed.
			allocated := stats.NumAllocBytes.Load() - before
			t.Logf("rows=%d sidecar bytes=%d", n, allocated)
			require.LessOrEqual(t, allocated, int64(8*n))
			require.Equal(t, types.StringSourceExpression, vec.GetStringSource())
			for i := 0; i < n; i++ {
				want := types.StringSourceLiteral
				if i >= n/2 {
					want = types.StringSourceCOMStmt
				}
				require.Equal(t, want, vec.GetStringSourceAt(i))
				require.Equal(t, int32(i), GetFixedAtNoTypeCheck[int32](vec, i))
			}
		})
	}
}

func TestAppendWithStringSourceRejectsInvalidInput(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int32.ToType())
	constant := NewConstNull(types.T_int32.ToType(), 1, mp)
	t.Cleanup(func() { vec.Free(mp); constant.Free(mp); require.Zero(t, mp.CurrNB()) })
	for _, test := range []struct {
		vec    *Vector
		mp     *mpool.MPool
		source types.StringSource
	}{
		{vec, mp, types.StringSource(255)},
		{vec, nil, types.StringSourceLiteral},
		{constant, mp, types.StringSourceLiteral},
	} {
		length := test.vec.Length()
		require.Error(t, AppendFixedWithStringSource(test.vec, int32(1), false, test.source, test.mp))
		require.Error(t, AppendBytesWithStringSource(test.vec, nil, true, test.source, test.mp))
		require.Equal(t, length, test.vec.Length())
	}
}

func TestAppendWithStringSourceAllocationFailure(t *testing.T) {
	for _, kind := range []string{"fixed-data", "fixed-sidecar", "null-admission", "bytes-sidecar", "bytes-area", "bytes-data"} {
		t.Run(kind, func(t *testing.T) {
			oid := types.T_int32
			if len(kind) >= 5 && kind[:5] == "bytes" {
				oid = types.T_varchar
			}
			budget := uint64(2 * oid.ToType().TypeSize())
			if kind == "bytes-area" {
				budget += 8
			}
			if kind == "bytes-data" || kind == "fixed-data" {
				budget = 1
			}
			state := newTestVectorAllocationAccount(t, budget, 16)
			mp := mpool.MustNewZero()
			vec := newAccountedTestVector(t, oid.ToType(), state.selection)
			t.Cleanup(func() {
				vec.Free(mp)
				finalizeTestVectorAllocationAccount(t, state)
				require.Zero(t, mp.CurrNB())
			})
			if kind != "bytes-data" && kind != "fixed-data" {
				require.NoError(t, vec.PreExtend(2, mp))
				if oid == types.T_int32 {
					require.NoError(t, AppendFixedWithStringSource(vec, int32(7), false, types.StringSourceLiteral, mp))
				} else {
					require.NoError(t, AppendBytesWithStringSource(vec, []byte("old"), false, types.StringSourceLiteral, mp))
				}
			}
			length, source := vec.Length(), vec.GetStringSource()
			var err error
			if oid == types.T_int32 {
				if kind == "null-admission" {
					err = AppendBytesWithStringSource(vec, nil, true, types.StringSourceCOMStmt, mp)
				} else {
					err = AppendFixedWithStringSource(vec, int32(9), false, types.StringSourceCOMStmt, mp)
				}
			} else {
				value := []byte("new")
				if kind == "bytes-area" {
					value = bytes.Repeat([]byte{'x'}, types.VarlenaInlineSize+1)
				}
				err = AppendBytesWithStringSource(vec, value, false, types.StringSourceCOMStmt, mp)
			}
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Equal(t, length, vec.Length())
			require.Equal(t, source, vec.GetStringSource())
			require.Nil(t, vec.GetStringSources())
			if length > 0 {
				require.Equal(t, types.StringSourceLiteral, vec.GetStringSourceAt(0))
				if oid == types.T_int32 {
					require.Equal(t, int32(7), GetFixedAtNoTypeCheck[int32](vec, 0))
				} else {
					require.Equal(t, "old", vec.GetStringAt(0))
				}
				// Retry with the original provenance and an inline/non-null value:
				// it fits the existing capacity and must not observe failed metadata.
				if oid == types.T_int32 {
					require.NoError(t, AppendFixedWithStringSource(vec, int32(9), false, source, mp))
				} else {
					require.NoError(t, AppendBytesWithStringSource(vec, []byte("retry"), false, source, mp))
				}
				require.Equal(t, 2, vec.Length())
				require.Equal(t, source, vec.GetStringSourceAt(1))
				require.Nil(t, vec.GetStringSources())
			}
		})
	}
}
