// Copyright 2026 Matrix Origin
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

package util

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCopyBatchCompactsAndFlattens(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	src := batch.NewWithSize(4)
	src.Attrs = []string{"fixed", "varlen", "const", "const_null"}

	fixed := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(fixed, []int64{11}, []bool{false}, mp))
	fixed.GetNulls().Add(9)
	fixed.GetGrouping().Add(0, 9)
	fixed.SetSorted(true)

	dead := []byte("dead-")
	dead = append(dead, make([]byte, 4096)...)
	live := []byte("live-")
	live = append(live, make([]byte, 64)...)
	varlen := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(varlen, dead, false, mp))
	require.NoError(t, vector.AppendBytes(varlen, live, false, mp))
	varlen.Shrink([]int64{1}, false)
	require.Greater(t, len(varlen.GetArea()), len(live))

	constVec, err := vector.NewConstBytes(
		types.T_varchar.ToType(),
		[]byte("a repeated value long enough to use the area"),
		1,
		mp,
	)
	require.NoError(t, err)
	constNull := vector.NewConstNull(types.T_int32.ToType(), 1, mp)

	src.SetVector(0, fixed)
	src.SetVector(1, varlen)
	src.SetVector(2, constVec)
	src.SetVector(3, constNull)
	src.SetRowCount(1)

	got, err := CopyBatch(src, proc)
	require.NoError(t, err)
	require.Equal(t, src.Attrs, got.Attrs)
	require.Equal(t, src.RowCount(), got.RowCount())
	for _, vec := range got.Vecs {
		require.False(t, vec.IsConst())
		require.Equal(t, 1, vec.Length())
	}
	require.False(t, got.Vecs[0].GetSorted())
	require.Equal(t, int64(11), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
	require.True(t, got.Vecs[0].GetGrouping().Contains(0))
	require.False(t, got.Vecs[0].GetNulls().Contains(9))
	require.False(t, got.Vecs[0].GetGrouping().Contains(9))
	require.Equal(t, live, got.Vecs[1].GetBytesAt(0))
	require.Len(t, got.Vecs[1].GetArea(), len(live))
	require.Equal(t, "a repeated value long enough to use the area", string(got.Vecs[2].GetBytesAt(0)))
	require.True(t, got.Vecs[3].GetNulls().Contains(0))

	require.NoError(t, vector.SetFixedAtNoTypeCheck(fixed, 0, int64(99)))
	require.NoError(t, vector.SetBytesAt(varlen, 0, []byte("changed source value"), mp))
	require.Equal(t, int64(11), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
	require.Equal(t, live, got.Vecs[1].GetBytesAt(0))

	src.Clean(mp)
	got.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func BenchmarkCopyBatchCompact(b *testing.B) {
	proc := testutil.NewProcess(b)
	mp := proc.Mp()
	src := batch.NewWithSize(10)
	for col := range src.Vecs {
		vec := vector.NewVec(types.T_varchar.ToType())
		for row := 0; row < 1024; row++ {
			value := []byte(fmt.Sprintf("column-%d-row-%d-", col, row))
			value = append(value, make([]byte, 64)...)
			require.NoError(b, vector.AppendBytes(vec, value, false, mp))
		}
		sels := make([]int64, 0, 128)
		for row := 0; row < 1024; row += 8 {
			sels = append(sels, int64(row))
		}
		vec.Shrink(sels, false)
		src.SetVector(int32(col), vec)
	}
	src.SetRowCount(128)
	b.Cleanup(func() { src.Clean(mp) })

	bench := func(b *testing.B, copyFn func() (*batch.Batch, error)) {
		b.Helper()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			got, err := copyFn()
			if err != nil {
				b.Fatal(err)
			}
			got.Clean(mp)
		}
	}

	b.Run("union-all", func(b *testing.B) {
		bench(b, func() (*batch.Batch, error) {
			return copyBatchWithUnionAll(src, mp)
		})
	})
	b.Run("compact", func(b *testing.B) {
		bench(b, func() (*batch.Batch, error) {
			return CopyBatch(src, proc)
		})
	})
}

func BenchmarkCopyBatchAlreadyCompact(b *testing.B) {
	proc := testutil.NewProcess(b)
	mp := proc.Mp()
	src := batch.NewWithSize(10)
	for col := range src.Vecs {
		vec := vector.NewVec(types.T_varchar.ToType())
		for row := 0; row < 128; row++ {
			value := []byte(fmt.Sprintf("column-%d-row-%d-", col, row))
			value = append(value, make([]byte, 64)...)
			require.NoError(b, vector.AppendBytes(vec, value, false, mp))
		}
		src.SetVector(int32(col), vec)
	}
	src.SetRowCount(128)
	b.Cleanup(func() { src.Clean(mp) })

	bench := func(b *testing.B, copyFn func() (*batch.Batch, error)) {
		b.Helper()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			got, err := copyFn()
			if err != nil {
				b.Fatal(err)
			}
			got.Clean(mp)
		}
	}

	b.Run("union-all", func(b *testing.B) {
		bench(b, func() (*batch.Batch, error) {
			return copyBatchWithUnionAll(src, mp)
		})
	})
	b.Run("compact", func(b *testing.B) {
		bench(b, func() (*batch.Batch, error) {
			return CopyBatch(src, proc)
		})
	})
}

func copyBatchWithUnionAll(src *batch.Batch, mp *mpool.MPool) (*batch.Batch, error) {
	dst := batch.NewWithSize(len(src.Vecs))
	dst.Attrs = append(dst.Attrs, src.Attrs...)
	for i, srcVec := range src.Vecs {
		vec := vector.NewVec(*srcVec.GetType())
		if err := vector.GetUnionAllFunction(*srcVec.GetType(), mp)(vec, srcVec); err != nil {
			vec.Free(mp)
			dst.Clean(mp)
			return nil, err
		}
		dst.SetVector(int32(i), vec)
	}
	dst.SetRowCount(src.RowCount())
	return dst, nil
}
