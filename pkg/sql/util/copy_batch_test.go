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
	require.NoError(t, varlen.Shrink([]int64{1}, false, mp))
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

func TestCopyBatchCrossesAllocationOwnershipBoundary(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(
		account,
		1,
		1,
		2,
		3,
		4,
	)
	require.NoError(t, err)

	src := batch.NewOffHeapWithSize(1)
	vec := vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, vec.SetAllocationAccount(selection))
	require.NoError(t, vector.AppendFixed(vec, int64(42), false, mp))
	src.SetVector(0, vec)
	src.SetRowCount(1)
	sourceUsed := account.Snapshot().Used
	require.Positive(t, sourceUsed)

	got, err := CopyBatch(src, proc)
	require.NoError(t, err)
	require.Nil(t, got.Vecs[0].AllocationAccountSelection())
	require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
	require.Equal(t, sourceUsed, account.Snapshot().Used)

	src.Clean(mp)
	require.Zero(t, account.Snapshot().Used)
	require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[0], 0))
	got.Clean(mp)

	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
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
		require.NoError(b, vec.Shrink(sels, false, mp))
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

func TestCopyBatchDestinationAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	src := batch.NewWithSize(2)
	src.Attrs = []string{"id", "payload"}
	src.SetVector(0, vector.NewVec(types.T_int64.ToType()))
	src.SetVector(1, vector.NewVec(types.T_varchar.ToType()))
	require.NoError(t, vector.AppendFixedList(src.Vecs[0], []int64{1, 2}, []bool{false, true}, proc.Mp()))
	require.NoError(t, vector.AppendBytes(src.Vecs[1], []byte("a sufficiently long variable payload"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(src.Vecs[1], nil, true, proc.Mp()))
	src.SetRowCount(2)
	defer src.Clean(proc.Mp())
	for _, limit := range []uint64{1, 32, 1 << 20} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			registry, err := mpool.NewAllocationAccountRegistry(2, 1024)
			require.NoError(t, err)
			account, err := registry.Open(limit)
			require.NoError(t, err)
			selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerTxnWorkspace, 1, 2, 3, 4)
			require.NoError(t, err)
			dst, err := CopyBatchWithAllocation(src, proc, selection)
			if limit == 1<<20 {
				require.NoError(t, err)
				require.Positive(t, account.Snapshot().Used)
				require.NotNil(t, dst.AllocationAccountSelection())
				require.Equal(t, src.RowCount(), dst.RowCount())
				require.Equal(t, src.Vecs[1].GetBytesAt(0), dst.Vecs[1].GetBytesAt(0))
				dst.Clean(proc.Mp())
			} else {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.Nil(t, dst)
			}
			require.Zero(t, account.Snapshot().Used)
			require.Equal(t, int64(1), vector.MustFixedColNoTypeCheck[int64](src.Vecs[0])[0])
			_, _, err = registry.CompleteTerminal(account)
			require.NoError(t, err)
		})
	}
}

func BenchmarkCopyBatchDestinationAdmission(b *testing.B) {
	proc := testutil.NewProcess(b)
	src := batch.NewWithSize(1)
	src.Attrs = []string{"payload"}
	src.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	for range 32 {
		require.NoError(b, vector.AppendBytes(src.Vecs[0], []byte("a moderately wide variable payload for a workspace batch"), false, proc.Mp()))
	}
	src.SetRowCount(32)
	defer src.Clean(proc.Mp())
	registry, err := mpool.NewAllocationAccountRegistry(2, 1024)
	require.NoError(b, err)
	account, err := registry.Open(64 << 20)
	require.NoError(b, err)
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerTxnWorkspace, 1, 2, 3, 4)
	require.NoError(b, err)
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(b, err)
	cnRegistry, err := generation.AllocationAccountRegistry()
	require.NoError(b, err)
	cnAccount, err := cnRegistry.OpenWithController(generation.Cap(), generation)
	require.NoError(b, err)
	b.Cleanup(func() {
		require.Zero(b, cnAccount.Snapshot().Used)
		_, _, err := cnRegistry.CompleteTerminal(cnAccount)
		require.NoError(b, err)
		generation.Close()
	})
	cnSelection, err := vector.NewAllocationAccountSelection(cnAccount, mpool.AllocationOwnerTxnWorkspace, 1, 2, 3, 4)
	require.NoError(b, err)
	for _, tc := range []struct {
		name      string
		selection *vector.AllocationAccountSelection
	}{{"generic", nil}, {"workspace", selection}, {"workspace_cn", cnSelection}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				dst, err := CopyBatchWithAllocation(src, proc, tc.selection)
				if err != nil {
					b.Fatal(err)
				}
				dst.Clean(proc.Mp())
			}
		})
	}
	require.Zero(b, account.Snapshot().Used)
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(b, err)
}

func TestCopyBatchKeepsSourceAndDestinationChargesIndependent(t *testing.T) {
	proc := testutil.NewProcess(t)
	registry, err := mpool.NewAllocationAccountRegistry(2, 1024)
	require.NoError(t, err)
	source, err := registry.Open(1 << 20)
	require.NoError(t, err)
	target, err := registry.Open(1 << 20)
	require.NoError(t, err)
	srcSelection, err := vector.NewAllocationAccountSelection(source, mpool.AllocationOwnerDML, 1, 2, 3, 4)
	require.NoError(t, err)
	dstSelection, err := vector.NewAllocationAccountSelection(target, mpool.AllocationOwnerTxnWorkspace, 1, 2, 3, 4)
	require.NoError(t, err)
	src := batch.NewOffHeapWithSize(1)
	require.NoError(t, src.SetAllocationAccount(srcSelection))
	src.SetVector(0, vector.NewOffHeapVecWithType(types.T_int64.ToType()))
	defer src.Clean(proc.Mp())
	require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(42), false, proc.Mp()))
	src.SetRowCount(1)
	used := source.Snapshot().Used
	dst, err := CopyBatchWithAllocation(src, proc, dstSelection)
	require.NoError(t, err)
	defer dst.Clean(proc.Mp())
	require.Equal(t, used, source.Snapshot().Used)
	require.Positive(t, target.Snapshot().Used)
	src.Clean(proc.Mp())
	require.Zero(t, source.Snapshot().Used)
	require.Positive(t, target.Snapshot().Used)
	require.Equal(t, int64(42), vector.MustFixedColNoTypeCheck[int64](dst.Vecs[0])[0])
	dst.Clean(proc.Mp())
	require.Zero(t, target.Snapshot().Used)
	_, _, err = registry.CompleteTerminal(source)
	require.NoError(t, err)
	_, _, err = registry.CompleteTerminal(target)
	require.NoError(t, err)
}
