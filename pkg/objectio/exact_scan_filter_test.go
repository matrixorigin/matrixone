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

package objectio

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func exactEntry(t *testing.T, v *vector.Vector) fileservice.IOEntry {
	t.Helper()
	payload, err := v.MarshalBinary()
	require.NoError(t, err)
	data := fileservice.NewBytes(append(EncodeIOEntryHeader(&IOEntryHeader{Type: IOET_ColData, Version: IOET_ColumnData_V2}), payload...))
	t.Cleanup(data.Release)
	return fileservice.IOEntry{CachedData: data}
}

func TestExactScanOwnsSelectedOutputAndReusesState(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	typ := types.T_int64.ToType()
	src := vector.NewVec(typ)
	t.Cleanup(func() { src.Free(mp) })
	require.NoError(t, vector.AppendFixedList(src, []int64{-2, -1, 0, 1, 2}, []bool{false, false, true, false, false}, mp))
	entry := exactEntry(t, src)
	before := bytes.Clone(entry.CachedData.Bytes())
	state := NewExactScanState(NewExactScanFilter([]ExactScanPredicate{{Column: 0, Type: typ, Op: ExactScanGE, Value: 0}, {Column: 0, Type: typ, Op: ExactScanLT, Value: 2}}))
	t.Cleanup(state.Close)
	dst := vector.NewVec(typ)
	t.Cleanup(func() { dst.Free(mp) })
	for pass := 0; pass < 2; pass++ {
		dst.CleanOnlyData()
		stats, err := MaterializeExactScan(t.Context(), []fileservice.IOEntry{entry}, []types.Type{typ}, []*vector.Vector{dst}, []int64{0, 2, 3, 4}, state, mp)
		require.NoError(t, err)
		require.True(t, stats.Applied)
		require.Equal(t, 4, stats.InputRows)
		require.Equal(t, 1, stats.OutputRows)
		require.Equal(t, []int64{1}, vector.MustFixedColNoTypeCheck[int64](dst))
		vector.MustFixedColNoTypeCheck[int64](dst)[0] = 99
		require.Equal(t, before, entry.CachedData.Bytes())
		for i := range state.views {
			require.Empty(t, state.views[i].GetData())
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	dst.CleanOnlyData()
	_, err := MaterializeExactScan(ctx, []fileservice.IOEntry{entry}, []types.Type{typ}, []*vector.Vector{dst}, nil, state, mp)
	require.ErrorIs(t, err, context.Canceled)
	for i := range state.views {
		require.Empty(t, state.views[i].GetData())
	}
	dst.CleanOnlyData()
	_, err = MaterializeExactScan(t.Context(), []fileservice.IOEntry{entry}, []types.Type{typ}, []*vector.Vector{dst}, []int64{1, 5}, state, mp)
	require.Error(t, err)
	require.Zero(t, dst.Length())
	state.Close()
	state.Close()
}

func TestExactScanScalarBoundariesAndConstants(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    types.Type
		values []uint64
		bound  uint64
		op     ExactScanOp
		want   []uint64
	}{
		{"unsigned", types.T_uint64.ToType(), []uint64{0, 1 << 63, math.MaxUint64}, 1 << 63, ExactScanGE, []uint64{1 << 63, math.MaxUint64}},
		{"decimal", types.New(types.T_decimal64, 18, 2), []uint64{math.MaxUint64 - 4, 0, 5}, 0, ExactScanLT, []uint64{math.MaxUint64 - 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
			src := vector.NewVec(tc.typ)
			t.Cleanup(func() { src.Free(mp) })
			require.NoError(t, vector.AppendFixedList(src, tc.values, nil, mp))
			entry := exactEntry(t, src)
			dst := vector.NewVec(tc.typ)
			t.Cleanup(func() { dst.Free(mp) })
			state := NewExactScanState(NewExactScanFilter([]ExactScanPredicate{{Column: 0, Type: tc.typ, Op: tc.op, Value: tc.bound}}))
			t.Cleanup(state.Close)
			stats, err := MaterializeExactScan(t.Context(), []fileservice.IOEntry{entry}, []types.Type{tc.typ}, []*vector.Vector{dst}, nil, state, mp)
			require.NoError(t, err)
			require.True(t, stats.Applied)
			require.Equal(t, tc.want, vector.MustFixedColNoTypeCheck[uint64](dst))
		})
	}
	for _, isNull := range []bool{false, true} {
		t.Run(map[bool]string{false: "constant_true", true: "constant_null"}[isNull], func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
			typ := types.T_int64.ToType()
			src, err := vector.NewConstFixed(typ, int64(7), 3, mp)
			require.NoError(t, err)
			if isNull {
				src.Free(mp)
				src = vector.NewConstNull(typ, 3, mp)
			}
			t.Cleanup(func() { src.Free(mp) })
			entry := exactEntry(t, src)
			dst := vector.NewVec(typ)
			t.Cleanup(func() { dst.Free(mp) })
			state := NewExactScanState(NewExactScanFilter([]ExactScanPredicate{{Column: 0, Type: typ, Op: ExactScanEQ, Value: 7}}))
			t.Cleanup(state.Close)
			stats, err := MaterializeExactScan(t.Context(), []fileservice.IOEntry{entry}, []types.Type{typ}, []*vector.Vector{dst}, nil, state, mp)
			require.NoError(t, err)
			require.True(t, stats.Applied)
			if isNull {
				require.Zero(t, dst.Length())
			} else {
				require.Equal(t, []int64{7, 7, 7}, vector.MustFixedColNoTypeCheck[int64](dst))
			}
		})
	}
}

func TestExactScanAllocationFailureClearsBorrowedViews(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(1)
	require.NoError(t, err)
	t.Cleanup(func() {
		snap := account.Seal()
		require.Zero(t, snap.Used)
		require.Zero(t, registry.LiveAllocationMetadata())
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	})
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	typ := types.T_int64.ToType()
	src := vector.NewVec(typ)
	t.Cleanup(func() { src.Free(mp) })
	require.NoError(t, vector.AppendFixedList(src, []int64{1, 2, 3}, nil, mp))
	entry := exactEntry(t, src)
	dst := vector.NewOffHeapVecWithType(typ)
	t.Cleanup(func() { dst.Free(mp) })
	require.NoError(t, dst.SetAllocationAccount(selection))
	state := NewExactScanState(NewExactScanFilter([]ExactScanPredicate{{Column: 0, Type: typ, Op: ExactScanGE, Value: 2}}))
	t.Cleanup(state.Close)
	_, err = MaterializeExactScan(t.Context(), []fileservice.IOEntry{entry}, []types.Type{typ}, []*vector.Vector{dst}, nil, state, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	for i := range state.views {
		require.Empty(t, state.views[i].GetData())
	}
	require.Zero(t, account.Snapshot().Used)
}

func TestExactScanPrimitiveTruthTables(t *testing.T) {
	for _, oid := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_date, types.T_decimal64} {
		t.Run(oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
			typ := oid.ToType()
			src := vector.NewVec(typ)
			payload := vector.NewVec(types.T_int32.ToType())
			t.Cleanup(func() { src.Free(mp); payload.Free(mp) })
			nulls := []bool{false, false, false, true}
			var err error
			bound := uint64(0)
			switch oid {
			case types.T_int8:
				err = vector.AppendFixedList(src, []int8{-1, 0, 1, 0}, nulls, mp)
			case types.T_int16:
				err = vector.AppendFixedList(src, []int16{-1, 0, 1, 0}, nulls, mp)
			case types.T_int32:
				err = vector.AppendFixedList(src, []int32{-1, 0, 1, 0}, nulls, mp)
			case types.T_int64:
				err = vector.AppendFixedList(src, []int64{-1, 0, 1, 0}, nulls, mp)
			case types.T_uint8:
				bound = 1
				err = vector.AppendFixedList(src, []uint8{0, 1, 2, 0}, nulls, mp)
			case types.T_uint16:
				bound = 1
				err = vector.AppendFixedList(src, []uint16{0, 1, 2, 0}, nulls, mp)
			case types.T_uint32:
				bound = 1
				err = vector.AppendFixedList(src, []uint32{0, 1, 2, 0}, nulls, mp)
			case types.T_uint64:
				bound = 1
				err = vector.AppendFixedList(src, []uint64{0, 1, 2, 0}, nulls, mp)
			case types.T_date:
				bound = 1
				err = vector.AppendFixedList(src, []types.Date{0, 1, 2, 0}, nulls, mp)
			case types.T_decimal64:
				err = vector.AppendFixedList(src, []types.Decimal64{types.Decimal64(math.MaxUint64), 0, 1, 0}, nulls, mp)
			}
			require.NoError(t, err)
			require.NoError(t, vector.AppendFixedList(payload, []int32{0, 1, 2, 3}, nil, mp))
			entries := []fileservice.IOEntry{exactEntry(t, src), exactEntry(t, payload)}
			for _, tc := range []struct {
				op   ExactScanOp
				want []int32
			}{
				{ExactScanEQ, []int32{1}}, {ExactScanNE, []int32{0, 2}},
				{ExactScanLT, []int32{0}}, {ExactScanLE, []int32{0, 1}},
				{ExactScanGT, []int32{2}}, {ExactScanGE, []int32{1, 2}},
			} {
				func() {
					state := NewExactScanState(NewExactScanFilter([]ExactScanPredicate{{Column: 0, Type: typ, Op: tc.op, Value: bound}}))
					defer state.Close()
					a, b := vector.NewVec(typ), vector.NewVec(types.T_int32.ToType())
					defer a.Free(mp)
					defer b.Free(mp)
					stats, readErr := MaterializeExactScan(t.Context(), entries, []types.Type{typ, types.T_int32.ToType()}, []*vector.Vector{a, b}, nil, state, mp)
					require.NoError(t, readErr)
					require.True(t, stats.Applied)
					require.Equal(t, tc.want, vector.MustFixedColNoTypeCheck[int32](b))
				}()
			}
		})
	}
}

func TestExactScanAllNoneAndMalformedSource(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	typ := types.T_int64.ToType()
	src := vector.NewVec(typ)
	t.Cleanup(func() { src.Free(mp) })
	require.NoError(t, vector.AppendFixedList(src, []int64{1, 2, 3}, nil, mp))
	entry := exactEntry(t, src)
	for _, bound := range []uint64{0, 4} {
		func() {
			state := NewExactScanState(NewExactScanFilter([]ExactScanPredicate{{Column: 0, Type: typ, Op: ExactScanGE, Value: bound}}))
			defer state.Close()
			dst := vector.NewVec(typ)
			defer dst.Free(mp)
			stats, err := MaterializeExactScan(t.Context(), []fileservice.IOEntry{entry}, []types.Type{typ}, []*vector.Vector{dst}, nil, state, mp)
			require.NoError(t, err)
			require.True(t, stats.Applied)
			if bound == 0 {
				require.Equal(t, []int64{1, 2, 3}, vector.MustFixedColNoTypeCheck[int64](dst))
			} else {
				require.Zero(t, dst.Length())
			}
		}()
	}
	state := NewExactScanState(NewExactScanFilter([]ExactScanPredicate{{Column: 0, Type: typ, Op: ExactScanGE}}))
	defer state.Close()
	dst := vector.NewVec(typ)
	defer dst.Free(mp)
	for _, tc := range []struct {
		rows []int64
		want []int64
	}{
		{[]int64{2, 1, 0}, []int64{3, 2, 1}},
		{[]int64{1, 1, 0}, []int64{2, 2, 1}},
	} {
		dst.CleanOnlyData()
		_, readErr := MaterializeExactScan(t.Context(), []fileservice.IOEntry{entry}, []types.Type{typ}, []*vector.Vector{dst}, tc.rows, state, mp)
		require.NoError(t, readErr)
		require.Equal(t, tc.want, vector.MustFixedColNoTypeCheck[int64](dst))
	}
	dst.CleanOnlyData()
	bad := fileservice.NewBytes([]byte{1})
	defer bad.Release()
	_, err := MaterializeExactScan(t.Context(), []fileservice.IOEntry{{CachedData: bad}}, []types.Type{typ}, []*vector.Vector{dst}, nil, state, mp)
	require.Error(t, err)
	require.Zero(t, dst.Length())
	for i := range state.views {
		require.Empty(t, state.views[i].GetData())
	}
}
