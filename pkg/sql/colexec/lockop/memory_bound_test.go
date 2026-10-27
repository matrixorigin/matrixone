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

package lockop

import (
	"bytes"
	"math"
	"reflect"
	"strconv"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func lockRowsAllocation(rows [][]byte) int64 {
	size := int64(cap(rows)) * int64(unsafe.Sizeof([]byte{}))
	for _, key := range rows {
		size += int64(cap(key))
	}
	return size
}

func TestLockMemoryBoundCoversRealFetcherOwners(t *testing.T) {
	for _, mode := range []string{"int64", "bool", "uuid", "nul-varchar", "shared-varchar"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(proc.Free)
			sourceMP := mpool.MustNew("lock-memory-input")
			t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
			typ := types.T_int64.ToType()
			switch mode {
			case "bool":
				typ = types.T_bool.ToType()
			case "uuid":
				typ = types.T_uuid.ToType()
			case "nul-varchar", "shared-varchar":
				typ = types.T_varchar.ToType()
			}
			const count = 65 // cross a null-bitmap word and small key-slice capacity.
			source := vector.NewOffHeapVecWithType(typ)
			t.Cleanup(func() { source.Free(sourceMP) })
			if mode == "shared-varchar" {
				constant, err := vector.NewConstBytes(typ, bytes.Repeat([]byte{0}, 8<<10), count, sourceMP)
				require.NoError(t, err)
				defer constant.Free(sourceMP)
				require.NoError(t, source.UnionBatch(constant, 0, count, nil, sourceMP))
			} else {
				for row := 0; row < count; row++ {
					switch mode {
					case "bool":
						require.NoError(t, vector.AppendFixed(source, row%2 == 0, false, sourceMP))
					case "uuid":
						require.NoError(t, vector.AppendFixed(source, types.Uuid{}, false, sourceMP))
					case "nul-varchar":
						require.NoError(t, vector.AppendBytes(source, bytes.Repeat([]byte{0}, 127+row), false, sourceMP))
					default:
						require.NoError(t, vector.AppendFixed(source, int64(row-32), false, sourceMP))
					}
				}
			}
			if typ.IsVarlen() {
				source.GetNulls().Add(0)
			}
			batchBytes := source.WindowSize(0, count)
			var rowBytes int64
			for row := 0; row < count; row++ {
				rowBytes = max(rowBytes, source.WindowSize(row, row+1))
			}
			bound, ok := MemoryBound([]MemoryTarget{{PrimaryColumn: 0, RefreshColumn: -1, Type: typ}}, batchBytes, rowBytes, count)
			require.True(t, ok)
			baseline := proc.Mp().AllocatedBytes()
			input, err := getVec(proc, source)
			require.NoError(t, err)
			if input != source {
				t.Cleanup(func() { input.Free(proc.Mp()) })
			}
			packer := types.NewPacker()
			t.Cleanup(packer.Close)
			fetch := GetFetchRowsFunc(typ)
			has, rows, _ := fetch(input, packer, typ, count+1, false, nil, nil)
			require.True(t, has)
			// Real LockWithMayUpgrade can own this additional key pair while
			// AddWaitLock still references the original fetched rows.
			_, upgrade, _ := fetch(input, packer, typ, count+1, true, nil, nil)
			class, valid := malloc.ClassAllocationSize(uint64(cap(packer.GetBuf())))
			require.True(t, valid)
			actual := proc.Mp().Stats().HighWaterMark.Load() - baseline
			if input != source {
				actual += int64(nulls.Allocated(input.GetNulls()) + nulls.Allocated(input.GetGrouping()) + int(unsafe.Sizeof(vector.Vector{})))
			}
			actual += lockRowsAllocation(rows) + lockRowsAllocation(upgrade)
			// Both Packer generations can coexist during its class checkout.
			actual += 2 * int64(class)
			require.LessOrEqual(t, actual, bound, "real copied keys/nullable vector, bitmap, slice and Packer class stores must fit the pre-Prepare promise")
			if mode == "shared-varchar" {
				require.Greater(t, batchBytes, int64(len(source.GetArea()))*32, "the contract must count repeated referenced payload rather than shared physical area")
			}
		})
	}
}

func TestLockMemoryBoundCoversRefreshTimestampGrowth(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	targets := []MemoryTarget{{PrimaryColumn: 0, RefreshColumn: 1, Type: types.T_int64.ToType()}, {PrimaryColumn: 0, RefreshColumn: 1, Type: types.T_int64.ToType()}}
	const rows = 65
	bound, ok := MemoryBound(targets, rows*8, 8, rows)
	require.True(t, ok)
	without := append([]MemoryTarget(nil), targets...)
	for i := range without {
		without[i].RefreshColumn = -1
	}
	base, ok := MemoryBound(without, rows*8, 8, rows)
	require.True(t, ok)
	vec := vector.NewOffHeapVecWithType(types.T_TS.ToType())
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	before := proc.Mp().AllocatedBytes()
	for range targets {
		for row := 0; row < rows; row++ {
			require.NoError(t, vector.AppendFixed(vec, types.BuildTS(int64(row), 0), false, proc.Mp()))
		}
	}
	require.LessOrEqual(t, proc.Mp().Stats().HighWaterMark.Load()-before, bound-base, "the timestamp-specific term independently covers actual old/new growth, even when targets append to one shared timestamp column")
}

func TestLockMemoryTargetContractAndInvalidBounds(t *testing.T) {
	op := &LockOp{targets: []lockTarget{{primaryColumnIndexInBatch: 2, refreshTimestampIndexInBatch: 3, primaryColumnType: types.T_int64.ToType()}}}
	targets, ok := op.MemoryTargets()
	require.True(t, ok)
	require.Equal(t, []MemoryTarget{{PrimaryColumn: 2, RefreshColumn: 3, Type: types.T_int64.ToType()}}, targets)
	got, ok := op.MemoryBound(800, 8, 100)
	require.True(t, ok)
	want, ok := MemoryBound(targets, 800, 8, 100)
	require.True(t, ok)
	require.Equal(t, want, got)
	empty, ok := MemoryBound(nil, 0, 0, 0)
	require.True(t, ok)
	require.Equal(t, int64(4096)+2*int64(unsafe.Sizeof(types.Packer{})), empty, "Prepare owns its base Packer backing and object even for an optimistic/empty call")
	for _, invalid := range []lockTarget{
		{primaryColumnIndexInBatch: -1, refreshTimestampIndexInBatch: -1, primaryColumnType: types.T_int64.ToType()},
		{primaryColumnIndexInBatch: 0, refreshTimestampIndexInBatch: -2, primaryColumnType: types.T_int64.ToType()},
		{primaryColumnIndexInBatch: 0, refreshTimestampIndexInBatch: -1, primaryColumnType: types.T_text.ToType()},
		{primaryColumnIndexInBatch: 0, refreshTimestampIndexInBatch: -1, primaryColumnType: types.T_int64.ToType(), lockRows: &plan.Expr{}},
		{primaryColumnIndexInBatch: 0, refreshTimestampIndexInBatch: -1, primaryColumnType: types.T_int64.ToType(), filter: func(int, []int32) bool { return true }},
	} {
		op.targets = []lockTarget{invalid}
		_, ok := op.MemoryTargets()
		require.False(t, ok)
	}
	for _, args := range [][3]int64{{-1, 8, 1}, {8, -1, 1}, {8, 8, -1}, {math.MaxInt64, math.MaxInt64, 8192}, {800, 8, math.MaxInt}, {1, 8, 2}} {
		_, ok := MemoryBound(targets, args[0], args[1], int(args[2]))
		require.False(t, ok)
	}
}

func TestLockMemoryBoundCoversEmptyInputTableLock(t *testing.T) {
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_uuid.ToType(), types.T_varchar.ToType()} {
		t.Run(typ.String(), func(t *testing.T) {
			target := MemoryTarget{PrimaryColumn: 0, RefreshColumn: -1, Type: typ, LockTableAtEnd: true}
			bound, ok := MemoryBound([]MemoryTarget{target}, 0, 0, 0)
			require.True(t, ok)
			plain, ok := MemoryBound(nil, 0, 0, 0)
			require.True(t, ok)
			// The EOF path keeps the operator Packer and creates another in
			// LockTable, independently of whether any input batch existed.
			mainPacker, helperPacker := types.NewPacker(), types.NewPacker()
			t.Cleanup(mainPacker.Close)
			t.Cleanup(helperPacker.Close)
			fetch := GetFetchRowsFunc(typ)
			has, rows, _ := fetch(nil, helperPacker, typ, 0, true, nil, nil)
			require.True(t, has)
			_, upgrade, _ := fetch(nil, helperPacker, typ, 0, true, nil, nil)
			analyzer := process.NewTempAnalyzer()
			analyzer.GetOpStats().AddOpMetric(process.OpWaitLockTime, 1)
			structBytes := int64(reflect.TypeOf(analyzer).Elem().Size())
			require.LessOrEqual(t, structBytes, lockTempAnalyzerStructBytes, "the private analyzer layout must remain within its documented static allowance")
			actual := int64(cap(helperPacker.GetBuf())) + int64(unsafe.Sizeof(types.Packer{}))
			actual += lockRowsAllocation(rows) + lockRowsAllocation(upgrade)
			actual += structBytes + int64(unsafe.Sizeof(*analyzer.GetOpStats())+unsafe.Sizeof(*analyzer.GetOpCounterSet())) + lockTempMetricMapBytes
			require.LessOrEqual(t, actual, bound-plain, "the terminal-only helper owners need their own budget without relying on a nonempty source")
			op := &LockOp{targets: []lockTarget{{primaryColumnIndexInBatch: 0, refreshTimestampIndexInBatch: -1, primaryColumnType: typ, lockTableAtTheEnd: true}}}
			metadata, ok := op.MemoryTargets()
			require.True(t, ok)
			require.True(t, metadata[0].LockTableAtEnd)
		})
	}
}

func TestLockMemoryBoundCoversPrepareSliceGrowth(t *testing.T) {
	for _, count := range []int{33, 65, 513} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(proc.Free)
			op := &LockOp{targets: make([]lockTarget, count)}
			t.Cleanup(func() { op.Free(proc, false, nil) })
			for i := range op.targets {
				op.targets[i] = lockTarget{primaryColumnIndexInBatch: 0, refreshTimestampIndexInBatch: -1, primaryColumnType: types.T_int64.ToType()}
			}
			require.NoError(t, op.Prepare(proc))
			bound, ok := op.MemoryBound(0, 0, 0)
			require.True(t, ok)
			base, ok := MemoryBound(nil, 0, 0, 0)
			require.True(t, ok)
			fnWidth := int64(unsafe.Sizeof(FetchLockRowsFunc(nil)))
			relations := int64(cap(op.ctr.relations)) * 2 * int64(unsafe.Sizeof(uintptr(0)))
			retained := int64(cap(op.ctr.fetchers))*fnWidth + relations
			require.LessOrEqual(t, retained, bound-base, "actual Prepare capacities must fit the metadata-only budget")
			var funcs []FetchLockRowsFunc
			var peak int64
			for i := 0; i < count; i++ {
				old := cap(funcs)
				funcs = append(funcs, GetFetchRowsFunc(types.T_int64.ToType()))
				live := cap(funcs)
				if live != old {
					live += old
				}
				peak = max(peak, int64(live)*fnWidth)
			}
			require.LessOrEqual(t, peak+relations, bound-base, "old and new Go append backing coexist, beyond merely the final retained capacity")
		})
	}
}

func TestLockMemoryTargetsFollowImmutableRefreshFeature(t *testing.T) {
	op := &LockOp{targets: []lockTarget{{primaryColumnIndexInBatch: 0, refreshTimestampIndexInBatch: 0, primaryColumnType: types.T_int64.ToType()}}}
	disabled, ok := op.MemoryTargetsForRefresh(false)
	require.True(t, ok)
	require.Equal(t, int32(-1), disabled[0].RefreshColumn, "the disabled runtime branch never accesses the default-zero timestamp field")
	enabled, ok := op.MemoryTargetsForRefresh(true)
	require.True(t, ok)
	require.Equal(t, int32(0), enabled[0].RefreshColumn)
	require.Equal(t, int32(0), op.targets[0].refreshTimestampIndexInBatch, "describing a promise cannot mutate the actual operator")
	conservative, ok := op.MemoryTargets()
	require.True(t, ok)
	require.Equal(t, enabled, conservative, "an unknown client cannot be assumed to disable refresh")
	without, ok := MemoryBound(disabled, 128, 16, 8)
	require.True(t, ok)
	with, ok := MemoryBound(enabled, 128, 16, 8)
	require.True(t, ok)
	require.Greater(t, with, without, "the enabled timestamp owner retains its separate append budget")
}

func TestLockColumnBoundExcludesUnrelatedPayload(t *testing.T) {
	targets := []MemoryTarget{{PrimaryColumn: 0, RefreshColumn: -1, Type: types.T_varchar.ToType()}}
	columns := []ColumnMemoryBound{{RowBytes: 64, BatchBytes: 64 * 128}, {RowBytes: 1 << 20, BatchBytes: 128 << 20}}
	bound, ok := MemoryBoundForColumns(targets, columns, 128)
	require.True(t, ok)
	legacy, ok := MemoryBound(targets, columns[0].BatchBytes+columns[1].BatchBytes, columns[0].RowBytes+columns[1].RowBytes, 128)
	require.True(t, ok)
	require.Greater(t, legacy, bound)
	columns[1] = ColumnMemoryBound{RowBytes: 10 << 20, BatchBytes: 1280 << 20}
	unchanged, ok := MemoryBoundForColumns(targets, columns, 128)
	require.True(t, ok)
	require.Equal(t, bound, unchanged, "the key encoder never reads an unrelated JSON column")
	columns[0] = ColumnMemoryBound{RowBytes: 128, BatchBytes: 128 * 128}
	larger, ok := MemoryBoundForColumns(targets, columns, 128)
	require.True(t, ok)
	require.Greater(t, larger, bound, "growing the referenced key still grows its complete owner promise")
}

func TestLockColumnBoundKeepsLegacyAndRefreshLifetimes(t *testing.T) {
	targets := []MemoryTarget{{PrimaryColumn: 0, RefreshColumn: -1, Type: types.T_int64.ToType(), LockTableAtEnd: true}}
	columns := []ColumnMemoryBound{{RowBytes: 8, BatchBytes: 1024}}
	for _, rows := range []int{0, 128} {
		legacy, ok := MemoryBound(targets, 1024, 8, rows)
		require.True(t, ok)
		bound, ok := MemoryBoundForColumns(targets, columns, rows)
		require.True(t, ok)
		require.Equal(t, legacy, bound, "one exact column must preserve the existing allocation contract including EOF")
	}
	plain, ok := MemoryBoundForColumns(targets, columns, 128)
	require.True(t, ok)
	targets[0].RefreshColumn = 1
	columns = append(columns, ColumnMemoryBound{RowBytes: int64(types.T_TS.ToType().TypeSize()), BatchBytes: 128 * int64(types.T_TS.ToType().TypeSize())})
	refreshed, ok := MemoryBoundForColumns(targets, columns, 128)
	require.True(t, ok)
	require.Greater(t, refreshed, plain)
	targets[0].PrimaryColumn = 2
	_, ok = MemoryBoundForColumns(targets, columns, 128)
	require.False(t, ok)
	targets[0].PrimaryColumn = 0
	columns[0].BatchBytes = -1
	_, ok = MemoryBoundForColumns(targets, columns, 128)
	require.False(t, ok)
}
