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

package aggexec

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func newAccountedPercentileSpillTest[T numeric | types.Decimal64 | types.Decimal128, R types.FixedSizeTExceptStrType](
	t *testing.T, mp *mpool.MPool, typ types.Type, mode orderedPercentileMode, groups int,
) *orderedPercentileExec[T, R] {
	t.Helper()
	id := AggIdOfPercentileCont
	if mode == orderedPercentileDiscrete {
		id = AggIdOfPercentileDisc
	}
	agg, err := makeOrderedPercentileExec(mp, id, false, typ, mode)
	require.NoError(t, err)
	exec := agg.(*orderedPercentileExec[T, R])
	// Test configuration-before-account as well as the production reverse order.
	ConfigureOrderedPercentileSpill(exec, 1, context.Background(),
		func() (*os.File, error) { return os.CreateTemp(t.TempDir(), "percentile-*") }, nil)
	registry, account, allocation := newTestAggregateAllocation(t)
	require.NoError(t, exec.SetAllocationAccount(allocation))
	require.NoError(t, exec.SetExtraInformation(EncodeOrderedPercentileConfig([]byte("0.5"), false), 0))
	require.NoError(t, exec.GroupGrow(groups))
	t.Cleanup(func() {
		file := exec.spillData
		exec.Free()
		if file != nil {
			_, err := file.Stat()
			require.Error(t, err)
		}
		finishTestAggregateAllocation(t, registry, account)
	})
	return exec
}

func TestOrderedPercentileAccountedSpillBoundsAndTail(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	exec := newAccountedPercentileSpillTest[int64, float64](t, mp,
		types.T_int64.ToType(), orderedPercentileContinuous, 3)
	values := make([]int64, 2049)
	groups := make([]uint64, len(values))
	for i := 0; i < 2048; i++ {
		values[i], groups[i] = int64(i), uint64(i%2+1)
	}
	groups[2048] = 3
	vec := buildFixedVec(t, mp, exec.argType, values)
	vec.SetNull(2048)
	defer vec.Free(mp)
	var spillRows, spillBytes int64
	exec.spillReport = func(size, rows, _ int64) { spillBytes += size; spillRows += rows }
	for i := 0; i < 67; i++ {
		for offset := 0; offset < len(groups); offset += hashmap.UnitLimit {
			work := groups[offset:min(offset+hashmap.UnitLimit, len(groups))]
			require.NoError(t, exec.PreflightBatchFill(offset, work, []*vector.Vector{vec}))
			require.NoError(t, exec.BatchFill(offset, work, []*vector.Vector{vec}))
		}
		require.Less(t, exec.activeOrderedMemorySize(), orderedPercentileMinRunSize)
		// Fixed arena/count floor plus bounded batch/scratch, not all-history.
		require.Less(t, exec.accounted.allocation.account.Snapshot().Used, uint64(2<<20))
		for _, runs := range exec.spillRuns {
			require.LessOrEqual(t, len(runs), orderedPercentileRunFanIn)
		}
	}
	require.Greater(t, spillRows, int64(100000))
	require.NoError(t, exec.spillOrderedState(context.Background()))
	tail := buildFixedVec(t, mp, exec.argType, []int64{1024})
	defer tail.Free(mp)
	require.NoError(t, exec.Fill(0, 0, []*vector.Vector{tail}))
	require.Equal(t, uint32(1), exec.accounted.state[0].argCnt[0])
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, 1024.0, vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
	require.Equal(t, 1024.0, vector.GetFixedAtNoTypeCheck[float64](result[0], 1))
	require.True(t, result[0].IsNull(2))
	result[0].Free(mp)
	require.Equal(t, int64(67*2048+1), spillRows)
	require.Equal(t, spillRows*8, spillBytes)
	// Repeated Flush reads the same published history, without duplicate tails.
	result, err = exec.Flush()
	require.NoError(t, err)
	require.Equal(t, 1024.0, vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
	result[0].Free(mp)
	require.Equal(t, int64(67*2048+1), spillRows)
}

func TestOrderedPercentileAccountedFillDoesNotSpillArenaFloor(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	exec := newAccountedPercentileSpillTest[int64, int64](t, mp,
		types.T_int64.ToType(), orderedPercentileDiscrete, 1)
	vec := buildFixedVec(t, mp, exec.argType, []int64{9})
	defer vec.Free(mp)
	for i := 0; i < 10; i++ {
		require.NoError(t, exec.Fill(0, 0, []*vector.Vector{vec}))
	}
	require.False(t, exec.hasSpillRuns())
	require.Nil(t, exec.spillData)
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(9), vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	result[0].Free(mp)
}

func TestOrderedPercentileAccountedPartialMergeAndGuards(t *testing.T) {
	for _, batchMerge := range []bool{false, true} {
		t.Run(map[bool]string{false: "Merge", true: "BatchMerge"}[batchMerge], func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			dst := newAccountedPercentileSpillTest[int64, float64](t, mp,
				types.T_int64.ToType(), orderedPercentileContinuous, 1)
			src := newAccountedPercentileSpillTest[int64, float64](t, mp,
				dst.argType, orderedPercentileContinuous, 1)
			src.spillLimit = 0 // A transport producer never owns local runs.
			values := make([]int64, 2048)
			for i := range values {
				values[i] = int64(i)
			}
			vec := buildFixedVec(t, mp, dst.argType, values)
			defer vec.Free(mp)
			require.NoError(t, src.BulkFill(0, []*vector.Vector{vec}))
			var encoded bytes.Buffer
			require.NoError(t, src.SaveIntermediateResultOfChunk(0, &encoded))
			require.NoError(t, src.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()), mp))
			for i := 0; i < 2; i++ {
				require.NoError(t, dst.PreflightBatchMerge(src, 0, []uint64{1}))
				if batchMerge {
					require.NoError(t, dst.BatchMerge(src, 0, []uint64{1}))
				} else {
					require.NoError(t, dst.Merge(src, 0, 0))
				}
			}
			require.True(t, dst.hasSpillRuns())
			var out bytes.Buffer
			require.Error(t, dst.SaveIntermediateResultOfChunk(0, &out))
			require.Error(t, dst.SaveIntermediateResult(1, [][]uint8{{1}}, &out))
			require.Error(t, dst.SaveSpillIntermediateRows(0, []int32{0}, &out))
			require.Zero(t, out.Len())
			require.Error(t, src.PreflightBatchMerge(dst, 0, []uint64{1}))
			require.Error(t, src.Merge(dst, 0, 0))
			require.Error(t, src.BatchMerge(dst, 0, []uint64{1}))
			result, err := dst.Flush()
			require.NoError(t, err)
			require.Equal(t, 1023.5, vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
			result[0].Free(mp)
			oldFile := dst.spillData
			require.Error(t, dst.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()[:5]), mp))
			require.Same(t, oldFile, dst.spillData)
			require.NoError(t, dst.UnmarshalFromReader(bytes.NewReader(encoded.Bytes()), mp))
			require.False(t, dst.hasSpillRuns())
			_, err = oldFile.Stat()
			require.Error(t, err)
			result, err = dst.Flush()
			require.NoError(t, err)
			require.Equal(t, 1023.5, vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
			result[0].Free(mp)
		})
	}
}

func TestOrderedPercentileAccountedSpillFailures(t *testing.T) {
	for _, failure := range []string{"create", "write", "second-group", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			exec := newAccountedPercentileSpillTest[int64, float64](t, mp,
				types.T_int64.ToType(), orderedPercentileContinuous, 2)
			exec.spillLimit = 0
			vec := buildFixedVec(t, mp, exec.argType, []int64{1, 3})
			defer vec.Free(mp)
			require.NoError(t, exec.BatchFill(0, []uint64{1, 2}, []*vector.Vector{vec}))
			create := exec.spillFile
			injected := errors.New("injected spill failure")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch failure {
			case "create":
				exec.spillFile = func() (*os.File, error) { return nil, injected }
			case "write":
				file, err := create()
				require.NoError(t, err)
				exec.spillData = file
				require.NoError(t, file.Close())
			case "second-group":
				exec.spillReport = func(_, _, _ int64) { cancel() }
			case "cancel":
				cancel()
			}
			require.Error(t, exec.spillOrderedState(ctx))
			require.False(t, exec.hasSpillRuns())
			require.Equal(t, []uint32{1, 1}, exec.accounted.state[0].argCnt[:2])
			exec.spillReport = nil
			if failure == "write" {
				exec.closeSpillData()
			}
			exec.spillFile = create
			require.NoError(t, exec.spillOrderedState(context.Background()))
			require.Equal(t, uint64(1), exec.spilledGroupRows(0))
			require.Equal(t, uint64(1), exec.spilledGroupRows(1))
			result, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, []float64{1, 3}, vector.MustFixedColNoTypeCheck[float64](result[0]))
			result[0].Free(mp)
			cancelled, stop := context.WithCancel(context.Background())
			stop()
			_, err = exec.FlushWithContext(cancelled)
			require.ErrorIs(t, err, context.Canceled)
			require.NoError(t, exec.spillData.Close())
			_, err = exec.Flush()
			require.Error(t, err)
		})
	}
}

func TestOrderedPercentileCompactionFailurePreservesRuns(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	exec := newAccountedPercentileSpillTest[int64, float64](t, mp,
		types.T_int64.ToType(), orderedPercentileContinuous, 1)
	exec.spillRuns = make([][]orderedPercentileRun, 1)
	for i := 0; i < orderedPercentileRunFanIn; i++ {
		require.NoError(t, exec.writeOrderedRun(context.Background(), 0, []int64{int64(i)}))
	}
	// A higher-level run at the front used to be overwritten by runs[:0]
	// before a failed compaction could publish its replacement.
	exec.spillRuns[0] = append([]orderedPercentileRun{{level: 1}}, exec.spillRuns[0]...)
	before := append([]orderedPercentileRun(nil), exec.spillRuns[0]...)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, exec.compactRunsAtLevel(ctx, 0, 0))
	require.Equal(t, before, exec.spillRuns[0])
}

func TestOrderedPercentileAccountedSpillAllocationFailure(t *testing.T) {
	for _, stage := range []string{"scratch", "reset"} {
		t.Run(stage, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			exec := newAccountedPercentileSpillTest[int64, float64](t, mp,
				types.T_int64.ToType(), orderedPercentileContinuous, 1)
			vec := buildFixedVec(t, mp, exec.argType, []int64{7})
			defer vec.Free(mp)
			require.NoError(t, exec.Fill(0, 0, []*vector.Vector{vec}))
			account := exec.accounted.allocation.account
			if stage == "scratch" {
				account.Seal()
			} else {
				exec.spillReport = func(_, _, _ int64) { account.Seal() }
			}
			require.Error(t, exec.spillOrderedState(context.Background()))
			if stage == "scratch" {
				require.False(t, exec.hasSpillRuns())
				require.Equal(t, uint32(1), exec.accounted.state[0].argCnt[0])
			} else {
				require.Equal(t, uint64(1), exec.spilledGroupRows(0))
				require.Zero(t, exec.accounted.GetNumGroups())
				require.Zero(t, account.Snapshot().Used)
			}
		})
	}
}

func TestOrderedPercentileAccountedDecimalSpillMatchesResident(t *testing.T) {
	for _, mode := range []orderedPercentileMode{orderedPercentileContinuous, orderedPercentileDiscrete} {
		for _, descending := range []bool{false, true} {
			for _, percentile := range []string{"0", "0.95", "1"} {
				t.Run(string(rune('0'+mode))+"/"+map[bool]string{false: "asc", true: "desc"}[descending]+"/"+percentile, func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
					typ := types.New(types.T_decimal128, 30, 3)
					values := make([]types.Decimal128, 2048)
					for i := range values {
						values[i] = types.Decimal128{B0_63: uint64(i * 12345)}
					}
					vec := buildFixedVec(t, mp, typ, values)
					defer vec.Free(mp)
					var got [2]types.Decimal128
					for i, spill := range []bool{false, true} {
						exec := newAccountedPercentileSpillTest[types.Decimal128, types.Decimal128](t, mp, typ, mode, 1)
						if !spill {
							exec.spillLimit = 0
						}
						require.NoError(t, exec.SetExtraInformation(EncodeOrderedPercentileConfig([]byte(percentile), descending), 0))
						require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
						require.Equal(t, spill, exec.hasSpillRuns())
						result, err := exec.Flush()
						require.NoError(t, err)
						got[i] = vector.GetFixedAtNoTypeCheck[types.Decimal128](result[0], 0)
						result[0].Free(mp)
					}
					require.Equal(t, got[0], got[1])
				})
			}
		}
	}
}
