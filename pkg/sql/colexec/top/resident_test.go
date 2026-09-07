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

package top

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestTopResidentMigrationFailureKeepsOwner(t *testing.T) {
	for _, failure := range []string{"cancel", "short write"} {
		t.Run(failure, func(t *testing.T) {
			tc := newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, 1, []*plan.OrderBySpec{{Expr: newExpression(0)}})
			state := installTopTestAllocation(t, tc.arg, tc.proc, 4<<20)
			require.NoError(t, tc.arg.Prepare(tc.proc))
			first := newNullableVarcharTopBatch(t, tc.proc, 1, bytes.Repeat([]byte("x"), 64), false)
			tc.arg.ctr.n = 2
			require.NoError(t, tc.arg.ctr.build(tc.arg, first, tc.proc, tc.arg.OpAnalyzer))
			owner := tc.arg.ctr.bat
			if failure == "cancel" {
				ctx, cancel := context.WithCancel(tc.proc.Ctx)
				tc.proc.Ctx = ctx
				cancel()
			} else {
				tc.arg.ctr.spillWriter = shortTopSpillWriter{}
			}
			err := tc.arg.ctr.startResidentSpill(tc.proc, tc.arg.OpAnalyzer)
			if failure == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, io.ErrShortWrite)
			}
			require.Same(t, owner, tc.arg.ctr.bat)
			require.False(t, tc.arg.ctr.spilling)
			first.Clean(tc.proc.Mp())
			tc.arg.Free(tc.proc, true, err)
			finalizeTopTestAllocation(t, tc.arg, state)
			tc.proc.Free()
			require.Zero(t, tc.proc.Mp().CurrNB())
		})
	}
}

func TestTopRejectedBatchesDoNotSpill(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		t.Run(map[bool]string{false: "resident", true: "ordered spill"}[ordered], func(t *testing.T) {
			tc := newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, 1, []*plan.OrderBySpec{{Expr: newExpression(0)}})
			tc.arg.OrderedOutput = ordered
			state := installTopTestAllocation(t, tc.arg, tc.proc, 4<<20)
			require.NoError(t, tc.arg.Prepare(tc.proc))
			first := newNullableVarcharTopBatch(t, tc.proc, 1, bytes.Repeat([]byte("x"), 64), false)
			tc.arg.ctr.n = 2
			require.NoError(t, tc.arg.ctr.build(tc.arg, first, tc.proc, tc.arg.OpAnalyzer))
			written := tc.arg.ctr.spillOffset
			for i := 0; i < 3; i++ {
				loser := newNullableVarcharTopBatch(t, tc.proc, 2, bytes.Repeat([]byte("y"), 64), false)
				require.NoError(t, tc.arg.ctr.build(tc.arg, loser, tc.proc, tc.arg.OpAnalyzer))
				loser.Clean(tc.proc.Mp())
				require.Equal(t, written, tc.arg.ctr.spillOffset)
			}
			if !ordered {
				require.Zero(t, written)
				require.Zero(t, state.generation.SpillFDUsed())
			}
			first.Clean(tc.proc.Mp())
			tc.arg.Free(tc.proc, false, nil)
			finalizeTopTestAllocation(t, tc.arg, state)
			tc.proc.Free()
			require.Zero(t, tc.proc.Mp().CurrNB())
		})
	}
}

func TestTopResidentWinnerAdmission(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		limit  int64
		wideAt int
	}{
		{name: "replacement history compacts", limit: 3, wideAt: -1},
		{name: "migrate full heap mid batch", limit: 3, wideAt: 8},
		{name: "migrate partial heap mid batch", limit: 16, wideAt: 3},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			tc := newTestCase(t, mpool.MustNewZero(), []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, scenario.limit, []*plan.OrderBySpec{{Expr: newExpression(0)}})
			state := installTopTestAllocation(t, tc.arg, tc.proc, 4<<20)
			tc.arg.ctr.residentByteLimit = 4096
			for attempt := 0; attempt < 2; attempt++ {
				bat := batch.NewWithSize(2)
				bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
				for row := 0; row < 128; row++ {
					key := int64(128 - row)
					size := 64
					if row == scenario.wideAt {
						size = 4096
					}
					require.NoError(t, vector.AppendFixed(bat.Vecs[0], key, false, tc.proc.Mp()))
					require.NoError(t, vector.AppendBytes(bat.Vecs[1], bytes.Repeat([]byte{byte(key)}, size), false, tc.proc.Mp()))
				}
				bat.SetRowCount(128)
				resetChildren(tc.arg, []*batch.Batch{bat})
				require.NoError(t, tc.arg.Prepare(tc.proc))
				var keys []int64
				for {
					result, err := vm.Exec(tc.arg, tc.proc)
					require.NoError(t, err)
					if result.Batch == nil {
						break
					}
					for row := 0; row < result.Batch.RowCount(); row++ {
						key := vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[0], row)
						keys = append(keys, key)
						require.Equal(t, bytes.Repeat([]byte{byte(key)}, 64), result.Batch.Vecs[1].GetBytesAt(row))
					}
				}
				require.Len(t, keys, int(scenario.limit))
				for i, key := range keys {
					require.Equal(t, int64(i+1), key)
				}
				if scenario.wideAt < 0 {
					require.Zero(t, tc.arg.ctr.spillOffset)
					require.LessOrEqual(t, tc.arg.ctr.residentBytes, uint64(4096))
				} else {
					require.Positive(t, tc.arg.ctr.spillOffset)
				}
				tc.arg.GetChildren(0).Free(tc.proc, false, nil)
				tc.arg.Reset(tc.proc, false, nil)
				require.Zero(t, state.account.Snapshot().Used)
				require.Zero(t, state.generation.SpillDiskUsed())
				require.Zero(t, state.generation.SpillFDUsed())
			}
			tc.arg.Free(tc.proc, false, nil)
			finalizeTopTestAllocation(t, tc.arg, state)
			tc.proc.Free()
			require.Zero(t, tc.proc.Mp().CurrNB())
		})
	}
}

// Same workload as review 5127191041. Input setup/cleanup is not timed.
// Keep this a benchmark, not a million-row ordinary unit test.
func BenchmarkReviewSmallVarlenTop(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tc := newTestCase(b, mpool.MustNewZero(), []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, 1, []*plan.OrderBySpec{{Expr: newExpression(0)}})
		bats := make([]*batch.Batch, 128)
		payload := bytes.Repeat([]byte("x"), 64)
		for k := range bats {
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			for j := 0; j < 8192; j++ {
				require.NoError(b, vector.AppendFixed(bat.Vecs[0], int64(k*8192+j), false, tc.proc.Mp()))
				require.NoError(b, vector.AppendBytes(bat.Vecs[1], payload, false, tc.proc.Mp()))
			}
			bat.SetRowCount(8192)
			bats[k] = bat
		}
		resetChildren(tc.arg, bats)
		b.StartTimer()
		require.NoError(b, tc.arg.Prepare(tc.proc))
		result, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(b, err)
		require.NotNil(b, result.Batch)
		require.Equal(b, 1, result.Batch.RowCount())
		require.Equal(b, int64(0), vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[0], 0))
		b.StopTimer()
		b.ReportMetric(float64(tc.arg.ctr.spillOffset), "spill-bytes/op")
		tc.arg.Free(tc.proc, false, nil)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Zero(b, tc.proc.Mp().CurrNB())
	}
}
