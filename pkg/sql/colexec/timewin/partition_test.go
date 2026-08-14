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

package timewin

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// nullPart stands in for a NULL partition key in the decoded output.
const nullPart = int64(-9999)

// row is one input tuple: a timestamp, the value to aggregate, and the
// partition key. Input must already be ordered by (part, ts), which is what
// the planner's ORDER BY guarantees.
type row struct {
	ts   string
	val  int32
	part int64
}

func mustDatetime(t *testing.T, s string) types.Datetime {
	t.Helper()
	d, err := types.ParseDatetime(s, 0)
	require.NoError(t, err)
	return d
}

func mustDatetimeScale(t *testing.T, s string, scale int32) types.Datetime {
	t.Helper()
	d, err := types.ParseDatetime(s, scale)
	require.NoError(t, err)
	return d
}

// makePartInput builds a batch shaped (ts datetime, val int32, part int64).
func makePartInput(t *testing.T, mp *mpool.MPool, rows []row) *batch.Batch {
	t.Helper()
	ts := make([]string, 0, len(rows))
	vals := make([]int32, 0, len(rows))
	parts := make([]int64, 0, len(rows))
	for _, r := range rows {
		ts = append(ts, r.ts)
		vals = append(vals, r.val)
		parts = append(parts, r.part)
	}

	bat := batch.New([]string{"ts", "val", "part"})
	bat.Vecs[0] = testutil.NewVector(len(rows), types.T_datetime.ToType(), mp, false, ts)
	bat.Vecs[1] = testutil.NewVector(len(rows), types.T_int32.ToType(), mp, false, vals)
	bat.Vecs[2] = testutil.NewVector(len(rows), types.T_int64.ToType(), mp, false, parts)
	bat.SetRowCount(len(rows))
	return bat
}

func newPartArg(t *testing.T, proc *process.Process, sliding types.Datetime, withPartition bool) *TimeWin {
	t.Helper()
	arg := &TimeWin{
		WStart: true,
		WEnd:   false,
		Types:  []types.Type{types.T_int32.ToType()},
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				function.AggSumOverloadID, false, []*plan.Expr{newExpression(1)}, nil),
		},
		TsType:   plan.Type{Id: int32(types.T_datetime)},
		Ts:       newExpression(0),
		Interval: makeInterval(),
		Sliding:  sliding,
	}
	if withPartition {
		partExpr := newExpression(2)
		partExpr.Typ = plan.Type{Id: int32(types.T_int64)}
		arg.PartitionBy = []*plan.Expr{partExpr}
	}
	return arg
}

func newPreparedTextPartArg(t *testing.T, sliding types.Datetime) *TimeWin {
	t.Helper()
	partExpr := newExpression(2)
	partExpr.Typ = plan.Type{Id: int32(types.T_text)}
	return &TimeWin{
		WStart: true,
		Types:  []types.Type{types.T_int32.ToType()},
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				function.AggSumOverloadID, false, []*plan.Expr{newExpression(1)}, nil),
		},
		TsType:      plan.Type{Id: int32(types.T_datetime)},
		Ts:          newExpression(0),
		Interval:    makeInterval(),
		Sliding:     sliding,
		PartitionBy: []*plan.Expr{partExpr},
	}
}

func makePreparedTextPartInput(
	t *testing.T,
	mp *mpool.MPool,
	key string,
	kinds []vector.PrepareParamKind,
) *batch.Batch {
	t.Helper()
	rows := len(kinds)
	timestamps := make([]string, rows)
	values := make([]int32, rows)
	keys := make([][]byte, rows)
	for row := range kinds {
		timestamps[row] = "2023-08-01 00:00:00"
		if row > 0 {
			timestamps[row] = "2023-08-01 00:00:12"
		}
		values[row] = int32(row + 1)
		keys[row] = []byte(key)
	}
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = testutil.NewVector(rows, types.T_datetime.ToType(), mp, false, timestamps)
	bat.Vecs[1] = testutil.NewVector(rows, types.T_int32.ToType(), mp, false, values)
	bat.Vecs[2] = vector.NewVec(types.T_text.ToType())
	for _, value := range keys {
		require.NoError(t, vector.AppendBytes(bat.Vecs[2], value, false, mp))
	}
	require.NoError(t, bat.Vecs[2].SetPrepareParamKindsWithMP(kinds, mp))
	bat.SetRowCount(rows)
	return bat
}

func TestTimeWinBroadcastsPreparedPartitionKind(t *testing.T) {
	sliding, err := calcDatetime(5, 2)
	require.NoError(t, err)
	for _, gapFill := range []bool{false, true} {
		t.Run(map[bool]string{false: "sliding", true: "gapfill"}[gapFill], func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			var first, second *batch.Batch
			var arg *TimeWin
			t.Cleanup(func() {
				if arg != nil {
					arg.Free(proc, false, nil)
				}
				if first != nil {
					first.Clean(proc.Mp())
				}
				if second != nil {
					second.Clean(proc.Mp())
				}
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			first = makePreparedTextPartInput(t, proc.Mp(), "5", []vector.PrepareParamKind{
				vector.PrepareParamFloat,
				vector.PrepareParamInteger,
			})
			second = makePreparedTextPartInput(t, proc.Mp(), "6", []vector.PrepareParamKind{
				vector.PrepareParamDecimal,
				vector.PrepareParamBoolean,
			})
			arg = newPreparedTextPartArg(t, sliding)
			arg.GapFill = gapFill
			arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second}))
			require.NoError(t, arg.Prepare(proc))
			want := map[string]vector.PrepareParamKind{
				"5": vector.PrepareParamFloat,
				"6": vector.PrepareParamDecimal,
			}
			seen := make(map[string]vector.PrepareParamKind)
			rows := 0
			for {
				result, callErr := vm.Exec(arg, proc)
				require.NoError(t, callErr)
				if result.Batch == nil || result.Status == vm.ExecStop {
					break
				}
				part := result.Batch.Vecs[2]
				for row := 0; row < part.Length(); row++ {
					key := string(part.GetBytesAt(row))
					require.Contains(t, want, key)
					seen[key] = part.GetPrepareParamKindAt(row)
				}
				rows += part.Length()
			}
			require.Positive(t, rows)
			require.Equal(t, want, seen)
		})
	}
}

// runPartArg drives the operator to exhaustion and returns (wstart, max, part)
// per output row.
func runPartArg(t *testing.T, arg *TimeWin, proc *process.Process, in *batch.Batch) (starts []types.Datetime, sums []int64, parts []int64) {
	t.Helper()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{in})
	arg.Children = nil
	arg.AppendChild(op)
	require.NoError(t, arg.Prepare(proc))

	for {
		res, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if res.Batch == nil || res.Status == vm.ExecStop {
			if res.Batch == nil {
				break
			}
		}
		bat := res.Batch
		n := bat.Vecs[0].Length()
		// Layout: [max, _wstart, part...]
		sums = append(sums, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])[:n]...)
		starts = append(starts, vector.MustFixedColNoTypeCheck[types.Datetime](bat.Vecs[1])[:n]...)
		if len(arg.PartitionBy) > 0 {
			pv := bat.Vecs[2]
			for i := 0; i < n; i++ {
				switch {
				case pv.IsConstNull():
					// A null partition key broadcasts as a const-null vector,
					// which carries no data to index.
					parts = append(parts, nullPart)
				case pv.IsConst():
					parts = append(parts, vector.MustFixedColNoTypeCheck[int64](pv)[0])
				default:
					parts = append(parts, vector.MustFixedColNoTypeCheck[int64](pv)[i])
				}
			}
		}
		if res.Status == vm.ExecStop {
			break
		}
	}
	return
}

func TestTimeWinSlidingKeepsZeroDatetimeSeparateFromEpoch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sliding, err := calcDatetime(5, types.Second)
	require.NoError(t, err)

	in := makePartInput(t, proc.Mp(), []row{
		{"0000-00-00 00:00:00", 10, 1},
		{"0001-01-01 00:00:00", 20, 1},
	})
	arg := newPartArg(t, proc, sliding, false)
	starts, sums, _ := runPartArg(t, arg, proc, in)

	require.Equal(t, []types.Datetime{types.ZeroDatetime, types.DatetimeEpoch}, starts)
	require.Equal(t, []int64{10, 20}, sums)

	arg.Free(proc, false, nil)
	in.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestTimeWinSlidingMicrosecondWindows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	interval, err := calcDatetime(1000, types.MicroSecond)
	require.NoError(t, err)
	sliding, err := calcDatetime(500, types.MicroSecond)
	require.NoError(t, err)

	in := batch.New([]string{"ts", "val"})
	in.Vecs[0] = vector.NewVec(types.T_datetime.ToTypeWithScale(6))
	in.Vecs[1] = testutil.MakeInt32Vector([]int32{10, 20, 30}, nil, proc.Mp())
	require.NoError(t, vector.AppendFixedList(in.Vecs[0], []types.Datetime{
		mustDatetimeScale(t, "2026-08-12 10:00:00.000100", 6),
		mustDatetimeScale(t, "2026-08-12 10:00:00.000900", 6),
		mustDatetimeScale(t, "2026-08-12 10:00:00.001100", 6),
	}, nil, proc.Mp()))
	in.SetRowCount(3)

	arg := newPartArg(t, proc, sliding, false)
	arg.Interval = interval
	arg.TsType = plan.Type{Id: int32(types.T_datetime), Scale: 6}
	starts, sums, _ := runPartArg(t, arg, proc, in)

	require.Equal(t, []types.Datetime{
		mustDatetimeScale(t, "2026-08-12 10:00:00.000000", 6),
		mustDatetimeScale(t, "2026-08-12 10:00:00.000500", 6),
		mustDatetimeScale(t, "2026-08-12 10:00:00.001000", 6),
	}, starts)
	require.Equal(t, []int64{30, 50, 30}, sums)

	arg.Free(proc, false, nil)
	in.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestTimeWinMicrosecondBoundariesPreservePrecisionAcrossInputScales(t *testing.T) {
	for _, scale := range []int32{0, 3, 6} {
		t.Run(types.T_datetime.ToTypeWithScale(scale).String(), func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			oneMicrosecond, err := calcDatetime(1, types.MicroSecond)
			require.NoError(t, err)

			in := batch.New([]string{"ts", "val"})
			in.Vecs[0] = vector.NewVec(types.T_datetime.ToTypeWithScale(scale))
			in.Vecs[1] = testutil.MakeInt32Vector([]int32{10}, nil, proc.Mp())
			require.NoError(t, vector.AppendFixedList(in.Vecs[0], []types.Datetime{
				mustDatetimeScale(t, "2026-08-12 10:00:00", scale),
			}, nil, proc.Mp()))
			in.SetRowCount(1)

			arg := newPartArg(t, proc, oneMicrosecond, false)
			arg.WEnd = true
			arg.Interval = oneMicrosecond
			arg.TsType = plan.Type{Id: int32(types.T_datetime), Scale: 6}
			op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{in})
			arg.Children = nil
			arg.AppendChild(op)
			require.NoError(t, arg.Prepare(proc))

			res, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			require.NotNil(t, res.Batch)
			require.GreaterOrEqual(t, len(res.Batch.Vecs), 3)

			startVec := res.Batch.Vecs[1]
			endVec := res.Batch.Vecs[2]
			require.Equal(t, int32(6), startVec.GetType().Scale)
			require.Equal(t, int32(6), endVec.GetType().Scale)
			start := vector.MustFixedColNoTypeCheck[types.Datetime](startVec)[0]
			end := vector.MustFixedColNoTypeCheck[types.Datetime](endVec)[0]
			require.Equal(t, types.Datetime(1), end-start)

			arg.Free(proc, false, nil)
			in.Clean(proc.Mp())
			proc.Free()
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

// Sliding windows carry state across rows, so a partition boundary has to
// restart that state. Each partition must produce exactly the windows it would
// have produced on its own.
func TestTimeWinSlidingPartitionResetsWindowState(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sliding, err := calcDatetime(5, 2) // 5 seconds, interval is 5s => tumbling-by-slide
	require.NoError(t, err)

	in := makePartInput(t, proc.Mp(), []row{
		{"2023-08-01 00:00:00", 10, 1},
		{"2023-08-01 00:00:01", 20, 1},
		{"2023-08-01 00:00:06", 30, 1},
		{"2023-08-01 00:00:00", 100, 2},
		{"2023-08-01 00:00:07", 400, 2},
	})

	arg := newPartArg(t, proc, sliding, true)
	starts, sums, parts := runPartArg(t, arg, proc, in)

	require.Equal(t, []types.Datetime{
		mustDatetime(t, "2023-08-01 00:00:00"),
		mustDatetime(t, "2023-08-01 00:00:05"),
		mustDatetime(t, "2023-08-01 00:00:00"),
		mustDatetime(t, "2023-08-01 00:00:05"),
	}, starts)
	// Partition 1's sum must never absorb partition 2's rows: a leak of
	// partition 2's first row would make the first window 130 instead of 30.
	require.Equal(t, []int64{30, 30, 100, 400}, sums)
	require.Equal(t, []int64{1, 1, 2, 2}, parts)

	arg.Free(proc, false, nil)
	in.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// A partition whose last windows are still pending when the boundary arrives
// must emit them before the next partition starts; dropping them silently
// loses rows.
func TestTimeWinPartitionEmitsTrailingWindows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sliding, err := calcDatetime(5, 2)
	require.NoError(t, err)

	in := makePartInput(t, proc.Mp(), []row{
		// partition 1 spans three windows
		{"2023-08-01 00:00:00", 1, 1},
		{"2023-08-01 00:00:06", 2, 1},
		{"2023-08-01 00:00:12", 3, 1},
		// partition 2 has a single row
		{"2023-08-01 00:00:00", 9, 2},
	})

	arg := newPartArg(t, proc, sliding, true)
	starts, sums, parts := runPartArg(t, arg, proc, in)

	require.Equal(t, []int64{1, 1, 1, 2}, parts, "partition 1 must not lose its trailing windows")
	require.Equal(t, []int64{1, 2, 3, 9}, sums)
	require.Len(t, starts, 4)

	arg.Free(proc, false, nil)
	in.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestGapFillGeneratesOnlyInteriorBuckets(t *testing.T) {
	for _, tc := range []struct {
		name       string
		gapFill    bool
		wantStarts []string
		wantNull   []bool
	}{
		{name: "legacy", wantStarts: []string{"2023-08-01 00:00:00", "2023-08-01 00:00:10"}, wantNull: []bool{false, false}},
		{name: "gapfill", gapFill: true, wantStarts: []string{"2023-08-01 00:00:00", "2023-08-01 00:00:05", "2023-08-01 00:00:10"}, wantNull: []bool{false, true, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			sliding, err := calcDatetime(5, types.Second)
			require.NoError(t, err)
			in := makePartInput(t, proc.Mp(), []row{
				{"2023-08-01 00:00:00", 1, 1},
				{"2023-08-01 00:00:12", 3, 1},
			})
			arg := newPartArg(t, proc, sliding, false)
			arg.GapFill = tc.gapFill
			op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{in})
			arg.AppendChild(op)
			require.NoError(t, arg.Prepare(proc))
			var starts []types.Datetime
			var nulls []bool
			for {
				res, callErr := vm.Exec(arg, proc)
				require.NoError(t, callErr)
				if res.Batch == nil {
					break
				}
				for i := 0; i < res.Batch.RowCount(); i++ {
					starts = append(starts, vector.GetFixedAtNoTypeCheck[types.Datetime](res.Batch.Vecs[1], i))
					nulls = append(nulls, res.Batch.Vecs[0].IsNull(uint64(i)))
				}
			}
			want := make([]types.Datetime, len(tc.wantStarts))
			for i := range want {
				want[i] = mustDatetime(t, tc.wantStarts[i])
			}
			require.Equal(t, want, starts)
			require.Equal(t, tc.wantNull, nulls)
			arg.Free(proc, false, nil)
			in.Clean(proc.Mp())
			proc.Free()
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func TestGapFillResourceAccountingLimits(t *testing.T) {
	arg := &TimeWin{GapFill: true}
	ctr := container{partitionWindows: maxGapFillRowsPerPartition + 1}
	require.ErrorContains(t, ctr.accountGapFillWindow(arg), "partition")

	ctr = container{gapFillWindows: maxGapFillRowsTotal}
	require.ErrorContains(t, ctr.accountGapFillWindow(arg), "total")

	ctr = container{partitionWindows: maxGapFillRowsPerPartition - 1, gapFillWindows: maxGapFillRowsTotal - 1}
	require.NoError(t, ctr.accountGapFillWindow(arg))

	// The post-flush transition owns the next GAPFILL window's accounting.
	// Verify that its limit error reaches the operator caller instead of being
	// swallowed while the replacement aggregate generation is resumed.
	arg.ctr = container{
		status:           resumeAfterFlush,
		partitionWindows: maxGapFillRowsPerPartition,
	}
	_, err := arg.Call(nil)
	require.ErrorContains(t, err, "partition")
}

// A single partition must behave exactly like the unpartitioned operator.
func TestTimeWinSinglePartitionMatchesNoPartition(t *testing.T) {
	rows := []row{
		{"2023-08-01 00:00:00", 10, 7},
		{"2023-08-01 00:00:01", 20, 7},
		{"2023-08-01 00:00:06", 30, 7},
	}
	sliding, err := calcDatetime(5, 2)
	require.NoError(t, err)

	procA := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	inA := makePartInput(t, procA.Mp(), rows)
	argA := newPartArg(t, procA, sliding, true)
	startsA, sumsA, partsA := runPartArg(t, argA, procA, inA)

	procB := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	inB := makePartInput(t, procB.Mp(), rows)
	argB := newPartArg(t, procB, sliding, false)
	startsB, sumsB, partsB := runPartArg(t, argB, procB, inB)

	require.Equal(t, startsB, startsA)
	require.Equal(t, sumsB, sumsA)
	require.Empty(t, partsB)
	require.Equal(t, []int64{7, 7}, partsA)

	argA.Free(procA, false, nil)
	inA.Clean(procA.Mp())
	procA.Free()
	require.Equal(t, int64(0), procA.Mp().CurrNB())

	argB.Free(procB, false, nil)
	inB.Clean(procB.Mp())
	procB.Free()
	require.Equal(t, int64(0), procB.Mp().CurrNB())
}

// GROUP BY folds NULL keys into one group, so the window must treat two NULL
// keys as the same partition rather than as a boundary.
func TestTimeWinPartitionNullKeysGroupTogether(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	bat := batch.New([]string{"ts", "val", "part"})
	bat.Vecs[0] = testutil.NewVector(2, types.T_datetime.ToType(), proc.Mp(), false,
		[]string{"2023-08-01 00:00:00", "2023-08-01 00:00:01"})
	bat.Vecs[1] = testutil.NewVector(2, types.T_int32.ToType(), proc.Mp(), false, []int32{10, 20})
	partVec := testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{0, 0})
	partVec.GetNulls().Add(0)
	partVec.GetNulls().Add(1)
	bat.Vecs[2] = partVec
	bat.SetRowCount(2)

	sliding, err := calcDatetime(5, 2)
	require.NoError(t, err)
	arg := newPartArg(t, proc, sliding, true)
	_, sums, _ := runPartArg(t, arg, proc, bat)

	// One partition, one window: both rows aggregate together.
	require.Equal(t, []int64{30}, sums)

	arg.Free(proc, false, nil)
	bat.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestTimeWinAnyNullPartitionKey(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := []row{
		{"2023-08-01 00:00:00", 10, 0},
		{"2023-08-01 00:00:01", 20, 0},
		{"2023-08-01 00:00:06", 30, 0},
	}
	in := makePartInput(t, proc.Mp(), rows)
	in.Vecs[2].Free(proc.Mp())
	in.Vecs[2] = vector.NewConstNull(types.T_any.ToType(), len(rows), proc.Mp())

	sliding, err := calcDatetime(5, 2)
	require.NoError(t, err)
	arg := newPartArg(t, proc, sliding, true)
	arg.PartitionBy[0].Typ = plan.Type{Id: int32(types.T_any)}

	_, sums, parts := runPartArg(t, arg, proc, in)
	require.Equal(t, []int64{30, 30}, sums)
	require.Equal(t, []int64{nullPart, nullPart}, parts)

	arg.Free(proc, false, nil)
	in.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestAnyPartitionKeyRejectsNonNullValue(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_any.ToType()
	src := vector.NewVec(typ)
	src.SetLength(1)
	dst := vector.NewVec(typ)

	err := getPartitionSetFunction(typ, mp)(dst, src, 0, 1)
	require.ErrorContains(t, err, "non-NULL T_any partition key")

	src.Free(mp)
	dst.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestSamePartition(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	ctr := &container{}
	ctr.partExe = make([]colexec.ExpressionExecutor, 1)

	v := testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{5, 5, 9})
	v.GetNulls().Add(2)
	ctr.partVec = [][]*vector.Vector{{v}}

	require.True(t, ctr.samePartition(0, 0, 0, 1), "equal keys are one partition")
	require.False(t, ctr.samePartition(0, 0, 0, 2), "a null key differs from a non-null one")
	require.True(t, ctr.samePartition(0, 2, 0, 2), "a null key equals itself")

	v.Free(proc.Mp())
	proc.Free()
}

// Without sliding the operator takes the `interval` path, where the child
// aggregate has already reduced each (partition, window) to a single row and
// the operator only decorates it. The partition key must pass straight through
// rather than be broadcast per window.
func TestTimeWinIntervalPathForwardsPartitionKeys(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	in := makePartInput(t, proc.Mp(), []row{
		{"2023-08-01 00:00:00", 10, 1},
		{"2023-08-01 00:00:06", 30, 1},
		{"2023-08-01 00:00:00", 100, 2},
	})

	arg := newPartArg(t, proc, 0, true)
	// A non-nil EndExpr is what selects the pass-through path.
	arg.EndExpr = newExpression(0)
	arg.WEnd = true

	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{in})
	arg.Children = nil
	arg.AppendChild(op)
	require.NoError(t, arg.Prepare(proc))

	res, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)

	// Layout: [sum, _wstart, _wend, part]
	require.Len(t, res.Batch.Vecs, 4)
	parts := vector.MustFixedColNoTypeCheck[int64](res.Batch.Vecs[3])
	require.Equal(t, []int64{1, 1, 2}, parts[:res.Batch.Vecs[0].Length()],
		"each input row keeps its own key on the pass-through path")

	arg.Free(proc, false, nil)
	in.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// A Reset/Prepare cycle must be a full generation boundary: the second run
// gets the same output a fresh operator would produce, from cursors and
// buffers restarted at zero, and Free still returns every byte. Before the
// fix, the second run resumed through nextWindow with flushed-out aggregate
// state and crashed in GroupGrow.
func TestTimeWinReuseAfterReset(t *testing.T) {
	rows := []row{
		{"2023-08-01 00:00:00", 10, 1},
		{"2023-08-01 00:00:01", 20, 1},
		{"2023-08-01 00:00:06", 30, 1},
		{"2023-08-01 00:00:00", 100, 2},
		{"2023-08-01 00:00:07", 400, 2},
	}
	sliding, err := calcDatetime(5, 2)
	require.NoError(t, err)

	cases := []struct {
		name          string
		withPartition bool
	}{
		{"partitioned sliding", true},
		{"sliding without partitions", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

			arg := newPartArg(t, proc, sliding, tc.withPartition)

			// The first generation reads two child batches so its buffers hold
			// two entries; the second reads one, which must land in entry 0
			// rather than continue after the stale pair.
			bats1 := []*batch.Batch{
				makePartInput(t, proc.Mp(), rows[:3]),
				makePartInput(t, proc.Mp(), rows[3:]),
			}
			starts1, sums1, parts1 := runPartArgBats(t, arg, proc, bats1)
			require.NotEmpty(t, sums1)

			arg.Reset(proc, false, nil)

			in2 := makePartInput(t, proc.Mp(), rows)
			starts2, sums2, parts2 := runPartArg(t, arg, proc, in2)

			require.Equal(t, starts1, starts2)
			require.Equal(t, sums1, sums2)
			require.Equal(t, parts1, parts2)

			arg.Free(proc, false, nil)
			for _, b := range bats1 {
				b.Clean(proc.Mp())
			}
			in2.Clean(proc.Mp())
			proc.Free()
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

// The interval pass-through path buffers one vector set per child batch,
// indexed by the same cursor; reuse must restart it at zero as well.
func TestTimeWinIntervalPathReuseAfterReset(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	rows := []row{
		{"2023-08-01 00:00:00", 10, 1},
		{"2023-08-01 00:00:06", 30, 1},
		{"2023-08-01 00:00:00", 100, 2},
	}

	arg := newPartArg(t, proc, 0, true)
	// A non-nil EndExpr selects the pass-through path.
	arg.EndExpr = newExpression(0)
	arg.WEnd = true

	// Layout: [val, _wstart, _wend, part]; the values pass through unreduced.
	run := func(in *batch.Batch) (vals []int32, parts []int64) {
		op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{in})
		arg.Children = nil
		arg.AppendChild(op)
		require.NoError(t, arg.Prepare(proc))
		for {
			res, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			if res.Batch == nil {
				break
			}
			n := res.Batch.Vecs[0].Length()
			vals = append(vals, vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])[:n]...)
			parts = append(parts, vector.MustFixedColNoTypeCheck[int64](res.Batch.Vecs[3])[:n]...)
			if res.Status == vm.ExecStop {
				break
			}
		}
		return
	}

	in1 := makePartInput(t, proc.Mp(), rows)
	vals1, parts1 := run(in1)
	require.Equal(t, []int32{10, 30, 100}, vals1)

	arg.Reset(proc, false, nil)

	in2 := makePartInput(t, proc.Mp(), rows)
	vals2, parts2 := run(in2)
	require.Equal(t, vals1, vals2)
	require.Equal(t, parts1, parts2)

	arg.Free(proc, false, nil)
	in1.Clean(proc.Mp())
	in2.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// runPartArgBats drives the operator over several child batches; the
// partition state machine must behave identically no matter where the
// batch boundaries fall.
func runPartArgBats(t *testing.T, arg *TimeWin, proc *process.Process, bats []*batch.Batch) (starts []types.Datetime, sums []int64, parts []int64) {
	t.Helper()
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
	require.NoError(t, arg.Prepare(proc))

	for {
		res, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
		bat := res.Batch
		n := bat.Vecs[0].Length()
		sums = append(sums, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])[:n]...)
		starts = append(starts, vector.MustFixedColNoTypeCheck[types.Datetime](bat.Vecs[1])[:n]...)
		if len(arg.PartitionBy) > 0 {
			pv := bat.Vecs[2]
			for i := 0; i < n; i++ {
				if pv.IsConst() {
					parts = append(parts, vector.MustFixedColNoTypeCheck[int64](pv)[0])
				} else {
					parts = append(parts, vector.MustFixedColNoTypeCheck[int64](pv)[i])
				}
			}
		}
		if res.Status == vm.ExecStop {
			break
		}
	}
	return
}

func makeFlushBoundaryRows(gap int, part int64) []row {
	start := time.Date(2023, time.August, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Duration(gap) * 5 * time.Second)
	return []row{
		{start.Format("2006-01-02 15:04:05"), 1, part},
		{end.Format("2006-01-02 15:04:05"), 2, part},
	}
}

func makeDenseWindowRows(count int, part int64) []row {
	start := time.Date(2023, time.August, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]row, count)
	for i := range rows {
		rows[i] = row{
			ts:   start.Add(time.Duration(i) * 5 * time.Second).Format("2006-01-02 15:04:05"),
			val:  int32(i + 1),
			part: part,
		}
	}
	return rows
}

func requireStrictWindowSequence(t *testing.T, starts []types.Datetime, sliding types.Datetime, want int) {
	t.Helper()
	require.Len(t, starts, want)
	for i := 1; i < len(starts); i++ {
		require.Equal(t, sliding, starts[i]-starts[i-1], "window %d must advance exactly one slide", i)
	}
}

func TestTimeWinSkipsInvisibleEmptySlidingWindows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	interval, err := calcDatetime(10, types.Second)
	require.NoError(t, err)
	sliding, err := calcDatetime(5, types.Second)
	require.NoError(t, err)

	base := mustDatetime(t, "2023-08-01 00:00:00")
	nextValue := base + types.Datetime(maxTimeWindowRows+100)*sliding

	ts := vector.NewVec(types.T_datetime.ToType())
	require.NoError(t, vector.AppendFixed(ts, nextValue, false, proc.Mp()))
	defer ts.Free(proc.Mp())

	ctr := &container{
		tsVec:       []*vector.Vector{ts},
		left:        base,
		right:       base + interval,
		nextLeft:    base + sliding,
		nextRight:   base + sliding + interval,
		withoutFill: true,
		status:      fill,
	}

	require.NoError(t, ctr.fillRows(&TimeWin{Interval: interval, Sliding: sliding}))
	require.Equal(t, int32(fill), ctr.status)
	require.LessOrEqual(t, ctr.left, nextValue)
	require.Greater(t, ctr.right, nextValue)
}

func TestTimeWinDoesNotSkipGapFillEmptySlidingWindows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	interval, err := calcDatetime(10, types.Second)
	require.NoError(t, err)
	sliding, err := calcDatetime(5, types.Second)
	require.NoError(t, err)

	base := mustDatetime(t, "2023-08-01 00:00:00")
	nextValue := base + types.Datetime(maxTimeWindowRows+100)*sliding

	ts := vector.NewVec(types.T_datetime.ToType())
	require.NoError(t, vector.AppendFixed(ts, nextValue, false, proc.Mp()))
	defer ts.Free(proc.Mp())

	ctr := &container{
		tsVec:       []*vector.Vector{ts},
		left:        base,
		right:       base + interval,
		nextLeft:    base + sliding,
		nextRight:   base + sliding + interval,
		withoutFill: true,
		status:      fill,
	}

	require.NoError(t, ctr.fillRows(&TimeWin{Interval: interval, Sliding: sliding, GapFill: true}))
	require.Equal(t, int32(nextWindow), ctr.status)
	require.Equal(t, base, ctr.left)
	require.Equal(t, base+interval, ctr.right)
}

// The boundary window is already included in the flushed generation. The
// replacement generation must start at the following window, including when a
// second internal flush is required.
func TestTimeWinGapFillInternalFlushKeepsWindowsUnique(t *testing.T) {
	sliding, err := calcDatetime(5, types.Second)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		gap  int
	}{
		{name: "adjacent control", gap: maxTimeWindowRows + 1},
		{name: "first internal flush", gap: maxTimeWindowRows + 2},
		{name: "second internal flush", gap: 2*maxTimeWindowRows + 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			rows := makeFlushBoundaryRows(tc.gap, 1)
			bats := []*batch.Batch{
				makePartInput(t, proc.Mp(), rows[:1]),
				makePartInput(t, proc.Mp(), rows[1:]),
			}
			arg := newPartArg(t, proc, sliding, true)
			arg.GapFill = true

			starts, sums, parts := runPartArgBats(t, arg, proc, bats)
			requireStrictWindowSequence(t, starts, sliding, tc.gap+1)
			require.Len(t, sums, tc.gap+1)
			require.Equal(t, int64(1), sums[0])
			require.Equal(t, int64(2), sums[len(sums)-1])
			require.Len(t, parts, tc.gap+1)
			for _, part := range parts {
				require.Equal(t, int64(1), part)
			}
			require.Equal(t, int64(tc.gap+1), arg.ctr.partitionWindows)
			require.Equal(t, int64(tc.gap+1), arg.ctr.gapFillWindows)

			arg.Free(proc, false, nil)
			for _, bat := range bats {
				bat.Clean(proc.Mp())
			}
			proc.Free()
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

// A dense non-GAPFILL input also crosses the result flush threshold. Its
// existing semantics must remain unchanged even though the replacement
// generation now resumes through the explicit post-flush state.
func TestTimeWinInternalFlushPreservesDenseWindowsWithoutGapFill(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sliding, err := calcDatetime(5, types.Second)
	require.NoError(t, err)
	rows := makeDenseWindowRows(maxTimeWindowRows+3, 1)
	bats := []*batch.Batch{
		makePartInput(t, proc.Mp(), rows[:maxTimeWindowRows]),
		makePartInput(t, proc.Mp(), rows[maxTimeWindowRows:]),
	}
	arg := newPartArg(t, proc, sliding, true)

	starts, sums, parts := runPartArgBats(t, arg, proc, bats)
	requireStrictWindowSequence(t, starts, sliding, len(rows))
	require.Len(t, sums, len(rows))
	for i, sum := range sums {
		require.Equal(t, int64(i+1), sum)
	}
	require.Len(t, parts, len(rows))
	for _, part := range parts {
		require.Equal(t, int64(1), part)
	}
	require.Zero(t, arg.ctr.gapFillWindows)

	arg.Free(proc, false, nil)
	for _, bat := range bats {
		bat.Clean(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestTimeWinGapFillInternalFlushResetsPerPartition(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sliding, err := calcDatetime(5, types.Second)
	require.NoError(t, err)
	rows := append(makeFlushBoundaryRows(maxTimeWindowRows+2, 1), makeFlushBoundaryRows(maxTimeWindowRows+1, 2)...)
	bats := []*batch.Batch{
		makePartInput(t, proc.Mp(), rows[:2]),
		makePartInput(t, proc.Mp(), rows[2:]),
	}
	arg := newPartArg(t, proc, sliding, true)
	arg.GapFill = true

	starts, _, parts := runPartArgBats(t, arg, proc, bats)
	wantCounts := map[int64]int{1: maxTimeWindowRows + 3, 2: maxTimeWindowRows + 2}
	seen := make(map[int64]int)
	last := make(map[int64]types.Datetime)
	for i, part := range parts {
		seen[part]++
		if count := seen[part]; count > 1 {
			require.Equal(t, sliding, starts[i]-last[part], "partition %d window %d must advance one slide", part, count)
		}
		last[part] = starts[i]
	}
	require.Equal(t, wantCounts, seen)
	require.Equal(t, int64(maxTimeWindowRows+2), arg.ctr.partitionWindows)
	require.Equal(t, int64(2*maxTimeWindowRows+5), arg.ctr.gapFillWindows)

	arg.Free(proc, false, nil)
	for _, bat := range bats {
		bat.Clean(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestTimeWinGapFillInternalFlushReuseAfterReset(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sliding, err := calcDatetime(5, types.Second)
	require.NoError(t, err)
	rows := makeFlushBoundaryRows(maxTimeWindowRows+2, 1)
	arg := newPartArg(t, proc, sliding, true)
	arg.GapFill = true

	bats1 := []*batch.Batch{
		makePartInput(t, proc.Mp(), rows[:1]),
		makePartInput(t, proc.Mp(), rows[1:]),
	}
	starts1, sums1, parts1 := runPartArgBats(t, arg, proc, bats1)
	requireStrictWindowSequence(t, starts1, sliding, maxTimeWindowRows+3)

	arg.Reset(proc, false, nil)
	bats2 := []*batch.Batch{
		makePartInput(t, proc.Mp(), rows[:1]),
		makePartInput(t, proc.Mp(), rows[1:]),
	}
	starts2, sums2, parts2 := runPartArgBats(t, arg, proc, bats2)
	require.Equal(t, starts1, starts2)
	require.Equal(t, sums1, sums2)
	require.Equal(t, parts1, parts2)

	arg.Free(proc, false, nil)
	for _, bat := range bats1 {
		bat.Clean(proc.Mp())
	}
	for _, bat := range bats2 {
		bat.Clean(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// The same rows produce the same windows regardless of how the child chops
// them into batches: one partition spanning two batches, a partition starting
// exactly on a batch's first row, and a boundary in the middle of the second
// batch must all match the single-batch run.
func TestTimeWinPartitionAcrossChildBatches(t *testing.T) {
	rows := []row{
		{"2023-08-01 00:00:00", 10, 1},
		{"2023-08-01 00:00:01", 20, 1},
		{"2023-08-01 00:00:06", 30, 1},
		{"2023-08-01 00:00:00", 100, 2},
		{"2023-08-01 00:00:07", 400, 2},
		{"2023-08-01 00:00:02", 7, 3},
	}
	splits := []struct {
		name  string
		sizes []int
	}{
		{"single batch", []int{6}},
		{"partition 1 spans two batches", []int{2, 4}},
		{"partition 2 starts a new batch", []int{3, 3}},
		{"boundary inside the second batch", []int{3, 2, 1}},
	}

	sliding, err := calcDatetime(5, 2)
	require.NoError(t, err)

	var wantStarts []types.Datetime
	var wantSums, wantParts []int64
	for _, split := range splits {
		sizes := split.sizes
		t.Run(split.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

			var bats []*batch.Batch
			offset := 0
			for _, size := range sizes {
				bats = append(bats, makePartInput(t, proc.Mp(), rows[offset:offset+size]))
				offset += size
			}
			require.Equal(t, len(rows), offset)

			arg := newPartArg(t, proc, sliding, true)
			starts, sums, parts := runPartArgBats(t, arg, proc, bats)

			if wantSums == nil {
				// The single-batch run is the reference every split must match.
				wantStarts, wantSums, wantParts = starts, sums, parts
				require.Equal(t, []int64{30, 30, 100, 400, 7}, sums)
				require.Equal(t, []int64{1, 1, 2, 2, 3}, parts)
			} else {
				require.Equal(t, wantStarts, starts)
				require.Equal(t, wantSums, sums)
				require.Equal(t, wantParts, parts)
			}

			arg.Free(proc, false, nil)
			for _, b := range bats {
				b.Clean(proc.Mp())
			}
			proc.Free()
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}
