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
	"context"
	"fmt"
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

func datetimeBound(t testing.TB, s string) *plan.Expr {
	t.Helper()
	d, err := types.ParseDatetime(s, 6)
	require.NoError(t, err)
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_datetime), Scale: 6},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Datetimeval{Datetimeval: int64(d)},
		}},
	}
}

func timestampBound(t testing.TB, loc *time.Location, s string) *plan.Expr {
	t.Helper()
	ts, err := types.ParseTimestamp(loc, s, 6)
	require.NoError(t, err)
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_timestamp), Scale: 6},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Timestampval{Timestampval: int64(ts)},
		}},
	}
}

func dateBound(t testing.TB, s string) *plan.Expr {
	t.Helper()
	d, err := types.ParseDateCast(s)
	require.NoError(t, err)
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_date)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Dateval{Dateval: int32(d)},
		}},
	}
}

func timeBound(t testing.TB, s string) *plan.Expr {
	t.Helper()
	value, err := types.ParseTime(s, 6)
	require.NoError(t, err)
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_time), Scale: 6},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Timeval{Timeval: int64(value)},
		}},
	}
}

func yearBound(t testing.TB, year string) *plan.Expr {
	t.Helper()
	from := types.T_varchar.ToType()
	to := types.T_year.ToType()
	cast, err := function.GetFunctionByName(context.Background(), "cast", []types.Type{from, to})
	require.NoError(t, err)
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_year)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: cast.GetEncodedOverloadID(), ObjName: "cast"},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_varchar)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{
						Value: &plan.Literal_Sval{Sval: year},
					}},
				},
				{
					Typ:  plan.Type{Id: int32(types.T_year)},
					Expr: &plan.Expr_T{T: &plan.TargetType{}},
				},
			},
		}},
	}
}

func castTemporalBoundToDatetime(t testing.TB, expr *plan.Expr) *plan.Expr {
	t.Helper()
	from := types.NewWithCharset(
		types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale, uint8(expr.Typ.Charset))
	to := types.T_datetime.ToTypeWithScale(6)
	cast, err := function.GetFunctionByName(context.Background(), "cast", []types.Type{from, to})
	require.NoError(t, err)
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_datetime), Scale: 6},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: cast.GetEncodedOverloadID(), ObjName: "cast"},
			Args: []*plan.Expr{
				expr,
				{
					Typ:  plan.Type{Id: int32(types.T_datetime), Scale: 6},
					Expr: &plan.Expr_T{T: &plan.TargetType{}},
				},
			},
		}},
	}
}

func preparedDatetimeBound(t testing.TB, pos int32) *plan.Expr {
	t.Helper()
	from := types.T_text.ToType()
	to := types.T_datetime.ToTypeWithScale(6)
	cast, err := function.GetFunctionByName(context.Background(), "cast", []types.Type{from, to})
	require.NoError(t, err)
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_datetime), Scale: 6},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: cast.GetEncodedOverloadID(), ObjName: "cast"},
			Args: []*plan.Expr{
				{
					Typ:  plan.Type{Id: int32(types.T_text)},
					Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: pos}},
				},
				{
					Typ:  plan.Type{Id: int32(types.T_datetime), Scale: 6},
					Expr: &plan.Expr_T{T: &plan.TargetType{}},
				},
			},
		}},
	}
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

func newPartArg(t testing.TB, proc *process.Process, sliding types.Datetime, withPartition bool) *TimeWin {
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

func newBoundedPartArg(
	t testing.TB,
	proc *process.Process,
	start, finish string,
	withPartition bool,
) *TimeWin {
	t.Helper()
	arg := newPartArg(t, proc, makeInterval(), withPartition)
	arg.GapFill = true
	arg.GapFillStart = datetimeBound(t, start)
	arg.GapFillEnd = datetimeBound(t, finish)
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
func runPartArg(t testing.TB, arg *TimeWin, proc *process.Process, in *batch.Batch) (starts []types.Datetime, sums []int64, parts []int64) {
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

func runTemporalBoundArg(t testing.TB, arg *TimeWin, proc *process.Process, inputs ...*batch.Batch) (starts []types.Datetime, sums []int64) {
	t.Helper()
	arg.Children = nil
	arg.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
	require.NoError(t, arg.Prepare(proc))

	for {
		res, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
		n := res.Batch.Vecs[0].Length()
		sums = append(sums, vector.MustFixedColNoTypeCheck[int64](res.Batch.Vecs[0])[:n]...)
		startVec := res.Batch.Vecs[1]
		switch startVec.GetType().Oid {
		case types.T_date:
			for _, value := range vector.MustFixedColNoTypeCheck[types.Date](startVec)[:n] {
				starts = append(starts, value.ToDatetime())
			}
		case types.T_datetime:
			starts = append(starts, vector.MustFixedColNoTypeCheck[types.Datetime](startVec)[:n]...)
		case types.T_timestamp:
			for _, value := range vector.MustFixedColNoTypeCheck[types.Timestamp](startVec)[:n] {
				starts = append(starts, value.ToDatetime(proc.GetSessionInfo().TimeZone))
			}
		default:
			t.Fatalf("unexpected bounded time-window output type %s", startVec.GetType().Oid)
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

func TestBoundedGapFillZeroTemporalLiteralFallsBackToObservedRange(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	in := makePartInput(t, proc.Mp(), []row{
		{"0000-00-00 00:00:00", 10, 1},
		{"0001-01-01 00:00:00", 20, 1},
	})
	arg := newBoundedPartArg(
		t, proc,
		"0000-00-00 00:00:00", "0001-01-01 00:00:05",
		false,
	)

	starts, sums, _ := runPartArg(t, arg, proc, in)
	require.Equal(t, []types.Datetime{types.ZeroDatetime, types.DatetimeEpoch}, starts)
	require.Equal(t, []int64{10, 20}, sums)
	require.False(t, arg.ctr.boundedGapFill)

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

func TestBoundedGapFillPreparedZeroFallbackReevaluatesAfterReset(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(
		params, []byte("0000-00-00 00:00:00"), false, proc.Mp()))
	proc.SetPrepareParams(params)

	arg := newBoundedPartArg(
		t, proc,
		"0000-00-00 00:00:00", "0001-01-01 00:00:05",
		false,
	)
	arg.GapFillStart = preparedDatetimeBound(t, 0)
	in := makePartInput(t, proc.Mp(), []row{
		{"0000-00-00 00:00:00", 10, 1},
		{"0001-01-01 00:00:00", 20, 1},
	})
	starts, sums, _ := runPartArg(t, arg, proc, in)
	require.Equal(t, []types.Datetime{types.ZeroDatetime, types.DatetimeEpoch}, starts)
	require.Equal(t, []int64{10, 20}, sums)
	require.False(t, arg.ctr.boundedGapFill)

	arg.Reset(proc, false, nil)
	proc.SetPrepareParams(nil)
	params.Free(proc.Mp())
	params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(
		params, []byte("0001-01-01 00:00:00"), false, proc.Mp()))
	proc.SetPrepareParams(params)
	starts, sums, _ = runPartArgBats(t, arg, proc, nil)
	require.Equal(t, []types.Datetime{types.DatetimeEpoch}, starts)
	require.Equal(t, []int64{0}, sums)
	require.True(t, arg.ctr.boundedGapFill)

	arg.Free(proc, false, nil)
	in.Clean(proc.Mp())
	proc.SetPrepareParams(nil)
	params.Free(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
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

func TestBoundedGapFillCoversLeadingAndTrailingBuckets(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	in := makePartInput(t, proc.Mp(), []row{
		{"2023-08-01 00:00:10", 20, 0},
		{"2023-08-01 00:00:20", 40, 0},
	})
	arg := newBoundedPartArg(
		t, proc,
		"2023-08-01 00:00:00", "2023-08-01 00:00:30",
		false,
	)

	starts, sums, _ := runPartArg(t, arg, proc, in)
	wantStarts := make([]types.Datetime, 6)
	for i := range wantStarts {
		wantStarts[i] = mustDatetime(t, "2023-08-01 00:00:00") + types.Datetime(i)*makeInterval()
	}
	require.Equal(t, wantStarts, starts)
	require.Equal(t, []int64{0, 0, 20, 0, 40, 0}, sums)
	for row, wantNull := range []bool{true, true, false, true, false, true} {
		require.Equal(t, wantNull, arg.ctr.bat.Vecs[0].IsNull(uint64(row)))
	}
	require.Equal(t, int64(6), arg.ctr.partitionWindows)

	arg.Free(proc, false, nil)
	in.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestBoundedGapFillPreservesSlidingWindowOverlap(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	in := makePartInput(t, proc.Mp(), []row{
		{"2023-08-01 00:00:07", 10, 0},
	})
	arg := newBoundedPartArg(
		t, proc,
		"2023-08-01 00:00:00", "2023-08-01 00:00:20",
		false,
	)
	arg.Interval = 10 * types.Datetime(types.MicroSecsPerSec)
	arg.Sliding = 5 * types.Datetime(types.MicroSecsPerSec)

	starts, sums, _ := runPartArg(t, arg, proc, in)
	require.Equal(t, []types.Datetime{
		mustDatetime(t, "2023-08-01 00:00:00"),
		mustDatetime(t, "2023-08-01 00:00:05"),
		mustDatetime(t, "2023-08-01 00:00:10"),
		mustDatetime(t, "2023-08-01 00:00:15"),
	}, starts)
	require.Equal(t, []int64{10, 10, 0, 0}, sums)

	arg.Free(proc, false, nil)
	in.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestBoundedGapFillSlidingReplaysAllBoundaryTiesAcrossBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := []row{
		{"2023-08-01 00:00:03", 6, 0},
		{"2023-08-01 00:00:07", 4, 0},
		{"2023-08-01 00:00:08", 6, 0},
		{"2023-08-01 00:00:12", 7, 0},
		{"2023-08-01 00:00:12", 1, 0},
		{"2023-08-01 00:00:12", 1, 0},
		{"2023-08-01 00:00:12", 4, 0},
		{"2023-08-01 00:00:14", 7, 0},
	}
	bats := []*batch.Batch{
		makePartInput(t, proc.Mp(), rows[:5]),
		makePartInput(t, proc.Mp(), rows[5:]),
	}
	arg := newBoundedPartArg(
		t, proc,
		"2023-08-01 00:00:03", "2023-08-01 00:00:15",
		false,
	)
	arg.Interval = 4 * types.Datetime(types.MicroSecsPerSec)
	arg.Sliding = 3 * types.Datetime(types.MicroSecsPerSec)

	starts, sums, _ := runPartArgBats(t, arg, proc, bats)
	require.Equal(t, []types.Datetime{
		mustDatetime(t, "2023-08-01 00:00:00"),
		mustDatetime(t, "2023-08-01 00:00:03"),
		mustDatetime(t, "2023-08-01 00:00:06"),
		mustDatetime(t, "2023-08-01 00:00:09"),
		mustDatetime(t, "2023-08-01 00:00:12"),
	}, starts)
	require.Equal(t, []int64{6, 6, 10, 13, 20}, sums)

	arg.Free(proc, false, nil)
	for _, bat := range bats {
		bat.Clean(proc.Mp())
	}
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestBoundedGapFillConvertsTimestampBoundsInSessionTimezone(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	zone := time.FixedZone("UTC+08", 8*60*60)
	proc.GetSessionInfo().TimeZone = zone
	arg := newBoundedPartArg(
		t, proc,
		"2023-08-01 00:00:00", "2023-08-01 00:00:15",
		false,
	)
	arg.GapFillStart = timestampBound(t, zone, "2023-08-01 00:00:00")
	arg.GapFillEnd = timestampBound(t, zone, "2023-08-01 00:00:15")
	arg.TsType = plan.Type{Id: int32(types.T_timestamp), Scale: 6}

	// The planner feeds TimeWin DATETIME keys produced by mo_win_truncate. For a
	// TIMESTAMP source these carry the raw instant microseconds, not local wall
	// clock microseconds. Include an observed row to ensure inferred bounds use
	// that same coordinate instead of merely producing correctly labelled gaps.
	observed, err := types.ParseTimestamp(zone, "2023-08-01 00:00:05", 6)
	require.NoError(t, err)
	in := batch.New([]string{"ts", "val"})
	in.Vecs[0] = vector.NewVec(types.T_datetime.ToTypeWithScale(6))
	require.NoError(t, vector.AppendFixed(in.Vecs[0], types.Datetime(observed), false, proc.Mp()))
	in.Vecs[1] = testutil.MakeInt32Vector([]int32{7}, nil, proc.Mp())
	in.SetRowCount(1)

	starts, sums := runTemporalBoundArg(t, arg, proc, in)
	require.Equal(t, []types.Datetime{
		mustDatetime(t, "2023-08-01 00:00:00"),
		mustDatetime(t, "2023-08-01 00:00:05"),
		mustDatetime(t, "2023-08-01 00:00:10"),
	}, starts)
	require.Equal(t, []int64{0, 7, 0}, sums)

	arg.Free(proc, false, nil)
	in.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestTimestampDayWindowsPreserveDSTCivilBoundaries(t *testing.T) {
	zone, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	for _, tc := range []struct {
		name      string
		start     string
		end       string
		secondDay string
	}{
		{name: "spring-forward", start: "2026-03-08 00:00:00", end: "2026-03-10 00:00:00", secondDay: "2026-03-09 00:00:00"},
		{name: "fall-back", start: "2026-11-01 00:00:00", end: "2026-11-03 00:00:00", secondDay: "2026-11-02 00:00:00"},
	} {
		for _, gapFill := range []bool{false, true} {
			t.Run(tc.name+"/"+map[bool]string{false: "ordinary", true: "gapfill"}[gapFill], func(t *testing.T) {
				proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
				var arg *TimeWin
				if gapFill {
					arg = newBoundedPartArg(t, proc, tc.start, tc.end, false)
				} else {
					arg = newPartArg(t, proc, types.Datetime(types.SecsPerDay*types.MicroSecsPerSec), false)
				}
				arg.TsType = plan.Type{Id: int32(types.T_timestamp), Scale: 6}
				arg.Interval = types.Datetime(types.SecsPerDay * types.MicroSecsPerSec)
				arg.Sliding = arg.Interval
				proc.GetSessionInfo().TimeZone = zone

				startTS, parseErr := types.ParseTimestamp(zone, tc.start, 6)
				require.NoError(t, parseErr)
				if gapFill {
					arg.GapFillStart = timestampBound(t, zone, tc.start)
					arg.GapFillEnd = timestampBound(t, zone, tc.end)
				}
				secondDay, parseErr := types.ParseDatetime(tc.secondDay, 6)
				require.NoError(t, parseErr)

				in := batch.New([]string{"ts", "val"})
				in.Vecs[0] = vector.NewVec(types.T_datetime.ToTypeWithScale(6))
				require.NoError(t, vector.AppendFixedList(in.Vecs[0], []types.Datetime{
					types.Datetime(startTS),
					types.Datetime(secondDay.ToTimestamp(zone)),
				}, nil, proc.Mp()))
				in.Vecs[1] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
				in.SetRowCount(2)

				starts, sums := runTemporalBoundArg(t, arg, proc, in)
				require.Equal(t, []types.Datetime{
					mustDatetime(t, tc.start),
					secondDay,
				}, starts)
				require.Equal(t, []int64{1, 2}, sums)

				arg.Free(proc, false, nil)
				in.Clean(proc.Mp())
				proc.Free()
				require.Equal(t, int64(0), proc.Mp().CurrNB())
			})
		}
	}
}

func TestTimestampFoldGapFillKeepsBothCivilOccurrences(t *testing.T) {
	zone, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	hour := types.Datetime(types.SecsPerHour * types.MicroSecsPerSec)
	first := types.UnixMicroToTimestamp(time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC).UnixMicro())
	second := types.UnixMicroToTimestamp(time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC).UnixMicro())

	for _, gapFill := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "gapfill"}[gapFill], func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			arg := newPartArg(t, proc, hour, false)
			arg.TsType = plan.Type{Id: int32(types.T_timestamp), Scale: 6}
			arg.Interval = hour
			arg.Sliding = hour
			arg.GapFill = gapFill
			proc.GetSessionInfo().TimeZone = zone

			in := batch.New([]string{"ts", "val"})
			in.Vecs[0] = vector.NewVec(types.T_datetime.ToTypeWithScale(6))
			require.NoError(t, vector.AppendFixedList(in.Vecs[0], []types.Datetime{
				types.Datetime(first),
				types.Datetime(second),
			}, nil, proc.Mp()))
			in.Vecs[1] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
			in.SetRowCount(2)

			starts, sums := runTemporalBoundArg(t, arg, proc, in)
			require.Equal(t, []types.Datetime{
				mustDatetime(t, "2026-11-01 01:00:00"),
				mustDatetime(t, "2026-11-01 01:00:00"),
			}, starts)
			require.Equal(t, []int64{1, 2}, sums)

			arg.Free(proc, false, nil)
			in.Clean(proc.Mp())
			proc.Free()
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func TestBoundedGapFillConvertsDateBounds(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := newBoundedPartArg(
		t, proc,
		"1992-01-01 00:00:00", "1992-01-03 00:00:00",
		false,
	)
	arg.GapFillStart = dateBound(t, "1992-01-01")
	arg.GapFillEnd = dateBound(t, "1992-01-03")
	day := types.Datetime(types.SecsPerDay * types.MicroSecsPerSec)
	arg.Interval = day
	arg.Sliding = day
	arg.TsType = plan.Type{Id: int32(types.T_date)}

	starts, sums := runTemporalBoundArg(t, arg, proc)
	require.Equal(t, []types.Datetime{
		mustDatetime(t, "1992-01-01 00:00:00"),
		mustDatetime(t, "1992-01-02 00:00:00"),
	}, starts)
	require.Equal(t, []int64{0, 0}, sums)

	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestBoundedGapFillExecutesNormalizedTimeAndYearBounds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		start    *plan.Expr
		finish   *plan.Expr
		wantRows int
		interval types.Datetime
	}{
		{
			name:     "time",
			start:    timeBound(t, "00:00:00"),
			finish:   timeBound(t, "00:00:15"),
			wantRows: 3,
			interval: 5 * types.Datetime(types.MicroSecsPerSec),
		},
		{
			name:     "year",
			start:    yearBound(t, "2023"),
			finish:   yearBound(t, "2024"),
			wantRows: 0,
			interval: types.Datetime(types.SecsPerDay * types.MicroSecsPerSec),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			arg := newBoundedPartArg(
				t, proc,
				"2023-08-01 00:00:00", "2023-08-01 00:00:15",
				false,
			)
			arg.GapFillStart = castTemporalBoundToDatetime(t, tc.start)
			arg.GapFillEnd = castTemporalBoundToDatetime(t, tc.finish)
			arg.Interval = tc.interval
			arg.Sliding = tc.interval

			starts, _, _ := runPartArgBats(t, arg, proc, nil)
			require.Len(t, starts, tc.wantRows)

			arg.Free(proc, false, nil)
			proc.Free()
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func TestBoundedGapFillEmitsGridForEmptyInput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := newBoundedPartArg(
		t, proc,
		"2023-08-01 00:00:00", "2023-08-01 00:00:15",
		false,
	)

	starts, sums, parts := runPartArgBats(t, arg, proc, nil)
	wantStarts := []types.Datetime{
		mustDatetime(t, "2023-08-01 00:00:00"),
		mustDatetime(t, "2023-08-01 00:00:05"),
		mustDatetime(t, "2023-08-01 00:00:10"),
	}
	require.Equal(t, wantStarts, starts)
	require.Equal(t, []int64{0, 0, 0}, sums)
	for row := range wantStarts {
		require.True(t, arg.ctr.bat.Vecs[0].IsNull(uint64(row)), "row %d must be an empty aggregate", row)
	}
	require.Empty(t, parts)
	require.Equal(t, int64(3), arg.ctr.gapFillWindows)

	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestBoundedGapFillTimestampDSTBoundarySequence(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	zone, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	proc.GetSessionInfo().TimeZone = zone
	hour := types.Datetime(types.SecsPerHour * types.MicroSecsPerSec)
	parse := func(s string) types.Timestamp {
		ts, err := types.ParseTimestamp(zone, s, 6)
		require.NoError(t, err)
		return ts
	}
	firstFold := types.UnixMicroToTimestamp(time.Date(2026, 11, 1, 5, 0, 0, 0, time.UTC).UnixMicro())
	secondFold := types.UnixMicroToTimestamp(time.Date(2026, 11, 1, 6, 0, 0, 0, time.UTC).UnixMicro())

	t.Run("observed-row-skip-keeps-both-fold-instants", func(t *testing.T) {
		arg := &TimeWin{GapFill: true, Interval: hour, Sliding: hour}
		ctr := &container{
			tsOid:      types.T_timestamp,
			left:       types.Datetime(parse("2026-11-01 00:00:00")),
			gapFillEnd: types.Datetime(parse("2026-11-01 03:00:00")),
		}
		flushed, err := ctr.advanceBoundedTumblingGap(arg, types.Datetime(parse("2026-11-01 02:00:00")), proc)
		require.NoError(t, err)
		require.False(t, flushed)
		require.Equal(t, []types.Datetime{
			types.Datetime(parse("2026-11-01 00:00:00")),
			types.Datetime(firstFold),
			types.Datetime(secondFold),
		}, ctr.wStart)
		require.Equal(t, types.Datetime(parse("2026-11-01 02:00:00")), ctr.left)
	})

	t.Run("empty-spring-forward-domain-excludes-finish", func(t *testing.T) {
		arg := &TimeWin{GapFill: true, Interval: hour, Sliding: hour}
		ctr := &container{
			tsOid:      types.T_timestamp,
			left:       types.Datetime(parse("2026-03-08 00:00:00")),
			gapFillEnd: types.Datetime(parse("2026-03-08 04:00:00")),
		}
		complete, err := ctr.closeBoundedGapFillTail(arg, proc)
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, []types.Datetime{
			types.Datetime(parse("2026-03-08 00:00:00")),
			types.Datetime(parse("2026-03-08 01:00:00")),
			types.Datetime(parse("2026-03-08 03:00:00")),
		}, ctr.wStart)
	})

	t.Run("empty-fall-back-domain-keeps-both-fold-instants", func(t *testing.T) {
		arg := &TimeWin{GapFill: true, Interval: hour, Sliding: hour}
		ctr := &container{
			tsOid:      types.T_timestamp,
			left:       types.Datetime(parse("2026-11-01 00:00:00")),
			gapFillEnd: types.Datetime(parse("2026-11-01 03:00:00")),
		}
		complete, err := ctr.closeBoundedGapFillTail(arg, proc)
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, []types.Datetime{
			types.Datetime(parse("2026-11-01 00:00:00")),
			types.Datetime(firstFold),
			types.Datetime(secondFold),
			types.Datetime(parse("2026-11-01 02:00:00")),
		}, ctr.wStart)
	})

	t.Run("sub-hour-second-fold-advances-forward", func(t *testing.T) {
		halfHour := types.Datetime(30 * types.SecsPerMinute * types.MicroSecsPerSec)
		secondFoldHalfHour := types.UnixMicroToTimestamp(time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC).UnixMicro())
		secondFoldTwoHours := types.UnixMicroToTimestamp(time.Date(2026, 11, 1, 7, 0, 0, 0, time.UTC).UnixMicro())
		arg := &TimeWin{GapFill: true, Interval: halfHour, Sliding: halfHour}
		ctr := &container{
			tsOid:      types.T_timestamp,
			left:       types.Datetime(secondFold),
			gapFillEnd: types.Datetime(secondFoldTwoHours),
		}
		complete, err := ctr.closeBoundedGapFillTail(arg, proc)
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, []types.Datetime{
			types.Datetime(secondFold),
			types.Datetime(secondFoldHalfHour),
		}, ctr.wStart)
		require.Equal(t, []types.Datetime{
			types.Datetime(secondFoldHalfHour),
			types.Datetime(secondFoldTwoHours),
		}, ctr.wEnd)
	})

	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestUnboundedGapFillEmptyInputDoesNotSynthesizeRows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := newPartArg(t, proc, makeInterval(), false)
	arg.GapFill = true

	starts, sums, parts := runPartArgBats(t, arg, proc, nil)
	require.Empty(t, starts)
	require.Empty(t, sums)
	require.Empty(t, parts)

	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestBoundedGapFillSkipsEmptyChildBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	in := makePartInput(t, proc.Mp(), nil)
	arg := newBoundedPartArg(
		t, proc,
		"2023-08-01 00:00:00", "2023-08-01 00:00:15",
		false,
	)

	starts, sums, _ := runPartArg(t, arg, proc, in)
	require.Equal(t, []types.Datetime{
		mustDatetime(t, "2023-08-01 00:00:00"),
		mustDatetime(t, "2023-08-01 00:00:05"),
		mustDatetime(t, "2023-08-01 00:00:10"),
	}, starts)
	require.Equal(t, []int64{0, 0, 0}, sums)

	arg.Free(proc, false, nil)
	in.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestBoundedGapFillValidatesDomain(t *testing.T) {
	for _, tc := range []struct {
		name         string
		start        string
		finish       string
		dropFinish   bool
		wantError    string
		wantRowCount int
	}{
		{name: "aligned empty", start: "2023-08-01 00:00:00", finish: "2023-08-01 00:00:00"},
		{name: "unaligned empty", start: "2023-08-01 00:00:02", finish: "2023-08-01 00:00:02"},
		{name: "reversed", start: "2023-08-01 00:00:05", finish: "2023-08-01 00:00:00"},
		{name: "unpaired", start: "2023-08-01 00:00:00", finish: "2023-08-01 00:00:05", dropFinish: true, wantError: "both start and finish"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			arg := newBoundedPartArg(t, proc, tc.start, tc.finish, false)
			if tc.dropFinish {
				arg.GapFillEnd = nil
			}

			if tc.wantError != "" {
				require.ErrorContains(t, arg.Prepare(proc), tc.wantError)
			} else {
				starts, _, _ := runPartArgBats(t, arg, proc, nil)
				require.Len(t, starts, tc.wantRowCount)
			}

			arg.Free(proc, false, nil)
			proc.Free()
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func TestBoundedGapFillTreatsPreparedNullBoundAsEmptyAndReevaluatesAfterReset(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, nil, true, proc.Mp()))
	proc.SetPrepareParams(params)

	arg := newBoundedPartArg(
		t, proc,
		"2023-08-01 00:00:00", "2023-08-01 00:00:15",
		false,
	)
	arg.GapFillStart = preparedDatetimeBound(t, 0)
	starts, _, _ := runPartArgBats(t, arg, proc, nil)
	require.Empty(t, starts)

	arg.Reset(proc, false, nil)
	proc.SetPrepareParams(nil)
	params.Free(proc.Mp())

	params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(
		params, []byte("2023-08-01 00:00:00"), false, proc.Mp()))
	proc.SetPrepareParams(params)
	starts, _, _ = runPartArgBats(t, arg, proc, nil)
	require.Len(t, starts, 3)

	arg.Free(proc, false, nil)
	proc.SetPrepareParams(nil)
	params.Free(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestBoundedGapFillReuseAfterReset(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := newBoundedPartArg(
		t, proc,
		"2023-08-01 00:00:00", "2023-08-01 00:00:15",
		false,
	)

	starts1, sums1, _ := runPartArgBats(t, arg, proc, nil)
	arg.Reset(proc, false, nil)
	starts2, sums2, _ := runPartArgBats(t, arg, proc, nil)
	require.Equal(t, starts1, starts2)
	require.Equal(t, sums1, sums2)

	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestBoundedGapFillEmptyInputAcrossInternalFlushes(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	start := time.Date(2023, time.August, 1, 0, 0, 0, 0, time.UTC)
	windowCount := 2*maxTimeWindowRows + 5
	finish := start.Add(time.Duration(windowCount) * 5 * time.Second)
	arg := newBoundedPartArg(
		t, proc,
		start.Format("2006-01-02 15:04:05"),
		finish.Format("2006-01-02 15:04:05"),
		false,
	)

	starts, sums, _ := runPartArgBats(t, arg, proc, nil)
	requireStrictWindowSequence(t, starts, makeInterval(), windowCount)
	require.Len(t, sums, windowCount)
	for _, sum := range sums {
		require.Zero(t, sum)
	}
	require.Equal(t, int64(windowCount), arg.ctr.gapFillWindows)

	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestBoundedGapFillObservedRowAcrossInternalFlushes(t *testing.T) {
	for _, observedWindow := range []int{maxTimeWindowRows, maxTimeWindowRows + 2} {
		t.Run(fmt.Sprintf("observed_window_%d", observedWindow), func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			start := time.Date(2023, time.August, 1, 0, 0, 0, 0, time.UTC)
			windowCount := 2*maxTimeWindowRows + 5
			finish := start.Add(time.Duration(windowCount) * 5 * time.Second)
			in := makePartInput(t, proc.Mp(), []row{{
				ts:  start.Add(time.Duration(observedWindow) * 5 * time.Second).Format("2006-01-02 15:04:05"),
				val: 42,
			}})
			arg := newBoundedPartArg(
				t, proc,
				start.Format("2006-01-02 15:04:05"),
				finish.Format("2006-01-02 15:04:05"),
				false,
			)

			starts, sums, _ := runPartArg(t, arg, proc, in)
			requireStrictWindowSequence(t, starts, makeInterval(), windowCount)
			require.Len(t, sums, windowCount)
			for window, sum := range sums {
				if window == observedWindow {
					require.Equal(t, int64(42), sum)
				} else {
					require.Zero(t, sum)
				}
			}

			arg.Free(proc, false, nil)
			in.Clean(proc.Mp())
			proc.Free()
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func TestBoundedGapFillAppliesDomainPerObservedPartition(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	in := makePartInput(t, proc.Mp(), []row{
		{"2023-08-01 00:00:05", 10, 1},
		{"2023-08-01 00:00:10", 20, 2},
	})
	arg := newBoundedPartArg(
		t, proc,
		"2023-08-01 00:00:00", "2023-08-01 00:00:15",
		true,
	)

	starts, sums, parts := runPartArg(t, arg, proc, in)
	require.Equal(t, []types.Datetime{
		mustDatetime(t, "2023-08-01 00:00:00"),
		mustDatetime(t, "2023-08-01 00:00:05"),
		mustDatetime(t, "2023-08-01 00:00:10"),
		mustDatetime(t, "2023-08-01 00:00:00"),
		mustDatetime(t, "2023-08-01 00:00:05"),
		mustDatetime(t, "2023-08-01 00:00:10"),
	}, starts)
	require.Equal(t, []int64{0, 10, 0, 0, 0, 20}, sums)
	require.Equal(t, []int64{1, 1, 1, 2, 2, 2}, parts)
	require.Equal(t, int64(6), arg.ctr.gapFillWindows)

	arg.Free(proc, false, nil)
	in.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestBoundedGapFillRejectsOversizedDomainBeforeReading(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := newBoundedPartArg(
		t, proc,
		"2023-01-01 00:00:00", "2023-03-01 00:00:00",
		false,
	)
	arg.Sliding = types.Datetime(types.MicroSecsPerSec)
	arg.Interval = arg.Sliding

	err := arg.Prepare(proc)
	require.ErrorContains(t, err, "partition")

	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestGapFillResourceAccountingLimits(t *testing.T) {
	arg := &TimeWin{GapFill: true}
	ctr := container{partitionWindows: maxGapFillRowsPerPartition}
	require.ErrorContains(t, ctr.accountGapFillWindows(arg, 1), "partition")

	ctr = container{gapFillWindows: maxGapFillRowsTotal}
	require.ErrorContains(t, ctr.accountGapFillWindows(arg, 1), "total")

	ctr = container{partitionWindows: maxGapFillRowsPerPartition - 1, gapFillWindows: maxGapFillRowsTotal - 1}
	require.NoError(t, ctr.accountGapFillWindows(arg, 1))

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
func runPartArgBats(t testing.TB, arg *TimeWin, proc *process.Process, bats []*batch.Batch) (starts []types.Datetime, sums []int64, parts []int64) {
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

func BenchmarkBoundedGapFillEmptyRange(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	start := time.Date(2023, time.August, 1, 0, 0, 0, 0, time.UTC)
	const windows = 100_000
	finish := start.Add(windows * 5 * time.Second)
	arg := newBoundedPartArg(
		b, proc,
		start.Format("2006-01-02 15:04:05"),
		finish.Format("2006-01-02 15:04:05"),
		false,
	)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		arg.Children = nil
		arg.AppendChild(colexec.NewMockOperator())
		require.NoError(b, arg.Prepare(proc))
		rows := 0
		for {
			result, err := vm.Exec(arg, proc)
			require.NoError(b, err)
			if result.Batch == nil {
				break
			}
			rows += result.Batch.RowCount()
		}
		require.Equal(b, windows, rows)
		if iteration+1 < b.N {
			arg.Reset(proc, false, nil)
		}
	}
	b.StopTimer()
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(b, int64(0), proc.Mp().CurrNB())
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

	require.NoError(t, ctr.fillRows(&TimeWin{Interval: interval, Sliding: sliding}, proc))
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

	require.NoError(t, ctr.fillRows(&TimeWin{Interval: interval, Sliding: sliding, GapFill: true}, proc))
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
