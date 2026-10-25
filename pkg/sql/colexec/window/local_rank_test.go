// Copyright 2026 Matrix Origin
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

package window

// Operator-level proof for the local sliding-window ranks of issue #29947
// (mrank/cumrank/tmrank in processValueFuncRange). Every expectation here is
// either the issue's independent SQL control, a hand-computed physical-row
// profile, or a brute-force oracle over enumerated fixtures — independent of
// the production kernel implementation.

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// localRankSpec builds a WIN_VALUE window spec for one of the local rank
// functions. fnArgs mirror the binder's published argument shape; the
// operator's aggregate evaluation uses exactly those expressions.
func localRankSpec(
	name string,
	fnArgs []*plan.Expr,
	orderByPos int32,
	orderByType types.Type,
	partitionBy []*plan.Expr,
) *plan.Expr {
	var orderBy []*plan.OrderBySpec
	if orderByPos >= 0 {
		orderBy = []*plan.OrderBySpec{{Expr: newColExprWithType(orderByPos, orderByType)}}
	}
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_uint64)},
		Expr: &plan.Expr_W{W: &plan.WindowSpec{
			Name: name,
			WindowFunc: &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_uint64)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: name},
					Args: fnArgs,
				}},
			},
			OrderBy:     orderBy,
			PartitionBy: partitionBy,
		}},
	}
}

// nullMarkedCount builds the null-marked normalized count literal the D17
// NULL-arithmetic publication produces (Lit.Isnull on the int64 literal).
func nullMarkedCount() *plan.Expr {
	cnt := intervalInt64(0)
	cnt.Typ.NotNullable = false
	cnt.GetLit().Isnull = true
	return cnt
}

// rawStringIntervalUnit builds the raw unit literal (MySQL-string, T_varchar)
// that marks a raw (un-normalized) published width pair.
func rawStringIntervalUnit(unit string) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: unit}}},
	}
}

func rawStringIntervalCount(text string) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: text}}},
	}
}

// runLocalRankWindow runs the window operator over the supplied batches and
// collects the rank output column with its NULL bits. With PartitionBy set
// each input batch is one generation (the phase machine mirrors production's
// per-partition-group batches); with no partition one generation covers all.
func runLocalRankWindow(
	t *testing.T,
	spec *plan.Expr,
	bats []*batch.Batch,
	mp *mpool.MPool,
) (got []uint64, nulls []bool, err error) {
	proc := testutil.NewProcessWithMPool(t, "", mp)
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, spec.Expr.(*plan.Expr_W).W.WindowFunc.GetF().Args, nil),
		},
	}
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	for {
		result, execErr := vm.Exec(arg, proc)
		if execErr != nil {
			arg.Free(proc, false, nil)
			proc.Free()
			return nil, nil, execErr
		}
		if result.Batch == nil {
			break
		}
		if result.Batch.IsEmpty() {
			continue
		}
		vec := result.Batch.Vecs[len(result.Batch.Vecs)-1]
		if vec.Length() > 0 {
			got = append(got, vector.MustFixedColWithTypeCheck[uint64](vec)...)
			for i := 0; i < vec.Length(); i++ {
				nulls = append(nulls, vec.IsNull(uint64(i)))
			}
		}
	}
	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	return got, nulls, nil
}

// runLocalRankWindowProc runs the harness with a caller-built process, for
// probes that need their own session timezone or query-context cancellation.
// It returns the values, the live operator and the size of the operator-owned
// civil-order index cache captured before teardown (Free resets the cache);
// the caller owns the process and frees it after the run.
func runLocalRankWindowProc(
	t *testing.T,
	spec *plan.Expr,
	bats []*batch.Batch,
	proc *process.Process,
) ([]uint64, []bool, error, *Window, int) {
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, spec.Expr.(*plan.Expr_W).W.WindowFunc.GetF().Args, nil),
		},
	}
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	var got []uint64
	var nulls []bool
	for {
		result, execErr := vm.Exec(arg, proc)
		if execErr != nil {
			civilOrderIndexes := len(arg.ctr.timestampCivilOrder)
			arg.Free(proc, false, nil)
			op.Free(proc, false, nil)
			return nil, nil, execErr, arg, civilOrderIndexes
		}
		if result.Batch == nil {
			break
		}
		if result.Batch.IsEmpty() {
			continue
		}
		vec := result.Batch.Vecs[len(result.Batch.Vecs)-1]
		if vec.Length() > 0 {
			got = append(got, vector.MustFixedColWithTypeCheck[uint64](vec)...)
			for i := 0; i < vec.Length(); i++ {
				nulls = append(nulls, vec.IsNull(uint64(i)))
			}
		}
	}
	civilOrderIndexes := len(arg.ctr.timestampCivilOrder)
	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	return got, nulls, nil, arg, civilOrderIndexes
}

// requireRankOutput compares the operator output against the expectation,
// masking values at NULL rows. gotNulls and wantNulls must have equal length
// and match the total row count.
func requireRankOutput(
	t *testing.T,
	name string,
	got []uint64,
	gotNulls []bool,
	wantVals []uint64,
	wantNulls []bool,
) {
	require.Len(t, gotNulls, len(wantNulls), "%s: output row count differs from expectation", name)
	for i := range gotNulls {
		require.Equal(t, wantNulls[i], gotNulls[i],
			"%s: row %d NULL bit differs: got nulls %v, values %v", name, i, gotNulls, got)
		if !wantNulls[i] {
			require.Equal(t, wantVals[i], got[i],
				"%s: row %d rank differs: got nulls %v, values %v", name, i, gotNulls, got)
		}
	}
}

func localRankBatch(mp *mpool.MPool, rows int, vecs ...*vector.Vector) *batch.Batch {
	bat := batch.NewWithSize(len(vecs))
	for i := range vecs {
		bat.Vecs[i] = vecs[i]
	}
	bat.SetRowCount(rows)
	return bat
}

func appendFixedVec[T any](t *testing.T, mp *mpool.MPool, oid types.T, vals []T, nulls []bool) *vector.Vector {
	vec := vector.NewVec(oid.ToType())
	require.NoError(t, vector.AppendFixedList(vec, vals, nulls, mp))
	return vec
}

func int32RankVec(t *testing.T, mp *mpool.MPool, vals []int32, nulls []bool) *vector.Vector {
	return appendFixedVec(t, mp, types.T_int32, vals, nulls)
}

func int64RankVec(t *testing.T, mp *mpool.MPool, vals []int64, nulls []bool) *vector.Vector {
	return appendFixedVec(t, mp, types.T_int64, vals, nulls)
}

func uint64RankVec(t *testing.T, mp *mpool.MPool, vals []uint64, nulls []bool) *vector.Vector {
	return appendFixedVec(t, mp, types.T_uint64, vals, nulls)
}

func float64RankVec(t *testing.T, mp *mpool.MPool, vals []float64, nulls []bool) *vector.Vector {
	return appendFixedVec(t, mp, types.T_float64, vals, nulls)
}

func decimal64RankVec(t *testing.T, mp *mpool.MPool, raw []float64) *vector.Vector {
	vec := vector.NewVec(types.T_decimal64.ToType())
	for _, f := range raw {
		d, err := types.Decimal64FromFloat64(f, 10, 2)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixed(vec, d, false, mp))
	}
	return vec
}

func datetimeRankVec(t *testing.T, mp *mpool.MPool, vals ...types.Datetime) *vector.Vector {
	vec := vector.NewVec(types.T_datetime.ToType())
	for _, d := range vals {
		require.NoError(t, vector.AppendFixed(vec, d, false, mp))
	}
	return vec
}

func dateRankVec(t *testing.T, mp *mpool.MPool, vals ...types.Date) *vector.Vector {
	vec := vector.NewVec(types.T_date.ToType())
	for _, d := range vals {
		require.NoError(t, vector.AppendFixed(vec, d, false, mp))
	}
	return vec
}

func timestampRankVec(t *testing.T, mp *mpool.MPool, vals ...types.Timestamp) *vector.Vector {
	vec := vector.NewVec(types.T_timestamp.ToType())
	for _, ts := range vals {
		require.NoError(t, vector.AppendFixed(vec, ts, false, mp))
	}
	return vec
}

func constInt64Vec(t *testing.T, mp *mpool.MPool, rows int, value int64) *vector.Vector {
	vals := make([]int64, rows)
	for i := range vals {
		vals[i] = value
	}
	return appendFixedVec(t, mp, types.T_int64, vals, nil)
}

// TestLocalRankMrankIssueProfile pins the issue's six-row control profile:
// ascending competition rank of the current non-NULL value in the last three
// physical rows — the working SQL control returned NULL,1,2,2,NULL,1.
func TestLocalRankMrankIssueProfile(t *testing.T) {
	mp := mpool.MustNewZero()
	vNulls := []bool{true, false, false, false, true, false}
	bat := localRankBatch(mp, 6,
		float64RankVec(t, mp, []float64{0, 2, 4, 4, 0, -1}, vNulls),
		constInt64Vec(t, mp, 6, 3),
		int64RankVec(t, mp, []int64{1, 2, 3, 4, 5, 6}, nil),
	)
	spec := localRankSpec("mrank",
		[]*plan.Expr{
			newColExprWithType(0, types.T_float64.ToType()),
			newColExprWithType(1, types.T_int64.ToType()),
		}, 2, types.T_int64.ToType(), nil)
	got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
	require.NoError(t, err)
	requireRankOutput(t, "mrank profile", got, gotNulls,
		[]uint64{0, 1, 2, 2, 0, 1},
		[]bool{true, false, false, false, true, false})
}

// TestLocalRankMrankPhysicalOrderNoOrderBy pins the trailing positional frame
// against the SUPPLIED physical order (no ORDER BY, no sorting): each row's
// frame is the last N physical rows ending at it. For v = 3,1,2 with width 10
// the frames are {3}, {3,1}, {3,1,2}: the ranks 1,1,2 follow the supplied order
// rather than a value ascent.
func TestLocalRankMrankPhysicalOrderNoOrderBy(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := localRankBatch(mp, 3,
		int32RankVec(t, mp, []int32{3, 1, 2}, nil),
		constInt64Vec(t, mp, 3, 10),
	)
	spec := localRankSpec("mrank",
		[]*plan.Expr{
			newColExprWithType(0, types.T_int32.ToType()),
			newColExprWithType(1, types.T_int64.ToType()),
		}, -1, types.Type{}, nil)
	got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
	require.NoError(t, err)
	requireRankOutput(t, "mrank physical order", got, gotNulls,
		[]uint64{1, 1, 2}, []bool{false, false, false})
}

// TestLocalRankMrankEmptyAndTrailingTies covers N=1, ties inside the trailing
// frame, all-NULL frames, a NULL-only prefix, and N wider than the rows.
func TestLocalRankMrankEmptyAndTrailingTies(t *testing.T) {
	cases := []struct {
		name  string
		vals  []int32
		nulls []bool
		width int64
		want  []uint64
		wantN []bool
	}{
		{
			name:  "N1 ranks alone",
			vals:  []int32{3, 1, 2},
			width: 1,
			want:  []uint64{1, 1, 1},
			wantN: []bool{false, false, false},
		},
		{
			name:  "ties share the rank",
			vals:  []int32{5, 5, 5},
			width: 3,
			want:  []uint64{1, 1, 1},
			wantN: []bool{false, false, false},
		},
		{
			name:  "all values NULL",
			vals:  []int32{9, 9, 9},
			nulls: []bool{true, true, true},
			width: 3,
			want:  []uint64{0, 0, 0},
			wantN: []bool{true, true, true},
		},
		{
			name:  "null prefix stays out of counts",
			vals:  []int32{7, 7, 2},
			nulls: []bool{true, true, false},
			width: 3,
			want:  []uint64{0, 0, 1},
			wantN: []bool{true, true, false},
		},
		{
			name:  "frame entry and exit",
			vals:  []int32{5, 5, 1, 5},
			width: 3,
			// j0 {5}=1; j1 {5,5}=1; j2 {5,5,1}: rank(1)=1; j3 {5,1,5}: rank(5)=2.
			want:  []uint64{1, 1, 1, 2},
			wantN: []bool{false, false, false, false},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			if tc.nulls == nil {
				tc.nulls = make([]bool, len(tc.vals))
			}
			bat := localRankBatch(mp, len(tc.vals),
				int32RankVec(t, mp, tc.vals, tc.nulls),
				constInt64Vec(t, mp, len(tc.vals), tc.width),
			)
			spec := localRankSpec("mrank",
				[]*plan.Expr{
					newColExprWithType(0, types.T_int32.ToType()),
					newColExprWithType(1, types.T_int64.ToType()),
				}, -1, types.Type{}, nil)
			got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
			require.NoError(t, err)
			requireRankOutput(t, tc.name, got, gotNulls, tc.want, tc.wantN)
		})
	}
}

// bruteForceLocalRank is the independent oracle: the issue control's own
// formula (1 + count of strictly lower non-NULL values in the trailing frame),
// evaluated directly over the supplied physical rows. A NULL current value has
// no rank. lo < 0 clamps to the batch start.
func bruteForceLocalRank(
	vals []int,
	nulls []bool,
	lo, cur int,
) (uint64, bool) {
	if nulls[cur] {
		return 0, true
	}
	count := uint64(0)
	for k := max(lo, 0); k <= cur; k++ {
		if !nulls[k] && vals[k] < vals[cur] {
			count++
		}
	}
	return count + 1, false
}

// enumerateLocalRankFixture decodes code into a length-row sequence over the
// alphabet {-1, 0, 1, NULL}.
func enumerateLocalRankFixture(code, length int) (vals []int, nulls []bool) {
	vals = make([]int, length)
	nulls = make([]bool, length)
	for i := 0; i < length; i++ {
		switch code % 4 {
		case 0:
			vals[i] = -1
		case 1:
			vals[i] = 0
		case 2:
			vals[i] = 1
		default:
			nulls[i] = true
		}
		code /= 4
	}
	return vals, nulls
}

// TestLocalRankMrankBruteForceOracle enumerates every sequence of length 1..4
// over {-1, 0, 1, NULL} at widths {1, 2, 3, 5} and requires the operator to
// reproduce the brute-force oracle. This covers empty frames, frame entry and
// exit, ties, NULL current values and NULL frame members for every reachable
// boundary combination.
func TestLocalRankMrankBruteForceOracle(t *testing.T) {
	widths := []int64{1, 2, 3, 5}
	for _, length := range []int{1, 2, 3, 4} {
		total := 1
		for i := 0; i < length; i++ {
			total *= 4
		}
		for code := 0; code < total; code++ {
			vals, nulls := enumerateLocalRankFixture(code, length)
			for _, w := range widths {
				mp := mpool.MustNewZero()
				vecVals := make([]int32, length)
				for i := range vals {
					vecVals[i] = int32(vals[i])
				}
				bat := localRankBatch(mp, length,
					int32RankVec(t, mp, vecVals, nulls),
					constInt64Vec(t, mp, length, w),
				)
				spec := localRankSpec("mrank",
					[]*plan.Expr{
						newColExprWithType(0, types.T_int32.ToType()),
						newColExprWithType(1, types.T_int64.ToType()),
					}, -1, types.Type{}, nil)
				got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
				require.NoError(t, err)
				for j := 0; j < length; j++ {
					wantVal, wantNull := bruteForceLocalRank(vals, nulls, j-int(w)+1, j)
					require.Equal(t, wantNull, gotNulls[j],
						"mrank oracle: vals %v nulls %v width %d row %d: got %v/%v",
						vals, nulls, w, j, gotNulls, got)
					if !wantNull {
						require.Equal(t, wantVal, got[j],
							"mrank oracle: vals %v nulls %v width %d row %d: got %v/%v",
							vals, nulls, w, j, gotNulls, got)
					}
				}
			}
		}
	}
}

// TestLocalRankMrankConstWidthArg pins the production shape of mrank's width
// slot: a literal `mrank(v, 3)` travels as a plan constant and evaluates to a
// ONE-physical-row constant vector. The operator must read it through the
// logical-row mapping; indexing by the logical row panics on every row after
// the first (the regression the first distributed BVT run exposed).
func TestLocalRankMrankConstWidthArg(t *testing.T) {
	mp := mpool.MustNewZero()
	vNulls := []bool{true, false, false, false, true, false}
	bat := localRankBatch(mp, 6,
		float64RankVec(t, mp, []float64{0, 2, 4, 4, 0, -1}, vNulls),
		int64RankVec(t, mp, []int64{1, 2, 3, 4, 5, 6}, nil),
	)
	spec := localRankSpec("mrank",
		[]*plan.Expr{
			newColExprWithType(0, types.T_float64.ToType()),
			intervalInt64(3),
		}, 1, types.T_int64.ToType(), nil)
	got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
	require.NoError(t, err)
	requireRankOutput(t, "mrank const width", got, gotNulls,
		[]uint64{0, 1, 2, 2, 0, 1},
		[]bool{true, false, false, false, true, false})
}

// TestLocalRankCumrankOracle pins the cumulative profile (frame = partition
// start .. current row) with the same enumerated oracle, plus the concrete
// [7,3,3,9] profile 1,1,1,4.
func TestLocalRankCumrankOracle(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := localRankBatch(mp, 4,
		int32RankVec(t, mp, []int32{7, 3, 3, 9}, nil),
	)
	spec := localRankSpec("cumrank",
		[]*plan.Expr{newColExprWithType(0, types.T_int32.ToType())}, -1, types.Type{}, nil)
	got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
	require.NoError(t, err)
	requireRankOutput(t, "cumrank [7,3,3,9]", got, gotNulls,
		[]uint64{1, 1, 1, 4}, []bool{false, false, false, false})

	for _, length := range []int{1, 2, 3, 4} {
		total := 1
		for i := 0; i < length; i++ {
			total *= 4
		}
		for code := 0; code < total; code++ {
			vals, nulls := enumerateLocalRankFixture(code, length)
			vecVals := make([]int32, length)
			for i := range vals {
				vecVals[i] = int32(vals[i])
			}
			sweep := localRankBatch(mp, length, int32RankVec(t, mp, vecVals, nulls))
			sweepSpec := localRankSpec("cumrank",
				[]*plan.Expr{newColExprWithType(0, types.T_int32.ToType())}, -1, types.Type{}, nil)
			got, gotNulls, err = runLocalRankWindow(t, sweepSpec, []*batch.Batch{sweep}, mp)
			require.NoError(t, err)
			for j := 0; j < length; j++ {
				wantVal, wantNull := bruteForceLocalRank(vals, nulls, 0, j)
				require.Equal(t, wantNull, gotNulls[j],
					"cumrank oracle: vals %v nulls %v row %d: got %v/%v",
					vals, nulls, j, gotNulls, got)
				if !wantNull {
					require.Equal(t, wantVal, got[j],
						"cumrank oracle: vals %v nulls %v row %d: got %v/%v",
						vals, nulls, j, gotNulls, got)
				}
			}
		}
	}
}

// TestLocalRankValueTypes expands value-domain coverage: every typed
// comparison path the catalog admits must reproduce the same ascending
// competition profile over hand-computed fixtures (int64/uint64/float64 and
// datetime values with ties and a NULL current value; decimal64 values compare
// by scaled digits; uint64 values above the int64 range must still rank).
func TestLocalRankValueTypes(t *testing.T) {
	fixture := func(
		name string,
		rows int,
		values func(mp *mpool.MPool) *vector.Vector,
		want []uint64,
		wantN []bool,
	) {
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			bat := localRankBatch(mp, rows,
				values(mp),
				constInt64Vec(t, mp, rows, 3),
			)
			got, gotNulls, err := runLocalRankWindow(t,
				localRankSpec("mrank",
					[]*plan.Expr{
						newColExprWithType(0, types.Type{Oid: bat.Vecs[0].GetType().Oid}),
						newColExprWithType(1, types.T_int64.ToType()),
					}, -1, types.Type{}, nil),
				[]*batch.Batch{bat}, mp)
			require.NoError(t, err)
			requireRankOutput(t, name, got, gotNulls, want, wantN)
		})
	}

	// Shared mixed profile: v = 2, 2, 4, NULL, 7 in physical order, N=3.
	// Ranks: {2}->1; {2,2}->1; {2,2,4}->3; NULL->NULL; {4,NULL,7}->2.
	sharedWant := []uint64{1, 1, 3, 0, 2}
	sharedNulls := []bool{false, false, false, true, false}
	sharedValueNulls := []bool{false, false, false, true, false}

	fixture("int64", 5, func(mp *mpool.MPool) *vector.Vector {
		return int64RankVec(t, mp, []int64{2, 2, 4, 0, 7}, sharedValueNulls)
	}, sharedWant, sharedNulls)
	fixture("uint64", 5, func(mp *mpool.MPool) *vector.Vector {
		return uint64RankVec(t, mp, []uint64{2, 2, 4, 0, 7}, sharedValueNulls)
	}, sharedWant, sharedNulls)
	fixture("float64", 5, func(mp *mpool.MPool) *vector.Vector {
		return float64RankVec(t, mp, []float64{2, 2, 4, 0, 7}, sharedValueNulls)
	}, sharedWant, sharedNulls)
	fixture("datetime values", 5, func(mp *mpool.MPool) *vector.Vector {
		// Datetimes rank chronologically, not by int64 payload: the same
		// 2,2,4,NULL,7 profile over Jan1, Jan1, Jan2, NULL, Jan3.
		vec := vector.NewVec(types.T_datetime.ToType())
		for i, day := range []uint8{1, 1, 2, 1, 3} {
			val := types.DatetimeFromClock(2026, 1, day, 0, 0, 0, 0)
			require.NoError(t, vector.AppendFixed(vec, val, sharedValueNulls[i], mp))
		}
		return vec
	}, sharedWant, sharedNulls)

	// decimal64: v = 2.5, 2.5, 4.5, 0, 7.5 (N=3).
	// Ranks: 1; 1; {2.5,2.5,4.5}->3; {2.5,4.5,0}: rank(0)->1; {4.5,0,7.5}->3.
	fixture("decimal64", 5, func(mp *mpool.MPool) *vector.Vector {
		return decimal64RankVec(t, mp, []float64{2.5, 2.5, 4.5, 0, 7.5})
	}, []uint64{1, 1, 3, 1, 3}, []bool{false, false, false, false, false})

	// uint64 above the int64 range still ranks by unsigned comparison:
	// v = high, 0, high (N=3): {high}->1; {high,0}->1; {high,0,high}: the
	// trailing frame ties high with its peer at rank 2.
	fixture("uint64 above int64", 3, func(mp *mpool.MPool) *vector.Vector {
		high := uint64(1) << 63
		return uint64RankVec(t, mp, []uint64{high, 0, high}, nil)
	}, []uint64{1, 1, 2}, []bool{false, false, false})
}

// TestLocalRankMrankWidthValidation pins the execution-time width validation:
// non-integer, non-positive and NULL row counts are named errors, mirroring the
// shared N-validation convention (ntile's bucket-count family).
func TestLocalRankMrankWidthValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		width  int64
		widthF float64
		null   bool
		errMsg string
	}{
		{name: "zero width", width: 0, errMsg: "mrank row count must be positive"},
		{name: "negative width", width: -1, errMsg: "mrank row count must be positive"},
		{name: "null width", null: true, errMsg: "mrank row count must be a non-NULL integer"},
		{name: "fractional width", widthF: 1.5, errMsg: "mrank row count must be positive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			var widthVec *vector.Vector
			var widthExpr *plan.Expr
			switch {
			case tc.null:
				widthVec = int64RankVec(t, mp, []int64{0}, []bool{true})
				widthExpr = newColExprWithType(1, types.T_int64.ToType())
			case tc.widthF != 0:
				widthVec = float64RankVec(t, mp, []float64{tc.widthF}, nil)
				widthExpr = newColExprWithType(1, types.T_float64.ToType())
			default:
				widthVec = constInt64Vec(t, mp, 1, tc.width)
				widthExpr = newColExprWithType(1, types.T_int64.ToType())
			}
			bat := localRankBatch(mp, 1,
				int32RankVec(t, mp, []int32{4}, nil),
				widthVec,
			)
			spec := localRankSpec("mrank",
				[]*plan.Expr{newColExprWithType(0, types.T_int32.ToType()), widthExpr},
				-1, types.Type{}, nil)
			_, _, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "error type %T: %s", err, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

// TestLocalRankGuards pins the internal-shape guards: an mrank without a width
// vector and a tmrank without its published width pair fail with the named
// internal errors, the same contract the binder-plus-compiler pair normally
// guarantees structurally.
func TestLocalRankGuards(t *testing.T) {
	t.Run("mrank missing width vector", func(t *testing.T) {
		mp := mpool.MustNewZero()
		bat := localRankBatch(mp, 1, int32RankVec(t, mp, []int32{4}, nil))
		spec := localRankSpec("mrank",
			[]*plan.Expr{newColExprWithType(0, types.T_int32.ToType())}, -1, types.Type{}, nil)
		_, _, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "mrank requires a row count argument")
	})
	t.Run("tmrank missing published pair", func(t *testing.T) {
		mp := mpool.MustNewZero()
		keysVec := datetimeRankVec(t, mp,
			types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0))
		bat := localRankBatch(mp, 1,
			int32RankVec(t, mp, []int32{4}, nil),
			keysVec,
		)
		spec := localRankSpec("tmrank",
			[]*plan.Expr{newColExprWithType(0, types.T_int32.ToType())}, -1, types.Type{}, nil)
		_, _, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "tmrank requires a value and an INTERVAL width")
	})
}

// TestLocalRankPartitions pins partition boundedness: batches arrive one per
// partition (the production shape for a partitioned window), and the trailing
// frame never reaches back over a partition boundary — the first row of each
// partition ranks alone even when the previous batch ends with smaller values.
func TestLocalRankPartitions(t *testing.T) {
	mp := mpool.MustNewZero()
	newBatch := func(key int32, vals []int32) *batch.Batch {
		vVec := int32RankVec(t, mp, vals, nil)
		keys := make([]int32, len(vals))
		for i := range keys {
			keys[i] = key
		}
		keyVec := appendFixedVec(t, mp, types.T_int32, keys, nil)
		nVec := constInt64Vec(t, mp, len(vals), 3)
		return localRankBatch(mp, len(vals), vVec, keyVec, nVec)
	}
	first := newBatch(1, []int32{9, 1, 3})
	second := newBatch(2, []int32{2, 8})
	spec := localRankSpec("mrank",
		[]*plan.Expr{
			newColExprWithType(0, types.T_int32.ToType()),
			newColExprWithType(2, types.T_int64.ToType()),
		}, -1, types.Type{},
		[]*plan.Expr{newColExprWithType(1, types.T_int32.ToType())})
	got, gotNulls, err := runLocalRankWindow(t, spec,
		[]*batch.Batch{first, second}, mp)
	require.NoError(t, err)
	// Partition 1: {9}->1; {9,1}->1; {9,1,3}->2. Partition 2 restarts: {2}->1;
	// {2,8}->2 — the 1 from partition 1 never leaks into partition 2's frame.
	requireRankOutput(t, "mrank partitions", got, gotNulls,
		[]uint64{1, 1, 2, 1, 2}, []bool{false, false, false, false, false})
}

// TestLocalRankPartitionsCumrank pins the cumulative profile resetting across
// partition batches.
func TestLocalRankPartitionsCumrank(t *testing.T) {
	mp := mpool.MustNewZero()
	newBatch := func(key int32, vals []int32) *batch.Batch {
		vVec := int32RankVec(t, mp, vals, nil)
		keys := make([]int32, len(vals))
		for i := range keys {
			keys[i] = key
		}
		keyVec := appendFixedVec(t, mp, types.T_int32, keys, nil)
		return localRankBatch(mp, len(vals), vVec, keyVec)
	}
	first := newBatch(1, []int32{9, 1, 3})
	second := newBatch(2, []int32{5})
	spec := localRankSpec("cumrank",
		[]*plan.Expr{newColExprWithType(0, types.T_int32.ToType())}, -1, types.Type{},
		[]*plan.Expr{newColExprWithType(1, types.T_int32.ToType())})
	got, gotNulls, err := runLocalRankWindow(t, spec,
		[]*batch.Batch{first, second}, mp)
	require.NoError(t, err)
	// Partition 1 cumulative: 9->1, 1->1, 3->2. Partition 2: 5->1.
	requireRankOutput(t, "cumrank partitions", got, gotNulls,
		[]uint64{1, 1, 2, 1}, []bool{false, false, false, false})
}

// TestLocalRankTmrankDayAndMicrosecondWidth pins the width semantics on a
// datetime ORDER BY key: a normalized 1 DAY pair, and the 1.5-DAY magnitude
// published as MICROSECOND count reaching back 30 hours — the issue's
// rounding-profile counterexample against injecting a fraction into the DAY
// pair.
func TestLocalRankTmrankDayAndMicrosecondWidth(t *testing.T) {
	// Every run owns its input batch: the operator frees the batch vectors at
	// teardown, so a shared batch would reach the next run emptied.
	newBatch := func(mp *mpool.MPool) *batch.Batch {
		keys := datetimeRankVec(t, mp,
			types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0),
			types.DatetimeFromClock(2026, 1, 2, 0, 0, 0, 0),
			types.DatetimeFromClock(2026, 1, 2, 6, 0, 0, 0),
			types.DatetimeFromClock(2026, 1, 3, 12, 0, 0, 0),
		)
		vals := float64RankVec(t, mp, []float64{5, 4, 9, 1}, nil)
		return localRankBatch(mp, 4, vals, keys)
	}

	run := func(count int64, unit int64) ([]uint64, []bool) {
		mp := mpool.MustNewZero()
		spec := localRankSpec("tmrank",
			[]*plan.Expr{
				newColExprWithType(0, types.T_float64.ToType()),
				intervalInt64(count),
				intervalInt64(unit),
			}, 1, types.T_datetime.ToType(), nil)
		got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{newBatch(mp)}, mp)
		require.NoError(t, err)
		return got, gotNulls
	}

	// 1 DAY membership: j0 {0}; j1 (>= Jan1) {0,1}; j2 (>= Jan1 06:00) {1,2};
	// j3 (>= Jan2 12:00) {2,3}. Ranks of 5,4,9,1: 1,1,2,1.
	got, gotNulls := run(1, int64(types.Day))
	requireRankOutput(t, "tmrank 1 DAY", got, gotNulls,
		[]uint64{1, 1, 2, 1}, []bool{false, false, false, false})

	// 30 hours as MICROSECOND count: j2 members (>= Jan1 00:00) {0,1,2};
	// j3 members (>= Jan2 06:00) {2,3}. Ranks: 1,1,3,1.
	got, gotNulls = run(129600000000, int64(types.MicroSecond))
	requireRankOutput(t, "tmrank 1.5 DAY as microseconds", got, gotNulls,
		[]uint64{1, 1, 3, 1}, []bool{false, false, false, false})
}

// TestLocalRankTmrankRawPair pins the raw publication route through the shared
// adapter: the string pair ("3", "DAY") normalizes to 3 DAY identical to the
// direct normalized publication; a raw NULL magnitude and a raw non-temporal
// text fail with the shared frame-bound validation errors before any row is
// ranked.
func TestLocalRankTmrankRawPair(t *testing.T) {
	// Every subtest owns its input batch (teardown frees the shared one).
	newBatch := func(mp *mpool.MPool) *batch.Batch {
		keys := datetimeRankVec(t, mp,
			types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0),
			types.DatetimeFromClock(2026, 1, 4, 0, 0, 0, 0),
			types.DatetimeFromClock(2026, 1, 5, 0, 0, 0, 0),
		)
		vals := float64RankVec(t, mp, []float64{10, 4, 9}, nil)
		return localRankBatch(mp, 3, vals, keys)
	}

	t.Run("string raw pair equals normalized", func(t *testing.T) {
		mp := mpool.MustNewZero()
		spec := localRankSpec("tmrank",
			[]*plan.Expr{
				newColExprWithType(0, types.T_float64.ToType()),
				rawStringIntervalCount("3"),
				rawStringIntervalUnit("DAY"),
			}, 1, types.T_datetime.ToType(), nil)
		got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{newBatch(mp)}, mp)
		require.NoError(t, err)
		// Members: j0 {0}; j1 (>= Jan1) {0,1}; j2 (>= Jan2) {1,2}.
		// Ranks of 10,4,9: 1; 1; 2.
		requireRankOutput(t, "tmrank raw pair", got, gotNulls,
			[]uint64{1, 1, 2}, []bool{false, false, false})
	})
	t.Run("raw null magnitude rejected", func(t *testing.T) {
		mp := mpool.MustNewZero()
		spec := localRankSpec("tmrank",
			[]*plan.Expr{
				newColExprWithType(0, types.T_float64.ToType()),
				nullMarkedCount(),
				rawStringIntervalUnit("DAY"),
			}, 1, types.T_datetime.ToType(), nil)
		_, _, err := runLocalRankWindow(t, spec, []*batch.Batch{newBatch(mp)}, mp)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrParseError), "error type %T: %s", err, err)
		require.Contains(t, err.Error(), "NULL or of non-integral type")
	})
	t.Run("raw negative magnitude rejected", func(t *testing.T) {
		mp := mpool.MustNewZero()
		spec := localRankSpec("tmrank",
			[]*plan.Expr{
				newColExprWithType(0, types.T_float64.ToType()),
				rawStringIntervalCount("-3"),
				rawStringIntervalUnit("DAY"),
			}, 1, types.T_datetime.ToType(), nil)
		_, _, err := runLocalRankWindow(t, spec, []*batch.Batch{newBatch(mp)}, mp)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrParseError), "error type %T: %s", err, err)
		require.Contains(t, err.Error(), "negative, NULL or of non-integral type")
	})
	t.Run("raw garbage string follows the shared MySQL prefix parse", func(t *testing.T) {
		mp := mpool.MustNewZero()
		spec := localRankSpec("tmrank",
			[]*plan.Expr{
				newColExprWithType(0, types.T_float64.ToType()),
				rawStringIntervalCount("not-a-magnitude"),
				rawStringIntervalUnit("DAY"),
			}, 1, types.T_datetime.ToType(), nil)
		got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{newBatch(mp)}, mp)
		require.NoError(t, err)
		// The shared window-interval grammar has no numeric prefix,'abc' DAY
		// reads as a zero width: each distinct-key row ranks alone.
		requireRankOutput(t, "tmrank garbage string", got, gotNulls,
			[]uint64{1, 1, 1}, []bool{false, false, false})
	})
}

// TestLocalRankTmrankTrailingCutoff pins the approved membership definition:
// reference RANGE membership intersected with k <= j. A zero-hour window's
// equal-instant peer group counts only rows at or before the current row —
// the first of two equal-timestamp rows ranks alone (1), while ranking against
// the complete RANGE CURRENT ROW peer group would produce 2.
func TestLocalRankTmrankTrailingCutoff(t *testing.T) {
	mp := mpool.MustNewZero()
	keys := datetimeRankVec(t, mp,
		types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0),
		types.DatetimeFromClock(2026, 1, 1, 5, 0, 0, 0),
		types.DatetimeFromClock(2026, 1, 1, 5, 0, 0, 0),
	)
	vals := float64RankVec(t, mp, []float64{7, 3, 5}, nil)
	bat := localRankBatch(mp, 3, vals, keys)
	spec := localRankSpec("tmrank",
		[]*plan.Expr{
			newColExprWithType(0, types.T_float64.ToType()),
			intervalInt64(0),
			intervalInt64(int64(types.Hour)),
		}, 1, types.T_datetime.ToType(), nil)
	got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
	require.NoError(t, err)
	// Zero-hour window keeps exact-instant peers only: j0 {7}->1; j1 {3}->1
	// (the trailing cutoff keeps the equal-instant later peer outside);
	// j2 {3,5}->2. A complete RANGE CURRENT ROW peer group would make j1 rank 2.
	requireRankOutput(t, "tmrank trailing cutoff", got, gotNulls,
		[]uint64{1, 1, 2}, []bool{false, false, false})
}

// TestLocalRankTmrankCalendarMonth pins calendar-unit membership on a DATE
// key: one MONTH back from March 31 lands on February 28 (month-end cap), so
// the February 28 row stays in frame. A flat 30-day subtraction would exclude
// it and the last rank would collapse to 1.
func TestLocalRankTmrankCalendarMonth(t *testing.T) {
	mp := mpool.MustNewZero()
	keys := dateRankVec(t, mp,
		types.DateFromCalendar(2026, 2, 28),
		types.DateFromCalendar(2026, 3, 31),
	)
	vals := float64RankVec(t, mp, []float64{5, 10}, nil)
	bat := localRankBatch(mp, 2, vals, keys)
	spec := localRankSpec("tmrank",
		[]*plan.Expr{
			newColExprWithType(0, types.T_float64.ToType()),
			intervalInt64(1),
			intervalInt64(int64(types.Month)),
		}, 1, types.T_date.ToType(), nil)
	got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
	require.NoError(t, err)
	requireRankOutput(t, "tmrank month calendar", got, gotNulls,
		[]uint64{1, 2}, []bool{false, false})
}

// TestLocalRankTmrankNullArithmetic pins the D17 key-dependent NULL-interval
// membership: with a null-marked magnitude the shared resolver gives non-NULL
// keys an EMPTY frame (a NULL rank, matching aggregates over an empty frame)
// while NULL order keys keep their ordinary NULL peer group, still cut at the
// current row.
func TestLocalRankTmrankNullArithmetic(t *testing.T) {
	mp := mpool.MustNewZero()
	keys := vector.NewVec(types.T_datetime.ToType())
	require.NoError(t, vector.AppendFixed(keys,
		types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0), true, mp))
	require.NoError(t, vector.AppendFixed(keys,
		types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0), true, mp))
	require.NoError(t, vector.AppendFixed(keys,
		types.DatetimeFromClock(2026, 1, 2, 0, 0, 0, 0), false, mp))
	require.NoError(t, vector.AppendFixed(keys,
		types.DatetimeFromClock(2026, 1, 3, 0, 0, 0, 0), false, mp))
	vals := float64RankVec(t, mp, []float64{7, 3, 5, 4}, nil)
	bat := localRankBatch(mp, 4, vals, keys)
	spec := localRankSpec("tmrank",
		[]*plan.Expr{
			newColExprWithType(0, types.T_float64.ToType()),
			nullMarkedCount(),
			intervalInt64(int64(types.Day)),
		}, 1, types.T_datetime.ToType(), nil)
	got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
	require.NoError(t, err)
	// NULL keys sort first (ascending default): their NULL peer group, cut at
	// the current row, ranks {7}->1 then {7,3}->1; both non-NULL keys get the
	// empty NULL-arithmetic frame and a NULL rank.
	requireRankOutput(t, "tmrank NULL arithmetic", got, gotNulls,
		[]uint64{1, 1, 0, 0},
		[]bool{false, false, true, true})
}

// TestLocalRankTmrankNullKeysWithEqualTimestampPeers pins the
// approved-membership intersection (RANGE membership ∩ k <= j) when NULL
// order keys share the window with equal-instant peers: the supplied
// physical order keeps the equal-key peers in input order, and the FIRST
// peer carries the HIGHER value of the pair, so its recorded rank proves the
// trailing cut directly - including the later, lower-valued peer (which a
// plain RANGE scan would do) would raise it to 2, while the recorded 1
// shows the later peer was cut. The public BVT pins the same intersection
// through value-equalized peers instead (its scan order is free to permute
// equal-instant rows); see window_local_rank.sql.
func TestLocalRankTmrankNullKeysWithEqualTimestampPeers(t *testing.T) {
	mp := mpool.MustNewZero()
	keys := vector.NewVec(types.T_datetime.ToType())
	require.NoError(t, vector.AppendFixed(keys,
		types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0), true, mp))
	require.NoError(t, vector.AppendFixed(keys,
		types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0), true, mp))
	require.NoError(t, vector.AppendFixed(keys,
		types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0), false, mp))
	require.NoError(t, vector.AppendFixed(keys,
		types.DatetimeFromClock(2026, 1, 1, 5, 0, 0, 0), false, mp))
	require.NoError(t, vector.AppendFixed(keys,
		types.DatetimeFromClock(2026, 1, 1, 5, 0, 0, 0), false, mp))
	require.NoError(t, vector.AppendFixed(keys,
		types.DatetimeFromClock(2026, 1, 2, 0, 0, 0, 0), false, mp))
	vals := float64RankVec(t, mp, []float64{2, 8, 7, 5, 3, 6}, nil)
	bat := localRankBatch(mp, 6, vals, keys)
	spec := localRankSpec("tmrank",
		[]*plan.Expr{
			newColExprWithType(0, types.T_float64.ToType()),
			intervalInt64(1),
			intervalInt64(int64(types.Day)),
		}, 1, types.T_datetime.ToType(), nil)
	got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
	require.NoError(t, err)
	// Ordinal under supplied physical order (NULL keys sort first, equal keys
	// keep input order): p0/p1 NULL peer group cut at the current row -> 1, 2;
	// p2 (Jan 1 00) sees only itself -> 1; p3 (first Jan 1 05 peer, v=5)
	// drops the later, lower-valued peer -> {7,5} -> 1 (a plain RANGE frame
	// would count the later 3 and rank it 2); p4 (later peer, v=3) keeps both
	// -> {7,5,3} -> less-than-3: none -> 1; p5 (Jan 2 00) sees {7,5,3,6} -> 3.
	requireRankOutput(t, "tmrank NULL keys with equal-instant peers", got, gotNulls,
		[]uint64{1, 2, 1, 1, 1, 3},
		[]bool{false, false, false, false, false, false})
}

// TestLocalRankTmrankNullValueLikeMrank pins NULL value semantics inside a
// time-bounded frame: a NULL current value ranks NULL and never contributes
// to another row's count.
func TestLocalRankTmrankNullValueLikeMrank(t *testing.T) {
	mp := mpool.MustNewZero()
	keys := datetimeRankVec(t, mp,
		types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0),
		types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0),
		types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0),
		types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0),
	)
	vals := float64RankVec(t, mp, []float64{0, 5, 2, 8},
		[]bool{true, false, false, false})
	bat := localRankBatch(mp, 4, vals, keys)
	spec := localRankSpec("tmrank",
		[]*plan.Expr{
			newColExprWithType(0, types.T_float64.ToType()),
			intervalInt64(1),
			intervalInt64(int64(types.Day)),
		}, 1, types.T_datetime.ToType(), nil)
	got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
	require.NoError(t, err)
	// Same-day membership for every row; the trailing cutoff keeps k <= j:
	// j0 NULL; j1 {NULL,5}->1; j2 {NULL,5,2}: rank(2)->1; j3 {5,2,8}: rank(8)->3.
	requireRankOutput(t, "tmrank null value", got, gotNulls,
		[]uint64{0, 1, 1, 3}, []bool{true, false, false, false})
}

// TestLocalRankTmrankTimestampKey pins the timestamp order-key domain (the
// D21 public surface also accepts TIMESTAMP) with exact-instant membership on
// a two-hour width.
func TestLocalRankTmrankTimestampKey(t *testing.T) {
	mp := mpool.MustNewZero()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	keys := timestampRankVec(t, mp,
		types.Timestamp(base.UnixMicro()),
		types.Timestamp(base.Add(time.Hour).UnixMicro()),
		types.Timestamp(base.Add(3*time.Hour).UnixMicro()),
	)
	vals := float64RankVec(t, mp, []float64{5, 4, 9}, nil)
	bat := localRankBatch(mp, 3, vals, keys)
	spec := localRankSpec("tmrank",
		[]*plan.Expr{
			newColExprWithType(0, types.T_float64.ToType()),
			intervalInt64(2),
			intervalInt64(int64(types.Hour)),
		}, 1, types.T_timestamp.ToType(), nil)
	got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
	require.NoError(t, err)
	// Two-hour trailing window on exact instants: j0 {5}->1; j1 {5,4}->1;
	// j2 (>= 01:00) {4,9}->2.
	requireRankOutput(t, "tmrank timestamp key", got, gotNulls,
		[]uint64{1, 1, 2}, []bool{false, false, false})
}

// TestLocalRankValueTypesYearAndDecimal256 pins the two catalog-admitted
// value domains the pre-PR review found missing from the typed comparator:
// YEAR (temporal, int16-backed MoYear) and DECIMAL256 (numeric) are
// comparable members of IsLocalRankValueDomain on every operator, with
// ties, NULLs and magnitudes beyond the int64 range.
func TestLocalRankValueTypesYearAndDecimal256(t *testing.T) {
	t.Run("cumrank year ties and boundaries", func(t *testing.T) {
		mp := mpool.MustNewZero()
		years := appendFixedVec(t, mp, types.T_year,
			[]types.MoYear{2024, 2024, 1999, 2155, 1901, 2023}, nil)
		bat := localRankBatch(mp, 6, years)
		spec := localRankSpec("cumrank",
			[]*plan.Expr{newColExprWithType(0, types.T_year.ToType())},
			-1, types.Type{}, nil)
		got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
		require.NoError(t, err)
		// 2024 tie shares rank 1; 1999 is a new low -> 1; 2155 the new high
		// ranks above all four; 1901 the new low; 2023 ranks over 1999/1901.
		requireRankOutput(t, "cumrank year", got, gotNulls,
			[]uint64{1, 1, 1, 4, 1, 3}, []bool{false, false, false, false, false, false})
	})

	t.Run("mrank decimal256 ties nulls and wide magnitudes", func(t *testing.T) {
		mp := mpool.MustNewZero()
		d := func(f float64) types.Decimal256 {
			v, derr := types.Decimal256FromFloat64(f, 60, 0)
			require.NoError(t, derr)
			return v
		}
		vals := appendFixedVec(t, mp, types.T_decimal256, []types.Decimal256{
			d(-1e30), d(5e29), d(5e29), d(0), d(1e30), d(-9e29),
		}, []bool{false, false, false, true, false, false})
		width := constInt64Vec(t, mp, 6, 3)
		bat := localRankBatch(mp, 6, vals, width)
		spec := localRankSpec("mrank",
			[]*plan.Expr{
				newColExprWithType(0, types.T_decimal256.ToType()),
				newColExprWithType(1, types.T_int64.ToType()),
			}, -1, types.Type{}, nil)
		got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
		require.NoError(t, err)
		// Width-3 frames on magnitudes far beyond int64: j0 1; j1 2; j2 the
		// decimal tie shares 2; j3 NULL value; j4 {5e29,NULL,1e30}->2; j5
		// {NULL,1e30,-9e29}->1.
		requireRankOutput(t, "mrank decimal256", got, gotNulls,
			[]uint64{1, 2, 2, 0, 2, 1},
			[]bool{false, false, false, true, false, false})
	})

	t.Run("tmrank decimal256 values", func(t *testing.T) {
		mp := mpool.MustNewZero()
		d := func(f float64) types.Decimal256 {
			v, derr := types.Decimal256FromFloat64(f, 20, 2)
			require.NoError(t, derr)
			return v
		}
		vals := appendFixedVec(t, mp, types.T_decimal256, []types.Decimal256{
			d(5), d(4), d(9), d(1),
		}, nil)
		keys := datetimeRankVec(t, mp,
			types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0),
			types.DatetimeFromClock(2026, 1, 2, 0, 0, 0, 0),
			types.DatetimeFromClock(2026, 1, 2, 6, 0, 0, 0),
			types.DatetimeFromClock(2026, 1, 3, 12, 0, 0, 0),
		)
		bat := localRankBatch(mp, 4, vals, keys)
		spec := localRankSpec("tmrank",
			[]*plan.Expr{
				newColExprWithType(0, types.T_decimal256.ToType()),
				intervalInt64(129600000000),
				intervalInt64(int64(types.MicroSecond)),
			}, 1, types.T_datetime.ToType(), nil)
		got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{bat}, mp)
		require.NoError(t, err)
		// The 30-hour magnitude reproduces the 1.5 DAY profile: 1,1,3,1.
		requireRankOutput(t, "tmrank decimal256", got, gotNulls,
			[]uint64{1, 1, 3, 1}, []bool{false, false, false, false})
	})
}

// TestLocalRankWidthUnsignedBoundaries pins the pre-PR review's unsigned-
// width finding: every catalog-admitted INTEGER domain validates in its own
// source domain, so unsigned widths above MaxInt64 are valid positive
// counts that simply cover the partition prefix; zero and NULL keep their
// named surfaces.
func TestLocalRankWidthUnsignedBoundaries(t *testing.T) {
	newBatch := func(mp *mpool.MPool, width uint64) *batch.Batch {
		vals := float64RankVec(t, mp, []float64{0, 2, 4, 4, 0, -1},
			[]bool{true, false, false, false, true, false})
		// A width column carries the repeated count per row, the shape a
		// non-const INTEGER width argument evaluates to.
		widths := appendFixedVec(t, mp, types.T_uint64,
			[]uint64{width, width, width, width, width, width}, nil)
		ids := int64RankVec(t, mp, []int64{1, 2, 3, 4, 5, 6}, nil)
		return localRankBatch(mp, 6, vals, widths, ids)
	}
	spec := localRankSpec("mrank",
		[]*plan.Expr{
			newColExprWithType(0, types.T_float64.ToType()),
			newColExprWithType(1, types.T_uint64.ToType()),
		}, 2, types.T_int64.ToType(), nil)

	widths := []struct {
		name   string
		width  uint64
		wantOK bool
	}{
		{"int64 max as uint64", uint64(9223372036854775807), true},
		{"one above int64 max", uint64(9223372036854775808), true},
		{"uint64 max", uint64(18446744073709551615), true},
		{"zero", 0, false},
	}
	for _, tc := range widths {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			got, gotNulls, err := runLocalRankWindow(t, spec, []*batch.Batch{newBatch(mp, tc.width)}, mp)
			if !tc.wantOK {
				require.Error(t, err)
				require.Contains(t, err.Error(), "mrank row count must be positive")
				return
			}
			require.NoError(t, err)
			// A width covering the partition prefix equals cumrank on the
			// issue's six-row profile.
			requireRankOutput(t, "mrank width "+tc.name, got, gotNulls,
				[]uint64{0, 1, 2, 2, 0, 1},
				[]bool{true, false, false, false, true, false})
		})
	}
}

// TestLocalRankTmrankSharesPartitionCivilOrderIndex pins the pre-PR review's
// index-reuse finding: tmrank resolves membership at PARTITION-STABLE bounds
// so the timestamp civil-order index is built once per partition and shared
// by every row, and the trailing intersection with k <= j reproduces exact
// instant membership on TIMESTAMP keys.
func TestLocalRankTmrankSharesPartitionCivilOrderIndex(t *testing.T) {
	t.Run("single partition one shared index", func(t *testing.T) {
		const rows = 37
		mp := mpool.MustNewZero()
		proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
		defer proc.Free()
		proc.GetSessionInfo().TimeZone = time.UTC

		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		keys := make([]types.Timestamp, rows)
		for i := range keys {
			keys[i] = types.Timestamp(base.Add(time.Duration(i) * 7 * time.Minute).UnixMicro())
		}
		vals := make([]float64, rows)
		for i := range vals {
			vals[i] = float64((i*17)%rows)/2 + float64(i%3)
		}
		valVec := float64RankVec(t, mp, vals, nil)
		keyVec := timestampRankVec(t, mp, keys...)
		bat := localRankBatch(mp, rows, valVec, keyVec)
		spec := localRankSpec("tmrank",
			[]*plan.Expr{
				newColExprWithType(0, types.T_float64.ToType()),
				intervalInt64(30),
				intervalInt64(int64(types.Minute)),
			}, 1, types.T_timestamp.ToType(), nil)
		got, gotNulls, err, _, idxCount := runLocalRankWindowProc(t, spec, []*batch.Batch{bat}, proc)
		require.NoError(t, err)

		// Brute-force oracle over exact instants with a 30-minute width:
		// the 7-minute spacing makes consecutive memberships overlap four
		// rows deep, so the ranks are non-trivial; membership is k <= j with
		// key in [key_j - 30m, key_j], and distinct instants leave the
		// trailing cut dropping only later rows.
		want := make([]uint64, rows)
		wantNulls := make([]bool, rows)
		for j := range want {
			lo := keys[j] - types.Timestamp(30*time.Minute.Microseconds())
			count := uint64(0)
			for k := 0; k <= j; k++ {
				if keys[k] >= lo && vals[k] < vals[j] {
					count++
				}
			}
			want[j] = count + 1
		}
		require.Equal(t, want, got)
		require.Equal(t, wantNulls, gotNulls)

		// The shared civil-order index: partition-stable bounds build exactly
		// one index per partition. A per-row end key would retain one entry
		// (and one whole-prefix scan) per row.
		require.Equal(t, 1, idxCount,
			"one partition must share one civil-order index, got %d", idxCount)
	})

	// The production partitioned window delivers one batch per partition:
	// a generation reuses the accumulated batch's storage vectors, so the
	// civil-order cache must be cleared between generations or the second
	// partition could hit the first partition's stale spans under an
	// identical cache key. Different spacings per partition make a stale hit
	// visibly mis-rank the second partition.
	t.Run("partition generations never share a stale index", func(t *testing.T) {
		const rows = 6
		mp := mpool.MustNewZero()
		proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
		defer proc.Free()
		proc.GetSessionInfo().TimeZone = time.UTC

		vals := []float64{3, 1, 7, 5, 2, 8}
		baseTS, terr := types.ParseTimestamp(time.UTC, "2026-01-01 00:00:00.000000", 6)
		require.NoError(t, terr)
		marTS, terr := types.ParseTimestamp(time.UTC, "2026-03-01 00:00:00.000000", 6)
		require.NoError(t, terr)
		// Partition A: 13-minute spacing inside the 30-minute width.
		// Partition B: 5-minute spacing, every earlier row stays inside.
		keysA := make([]types.Timestamp, rows)
		keysB := make([]types.Timestamp, rows)
		for i := range keysA {
			keysA[i] = baseTS + types.Timestamp(int64(i)*13*time.Minute.Microseconds())
			keysB[i] = marTS + types.Timestamp(int64(i)*5*time.Minute.Microseconds())
		}
		batA := localRankBatch(mp, rows, float64RankVec(t, mp, vals, nil), timestampRankVec(t, mp, keysA...))
		batB := localRankBatch(mp, rows, float64RankVec(t, mp, vals, nil), timestampRankVec(t, mp, keysB...))
		spec := localRankSpec("tmrank",
			[]*plan.Expr{
				newColExprWithType(0, types.T_float64.ToType()),
				intervalInt64(30),
				intervalInt64(int64(types.Minute)),
			}, 1, types.T_timestamp.ToType(),
			[]*plan.Expr{newColExprWithType(1, types.T_timestamp.ToType())})
		got, gotNulls, err, _, idxCount := runLocalRankWindowProc(t, spec,
			[]*batch.Batch{batA, batB}, proc)
		require.NoError(t, err)

		oracle := func(keys []types.Timestamp) []uint64 {
			want := make([]uint64, rows)
			for j := range want {
				lo := keys[j] - types.Timestamp(30*time.Minute.Microseconds())
				count := uint64(0)
				for k := 0; k <= j; k++ {
					if keys[k] >= lo && vals[k] < vals[j] {
						count++
					}
				}
				want[j] = count + 1
			}
			return want
		}
		want := append(oracle(keysA), oracle(keysB)...)
		requireRankOutput(t, "tmrank partition generations", got, gotNulls,
			want, make([]bool, 2*rows))

		// Only one generation is live at a time: its bounds stay
		// partition-stable, so the map holds exactly one shared index for
		// the second partition (a per-row end key would have built six),
		// and the generation-start clear keeps the first partition's
		// vector-identical entry from masking the second partition's rebuild.
		require.Equal(t, 1, idxCount,
			"the live partition must keep exactly one civil-order index, got %d", idxCount)
	})
}

// TestLocalRankTmrankFoldTrailingCut pins tmrank's membership across a
// fall-back fold: the fold-split civil spans resolve at partition-stable
// bounds and the trailing intersection keeps only rows at or before the
// current row, with an in-test civil-wall-time oracle.
func TestLocalRankTmrankFoldTrailingCut(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)

	// The fall-back fold of 2024-11-03: UTC 05:00..07:30 spans both civil
	// 01:xx passes, so wall-time membership is not an instant prefix.
	utcValues := []string{
		"2024-11-03 05:00:00.000000", // 01:00 EDT
		"2024-11-03 05:30:00.000000", // 01:30 EDT
		"2024-11-03 05:59:00.000000", // 01:59 EDT
		"2024-11-03 06:00:00.000000", // 01:00 EST
		"2024-11-03 06:30:00.000000", // 01:30 EST
		"2024-11-03 06:59:00.000000", // 01:59 EST
		"2024-11-03 07:00:00.000000", // 02:00 EST
		"2024-11-03 07:30:00.000000", // 02:30 EST
	}
	// The 2024-11-03 05:00 (EDT 01:00) row carries the HIGHER value of the
	// two equal-wall peers (the 06:00/EST pass carries the lower), so the
	// first peer's rank discriminates the trailing cut itself: without the
	// k<=j intersection the later, lower-valued peer would enter its count
	// and raise the rank to 2; the recorded 1 proves the cut.
	vals := []float64{5, 1, 7, 3, 2, 8, 4, 6}
	keys := make([]types.Timestamp, len(utcValues))
	for i, value := range utcValues {
		ts, perr := types.ParseTimestamp(time.UTC, value, 6)
		require.NoError(t, perr)
		keys[i] = ts
	}
	valVec := float64RankVec(t, mp, vals, nil)
	keyVec := timestampRankVec(t, mp, keys...)
	bat := localRankBatch(mp, len(utcValues), valVec, keyVec)
	spec := localRankSpec("tmrank",
		[]*plan.Expr{
			newColExprWithType(0, types.T_float64.ToType()),
			intervalInt64(30),
			intervalInt64(int64(types.Minute)),
		}, 1, types.T_timestamp.ToType(), nil)

	proc.GetSessionInfo().TimeZone = newYork
	got, gotNulls, runErr, _, _ := runLocalRankWindowProc(t, spec, []*batch.Batch{bat}, proc)
	require.NoError(t, runErr)
	proc.Free()

	// Civil oracle: membership is k <= j whose CIVIL wall time lies in
	// [wall_j - 30m, wall_j] in the session location. time.Time comparison
	// alone cannot express this across the fold (instants differ while the
	// walls repeat), so compare seconds-of-day clock components. The engine
	// converts a types.Timestamp by removing the year-1 epoch offset first
	// (window.go's civil machinery does the same); use its conversion so the
	// oracle sees the same instants.
	civilOfDay := func(ts types.Timestamp) int {
		c := time.UnixMicro(int64(ts) - int64(types.UnixToTimestamp(0))).In(newYork)
		hh, mm, ss := c.Clock()
		return hh*3600 + mm*60 + ss
	}
	want := make([]uint64, len(vals))
	for j := range want {
		wallJ := civilOfDay(keys[j])
		lower := wallJ - 30*60
		count := uint64(0)
		for k := 0; k <= j; k++ {
			wallK := civilOfDay(keys[k])
			if wallK >= lower && wallK <= wallJ && vals[k] < vals[j] {
				count++
			}
		}
		want[j] = count + 1
	}
	require.Equal(t, want, got)
	for i := range gotNulls {
		require.False(t, gotNulls[i])
	}
	require.Zero(t, mp.CurrNB())
}

// countedCancelAfterDoneContext cancels after a fixed number of context
// Done() calls and counts every call, so a test can pin WHICH surface
// observed the cancellation (vm.CancelCheck consumes exactly one Done call
// per check) instead of assuming it was the frame scan.
type countedCancelAfterDoneContext struct {
	context.Context
	done      chan struct{}
	remaining int32
	calls     int32
}

func newCountedCancelAfterDoneContext(parent context.Context, checks int32) *countedCancelAfterDoneContext {
	return &countedCancelAfterDoneContext{
		Context:   parent,
		done:      make(chan struct{}),
		remaining: checks,
	}
}

func (c *countedCancelAfterDoneContext) Done() <-chan struct{} {
	c.calls++
	if c.remaining--; c.remaining == 0 {
		close(c.done)
	}
	return c.done
}

func (c *countedCancelAfterDoneContext) DoneCalls() int32 { return c.calls }

func (c *countedCancelAfterDoneContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

// TestLocalRankScanHonorsCancellation pins the design's promised
// cancellation cases (design section 6, pre-PR review rounds): the frame
// scan kernel interrupts an ACTIVE, already-processing scan at its next
// polling boundary (not an already-canceled pre-scan refusal), and the
// operator surfaces (cumrank partition scan, large-N mrank frame scan)
// refuse an already-canceled context through the two pre-scan checks (one
// in vm.Exec, one at Window.Call's head) without scanning or ranking a
// single row. The done-call counts pin which surface fired, so a regression
// that makes the scan stop polling after entry shows up as a wrong count
// and a missing interruption error here.
func TestLocalRankScanHonorsCancellation(t *testing.T) {
	t.Run("frame-scan kernel interrupts an active scan at a later poll", func(t *testing.T) {
		const rows = cancellationCheckInterval * 2
		mp := mpool.MustNewZero()
		proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
		defer func() {
			proc.Free()
			require.Zero(t, mp.CurrNB())
		}()
		proc.GetSessionInfo().TimeZone = time.UTC
		cancelCtx := newCountedCancelAfterDoneContext(proc.Ctx, 2)
		proc.Ctx = cancelCtx
		valsData := make([]float64, rows)
		for i := range valsData {
			valsData[i] = 0.25 * float64(i%9)
		}
		vals := float64RankVec(t, mp, valsData, nil)
		defer vals.Free(mp)
		_, err := countLessInRankRange(proc, vals, 0, rows, rows-1)
		require.ErrorIs(t, err, context.Canceled)
		// The call sequence is deterministic: the k=0 poll starts the scan
		// and, with the kernel called directly (no vm.Exec, no Window.Call),
		// only the scan's own k=cancellationCheckInterval poll can be the
		// second Done call - so the interruption fired after 1024 rows were
		// compared, deep inside an active scan. Removing the kernel's inner
		// polling removes every Done call, so the error and this count both
		// fail on a kernel made uncancellable after entry.
		require.Equal(t, int32(2), cancelCtx.DoneCalls())
	})

	for _, tc := range []struct {
		name string
		spec func(mp *mpool.MPool) (*plan.Expr, []*batch.Batch, []float64, *vector.Vector)
	}{
		{
			name: "cumrank partition scan",
			spec: func(mp *mpool.MPool) (*plan.Expr, []*batch.Batch, []float64, *vector.Vector) {
				const rows = cancellationCheckInterval * 2
				vals := make([]float64, rows)
				for i := range vals {
					vals[i] = float64(i%7) + 0.25*float64(i%5)
				}
				valVec := float64RankVec(t, mp, vals, nil)
				bat := localRankBatch(mp, rows, valVec)
				spec := localRankSpec("cumrank",
					[]*plan.Expr{newColExprWithType(0, types.T_float64.ToType())},
					-1, types.Type{}, nil)
				return spec, []*batch.Batch{bat}, vals, valVec
			},
		},
		{
			name: "large-N mrank frame scan",
			spec: func(mp *mpool.MPool) (*plan.Expr, []*batch.Batch, []float64, *vector.Vector) {
				const rows = cancellationCheckInterval * 2
				vals := make([]float64, rows)
				for i := range vals {
					vals[i] = float64(rows-i) * 0.5
				}
				valVec := float64RankVec(t, mp, vals, nil)
				width := constInt64Vec(t, mp, rows, int64(rows+7))
				ids := int64RankVec(t, mp, seqInt64(rows), nil)
				bat := localRankBatch(mp, rows, valVec, width, ids)
				spec := localRankSpec("mrank",
					[]*plan.Expr{
						newColExprWithType(0, types.T_float64.ToType()),
						newColExprWithType(1, types.T_int64.ToType()),
					}, 2, types.T_int64.ToType(), nil)
				return spec, []*batch.Batch{bat}, vals, valVec
			},
		},
	} {
		t.Run(tc.name+" refuses an already-canceled context pre-scan", func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			defer func() {
				proc.Free()
				require.Zero(t, mp.CurrNB())
			}()
			proc.GetSessionInfo().TimeZone = time.UTC
			spec, bats, _, valVec := tc.spec(mp)
			// The first Done call is vm.Exec's check and the second is the
			// CancelCheck at Window.Call's head, both before the operator
			// receives input: an already-canceled query is refused without
			// scanning or ranking a single row (the mid-scan interruption is
			// pinned by the kernel subtest above).
			cancelCtx := newCountedCancelAfterDoneContext(proc.Ctx, 2)
			proc.Ctx = cancelCtx
			_, _, err, _, _ := runLocalRankWindowProc(t, spec, bats, proc)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, int32(2), cancelCtx.DoneCalls())
			_ = valVec
		})
	}
}

// seqInt64 builds 0..n-1 for the large-partition fixtures.
func seqInt64(n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = int64(i)
	}
	return out
}

// TestValidateIntervalFrameBoundsCoversTmrankWidths pins the D16 owner: the
// prepare-time walk validates tmrank's published width even when the physical
// window operator would be skipped (empty input / LIMIT 0), so an EXECUTE-d
// raw NULL width fails up front while a normalized width passes untouched.
func TestValidateIntervalFrameBoundsCoversTmrankWidths(t *testing.T) {
	mp := mpool.MustNewZero()
	planOf := func(count, unit *plan.Expr) *plan.Plan {
		spec := localRankSpec("tmrank",
			[]*plan.Expr{
				newColExprWithType(0, types.T_float64.ToType()),
				count,
				unit,
			}, 1, types.T_datetime.ToType(), nil)
		return &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
			Steps: []int32{0},
			Nodes: []*plan.Node{{
				NodeType:    plan.Node_WINDOW,
				WinSpecList: []*plan.Expr{spec},
			}},
		}}}
	}
	rawNull := nullMarkedCount()

	validate := func(count, unit *plan.Expr) error {
		proc := testutil.NewProcessWithMPool(t, "", mp)
		defer proc.Free()
		return ValidateIntervalFrameBounds(proc, planOf(count, unit))
	}

	require.NoError(t, validate(intervalInt64(3), intervalInt64(int64(types.Day))))
	require.NoError(t, validate(nullMarkedCount(), intervalInt64(int64(types.Day))))
	// A garbage raw string has no numeric prefix: the shared MySQL prefix
	// parse reads it as zero, so the walk accepts it.
	require.NoError(t, validate(rawStringIntervalCount("not-a-magnitude"),
		rawStringIntervalUnit("DAY")))

	err := validate(rawNull, rawStringIntervalUnit("DAY"))
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrParseError), "error type %T: %s", err, err)
	require.Contains(t, err.Error(), "NULL or of non-integral type")

	err = validate(rawStringIntervalCount("-3"), rawStringIntervalUnit("DAY"))
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrParseError), "error type %T: %s", err, err)
	require.Contains(t, err.Error(), "negative, NULL or of non-integral type")

	// The pre-PR review's normalized-overflow finding: a non-NULL normalized
	// (count, unit) pair is validated by the tmrank-owned adapter with the raw
	// pipeline's magnitude checks, so the row-independent owner rejects an
	// overflowing width on every route (constant plans, prepared EXECUTE
	// fills) exactly as row evaluation does, while the valid 1.5-DAY
	// microsecond pair and the NULL-arithmetic pair keep their documented
	// surfaces (ordinary RANGE bounds deliberately keep their clamping
	// behavior instead; see MaterializeLocalRankWindowInterval).
	err = validate(intervalInt64(9223372036854775807), intervalInt64(int64(types.Year)))
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidArg), "error type %T: %s", err, err)
	require.Contains(t, err.Error(), "interval")
	require.NoError(t, validate(intervalInt64(129600000000), intervalInt64(int64(types.MicroSecond))))
	require.NoError(t, validate(nullMarkedCount(), intervalInt64(int64(types.Year))))

	// The D16 owner borrows the logical plan and discards normalized copies:
	// a repeated validation of the same plan - both the accepted and the
	// rejected shape - observes identical results and leaves the reusable
	// plan untouched (the validate-discard sentinel of design section 6),
	// for NORMALIZED pairs and equally for the RAW published pairs the
	// prepared path carries (string-unit marked, marker or literal source).
	pn := planOf(intervalInt64(3), intervalInt64(int64(types.Day)))
	before := pn.String()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	require.NoError(t, ValidateIntervalFrameBounds(proc, pn))
	require.NoError(t, ValidateIntervalFrameBounds(proc, pn))
	require.Equal(t, before, pn.String())
	pnOverflow := planOf(intervalInt64(9223372036854775807), intervalInt64(int64(types.Year)))
	overflowBefore := pnOverflow.String()
	require.Error(t, ValidateIntervalFrameBounds(proc, pnOverflow))
	require.Equal(t, overflowBefore, pnOverflow.String())
	require.Error(t, ValidateIntervalFrameBounds(proc, pnOverflow))
	pnRaw := planOf(rawStringIntervalCount("3"), rawStringIntervalUnit("DAY"))
	rawBefore := pnRaw.String()
	require.NoError(t, ValidateIntervalFrameBounds(proc, pnRaw))
	require.NoError(t, ValidateIntervalFrameBounds(proc, pnRaw))
	require.Equal(t, rawBefore, pnRaw.String())
}

// TestLocalRankAdapterDistinguishesTypedWidths pins the EXECUTION-side
// materialization the prepared path actually uses (the r10 pre-PR review):
// a numeric (decimal) width source is carried as the raw pair and converted
// by the raw pipeline's MySQL nearest-integer reading (numeric 1.5 DAY ->
// 2 DAY), while a TEXT source keeps the string grammar's integral prefix
// (text '1.5' -> 1 DAY) - a different number than the BIND-TIME owner
// quantizes the same decimal constant to (36 hours in MICROSECOND; pinned
// separately in the bind suite), because the two owners intentionally use
// different conversions. The 40-hour witness in the BVT demonstrates the
// difference on real data through both execution protocols.
func TestLocalRankAdapterDistinguishesTypedWidths(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC

	decBits, decErr := types.Decimal64FromFloat64(1.5, 10, 1)
	require.NoError(t, decErr)
	decSource := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_decimal64), Scale: 1},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: int64(decBits)}}}},
	}
	textSource := rawStringIntervalCount("1.5")
	unitDay := rawStringIntervalUnit("day")

	materialized, err := MaterializeLocalRankWindowInterval(proc, decSource, unitDay)
	require.NoError(t, err)
	list := materialized.GetList().List
	require.Equal(t, int64(2), list[0].GetLit().GetI64Val(),
		"numeric 1.5 DAY must round to 2 DAY through the raw pipeline's nearest-integer reading: %s", materialized.String())
	require.Equal(t, int64(types.Day), list[1].GetLit().GetI64Val(), materialized.String())

	materializedText, err := MaterializeLocalRankWindowInterval(proc, textSource, unitDay)
	require.NoError(t, err)
	textList := materializedText.GetList().List
	require.Equal(t, int64(1), textList[0].GetLit().GetI64Val(),
		"text '1.5' DAY must keep the string grammar's integral prefix: %s", materializedText.String())
	require.Equal(t, int64(types.Day), textList[1].GetLit().GetI64Val(), materializedText.String())

	// The materialization borrows and never mutates the source exprs.
	decBefore, textBefore := decSource.String(), textSource.String()
	_, _ = MaterializeLocalRankWindowInterval(proc, decSource, unitDay)
	_, _ = MaterializeLocalRankWindowInterval(proc, textSource, unitDay)
	require.Equal(t, decBefore, decSource.String())
	require.Equal(t, textBefore, textSource.String())
	require.Zero(t, mp.CurrNB())
}

// TestLocalRankMemoryBalance runs the three function shapes on fresh pools
// and requires a zero net allocation balance after operator teardown, so the
// new value-window paths cannot leak result or scratch memory.
func TestLocalRankMemoryBalance(t *testing.T) {
	build := func(mp *mpool.MPool, name string) (*plan.Expr, []*batch.Batch) {
		switch name {
		case "mrank profile":
			bat := localRankBatch(mp, 6,
				float64RankVec(t, mp, []float64{0, 2, 4, 4, 0, -1},
					[]bool{true, false, false, false, true, false}),
				constInt64Vec(t, mp, 6, 3),
				int64RankVec(t, mp, []int64{1, 2, 3, 4, 5, 6}, nil),
			)
			spec := localRankSpec("mrank",
				[]*plan.Expr{
					newColExprWithType(0, types.T_float64.ToType()),
					newColExprWithType(1, types.T_int64.ToType()),
				}, 2, types.T_int64.ToType(), nil)
			return spec, []*batch.Batch{bat}
		case "cumrank":
			bat := localRankBatch(mp, 4,
				int32RankVec(t, mp, []int32{7, 3, 3, 9}, nil),
			)
			spec := localRankSpec("cumrank",
				[]*plan.Expr{newColExprWithType(0, types.T_int32.ToType())},
				-1, types.Type{}, nil)
			return spec, []*batch.Batch{bat}
		default:
			bat := localRankBatch(mp, 4,
				float64RankVec(t, mp, []float64{5, 4, 9, 1}, nil),
				datetimeRankVec(t, mp,
					types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0),
					types.DatetimeFromClock(2026, 1, 2, 0, 0, 0, 0),
					types.DatetimeFromClock(2026, 1, 2, 6, 0, 0, 0),
					types.DatetimeFromClock(2026, 1, 3, 12, 0, 0, 0),
				),
			)
			spec := localRankSpec("tmrank",
				[]*plan.Expr{
					newColExprWithType(0, types.T_float64.ToType()),
					intervalInt64(1),
					intervalInt64(int64(types.Day)),
				}, 1, types.T_datetime.ToType(), nil)
			return spec, []*batch.Batch{bat}
		}
	}
	for _, name := range []string{"mrank profile", "cumrank", "tmrank 1 DAY"} {
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			spec, bats := build(mp, name)
			got, _, err := runLocalRankWindow(t, spec, bats, mp)
			require.NoError(t, err)
			require.NotEmpty(t, got)
			require.Equal(t, int64(0), mp.CurrNB(),
				"local rank operator left %d bytes unbalanced on the pool", mp.CurrNB())
		})
	}
}
