// Copyright 2021 - 2026 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package window

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// The EWM dispatch lifecycle (§9/§10): the deterministic container-level
// rows — split-peer exactly-once publication, partition isolation, the
// LIMIT-then-Reset contract, the §7 error-with-clean-reuse contract and
// the bounded in-loop cancellation — plus the end-to-end config channel
// proof that the encoded configuration drives the operator's values.

// newEwmLifecycleArg builds one Window operator with a prepared container
// whose ordered row view, partitions and arg vectors are wired from the
// fixture columns. The int64 ORDER BY key domain drives the row-decay and
// event-time fixtures alike.
func newEwmLifecycleArg(
	t *testing.T,
	stat string,
	cfg aggexec.EwmConfig,
	keys []int64,
	x []float64,
	xNulls []bool,
	y []float64,
	yNulls []bool,
	partitions []int64,
) (*Window, *process.Process) {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	bat := batch.NewWithSize(2)
	keyVec := vector.NewVec(types.T_int64.ToType())
	xVec := vector.NewVec(types.T_float64.ToType())
	for i := range x {
		require.NoError(t, vector.AppendFixed(keyVec, keys[i], false, proc.Mp()))
		isNull := xNulls != nil && xNulls[i]
		require.NoError(t, vector.AppendFixed(xVec, x[i], isNull, proc.Mp()))
	}
	bat.Vecs[0] = keyVec
	bat.Vecs[1] = xVec
	sampleVecs := []*vector.Vector{xVec}
	if y != nil {
		yVec := vector.NewVec(types.T_float64.ToType())
		for i := range y {
			isNull := yNulls != nil && yNulls[i]
			require.NoError(t, vector.AppendFixed(yVec, y[i], isNull, proc.Mp()))
		}
		sampleVecs = append(sampleVecs, yVec)
		bat.Vecs = append(bat.Vecs, yVec)
	}
	bat.SetRowCount(len(x))

	expr := aggexec.MakeAggFunctionExpression(0, false, nil, aggexec.EncodeEwmConfig(cfg))
	spec := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_W{W: &plan.WindowSpec{Name: stat}},
	}
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{expr},
	}
	require.NoError(t, arg.Prepare(proc))
	arg.ctr.bat = bat
	arg.ctr.ps = partitions
	// the full-tuple peer-group directory exactly as the ordered tail
	// builds it (window.go: ctr.os = PartitionForOrder(...)): every
	// partition start plus each in-partition position whose single
	// ORDER BY key differs from its predecessor (NULLs equal).
	os := make([]int64, 0, 8)
	part := 0
	for i := range x {
		newGroup := len(os) == 0
		if part+1 < len(partitions) && int64(i) >= partitions[part+1] {
			part++
			newGroup = true
		}
		if !newGroup && i > 0 && keys[i] != keys[i-1] {
			newGroup = true
		}
		if newGroup {
			os = append(os, int64(i))
		}
	}
	arg.ctr.os = os
	arg.ctr.orderVecs = []colexec.ExprEvalVector{{Vec: []*vector.Vector{keyVec}}}
	arg.ctr.aggVecs = []colexec.ExprEvalVector{{Vec: sampleVecs}}
	t.Cleanup(func() {
		bat.Clean(proc.Mp())
		arg.Free(proc, true, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	return arg, proc
}

// runEwmRange publishes one output slice and returns it.
func runEwmRange(t *testing.T, arg *Window, proc *process.Process, start, end int) []float64 {
	t.Helper()
	vec, err := arg.ctr.processFuncRange(0, arg, proc, arg.OpAnalyzer, start, end)
	require.NoError(t, err)
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	require.Equal(t, end-start, vec.Length())
	col := vector.MustFixedColWithTypeCheck[float64](vec)
	out := make([]float64, len(col))
	copy(out, col)
	return out
}

// readEwmRange publishes one output slice and returns its values with
// their NULL bits.
func readEwmRange(t *testing.T, arg *Window, proc *process.Process, start, end int) ([]float64, []bool) {
	t.Helper()
	vec, err := arg.ctr.processFuncRange(0, arg, proc, arg.OpAnalyzer, start, end)
	require.NoError(t, err)
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	require.Equal(t, end-start, vec.Length())
	col := vector.MustFixedColWithTypeCheck[float64](vec)
	values := make([]float64, len(col))
	nulls := make([]bool, len(col))
	for row := 0; row < vec.Length(); row++ {
		values[row] = col[row]
		nulls[row] = vec.IsNull(uint64(row))
		if !nulls[row] {
			require.False(t, math.IsNaN(col[row]) && !math.IsInf(col[row], 0),
				"row %d: NaN outside the NULL contract", row)
		}
	}
	return values, nulls
}

// TestEwmDispatchSplitPeerExactlyOnce: a peer group straddling an output
// slice boundary publishes the identical group-post value at every row of
// the group exactly once, and the outputs do not depend on where the
// generation's slices split (§10 lifecycle (a)); the tie fixture and its
// literals are the certified ties_keys122_adjT corpus row.
func TestEwmDispatchSplitPeerExactlyOnce(t *testing.T) {
	cfg := aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: true}

	// keys at rows 1-2 tie; the boundary at row 2 splits the peer group
	arg, proc := newEwmLifecycleArg(t, "ewmmean", cfg,
		[]int64{1, 2, 2, 3}, []float64{0, 10, 20, 30}, nil, nil, nil, []int64{0})
	got := append(runEwmRange(t, arg, proc, 0, 2), runEwmRange(t, arg, proc, 2, 4)...)
	require.Equal(t, []float64{0, 12, 12, 20}, got,
		"the rows 1-2 peer group must publish its batch-post value exactly once")

	// a different split keeps the same outputs: the second slice does not
	// recompute (re-merge) the finished group
	arg2, proc2 := newEwmLifecycleArg(t, "ewmmean", cfg,
		[]int64{1, 2, 2, 3}, []float64{0, 10, 20, 30}, nil, nil, nil, []int64{0})
	got2 := append(runEwmRange(t, arg2, proc2, 0, 3), runEwmRange(t, arg2, proc2, 3, 4)...)
	require.Equal(t, got, got2, "outputs must not depend on slice boundaries")

	// the whole peer group carries the identical group-post value
	require.Equal(t, got[1], got[2])
}

// TestEwmDispatchPartitionIsolation: two partitions delivered across two
// slices reset the account state at the partition boundary while the
// generation cursor stays absolute (§10 lifecycle (b)).
func TestEwmDispatchPartitionIsolation(t *testing.T) {
	cfg := aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false}
	arg, proc := newEwmLifecycleArg(t, "ewmmean", cfg,
		[]int64{1, 2, 3, 4}, []float64{1, 2, 3, 4}, nil, nil, nil, []int64{0, 2})
	first := runEwmRange(t, arg, proc, 0, 2)
	require.Equal(t, []float64{1, 1.5}, first)

	// partition 2 starts fresh: a leaked state would continue 1.5 to 2.25
	second := runEwmRange(t, arg, proc, 2, 4)
	require.Equal(t, []float64{3, 3.5}, second)

	// partition 2's slice equals an independent fresh container over the
	// same rows as a single partition
	fresh, freshProc := newEwmLifecycleArg(t, "ewmmean", cfg,
		[]int64{3, 4}, []float64{3, 4}, nil, nil, nil, []int64{0})
	require.Equal(t, second, runEwmRange(t, fresh, freshProc, 0, 2))
}

// TestEwmDispatchLimitThenResetResumesFromScratch: a partially consumed
// generation stops consuming (downstream LIMIT), the framework resets
// the container state and the re-execution equals a fresh full run with
// no leaked cursor or account (§10 lifecycle (c)).
func TestEwmDispatchLimitThenResetResumesFromScratch(t *testing.T) {
	cfg := aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false}
	arg, proc := newEwmLifecycleArg(t, "ewmmean", cfg,
		[]int64{1, 2, 3, 4}, []float64{1, 2, 3, 4}, nil, nil, nil, []int64{0})
	part := runEwmRange(t, arg, proc, 0, 2)
	require.Equal(t, []float64{1, 1.5}, part)

	// the operator framework's eval reset frees the dispatch state
	arg.ctr.freeEwmState()
	resumed := runEwmRange(t, arg, proc, 0, 4)
	require.Equal(t, []float64{1, 1.5, 2.25, 3.125}, resumed)

	fresh, freshProc := newEwmLifecycleArg(t, "ewmmean", cfg,
		[]int64{1, 2, 3, 4}, []float64{1, 2, 3, 4}, nil, nil, nil, []int64{0})
	require.Equal(t, resumed, runEwmRange(t, fresh, freshProc, 0, 4))
}

// TestEwmDispatchErrorThenCleanReuse: a non-finite sample raises the §7
// row pre-scan error before any group is processed, the failed generation
// publishes nothing, and a fresh container handles finite input correctly
// afterwards (§10 lifecycle (d)).
func TestEwmDispatchErrorThenCleanReuse(t *testing.T) {
	cfg := aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false}
	arg, proc := newEwmLifecycleArg(t, "ewmmean", cfg,
		[]int64{1, 2, 3}, []float64{1, math.NaN(), 3}, nil, nil, nil, []int64{0})
	vec, err := arg.ctr.processFuncRange(0, arg, proc, arg.OpAnalyzer, 0, 3)
	require.Error(t, err)
	require.Nil(t, vec, "the failed generation must not publish partial output")
	require.Contains(t, err.Error(), "ewm sample values must be finite")
	require.Nil(t, arg.ctr.ewmAggs, "the error path frees the dispatch state")

	fresh, freshProc := newEwmLifecycleArg(t, "ewmmean", cfg,
		[]int64{1, 2, 3}, []float64{2, 4, 6}, nil, nil, nil, []int64{0})
	require.Equal(t, []float64{2, 3, 4.5},
		runEwmRange(t, fresh, freshProc, 0, 3))
}

// TestEwmDispatchDecimalKeys: DECIMAL ORDER BY keys travel the exact
// base-10^scale rational domain — the evh_decimal_keys profile: keys
// 1.5 and 3.5 at scale 1 with h=1 are two half-lives apart, so the
// adjust=true mean closes at 3.6 (a binary-side scale reading would
// treat the gap as ten half-lives and over-decay to 3.998).
func TestEwmDispatchDecimalKeys(t *testing.T) {
	cfg := aggexec.EwmConfig{Mode: aggexec.EwmModeTime, HalfLife: 1, Adjust: true}
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(func() { proc.Free() })

	d64 := types.T_decimal64.ToType()
	d64.Scale = 1
	key64 := testutil.NewDecimal64Vector(0, d64, proc.Mp(), false, nil,
		[]types.Decimal64{15, 35})
	d128 := types.T_decimal128.ToType()
	d128.Scale = 1
	key128 := testutil.NewDecimal128Vector(0, d128, proc.Mp(), false, nil,
		[]types.Decimal128{{B0_63: 15, B64_127: 0}, {B0_63: 35, B64_127: 0}})

	for name, keyVec := range map[string]*vector.Vector{
		"decimal64":  key64,
		"decimal128": key128,
	} {
		t.Run(name, func(t *testing.T) {
			bat := batch.NewWithSize(2)
			xVec := vector.NewVec(types.T_float64.ToType())
			require.NoError(t, vector.AppendFixed(xVec, 2.0, false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(xVec, 4.0, false, proc.Mp()))
			bat.Vecs[0] = keyVec
			bat.Vecs[1] = xVec
			bat.SetRowCount(2)
			expr := aggexec.MakeAggFunctionExpression(0, false, nil, aggexec.EncodeEwmConfig(cfg))
			spec := &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_float64)},
				Expr: &plan.Expr_W{W: &plan.WindowSpec{Name: "ewmmean"}},
			}
			arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{expr}}
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.bat = bat
			arg.ctr.ps = []int64{0}
			// the single-key peer directory: the distinct decimal keys
			// start one new peer group at row 1
			arg.ctr.os = []int64{0, 1}
			arg.ctr.orderVecs = []colexec.ExprEvalVector{{Vec: []*vector.Vector{keyVec}}}
			arg.ctr.aggVecs = []colexec.ExprEvalVector{{Vec: []*vector.Vector{xVec}}}
			t.Cleanup(func() {
				bat.Clean(proc.Mp())
				arg.Free(proc, true, nil)
			})
			vec, err := arg.ctr.processFuncRange(0, arg, proc, arg.OpAnalyzer, 0, 2)
			require.NoError(t, err)
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			require.Equal(t, []float64{2, 3.6},
				vector.MustFixedColWithTypeCheck[float64](vec))
		})
	}
}

// TestEwmDispatchCancellationMidGroup: a cancellation flagged after the
// single whole-partition peer group starts processing surfaces the
// cancellation error within the bounded poll interval with no output
// published, and a re-execution after the framework reset produces the
// complete correct results (§10 lifecycle (e)).
func TestEwmDispatchCancellationMidGroup(t *testing.T) {
	cfg := aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false}
	rows := cancellationCheckInterval * 3
	keys := make([]int64, rows)
	values := make([]float64, rows)
	for i := range keys {
		keys[i] = 1
		values[i] = float64(2)
	}
	arg, proc := newEwmLifecycleArg(t, "ewmmean", cfg,
		keys, values, nil, nil, nil, []int64{0})

	// cancel on the second in-loop check: the group is still being
	// processed (a single tie group spans every row of the partition)
	proc.Ctx = newCancelAfterDoneChecksContext(proc.Ctx, 2)
	vec, err := arg.ctr.processFuncRange(0, arg, proc, arg.OpAnalyzer, 0, rows)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, vec, "no partial group output may be published on cancellation")

	// a fresh execution after the framework reset publishes complete outputs
	fresh, freshProc := newEwmLifecycleArg(t, "ewmmean", cfg,
		keys, values, nil, nil, nil, []int64{0})
	out, nulls := readEwmRange(t, fresh, freshProc, 0, rows)
	for i := range out {
		require.False(t, nulls[i])
		require.Equal(t, 2.0, out[i], "constant resumption row %d", i)
	}
}

// TestEwmDispatchConfigChannelDrivesRows: the encoded configuration the
// compile boundary produces is the configuration the operator executes:
// alpha, adjust, ignoreNA and minPeriods each change the observable row
// results of the same fixture (§10 config channel).
func TestEwmDispatchConfigChannelDrivesRows(t *testing.T) {
	keys := []int64{1, 2, 3, 4}
	values := []float64{1, 2, 3, 4}

	// the issue's canonical profile: alpha=0.5, adjust=false
	profile := aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false}
	arg, proc := newEwmLifecycleArg(t, "ewmmean", profile,
		keys, values, nil, nil, nil, []int64{0})
	got, nulls := readEwmRange(t, arg, proc, 0, 4)
	require.Equal(t, []float64{1, 1.5, 2.25, 3.125}, got)
	require.Equal(t, []bool{false, false, false, false}, nulls)

	// alpha=1 replaces the entire history at each row
	alphaOne := aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 1, Adjust: false}
	arg, proc = newEwmLifecycleArg(t, "ewmmean", alphaOne,
		keys, values, nil, nil, nil, []int64{0})
	require.Equal(t, []float64{1, 2, 3, 4}, runEwmRange(t, arg, proc, 0, 4))

	// adjust=true weights the numerator by the share within the window
	adjustTrue := aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: true}
	arg, proc = newEwmLifecycleArg(t, "ewmmean", adjustTrue,
		keys, values, nil, nil, nil, []int64{0})
	gotAdjust, _ := readEwmRange(t, arg, proc, 0, 4)
	require.NotEqual(t, got, gotAdjust,
		"the adjust flag must be observable in the row results")

	// minPeriods=2 gates the single-observation prefix to NULL; the raw
	// value at a NULL-gated row is unobservable through SQL
	minTwo := aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5,
		Adjust: false, MinPeriods: 2}
	arg, proc = newEwmLifecycleArg(t, "ewmmean", minTwo,
		keys, values, nil, nil, nil, []int64{0})
	got, nulls = readEwmRange(t, arg, proc, 0, 4)
	require.Equal(t, []bool{true, false, false, false}, nulls)
	require.Equal(t, []float64{1.5, 2.25, 3.125}, got[1:])

	// the eleventh review round's boundary rows: §4 declares NO int32
	// limit, so 2147483647 and 2147483648 alike are ordinary thresholds
	// whose C-gate (C < max(1, minPeriods)) publishes NULL at every row
	// of this fixture; each travels the encoded config channel intact
	for _, bound := range []aggexec.EwmConfig{
		{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false, MinPeriods: 2147483647},
		{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false, MinPeriods: 2147483648},
		{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false, MinPeriods: math.MaxInt64},
	} {
		decoded, derr := aggexec.DecodeEwmConfig(aggexec.EncodeEwmConfig(bound))
		require.NoError(t, derr)
		require.Equal(t, bound.MinPeriods, decoded.MinPeriods,
			"the threshold round-trips the encoded channel exactly")
		arg, proc = newEwmLifecycleArg(t, "ewmmean", bound,
			keys, values, nil, nil, nil, []int64{0})
		got, nulls = readEwmRange(t, arg, proc, 0, 4)
		require.Equal(t, []bool{true, true, true, true}, nulls,
			"minPeriods=%d gates every row to NULL", bound.MinPeriods)
	}

	// the time-decay half-life reaches the engine through the event-time
	// quotient rule: BIGINT neighbor keys one half-life apart halve each
	// arrival's share (the certified §10 event-time precision row)
	halflifeOne := aggexec.EwmConfig{Mode: aggexec.EwmModeTime,
		HalfLife: 1, Adjust: false}
	arg, proc = newEwmLifecycleArg(t, "ewmmean", halflifeOne,
		[]int64{0, 1}, []float64{0, 10}, nil, nil, nil, []int64{0})
	got, nulls = readEwmRange(t, arg, proc, 0, 2)
	require.Equal(t, []bool{false, false}, nulls)
	require.Equal(t, []float64{0, 5}, got)

	// event-time ignoreNA publishes the NULL bit at the missing-input row
	// alone, while the carry policy keeps a published value there
	xNulls := []bool{false, true, false, false}
	gap := []float64{1, 0, 3, 4}
	eventIgnoreNA := aggexec.EwmConfig{Mode: aggexec.EwmModeTime,
		HalfLife: 10, Adjust: false, IgnoreNA: true}
	arg, proc = newEwmLifecycleArg(t, "ewmmean", eventIgnoreNA,
		[]int64{0, 10, 20, 30}, gap, xNulls, nil, nil, []int64{0})
	_, nulls = readEwmRange(t, arg, proc, 0, 4)
	require.Equal(t, []bool{false, true, false, false}, nulls)

	eventCarry := aggexec.EwmConfig{Mode: aggexec.EwmModeTime,
		HalfLife: 10, Adjust: false, IgnoreNA: false}
	arg, proc = newEwmLifecycleArg(t, "ewmmean", eventCarry,
		[]int64{0, 10, 20, 30}, gap, xNulls, nil, nil, []int64{0})
	_, nulls = readEwmRange(t, arg, proc, 0, 4)
	require.Equal(t, []bool{false, false, false, false}, nulls)
}

// TestEwmDispatchEventIgnoreNAPartitionsIndexLocally (tenth-review
// advisory 3): under event-time ignoreNA the emit callback reads its
// staged row's sample NULL bits, and the window framework calls FillSlice
// with PARTITION-LOCAL bounds — the callback row is partition-local, so
// the staged-row view must be indexed with that row directly. The
// previous spelling subtracted the absolute partition start
// (st.partStart) from the partition-local callback row and indexed
// before the slice on every partition after the first. Single-value and
// pair statistics alike, across both partitions of one output range.
func TestEwmDispatchEventIgnoreNAPartitionsIndexLocally(t *testing.T) {
	// the single-value statistic: two independent event-time partitions
	// (each t=[0,1]); with every sample present the NULL-bit overlay must
	// publish no NULLs and each partition's own values
	cfg := aggexec.EwmConfig{Mode: aggexec.EwmModeTime,
		HalfLife: 1, Adjust: false, IgnoreNA: true}
	arg, proc := newEwmLifecycleArg(t, "ewmmean", cfg,
		[]int64{0, 1, 0, 1}, []float64{0, 10, 20, 30},
		nil, nil, nil, []int64{0, 2})
	got, nulls := readEwmRange(t, arg, proc, 0, 4)
	require.Equal(t, []bool{false, false, false, false}, nulls,
		"the ignoreNA NULL-bit overlay must read each partition's own staged rows")
	require.Equal(t, []float64{0, 5, 20, 25}, got,
		"partition two publishes its own account: (0.5*20+0.5*30)=25 under h=1 adjust=false")

	// the pair statistic: the corr branch reads both staged columns; a
	// perfectly correlated pair keeps corr 1.0 after the first
	// observation, gated NULL at the single-observation row (§6)
	pairCfg := aggexec.EwmConfig{Mode: aggexec.EwmModeTime,
		HalfLife: 1, Adjust: false, IgnoreNA: true}
	pairArg, pairProc := newEwmLifecycleArg(t, "ewmcorr", pairCfg,
		[]int64{0, 1, 0, 1}, []float64{0, 10, 1, 2}, nil,
		[]float64{0, 10, 1, 2}, nil, []int64{0, 2})
	got, nulls = readEwmRange(t, pairArg, pairProc, 0, 4)
	require.Equal(t, []bool{true, false, true, false}, nulls,
		"each partition's single-observation correlation gated NULL, then the perfect pair publishes")
	require.Equal(t, 1.0, got[1], "partition one's perfect pair correlates to 1")
	require.Equal(t, 1.0, got[3], "partition two's perfect pair correlates to 1")
}
