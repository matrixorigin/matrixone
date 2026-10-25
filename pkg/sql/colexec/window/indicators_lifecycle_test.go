// Copyright 2026 MatrixOrigin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package window

// indicators_lifecycle_test.go holds the operator-level lifecycle proofs
// required by docs/design/technical_indicator_window_kernels.md section 9:
// every case drives the real Window operator through Prepare and Call (the
// existing Window.Call fixtures), exercising the actual receiveAll/receive
// transitions, chunked emission, failure/cancellation mid-generation, and
// the Reset/Free discipline asserted through the container accounting -
// plus the stage-seed-ring and in-scan cancellation-cadence pins of the
// pre-PR review findings.

import (
	"context"
	"testing"

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

// indInt64LitExpr builds an int64 constant expression argument.
func indInt64LitExpr(v int64) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: v}},
		},
	}
}

// indLifecycleWindow builds a real Window operator for one indicator
// specification: value column 0 (double), constant period (x, n), ORDER BY
// column 1, PARTITION BY column 2 when partitioned. The Aggs entry mirrors
// constructWindow's wiring (functionID + argument expressions); the
// indicator dispatch reads only the name and the materialized arguments.
func indLifecycleWindow(name string, fid int64, period int64, retT types.T, partitioned bool) *Window {
	spec := &plan.WindowSpec{
		Name: name,
		WindowFunc: &plan.Expr{
			Typ:  plan.Type{Id: int32(retT)},
			Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: name}}},
		},
		OrderBy: []*plan.OrderBySpec{{Expr: newColExprWithType(1, types.T_int64.ToType())}},
	}
	if partitioned {
		spec.PartitionBy = []*plan.Expr{newColExprWithType(2, types.T_int64.ToType())}
	}
	args := []*plan.Expr{newColExprWithType(0, types.T_float64.ToType())}
	if period >= 0 {
		args = append(args, indInt64LitExpr(period))
	}
	return &Window{
		WinSpecList:    []*plan.Expr{{Expr: &plan.Expr_W{W: spec}}},
		Aggs:           []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(fid, false, args, nil)},
		SpillThreshold: 1 << 40,
	}
}

// indLifecycle3ColBatch builds the (x, id, part) batch the lifecycle specs
// read: column 0 double value, column 1 int64 series id, column 2 int64
// partition key.
func indLifecycle3ColBatch(
	t *testing.T, proc *process.Process,
	vals []float64, nulls []bool, ids []int64, parts []int64,
) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = indMakeFloat64Vector(t, proc, vals, nulls)
	bat.Vecs[1] = indMakeInt64Vector(t, proc, ids, nil)
	if parts == nil {
		parts = make([]int64, len(ids))
	}
	bat.Vecs[2] = indMakeInt64Vector(t, proc, parts, nil)
	bat.SetRowCount(len(ids))
	return bat
}

// indDrive collects every output chunk of the operator through real Call
// transitions, returning the concatenated result column and NULL bits.
func indDrive(
	t *testing.T, arg *Window, proc *process.Process, retT types.T,
) ([]float64, []bool, error) {
	t.Helper()
	var (
		vals  []float64
		nulls []bool
		resIx = -1
	)
	for {
		result, err := vm.Exec(arg, proc)
		if err != nil {
			return nil, nil, err
		}
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		if result.Batch.IsEmpty() {
			continue
		}
		if resIx < 0 {
			resIx = len(result.Batch.Vecs) - 1
		}
		vec := result.Batch.Vecs[resIx]
		if retT == types.T_int64 {
			col := vector.MustFixedColWithTypeCheck[int64](vec)
			for i := range col {
				vals = append(vals, float64(col[i]))
				nulls = append(nulls, vec.IsNull(uint64(i)))
			}
		} else {
			col := vector.MustFixedColWithTypeCheck[float64](vec)
			for i := range col {
				vals = append(vals, col[i])
				nulls = append(nulls, vec.IsNull(uint64(i)))
			}
		}
	}
	return vals, nulls, nil
}

func indAssertReference(
	t *testing.T, label string,
	got []float64, gotNulls []bool,
	name string, period int64, vals []float64, nulls []bool,
) {
	t.Helper()
	wantV, wantN := refIndicatorFloat(name, int(period), vals, nulls)
	if len(got) != len(wantV) {
		t.Fatalf("%s: got %d rows want %d", label, len(got), len(wantV))
	}
	for i := range wantV {
		if wantN[i] != gotNulls[i] {
			t.Fatalf("%s row %d: null bit got %v want %v", label, i+1, gotNulls[i], wantN[i])
		}
		if !wantN[i] && got[i] != wantV[i] {
			t.Fatalf("%s row %d: got %v want %v", label, i+1, got[i], wantV[i])
		}
	}
}

// TestIndicatorOperatorEmitAcrossModes drives generation semantics of both
// receive modes through the real Call transitions (design section 9):
// receiveAll accumulates multiple child batches into ONE generation - the
// warmup chain spans the batch boundary and equals the fresh single-batch
// computation - while partitioned receive re-enters eval per child batch,
// so every partition is a fresh computation and no cross-partition state
// can leak (partition 2 is a short series whose output would be corrupted
// by any stale prefix).
func TestIndicatorOperatorEmitAcrossModes(t *testing.T) {
	proc := newWindowSpillProcess(t)

	// receiveAll: two child batches, one unpartitioned series.
	vals := make([]float64, 40)
	for i := range vals {
		vals[i] = float64(i+1) + 0.25
	}
	idA := make([]int64, 17)
	idB := make([]int64, 23)
	for i := range idA {
		idA[i] = int64(i)
	}
	for i := range idB {
		idB[i] = int64(17 + i)
	}
	batA := indLifecycle3ColBatch(t, proc, vals[:17], nil, idA, nil)
	batB := indLifecycle3ColBatch(t, proc, vals[17:], nil, idB, nil)

	arg := indLifecycleWindow("ema", function.EMA, 3, types.T_float64, false)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{batA, batB})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))
	got, gotN, err := indDrive(t, arg, proc, types.T_float64)
	require.NoError(t, err)
	indAssertReference(t, "receiveAll", got, gotN, "ema", 3, vals, nil)

	child.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())

	// partitioned receive: each child batch is one partition and one
	// generation; partition 2's three-row series must be computed fresh.
	proc2 := newWindowSpillProcess(t)
	part1 := []float64{1, 1, 1, 1, 1, 1, 1, 1}
	part2 := []float64{10, 11, 12}
	id1 := make([]int64, len(part1))
	id2 := make([]int64, len(part2))
	for i := range id1 {
		id1[i] = int64(i)
	}
	for i := range id2 {
		id2[i] = int64(i)
	}
	pb1 := indLifecycle3ColBatch(t, proc2, part1, nil, id1, make([]int64, len(part1)))
	pb2 := indLifecycle3ColBatch(t, proc2, part2, nil, id2, ones64(len(part2)))
	arg2 := indLifecycleWindow("sma", function.SMA, 2, types.T_float64, true)
	child2 := colexec.NewMockOperator().WithBatchs([]*batch.Batch{pb1, pb2})
	arg2.AppendChild(child2)
	require.NoError(t, arg2.Prepare(proc2))
	got, gotN, err = indDrive(t, arg2, proc2, types.T_float64)
	require.NoError(t, err)
	want2V, want2N := refIndicatorFloat("sma", 2, part2, nil)
	indAssertReference(t, "partition-1", got[:len(part1)], gotN[:len(part1)], "sma", 2, part1, nil)
	indAssertReference(t, "partition-2", got[len(part1):], gotN[len(part1):], "sma", 2, part2, nil)
	// explicit stale-state check: partition 2 is the first defined row of a
	// fresh warmup, so the emitted mean is exactly (10+11)/2.
	require.True(t, want2N[0])
	require.False(t, want2N[1])
	require.Equal(t, want2V[1], 10.5)

	child2.Free(proc2, false, nil)
	arg2.Free(proc2, false, nil)
	proc2.Free()
	require.Zero(t, proc2.Mp().CurrNB())
}

func ones64(n int) []int64 {
	s := make([]int64, n)
	for i := range s {
		s[i] = 1
	}
	return s
}

// TestIndicatorOperatorGenerationIsolationAfterReset runs the same operator
// twice with different inputs, re-entering eval through the pipeline-reuse
// path (Reset -> Prepare) between the rounds: the second round must equal a
// fresh computation of its own input only; a stale first-generation prefix
// must be impossible under receiveAll (design section 9 generation
// isolation).
func TestIndicatorOperatorGenerationIsolationAfterReset(t *testing.T) {
	proc := newWindowSpillProcess(t)
	round1 := []float64{2, 4, 6, 8, 10, 12}
	round2 := []float64{7, 7, 7, 7, 9}
	mkIDs := func(n int) []int64 {
		s := make([]int64, n)
		for i := range s {
			s[i] = int64(i)
		}
		return s
	}
	b1 := indLifecycle3ColBatch(t, proc, round1, nil, mkIDs(len(round1)), nil)
	b2 := indLifecycle3ColBatch(t, proc, round2, nil, mkIDs(len(round2)), nil)

	arg := indLifecycleWindow("ema", function.EMA, 3, types.T_float64, false)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{b1})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))
	// round 1
	got, gotN, err := indDrive(t, arg, proc, types.T_float64)
	require.NoError(t, err)
	indAssertReference(t, "round-1", got, gotN, "ema", 3, round1, nil)
	require.NotEmpty(t, arg.ctr.indicatorEvals)

	// pipeline reuse: the pool returns the operator and the next query
	// re-enters receiveAll -> eval with different input; the Reset
	// discipline must have freed the first generation's prefix. A fresh
	// child stream replaces the exhausted one.
	arg.Reset(proc, false, nil)
	require.Empty(t, arg.ctr.indicatorEvals)
	child2 := colexec.NewMockOperator()
	child2.WithBatchs([]*batch.Batch{b2})
	arg.SetChild(child2, 0)
	require.NoError(t, arg.Prepare(proc))
	got, gotN, err = indDrive(t, arg, proc, types.T_float64)
	require.NoError(t, err)
	indAssertReference(t, "round-2", got, gotN, "ema", 3, round2, nil)

	child.Free(proc, false, nil)
	child2.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

// TestIndicatorChunkedEmitAndLimitEarlyExit pins the chunked-emission and
// LIMIT contracts at window size 8192: a >8192-row partition emits two
// chunks whose concatenation equals the single-pass result, a LIMIT-style
// consumer can abandon the operator mid-emit (the operator stays in the
// emit state and Reset releases the retained generation cleanly), and the
// accounting returns to zero through the reuse/free pair.
func TestIndicatorChunkedEmitAndLimitEarlyExit(t *testing.T) {
	proc := newWindowSpillProcess(t)
	rows := 9000
	vals := make([]float64, rows)
	for i := range vals {
		vals[i] = float64(i%977) + 0.5
	}
	ids := make([]int64, rows)
	for i := range ids {
		ids[i] = int64(i)
	}
	bat := indLifecycle3ColBatch(t, proc, vals, nil, ids, nil)

	wantV, wantN := refIndicatorFloat("sma", 3, vals, nil)

	// full run: two chunks
	arg := indLifecycleWindow("sma", function.SMA, 3, types.T_float64, false)
	child := colexec.NewMockOperator()
	child.WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))
	var got []float64
	var gotN []bool
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		vec := result.Batch.Vecs[len(result.Batch.Vecs)-1]
		col := vector.MustFixedColWithTypeCheck[float64](vec)
		for i := range col {
			got = append(got, col[i])
			gotN = append(gotN, vec.IsNull(uint64(i)))
		}
	}
	require.Len(t, got, rows)
	indAssertReference(t, "two-chunk", got, gotN, "sma", 3, vals, nil)
	child.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())

	// LIMIT early exit: consume exactly one chunk, stop calling, then
	// release the operator through Reset mid-generation.
	proc2 := newWindowSpillProcess(t)
	bat2 := indLifecycle3ColBatch(t, proc2, vals, nil, ids, nil)
	arg2 := indLifecycleWindow("sma", function.SMA, 3, types.T_float64, false)
	child2 := colexec.NewMockOperator()
	child2.WithBatchs([]*batch.Batch{bat2})
	arg2.AppendChild(child2)
	require.NoError(t, arg2.Prepare(proc2))
	result, err := vm.Exec(arg2, proc2)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 8192, result.Batch.RowCount())
	resVec := result.Batch.Vecs[len(result.Batch.Vecs)-1]
	require.Equal(t, 8192, resVec.Length())
	for i := 0; i < 8192; i++ {
		if wantN[i] != resVec.IsNull(uint64(i)) {
			t.Fatalf("limit chunk row %d: null bit mismatch", i+1)
		}
		if !wantN[i] {
			require.Equal(t, wantV[i], vector.MustFixedColWithTypeCheck[float64](resVec)[i])
		}
	}
	// mid-emit abandon: the operator is still in the emit state and holds
	// its computed prefix; Reset must release that generation without a
	// leak.
	require.Len(t, arg2.ctr.indicatorEvals, 1)
	arg2.Reset(proc2, false, nil)
	require.Empty(t, arg2.ctr.indicatorEvals)

	child2.Free(proc2, false, nil)
	arg2.Free(proc2, false, nil)
	proc2.Free()
	require.Zero(t, proc2.Mp().CurrNB())
}

// TestIndicatorOperatorFailureThenReuse fails one generation mid-emit with
// the documented drawdown execution error, checks the failure is reported
// and stable on retry, and then drives a clean generation after Reset:
// correct results and no accounting leak (design section 9
// failure-then-reuse).
func TestIndicatorOperatorFailureThenReuse(t *testing.T) {
	proc := newWindowSpillProcess(t)

	// partition 1: [1e-308, -1e308] with n = 2 makes the second row's
	// quotient -Inf - the documented range error, raised mid-emit.
	poison := []float64{1e-308, -1e308}
	clean := []float64{1, 3, 2, 5, 4}
	idP := []int64{0, 1}
	idC := make([]int64, len(clean))
	for i := range idC {
		idC[i] = int64(i)
	}
	b1 := indLifecycle3ColBatch(t, proc, poison, nil, idP, zeros64(2))
	b2 := indLifecycle3ColBatch(t, proc, clean, nil, idC, ones64(len(clean)))

	arg := indLifecycleWindow("mmaxdrawdown", function.MMAXDRAWDOWN, 2, types.T_float64, true)
	child := colexec.NewMockOperator()
	child.WithBatchs([]*batch.Batch{b1, b2})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	_, _, err := indDrive(t, arg, proc, types.T_float64)
	require.Error(t, err)
	require.Contains(t, err.Error(), "drawdown ratio is not finite for mmaxdrawdown at row 2")

	// the failed generation is still materialized: a retry Call must not
	// corrupt anything and returns the same error.
	_, err = vm.Exec(arg, proc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "drawdown ratio is not finite for mmaxdrawdown at row 2")
	require.Len(t, arg.ctr.indicatorEvals, 1)

	// reuse: Reset releases the failed generation (an error exit from
	// Call skips normal end-of-generation cleanup), then the clean
	// partition computes correctly through a fresh child stream.
	arg.Reset(proc, true, err)
	require.Empty(t, arg.ctr.indicatorEvals)
	cleanBatch := indLifecycle3ColBatch(t, proc, clean, nil, idC, ones64(len(clean)))
	childReuse := colexec.NewMockOperator()
	childReuse.WithBatchs([]*batch.Batch{cleanBatch})
	arg.SetChild(childReuse, 0)
	require.NoError(t, arg.Prepare(proc))
	got, gotN, err := indDrive(t, arg, proc, types.T_float64)
	require.NoError(t, err)
	indAssertReference(t, "post-failure partition", got, gotN, "mmaxdrawdown", 2, clean, nil)

	child.Free(proc, false, nil)
	childReuse.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func zeros64(n int) []int64 {
	s := make([]int64, n)
	return s
}

// TestIndicatorOperatorCancelMidGenerationThenReuse cancels one generation
// mid-emit (the cancellation lands inside the live Call through the
// materialized argument reader), then reuses the operator for a clean
// generation after Reset and checks results and accounting.
func TestIndicatorOperatorCancelMidGenerationThenReuse(t *testing.T) {
	proc := newWindowSpillProcess(t)
	rows := 9000
	vals := make([]float64, rows)
	for i := range vals {
		vals[i] = float64(i%331) + 0.75
	}
	ids := make([]int64, rows)
	for i := range ids {
		ids[i] = int64(i)
	}
	bat := indLifecycle3ColBatch(t, proc, vals, nil, ids, nil)

	arg := indLifecycleWindow("ema", function.EMA, 3, types.T_float64, false)
	child := colexec.NewMockOperator()
	child.WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	// first chunk drains the input and emits [0, 8192).
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 8192, result.Batch.RowCount())
	require.Len(t, arg.ctr.indicatorEvals, 1)

	// cancel deterministically mid-generation: the materialized argument
	// reader swaps in an already-dead process context on the second
	// chunk's first row, so the running Call observes the cancellation
	// through its poll points instead of rejecting at the entry gate.
	dead, deadCancel := context.WithCancel(context.Background())
	deadCancel()
	ev := arg.ctr.indicatorEvals[0]
	orig := ev.rowFn
	ev.rowFn = func(row, pos int) (float64, bool, error) {
		proc.Ctx = dead
		return orig(row, pos)
	}
	_, err = vm.Exec(arg, proc)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	// restore the live context before the reuse round.
	proc.Ctx = context.Background()

	// reuse: Reset releases the interrupted generation, the operator is
	// re-driven end-to-end over a fresh input, and the accounting closes.
	arg.Reset(proc, true, err)
	require.Empty(t, arg.ctr.indicatorEvals)
	reuse := []float64{5, 6, 7, 8, 9, 10}
	ids2 := make([]int64, len(reuse))
	for i := range ids2 {
		ids2[i] = int64(i)
	}
	bat2 := indLifecycle3ColBatch(t, proc, reuse, nil, ids2, nil)
	child2 := colexec.NewMockOperator()
	child2.WithBatchs([]*batch.Batch{bat2})
	arg.SetChild(child2, 0)
	require.NoError(t, arg.Prepare(proc))
	got, gotN, err := indDrive(t, arg, proc, types.T_float64)
	require.NoError(t, err)
	indAssertReference(t, "post-cancel reuse", got, gotN, "ema", 3, reuse, nil)

	child.Free(proc, false, nil)
	child2.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

// TestIndicatorScanCancellationLatency pins the in-scan cancellation
// cadence of the pre-PR review finding: the sparse source scan (sma over a
// long NULL gap) and the mmaxDrawdown frame scan must surface a
// cancellation within one 64-visit poll of the cancel point - the aborted
// scans visit at most cancelPoint + indicatorScanPoll elements and leave
// the computed prefix untouched (nothing is appended for the aborted row).
// The stale variant - polling only at the per-row 1024 stride or at the
// frame's first element - walks the whole gap and appends the row.
func TestIndicatorScanCancellationLatency(t *testing.T) {
	type probe struct {
		cancelAt int
		maxVisit int
	}
	const slack = 2

	// sparse sma scan across a >32k NULL gap: the first valid row after
	// the gap sits exactly at the 8192-row chunk boundary 32768, so the
	// chunk's first appended row performs the sparse scan that crosses the
	// gap.
	proc := newWindowSpillProcess(t)
	gap := 32768
	rows := gap + 64
	vals := make([]float64, rows)
	nulls := make([]bool, rows)
	vals[0], vals[1], vals[2] = 1, 2, 3
	// rows 3..gap-1: a >32k NULL gap the sparse scan must cross
	for i := 3; i < gap; i++ {
		nulls[i] = true
	}
	for i := gap; i < rows; i++ {
		vals[i] = float64(i)
	}
	ids := make([]int64, rows)
	for i := range ids {
		ids[i] = int64(i)
	}
	bat := indLifecycle3ColBatch(t, proc, vals, nulls, ids, nil)
	arg := indLifecycleWindow("sma", function.SMA, 3, types.T_float64, false)
	child := colexec.NewMockOperator()
	child.WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	// chunk 1..4 advance the cheap NULL rows; the scan runs at the first
	// valid tail row (index gap), inside chunk ceil(gap+1/8192).
	for chunk := 0; chunk < 4; chunk++ {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		require.NotNil(t, result.Batch)
		require.Equal(t, 8192, result.Batch.RowCount())
	}
	require.Len(t, arg.ctr.indicatorEvals, 1)
	require.Equal(t, 32768, arg.ctr.indicatorEvals[0].prefix.Length())

	p := probe{cancelAt: 1, maxVisit: 1 + indicatorScanPoll + slack}
	ev := arg.ctr.indicatorEvals[0]
	orig := ev.rowFn
	visits := 0
	dead, deadCancel := context.WithCancel(context.Background())
	deadCancel()
	ev.rowFn = func(row, pos int) (float64, bool, error) {
		visits++
		if visits == p.cancelAt {
			proc.Ctx = dead
		}
		return orig(row, pos)
	}
	_, err := vm.Exec(arg, proc)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	if visits > p.maxVisit {
		t.Fatalf("sparse scan cancellation deferred: visited %d elements after the cancel point, bound %d",
			visits-p.cancelAt, p.maxVisit-p.cancelAt)
	}
	require.Equal(t, 32768, arg.ctr.indicatorEvals[0].prefix.Length(),
		"the aborted scan must not append the aborted row to the prefix")
	child.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())

	// drawdown frame scan: a fully-valid n=1024 series, canceled at the
	// first frame visit of the second chunk's first row.
	proc2 := newWindowSpillProcess(t)
	rows2 := 9000
	vals2 := make([]float64, rows2)
	for i := range vals2 {
		vals2[i] = float64(i%101) + 1
	}
	ids2 := make([]int64, rows2)
	for i := range ids2 {
		ids2[i] = int64(i)
	}
	bat2 := indLifecycle3ColBatch(t, proc2, vals2, nil, ids2, nil)
	arg2 := indLifecycleWindow("mmaxdrawdown", function.MMAXDRAWDOWN, 1024, types.T_float64, false)
	child2 := colexec.NewMockOperator()
	child2.WithBatchs([]*batch.Batch{bat2})
	arg2.AppendChild(child2)
	require.NoError(t, arg2.Prepare(proc2))
	result, err := vm.Exec(arg2, proc2)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 8192, result.Batch.RowCount())
	require.Len(t, arg2.ctr.indicatorEvals, 1)

	p2 := probe{cancelAt: 2, maxVisit: 2 + indicatorScanPoll + slack}
	ev2 := arg2.ctr.indicatorEvals[0]
	orig2 := ev2.rowFn
	visits = 0
	dead2, deadCancel2 := context.WithCancel(context.Background())
	deadCancel2()
	ev2.rowFn = func(row, pos int) (float64, bool, error) {
		visits++
		if visits == p2.cancelAt {
			proc2.Ctx = dead2
		}
		return orig2(row, pos)
	}
	_, err = vm.Exec(arg2, proc2)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	if visits > p2.maxVisit {
		t.Fatalf("drawdown frame scan cancellation deferred: %d visits after cancel point, bound %d",
			visits-p2.cancelAt, p2.maxVisit-p2.cancelAt)
	}
	require.Equal(t, 8192, arg2.ctr.indicatorEvals[0].prefix.Length(),
		"the aborted frame scan must not append the aborted row to the prefix")
	child2.Free(proc2, false, nil)
	arg2.Free(proc2, false, nil)
	proc2.Free()
	require.Zero(t, proc2.Mp().CurrNB())
}

// TestIndicatorAllNullAndEmptyPartition drives an empty child batch (no
// generation starts) and an all-NULL partition (per-row NULL emission,
// warmup never satisfied) through the partitioned receive mode, then a
// trailing healthy partition must still compute exactly (design section 9
// empty/all-NULL partitions).
func TestIndicatorAllNullAndEmptyPartition(t *testing.T) {
	proc := newWindowSpillProcess(t)

	empty := batch.NewWithSize(3)
	empty.Vecs[0] = indMakeFloat64Vector(t, proc, nil, nil)
	empty.Vecs[1] = indMakeInt64Vector(t, proc, nil, nil)
	empty.Vecs[2] = indMakeInt64Vector(t, proc, nil, nil)
	empty.SetRowCount(0)

	allNull := []float64{0, 0, 0, 0}
	allNullN := []bool{true, true, true, true}
	idN := []int64{0, 1, 2, 3}

	clean := []float64{2, 4, 6, 8, 10}
	idC := make([]int64, len(clean))
	for i := range idC {
		idC[i] = int64(i)
	}

	bNull := indLifecycle3ColBatch(t, proc, allNull, allNullN, idN, zeros64(4))
	bClean := indLifecycle3ColBatch(t, proc, clean, nil, idC, ones64(len(clean)))

	arg := indLifecycleWindow("sma", function.SMA, 3, types.T_float64, true)
	child := colexec.NewMockOperator()
	child.WithBatchs([]*batch.Batch{empty, bNull, bClean})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	got, gotN, err := indDrive(t, arg, proc, types.T_float64)
	require.NoError(t, err)
	require.Len(t, got, 4+len(clean))
	for i := 0; i < 4; i++ {
		if !gotN[i] {
			t.Fatalf("all-NULL partition row %d: want NULL", i+1)
		}
	}
	indAssertReference(t, "healthy partition after empty+all-null",
		got[4:], gotN[4:], "sma", 3, clean, nil)

	child.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

// TestIndicatorStageSeedRingVerification pins the bounded stage-seed-ring
// lifecycle of the pre-PR review finding (storage must stop growing after
// warmup): during warmup no feeder ring ever holds more than n values, a
// retired ring stays nil for the rest of the partition, and after every
// consumer stage has seeded all rings are nil - so per-row steady-state
// feeder storage is zero for the remaining rows of a long partition.
func TestIndicatorStageSeedRingVerification(t *testing.T) {
	for _, tc := range []struct {
		name   string
		n      int64
		rows   int
		stages int
	}{
		{"dema", 4, 400, 2},
		{"tema", 4, 400, 3},
		{"t3", 4, 1200, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer func() { proc.Free() }()
			vals := make([]float64, tc.rows)
			for i := range vals {
				vals[i] = float64(i%89) + 1.5
			}
			xVec := indMakeFloat64Vector(t, proc, vals, nil)
			defer xVec.Free(proc.Mp())
			per := make([]int64, tc.rows)
			for i := range per {
				per[i] = tc.n
			}
			nVec := indMakeInt64Vector(t, proc, per, nil)
			defer nVec.Free(proc.Mp())

			arg := &Window{WinSpecList: []*plan.Expr{indSpec(tc.name, types.T_float64)}}
			require.NoError(t, arg.Prepare(proc))
			defer arg.Reset(proc, false, nil)
			arg.ctr.bat = &batch.Batch{}
			arg.ctr.bat.Vecs = []*vector.Vector{xVec}
			arg.ctr.bat.SetRowCount(tc.rows)
			arg.ctr.ps = nil
			arg.ctr.aggVecs = []colexec.ExprEvalVector{
				{Vec: []*vector.Vector{xVec, nVec}},
			}
			require.NoError(t, arg.ctr.validateIndicatorPeriods(0, arg, proc))

			rings := tc.stages - 1
			retired := make([]int, rings) // first row (1-based) at which ring went nil
			maxLen := make([]int, rings)  // largest live length observed
			for i := 0; i < tc.rows; i++ {
				res, err := arg.ctr.processIndicatorFuncRange(0, arg, proc, i, i+1)
				require.NoError(t, err)
				res.Free(proc.Mp())
				ev := arg.ctr.indicatorEvals[0]
				for r := 0; r < rings; r++ {
					switch ring := ev.stageSeed[r]; {
					case ring == nil:
						if retired[r] == 0 {
							retired[r] = i + 1
						}
					default:
						if l := len(ring); l > int(tc.n) {
							t.Fatalf("%s ring %d length %d exceeds n at row %d",
								tc.name, r, l, i+1)
						} else if l > maxLen[r] {
							maxLen[r] = l
						}
					}
				}
			}
			ev := arg.ctr.indicatorEvals[0]
			for r := 0; r < rings; r++ {
				// a retired ring stays nil: feeder storage stopped
				// growing after warmup.
				if ev.stageSeed[r] != nil {
					t.Fatalf("%s ring %d not retired after %d rows", tc.name, r, tc.rows)
				}
				if retired[r] == 0 || retired[r] > 6*int(tc.n)-5 {
					t.Fatalf("%s ring %d retired after row %d; want within warmup M=%d",
						tc.name, r, retired[r], 6*int(tc.n)-5)
				}
				if maxLen[r] > int(tc.n) {
					t.Fatalf("%s ring %d held %d values, bound n=%d",
						tc.name, r, maxLen[r], tc.n)
				}
			}
		})
	}
}
