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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package window

import (
	"context"
	"errors"
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

// The review-round-2 dispatch regressions: constant-vector access (keys
// and samples keep one physical element), full-tuple peers driven by the
// window framework's sorted peer directory rather than reconstructed
// from unshuffled key components, row-decay ORDER BY latitude (strings
// and NULLs), the complete unsigned event-time key domain, the bounded
// cancellation through staging, and the statement-budget admission of
// the staged row view and peer scratch.

// ewmReviewProc owns the process the fixture's vectors and operator
// share, so every test-owned vector allocates and frees through it.
func ewmReviewProc(t *testing.T) (*process.Process, *mpool.MPool) {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	return proc, proc.Mp()
}

// ewmReviewWire prepares one Window operator and wires its container to
// the fixture: the evaluated ORDER BY components in their post-sort
// physical state (the in-memory path leaves earlier components in input
// order, the spilling path materializes every component sorted), the
// sample vectors in sorted order, the partition directory and the
// shared full-tuple peer directory.
func ewmReviewWire(
	t *testing.T,
	proc *process.Process,
	stat string,
	cfg aggexec.EwmConfig,
	orderVecs [][]*vector.Vector,
	x, y *vector.Vector,
	rows int,
	ps, os []int64,
) *Window {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = x
	bat.SetRowCount(rows)
	sampleVecs := []*vector.Vector{x}
	if y != nil {
		bat.Vecs = append(bat.Vecs, y)
		sampleVecs = append(sampleVecs, y)
	}
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
	arg.ctr.ps = ps
	arg.ctr.os = os
	arg.ctr.orderVecs = make([]colexec.ExprEvalVector, len(orderVecs))
	for i, vecs := range orderVecs {
		arg.ctr.orderVecs[i] = colexec.ExprEvalVector{Vec: vecs}
	}
	arg.ctr.aggVecs = []colexec.ExprEvalVector{{Vec: sampleVecs}}
	t.Cleanup(func() {
		bat.Clean(proc.Mp())
		arg.Free(proc, true, nil)
	})
	return arg
}

func ewmRunRange(
	t *testing.T, arg *Window, proc *process.Process, start, end int,
) []float64 {
	t.Helper()
	vec, err := arg.ctr.processFuncRange(0, arg, proc, arg.OpAnalyzer, start, end)
	require.NoError(t, err)
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	require.Equal(t, end-start, vec.Length())
	return vector.MustFixedColWithTypeCheck[float64](vec)
}

func ewmFloatVec(t *testing.T, mp *mpool.MPool, vals ...float64) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(types.T_float64.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	return vec
}

func ewmInt64Vec(t *testing.T, mp *mpool.MPool, vals ...int64) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(types.T_int64.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	return vec
}

func ewmInt32Vec(t *testing.T, mp *mpool.MPool, vals ...int32) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(types.T_int32.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	return vec
}

func ewmUint64Vec(t *testing.T, mp *mpool.MPool, vals ...uint64) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(types.T_uint64.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	return vec
}

// ewmStrVec builds a nullable varchar vector; "" marks a NULL key.
func ewmStrVec(t *testing.T, mp *mpool.MPool, vals ...string) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(types.T_varchar.ToType())
	for _, v := range vals {
		if v == "" {
			require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
			continue
		}
		require.NoError(t, vector.AppendBytes(vec, []byte(v), false, mp))
	}
	return vec
}

// TestEwmDispatchConstantVectors: constant sample and ORDER BY vectors
// hold one physical element; over multi-row input the dispatch reads
// them constant-aware — constant x publishes the signal itself, constant
// y pairs, a constant ORDER BY key forms one whole-partition arrival
// batch in either decay mode, and a constant NULL samples vector keeps
// the all-NULL rows NULL.
func TestEwmDispatchConstantVectors(t *testing.T) {
	// constant x over four ordered rows: the decay of a constant is the
	// constant, first-observation initialization included
	proc, mp := ewmReviewProc(t)
	constX, err := vector.NewConstFixed(types.T_float64.ToType(), 7.0, 4, mp)
	require.NoError(t, err)
	keys := ewmInt64Vec(t, mp, 1, 2, 3, 4)
	arg := ewmReviewWire(t, proc, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false},
		[][]*vector.Vector{{keys}}, constX, nil, 4, []int64{0}, []int64{0, 1, 2, 3})
	require.Equal(t, []float64{7, 7, 7, 7}, ewmRunRange(t, arg, proc, 0, 4))

	// constant NULL x: no observation ever, every row publishes NULL
	proc, mp = ewmReviewProc(t)
	constNullX := vector.NewConstNull(types.T_float64.ToType(), 4, mp)
	keys = ewmInt64Vec(t, mp, 1, 2, 3, 4)
	arg = ewmReviewWire(t, proc, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false},
		[][]*vector.Vector{{keys}}, constNullX, nil, 4, []int64{0}, []int64{0, 1, 2, 3})
	vec, err := arg.ctr.processFuncRange(0, arg, proc, arg.OpAnalyzer, 0, 4)
	require.NoError(t, err)
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	for row := 0; row < vec.Length(); row++ {
		require.True(t, vec.IsNull(uint64(row)), "constant NULL x row %d", row)
	}

	// constant x with a constant y: the pair stream sees the constant
	// pair, so the covariance stays exactly zero
	proc, mp = ewmReviewProc(t)
	constX, err = vector.NewConstFixed(types.T_float64.ToType(), 7.0, 3, mp)
	require.NoError(t, err)
	constY, err := vector.NewConstFixed(types.T_float64.ToType(), 7.0, 3, mp)
	require.NoError(t, err)
	keys = ewmInt64Vec(t, mp, 1, 2, 3)
	arg = ewmReviewWire(t, proc, "ewmcov",
		aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false},
		[][]*vector.Vector{{keys}}, constX, constY, 3, []int64{0}, []int64{0, 1, 2})
	require.Equal(t, []float64{0, 0, 0}, ewmRunRange(t, arg, proc, 0, 3),
		"a constant pair has zero covariance")

	// constant ORDER BY key in row mode: full-tuple peers make the whole
	// partition one arrival batch whose unadjusted publish is the batch
	// mean (the [0,10,20] -> 10 corpus rule)
	proc, mp = ewmReviewProc(t)
	constKey, err := vector.NewConstFixed(types.T_int64.ToType(), int64(9), 3, mp)
	require.NoError(t, err)
	x := ewmFloatVec(t, mp, 10, 20, 30)
	arg = ewmReviewWire(t, proc, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false},
		[][]*vector.Vector{{constKey}}, x, nil, 3, []int64{0}, []int64{0})
	require.Equal(t, []float64{20, 20, 20}, ewmRunRange(t, arg, proc, 0, 3),
		"one whole-partition batch under adjust=false publishes the batch mean")

	// constant ORDER BY key in event-time mode: one instant, one peer
	// group; adjust=true publishes the mass-normalized batch mean
	proc, mp = ewmReviewProc(t)
	x = ewmFloatVec(t, mp, 1, 2, 3)
	arg = ewmReviewWire(t, proc, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeTime, HalfLife: 10, Adjust: true},
		[][]*vector.Vector{{constKey}}, x, nil, 3, []int64{0}, []int64{0})
	require.Equal(t, []float64{2, 2, 2}, ewmRunRange(t, arg, proc, 0, 3),
		"a constant event key is one instant whose group mean is 2")
}

// TestEwmDispatchMultiKeyFullTuplePeers: with ORDER BY a,b the peer
// groups are the full-tuple groups of the window framework's sorted
// peer directory. The in-memory sort path leaves earlier key components
// in input order, so the dispatch must not reconstruct peers from them;
// input (a,b,x) = [(2,1,30),(1,1,10),(1,1,20)] orders to keys
// [(1,1),(1,1),(2,1)] with values [10,20,30] and publishes
// [15,15,22.5] under alpha=0.5, adjust=true.
func TestEwmDispatchMultiKeyFullTuplePeers(t *testing.T) {
	want := []float64{15, 15, 22.5}

	// component a stays in INPUT order (the in-memory path never
	// shuffles it); b is the shuffled last component
	proc, mp := ewmReviewProc(t)
	aIn := ewmInt32Vec(t, mp, 2, 1, 1)
	bSorted := ewmInt64Vec(t, mp, 1, 1, 1)
	x := ewmFloatVec(t, mp, 10, 20, 30)
	arg := ewmReviewWire(t, proc, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: true},
		[][]*vector.Vector{{aIn, bSorted}}, x, nil, 3, []int64{0}, []int64{0, 2, 3})
	require.Equal(t, want, ewmRunRange(t, arg, proc, 0, 3),
		"peers must come from the sorted peer directory, not unshuffled key components")

	// the spilling sort materializes every component in sorted order; the
	// same partition through that layout publishes the same values
	proc, mp = ewmReviewProc(t)
	aSorted := ewmInt32Vec(t, mp, 1, 1, 2)
	bSorted = ewmInt64Vec(t, mp, 1, 1, 1)
	x = ewmFloatVec(t, mp, 10, 20, 30)
	arg = ewmReviewWire(t, proc, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: true},
		[][]*vector.Vector{{aSorted, bSorted}}, x, nil, 3, []int64{0}, []int64{0, 2, 3})
	require.Equal(t, want, ewmRunRange(t, arg, proc, 0, 3),
		"the sorted and in-memory layouts must agree")
}

// TestEwmDispatchRowDecayKeyLatitude: row-decay ORDER BY keys follow the
// window framework's full-tuple peer semantics — string keys and NULL
// keys order and group identically to any other ORDER BY column; the
// event-time key restrictions stay out of row-decay evaluation.
func TestEwmDispatchRowDecayKeyLatitude(t *testing.T) {
	// sorted-DESC string ordering: 'bee' then the tied 'ant' rows
	proc, mp := ewmReviewProc(t)
	strKeys := ewmStrVec(t, mp, "bee", "ant", "ant")
	x := ewmFloatVec(t, mp, 10, 20, 30)
	arg := ewmReviewWire(t, proc, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: true},
		[][]*vector.Vector{{strKeys}}, x, nil, 3, []int64{0}, []int64{0, 1, 3})
	require.Equal(t, []float64{10, 22, 22}, ewmRunRange(t, arg, proc, 0, 3),
		"row decay orders and groups string keys like the window framework")

	// a NULL ORDER BY key row is a legal row-decay ordering: the NULL
	// rows form the first peer group under SQL nulls-first ordering
	proc, mp = ewmReviewProc(t)
	nullableKeys := ewmStrVec(t, mp, "", "x", "x")
	x = ewmFloatVec(t, mp, 10, 20, 30)
	arg = ewmReviewWire(t, proc, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: true},
		[][]*vector.Vector{{nullableKeys}}, x, nil, 3, []int64{0}, []int64{0, 1, 3})
	require.Equal(t, []float64{10, 22, 22}, ewmRunRange(t, arg, proc, 0, 3),
		"a NULL row-decay key row orders and groups with the framework semantics")

	// event-time keeps its domain rules: the same string ORDER BY is a
	// key-type rejection once the decay resolves to 'halflife'
	proc, mp = ewmReviewProc(t)
	strKeys = ewmStrVec(t, mp, "bee", "ant", "ant")
	x = ewmFloatVec(t, mp, 10, 20, 30)
	arg = ewmReviewWire(t, proc, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeTime, HalfLife: 10},
		[][]*vector.Vector{{strKeys}}, x, nil, 3, []int64{0}, []int64{0, 1, 3})
	_, err := arg.ctr.processFuncRange(0, arg, proc, arg.OpAnalyzer, 0, 3)
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"event-time ewm statistics require a numeric or temporal ORDER BY key")
}

// TestEwmDispatchUnsignedKeyDomain: BIGINT UNSIGNED event keys cover
// [0, 2^64-1]: adjacent keys 2^63 and 2^63+1 are one half-life apart
// under h=1 (the gap is carried inside the unsigned domain), the top of
// the domain works, and the full-magnitude gap from 0 to 2^64-1 decays
// the prior account to (numerically) zero. The public profile x=[0,10],
// adjust=false publishes [0,5] over a one-half-life gap and [0,10] over
// the full domain.
func TestEwmDispatchUnsignedKeyDomain(t *testing.T) {
	for _, fx := range []struct {
		name string
		keys []uint64
		want []float64
	}{
		{"crossing the signed fence", []uint64{1 << 63, 1<<63 + 1}, []float64{0, 5}},
		{"domain top", []uint64{math.MaxUint64 - 1, math.MaxUint64}, []float64{0, 5}},
		{"full-magnitude gap", []uint64{0, math.MaxUint64}, []float64{0, 10}},
	} {
		t.Run(fx.name, func(t *testing.T) {
			proc, mp := ewmReviewProc(t)
			keys := ewmUint64Vec(t, mp, fx.keys...)
			x := ewmFloatVec(t, mp, 0, 10)
			arg := ewmReviewWire(t, proc, "ewmmean",
				aggexec.EwmConfig{Mode: aggexec.EwmModeTime, HalfLife: 1, Adjust: false},
				[][]*vector.Vector{{keys}}, x, nil, 2, []int64{0}, []int64{0, 1})
			require.Equal(t, fx.want, ewmRunRange(t, arg, proc, 0, 2))
		})
	}

	// equality spans the fence too: two tied keys at 2^63 are one instant
	// whose unadjusted first-batch publish is the plain batch mean
	proc, mp := ewmReviewProc(t)
	tiedKeys := ewmUint64Vec(t, mp, 1<<63, 1<<63)
	tiedX := ewmFloatVec(t, mp, 10, 20)
	arg := ewmReviewWire(t, proc, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeTime, HalfLife: 1, Adjust: false},
		[][]*vector.Vector{{tiedKeys}}, tiedX, nil, 2, []int64{0}, []int64{0})
	require.Equal(t, []float64{15, 15}, ewmRunRange(t, arg, proc, 0, 2),
		"tied unsigned keys are one peer group under adjust=false")
}

// TestEwmDispatchStagingCancellation: partition staging is
// partition-sized work and must honor the bounded in-loop cancellation
// contract — a cancellation flagged mid-scan surfaces there, before the
// §7 finite pre-scan of a later row, and the generation publishes
// nothing; a fresh generation over valid input afterwards publishes the
// complete values.
func TestEwmDispatchStagingCancellation(t *testing.T) {
	rows := 3000
	proc, mp := ewmReviewProc(t)
	keys := make([]int64, rows)
	values := make([]float64, rows)
	for i := range keys {
		keys[i] = int64(i)
		values[i] = float64(i)
	}
	values[rows-1] = math.NaN()
	keyVec := vector.NewVec(types.T_int64.ToType())
	xVec := vector.NewVec(types.T_float64.ToType())
	for i := range keys {
		require.NoError(t, vector.AppendFixed(keyVec, keys[i], false, mp))
		require.NoError(t, vector.AppendFixed(xVec, values[i], false, mp))
	}
	os := make([]int64, rows)
	for i := range os {
		os[i] = int64(i)
	}
	arg := ewmReviewWire(t, proc, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false},
		[][]*vector.Vector{{keyVec}}, xVec, nil, rows, []int64{0}, os)
	proc.Ctx = newCancelAfterDoneChecksContext(proc.Ctx, 1)
	vec, err := arg.ctr.processFuncRange(0, arg, proc, arg.OpAnalyzer, 0, rows)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled),
		"staging must surface the bounded cancellation, got %v", err)
	require.NotContains(t, err.Error(), "must be finite",
		"the staging poll fires before the later row's finite pre-scan")
	require.Nil(t, vec, "a canceled generation publishes no partial output")

	// a fresh generation over finite input publishes complete values again
	proc2, mp2 := ewmReviewProc(t)
	keys2 := ewmInt64Vec(t, mp2, 1, 2, 3, 4)
	x2 := ewmFloatVec(t, mp2, 1, 2, 3, 4)
	arg2 := ewmReviewWire(t, proc2, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false},
		[][]*vector.Vector{{keys2}}, x2, nil, 4, []int64{0}, []int64{0, 1, 2, 3})
	require.Equal(t, []float64{1, 1.5, 2.25, 3.125},
		ewmRunRange(t, arg2, proc2, 0, 4))
}

// TestEwmDispatchAllocationAdmission: the staged row view, the span
// directory and the peer scratch are admitted through the statement's
// execution-resource budget before staging; a partition that would
// exceed the budget rejects with the capacity error before publishing
// anything, and its rejected attempt leaves the tiny generation at a
// zero charge, while a generation under a fitting budget publishes the
// same rows and returns to zero once the partition completes.
func TestEwmDispatchAllocationAdmission(t *testing.T) {
	rows := 4096
	build := func(t *testing.T, memCap uint64) (*Window, *process.Process, *process.ExecutionResourceGeneration) {
		t.Helper()
		proc, mp := ewmReviewProc(t)
		budget := process.MustNewExecutionResourceBudget(memCap, memCap)
		generation, err := budget.OpenGeneration(1)
		require.NoError(t, err)
		require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
		keys := make([]int64, rows)
		values := make([]float64, rows)
		for i := range keys {
			keys[i] = 1
			values[i] = 7
		}
		keyVec := ewmInt64Vec(t, mp, keys...)
		xVec := ewmFloatVec(t, mp, values...)
		arg := ewmReviewWire(t, proc, "ewmmean",
			aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false},
			[][]*vector.Vector{{keyVec}}, xVec, nil, rows, []int64{0}, []int64{0})
		// the metadata admission runs only for an accounted operator
		registry, rerr := mpool.NewAllocationAccountRegistry(1, 8)
		require.NoError(t, rerr)
		account, aerr := registry.Open(1 << 28)
		require.NoError(t, aerr)
		require.NoError(t, arg.SetAllocationAccount(account))
		t.Cleanup(func() {
			require.NoError(t, arg.ClearAllocationAccount(account))
			generation.Close()
			budget.Close()
		})
		return arg, proc, generation
	}

	t.Run("partition exceeding the budget rejects and releases", func(t *testing.T) {
		arg, proc, generation := build(t, 1)
		vec, err := arg.ctr.processFuncRange(0, arg, proc, arg.OpAnalyzer, 0, rows)
		require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity,
			"staging beyond the statement budget must surface the capacity error")
		require.Nil(t, vec)
		require.Zero(t, generation.Used(),
			"the rejected staging holds no EWM charge on the tiny generation")
	})

	t.Run("a fitting budget admits the same partition", func(t *testing.T) {
		arg, proc, generation := build(t, 1<<24)
		out := ewmRunRange(t, arg, proc, 0, rows)
		for i := range out {
			require.Equal(t, 7.0, out[i])
		}
		require.Zero(t, generation.Used(),
			"partition completion releases every staging and scratch charge")
	})
}
