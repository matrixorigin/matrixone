// Copyright 2025 MatrixOrigin
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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// indicators.go implements the technical-indicator and rolling-risk window
// family requested by issue #29949. The tested contract is
// docs/design/technical_indicator_window_kernels.md (revision
// technical-indicator-window-kernels-2026-10-24-r7):
//
//   - eleven profiles (sma, wma, trima, ema, wilder, kama, dema, tema, t3,
//     mmaxDrawdown, cumPositiveStreak) evaluated as per-partition sequential
//     state on the window operator (section 5.2);
//   - procedure M: the normalized compensated mean used by every
//     mean/weighted-mean/seed site;
//   - procedure U: the convex update used by every recurrence update step,
//     with the r7 accuracy contract
//     |y_out - (a*u + (1-a)*y)| <= 2^-51*max(|u|,|y|) + 2^-1074.
//
// Every windowed finite sum is one fresh pass over values derived from the
// materialized argument vector (or, for the dema/tema/t3 stage seeds, from
// the materialized stage prefix vectors below): no subtractive running
// aggregates, no uncompensated summation.

const (
	indicatorMinPeriod = 1
	indicatorMaxPeriod = 1024
	// indicatorScanPoll is the in-scan cancellation cadence of the
	// mmaxDrawdown frame scan (section 8 budget: every 64 frame elements).
	indicatorScanPoll = 64
)

type indicatorKernel int

// checkScanCanceled honors the in-scan cancellation cadence: one context
// check per indicatorScanPoll visited elements of a physical-row scan
// (section 5.2/8). It polls the context directly because the operator-level
// checkCanceled quantizes to the 1024-iteration stride, which would defer
// a 64-visit poll toward the scan's end.
func checkScanCanceled(proc *process.Process, visits int) error {
	if visits%indicatorScanPoll != 0 {
		return nil
	}
	if err, canceled := vm.CancelCheck(proc); canceled {
		return err
	}
	return nil
}

const (
	kernelNone indicatorKernel = iota
	kernelSMA
	kernelWMA
	kernelTRIMA
	kernelEMA
	kernelWILDER
	kernelKAMA
	kernelDEMA
	kernelTEMA
	kernelT3
	kernelMMaxDrawdown
	kernelCumPositiveStreak
)

var indicatorKernelByName = map[string]indicatorKernel{
	"sma":               kernelSMA,
	"wma":               kernelWMA,
	"trima":             kernelTRIMA,
	"ema":               kernelEMA,
	"wilder":            kernelWILDER,
	"kama":              kernelKAMA,
	"dema":              kernelDEMA,
	"tema":              kernelTEMA,
	"t3":                kernelT3,
	"mmaxdrawdown":      kernelMMaxDrawdown,
	"cumpositivestreak": kernelCumPositiveStreak,
}

// indicatorChainStages returns the number of chained EMA stages (0 except
// for dema/tema/t3).
func indicatorChainStages(k indicatorKernel) int {
	switch k {
	case kernelDEMA:
		return 2
	case kernelTEMA:
		return 3
	case kernelT3:
		return 6
	}
	return 0
}

// indicatorCombinationBound is the L of the dema/tema/t3 MAXFLOAT/L input
// admission bound; 0 means no per-value bound applies.
func indicatorCombinationBound(k indicatorKernel) float64 {
	switch k {
	case kernelDEMA:
		return 3
	case kernelTEMA:
		return 7
	case kernelT3:
		return 14
	}
	return 0
}

func indicatorRangeErrorText(k indicatorKernel) string {
	switch k {
	case kernelDEMA:
		return "MAXFLOAT/3"
	case kernelTEMA:
		return "MAXFLOAT/7"
	case kernelT3:
		return "MAXFLOAT/14"
	default:
		// KAMA reports the same error form; its single-difference
		// representability headroom is the halved double range (L = 2).
		return "MAXFLOAT/2"
	}
}

// neumaierAdd adds x into the (sum, comp) compensated accumulator.
func neumaierAdd(sum, comp, x float64) (float64, float64) {
	t := sum + x
	if math.Abs(sum) >= math.Abs(x) {
		comp += (sum - t) + x
	} else {
		comp += (x - t) + sum
	}
	return t, comp
}

// procedureM computes the normalized compensated mean of a window's finite
// admitted terms (procedure M, design section 3.1). weights is nil for
// plain means; weighted sites pass exact integer weights paired with the
// terms (newest-first) and the exact denominator W.
// Accuracy contract: |r - exact| <= 2^-40*T + 2^-1075 (plain) or
// |r - exact| <= 2^-51*(sum w_k|u_k|) + 2^-40*T + 2^-1075 (weighted),
// with T = max |u_k|.
func procedureM(terms []float64, weights []int64, denominator int64) float64 {
	T := math.Abs(terms[0])
	allNegativeZero := terms[0] == 0 && math.Signbit(terms[0])
	for _, u := range terms[1:] {
		if abs := math.Abs(u); abs > T {
			T = abs
		}
		if u != 0 || !math.Signbit(u) {
			allNegativeZero = false
		}
	}
	if T == 0 {
		if allNegativeZero {
			return math.Copysign(0, -1)
		}
		return 0
	}
	// Power-of-two scale from the exponent of T alone: frexp gives
	// T = frac*2^exp (frac in [0.5,1), so s = 2^(-exp-8) keeps T*s in
	// [2^-9, 2^-8) and every scaled term magnitude below 2^-8 - overflow
	// of any intermediate is structurally impossible. The only inexact
	// scalings are terms pushed below the subnormal domain; their total
	// contribution is below the accuracy contract.
	_, exp := math.Frexp(T)
	shift := -exp - 8
	var sum, comp float64
	if weights == nil {
		for _, u := range terms {
			sum, comp = neumaierAdd(sum, comp, math.Ldexp(u, shift))
		}
	} else {
		for i, u := range terms {
			sum, comp = neumaierAdd(sum, comp, mulRounded(math.Ldexp(u, shift), float64(weights[i])))
		}
	}
	mean := (sum + comp) / float64(denominator)
	return math.Ldexp(mean, -shift)
}

// mulRounded forces a float64 product to round to itself before any
// consuming add. The Go spec permits an implementation to fuse floating
// operations across statements on some architectures (arm64/ppc64 emit
// FMADD for `x*y + z` written as statements); the procedure contracts of
// this file (design section 3.1) specify per-product roundings and a single
// final add, so every product that feeds an addition goes through this
// //go:noinline barrier to keep the specified rounding sequence on every
// host.
//
//go:noinline
func mulRounded(a, b float64) float64 { return a * b }

// procedureU computes one convex recurrence update (procedure U) from the
// coefficient pair a_hat = fl(a), b_hat = fl(1-a), each correctly rounded
// from the profile's true rational coefficient. The result always lies
// inside the closed hull of u and y; no Inf or NaN is reachable on finite
// inputs.
func procedureU(aHat, bHat, u, y float64) float64 {
	lo, hi := u, y
	if lo > hi {
		lo, hi = hi, lo
	}
	p := mulRounded(aHat, u)
	q := mulRounded(bHat, y)
	t := p + q
	if math.IsInf(t, 0) {
		if t > 0 {
			return hi
		}
		return lo
	}
	var res float64
	if math.Abs(p) >= math.Abs(q) {
		res = (p - t) + q
	} else {
		res = (q - t) + p
	}
	out := t + res
	if out < lo {
		return lo
	}
	if out > hi {
		return hi
	}
	return out
}

// indicatorAlphaPair returns the ema (2/(n+1)) or wilder (1/n) coefficient
// pair, each correctly rounded from its exact rational.
func indicatorAlphaPair(k indicatorKernel, n int) (aHat, bHat float64) {
	switch k {
	case kernelWILDER:
		return 1.0 / float64(n), float64(n-1) / float64(n)
	default: // ema and every dema/tema/t3 chain stage
		return 2.0 / float64(n+1), float64(n-1) / float64(n+1)
	}
}

// newIndicatorValueRowFn builds the value input's row reader with the typed
// column resolved once per evaluator generation: the materialized argument
// vectors are immutable for the generation's lifetime, so the per-visit work
// reduces to the null check, the positional decode and the design-3.1
// finiteness admission. Every consumed value is finiteness-checked:
// +Inf/-Inf/NaN - including a decimal whose float64 conversion overflows -
// fails with the documented admission error before it can enter kernel
// state. A const argument vector (a literal series such as
// ema(CAST(2 AS DOUBLE), 3)) holds one physical element: every logical row
// reads row 0.
func newIndicatorValueRowFn(name string, vec *vector.Vector, ctx context.Context) (indicatorRowFn, error) {
	constRow := vec.IsConst()
	read := func(decode func(row int) float64) indicatorRowFn {
		return func(row, position int) (float64, bool, error) {
			if constRow {
				row = 0
			}
			if vec.IsNull(uint64(row)) {
				return 0, false, nil
			}
			v := decode(row)
			if math.IsInf(v, 0) || math.IsNaN(v) {
				return 0, false, moerr.NewInvalidInputf(ctx,
					"input value is not finite for %s at row %d", name, position)
			}
			return v, true, nil
		}
	}
	switch oid := vec.GetType().Oid; oid {
	case types.T_int8:
		col := vector.MustFixedColNoTypeCheck[int8](vec)
		return read(func(r int) float64 { return float64(col[r]) }), nil
	case types.T_int16:
		col := vector.MustFixedColNoTypeCheck[int16](vec)
		return read(func(r int) float64 { return float64(col[r]) }), nil
	case types.T_int32:
		col := vector.MustFixedColNoTypeCheck[int32](vec)
		return read(func(r int) float64 { return float64(col[r]) }), nil
	case types.T_int64:
		col := vector.MustFixedColNoTypeCheck[int64](vec)
		return read(func(r int) float64 { return float64(col[r]) }), nil
	case types.T_uint8:
		col := vector.MustFixedColNoTypeCheck[uint8](vec)
		return read(func(r int) float64 { return float64(col[r]) }), nil
	case types.T_uint16:
		col := vector.MustFixedColNoTypeCheck[uint16](vec)
		return read(func(r int) float64 { return float64(col[r]) }), nil
	case types.T_uint32:
		col := vector.MustFixedColNoTypeCheck[uint32](vec)
		return read(func(r int) float64 { return float64(col[r]) }), nil
	case types.T_uint64, types.T_bit:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		return read(func(r int) float64 { return float64(col[r]) }), nil
	case types.T_float32:
		col := vector.MustFixedColNoTypeCheck[float32](vec)
		return read(func(r int) float64 { return float64(col[r]) }), nil
	case types.T_float64:
		col := vector.MustFixedColNoTypeCheck[float64](vec)
		return read(func(r int) float64 { return col[r] }), nil
	case types.T_decimal64:
		scale := vec.GetType().Scale
		col := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
		return read(func(r int) float64 { return types.Decimal64ToFloat64(col[r], scale) }), nil
	case types.T_decimal128:
		scale := vec.GetType().Scale
		col := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
		return read(func(r int) float64 { return types.Decimal128ToFloat64(col[r], scale) }), nil
	case types.T_decimal256:
		scale := vec.GetType().Scale
		col := vector.MustFixedColNoTypeCheck[types.Decimal256](vec)
		return read(func(r int) float64 { return types.Decimal256ToFloat64(col[r], scale) }), nil
	default:
		// Unreachable for bound plans: the binder's section-4 validation
		// admits only numeric value arguments.
		return nil, moerr.NewInvalidInputf(ctx, "argument 1 of %s must be numeric", name)
	}
}

// indicatorRowFn reads one positioned source row (row is the bat row and
// position its 1-based position inside the logical partition).
type indicatorRowFn func(row int, position int) (float64, bool, error)

// indicatorEvaluator is one window expression's per-partition state machine.
// Lifecycle: created lazily inside one materialized generation; released at
// every eval entry, at Window.Reset and at Window.Free (design section 5.2,
// the three lifecycle reset points).
type indicatorEvaluator struct {
	kernel   indicatorKernel
	n        int
	funcName string
	retType  types.Type
	rowFn    indicatorRowFn

	// fixed profile terms
	aHat, bHat float64 // ema/wilder and chain-stage coefficients
	l          float64 // dema/tema/t3 admission bound (0 = none)
	weights    []int64 // wma/trima weights, newest-first paired with the scan
	denom      int64

	// per-partition sequential state (value axis)
	count  int
	y      float64
	streak int64

	// chain-stage state (indices 0..stages-1)
	stageCnt [6]int
	stageY   [6]float64

	// stageSeed[s] buffers stage s's defined outputs - in consumption
	// order - ONLY until stage s+1 has consumed its n seed values: ring
	// length is bounded by n, appends are gated on the consumer stage not
	// having seeded, and the ring retires to nil at the consumer's seed
	// (so steady-state member storage stays O(1) per section 5.2; the
	// emitted prefix is the only retained cache). Plain slices, not
	// mpool vectors: bounded by n <= 1024 and short-lived per partition.
	stageSeed [5][]float64

	// scratch fresh-pass buffer (newest-first terms)
	win []float64

	// prefix is the emitted result prefix.
	prefix *vector.Vector
}

// freeVectors releases every mpool-held vector of the evaluator and clears
// the stage seed rings.
func (ev *indicatorEvaluator) freeVectors(mp *mpool.MPool) {
	if ev.prefix != nil {
		ev.prefix.Free(mp)
		ev.prefix = nil
	}
	for i := range ev.stageSeed {
		ev.stageSeed[i] = nil
	}
}

// resetPartition zeroes the per-partition sequential state.
func (ev *indicatorEvaluator) resetPartition() {
	ev.count = 0
	ev.y = 0
	ev.streak = 0
	for i := range ev.stageCnt {
		ev.stageCnt[i] = 0
		ev.stageY[i] = 0
	}
	for i := range ev.stageSeed {
		// retired rings are already nil; keep the capacity of a live
		// (unretired) ring for the next partition's warmup.
		ev.stageSeed[i] = ev.stageSeed[i][:0]
	}
}

// scanSource collects up to need non-NULL source values ending at row j,
// newest-first, inside [start, j]. It walks physical rows, so the
// in-scan cancellation cadence applies: one context check per
// indicatorScanPoll visited elements - a long NULL gap cannot defer
// cancellation of a sparse scan (section 5.2/8).
func (ev *indicatorEvaluator) scanSource(
	proc *process.Process, start, j, need int,
) ([]float64, error) {
	ev.win = ev.win[:0]
	visits := 0
	for k := j; k >= start && len(ev.win) < need; k-- {
		visits++
		if err := checkScanCanceled(proc, visits); err != nil {
			return nil, err
		}
		v, valid, err := ev.rowFn(k, k-start+1)
		if err != nil {
			return nil, err
		}
		if valid {
			ev.win = append(ev.win, v)
		}
	}
	return ev.win, nil
}

// admissionError is the documented MAXFLOAT/L admission failure of the
// combination kernels (dema/tema/t3) and KAMA's diff/accumulation guard.
func (ev *indicatorEvaluator) admissionError(ctx context.Context) error {
	return moerr.NewInvalidInputf(ctx,
		"input magnitude exceeds the finite range of %s (period n = %d, bound %s)",
		ev.funcName, ev.n, indicatorRangeErrorText(ev.kernel))
}

// computeChainRow drives the dema/tema/t3 stage cascade for one source
// row. A stage's defined outputs feed the next stage through bounded
// seed rings (stageSeed): only the outputs a consumer stage needs before
// its procedure-M seed are kept (length <= n), appends stop once the
// consumer has seeded, and the ring retires to nil at the seed - so
// storage beyond warmup is O(1), matching the design's member-state
// contract. The combined value is defined only once the final stage is
// defined (warmup m = 2n-1 / 3n-2 / 6n-5).
func (ev *indicatorEvaluator) computeChainRow(
	proc *process.Process, start, j int, v float64, sourceValid bool,
) (float64, bool, error) {
	if sourceValid {
		if ev.l > 0 && math.Abs(v) > math.MaxFloat64/ev.l {
			return 0, false, ev.admissionError(proc.Ctx)
		}
	}
	stages := indicatorChainStages(ev.kernel)
	feeder := v
	feederDefined := sourceValid
	for s := 0; s < stages; s++ {
		var out float64
		defined := false
		if feederDefined {
			ev.stageCnt[s]++
			if ev.stageCnt[s] >= ev.n {
				if ev.stageCnt[s] == ev.n {
					// Seed: procedure M over the stage's first n consumed
					// values. Ring s-1 holds the feeder stage's first n
					// defined outputs in consumption order; presenting them
					// newest-first keeps the seed bit-identical to the
					// r7 fresh-pass scan it replaces.
					var vals []float64
					if s == 0 {
						var err error
						vals, err = ev.scanSource(proc, start, j, ev.n)
						if err != nil {
							return 0, false, err
						}
					} else {
						ring := ev.stageSeed[s-1]
						if len(ring) < ev.n {
							return 0, false, moerr.NewInternalErrorf(proc.Ctx,
								"indicator stage seed underflow for %s", ev.funcName)
						}
						ev.win = ev.win[:0]
						for i := ev.n - 1; i >= 0; i-- {
							ev.win = append(ev.win, ring[i])
						}
						vals = ev.win
					}
					if len(vals) == ev.n {
						ev.stageY[s] = procedureM(vals, nil, int64(ev.n))
						// The feeder ring's consumer has seeded: retire it
						// so feeder storage stops growing after warmup.
						if s > 0 {
							ev.stageSeed[s-1] = nil
						}
						out, defined = ev.stageY[s], true
					}
				} else {
					aHat, bHat := indicatorAlphaPair(ev.kernel, ev.n)
					ev.stageY[s] = procedureU(aHat, bHat, feeder, ev.stageY[s])
					out, defined = ev.stageY[s], true
				}
			}
		}
		if s < stages-1 && defined && ev.stageCnt[s+1] < ev.n {
			// Gate on the consumer's pre-consumption count: after a stage
			// has seeded, its feeder ring no longer grows.
			if cap(ev.stageSeed[s]) == 0 {
				ev.stageSeed[s] = make([]float64, 0, ev.n)
			}
			ev.stageSeed[s] = append(ev.stageSeed[s], out)
		}
		feeder, feederDefined = out, defined
	}
	if !feederDefined {
		return 0, false, nil
	}
	e1, e2, e3, e4, e5, e6 :=
		ev.stageY[0], ev.stageY[1], ev.stageY[2], ev.stageY[3], ev.stageY[4], ev.stageY[5]
	var r float64
	switch ev.kernel {
	case kernelDEMA:
		r = mulRounded(2, e1) - e2
	case kernelTEMA:
		r = (mulRounded(3, e1) - mulRounded(3, e2)) + e3
	default: // kernelT3: v = 0.7, Tillson generalized-DEMA coefficients
		const v = 0.7
		c3 := (1 + v) * (1 + v) * (1 + v)
		c4 := -3 * v * (1 + v) * (1 + v)
		c5 := 3 * v * v * (1 + v)
		c6 := -(v * v * v)
		r = ((mulRounded(c3, e3) + mulRounded(c4, e4)) + mulRounded(c5, e5)) + mulRounded(c6, e6)
	}
	if math.IsInf(r, 0) || math.IsNaN(r) {
		// Defensive checked arithmetic: the admission bound keeps every
		// real partial inside L*max|u| <= MAXFLOAT.
		return 0, false, ev.admissionError(proc.Ctx)
	}
	return r, true, nil
}

// computeRow produces row j's value; start is the partition-start row and
// pos the 1-based physical position of j inside its partition.
func (ev *indicatorEvaluator) computeRow(
	proc *process.Process, j, start, pos int,
) (float64, bool, error) {
	switch ev.kernel {
	case kernelSMA, kernelWMA, kernelTRIMA:
		_, valid, err := ev.rowFn(j, pos)
		if err != nil {
			return 0, false, err
		}
		if !valid {
			return 0, false, nil
		}
		ev.count++
		if ev.count < ev.n {
			return 0, false, nil
		}
		vals, err := ev.scanSource(proc, start, j, ev.n)
		if err != nil {
			return 0, false, err
		}
		return procedureM(vals, ev.weights, ev.denom), true, nil

	case kernelEMA, kernelWILDER:
		v, valid, err := ev.rowFn(j, pos)
		if err != nil {
			return 0, false, err
		}
		if !valid {
			return 0, false, nil
		}
		ev.count++
		if ev.count < ev.n {
			return 0, false, nil
		}
		if ev.count == ev.n {
			// Seed: procedure-M mean of the first n consumed values.
			vals, err := ev.scanSource(proc, start, j, ev.n)
			if err != nil {
				return 0, false, err
			}
			ev.y = procedureM(vals, nil, int64(ev.n))
		} else {
			ev.y = procedureU(ev.aHat, ev.bHat, v, ev.y)
		}
		return ev.y, true, nil

	case kernelKAMA:
		v, valid, err := ev.rowFn(j, pos)
		if err != nil {
			return 0, false, err
		}
		if !valid {
			return 0, false, nil
		}
		ev.count++
		if ev.count <= ev.n {
			if ev.count == ev.n {
				// Seed y := u_n at m = n; this row emits nothing.
				ev.y = v
			}
			return 0, false, nil
		}
		// m > n: fresh efficiency-ratio window over materialized values.
		vals, err := ev.scanSource(proc, start, j, ev.n+1)
		if err != nil {
			return 0, false, err
		}
		if len(vals) != ev.n+1 {
			return 0, false, nil
		}
		c := math.Abs(vals[0] - vals[ev.n])
		if math.IsInf(c, 0) || math.IsNaN(c) {
			return 0, false, ev.admissionError(proc.Ctx)
		}
		var vol, volComp float64
		for k := 0; k < ev.n; k++ {
			diff := math.Abs(vals[k] - vals[k+1])
			if math.IsInf(diff, 0) || math.IsNaN(diff) {
				return 0, false, ev.admissionError(proc.Ctx)
			}
			vol, volComp = neumaierAdd(vol, volComp, diff)
			if total := vol + volComp; math.IsInf(total, 0) || math.IsNaN(total) {
				return 0, false, ev.admissionError(proc.Ctx)
			}
		}
		vol += volComp
		er := 0.0
		if vol != 0 {
			er = c / vol
		}
		// Turtle convention: fast period 2, slow period 30.
		const (
			kamaFastTerm = 2.0 / 3.0
			kamaSlowTerm = 2.0 / 31.0
		)
		scBase := mulRounded(er, kamaFastTerm-kamaSlowTerm) + kamaSlowTerm
		sc := scBase * scBase
		ev.y = procedureU(sc, 1.0-sc, v, ev.y)
		return ev.y, true, nil

	case kernelDEMA, kernelTEMA, kernelT3:
		v, valid, err := ev.rowFn(j, pos)
		if err != nil {
			return 0, false, err
		}
		return ev.computeChainRow(proc, start, j, v, valid)

	case kernelMMaxDrawdown:
		_, valid, err := ev.rowFn(j, pos)
		if err != nil {
			return 0, false, err
		}
		// Physical axes: warmup counts partition rows; NULL rows occupy
		// frame slots; the own-row rule emits NULL on a NULL observation
		// even when the frame is complete.
		if !valid || pos < ev.n {
			return 0, false, nil
		}
		frameFirst := j - ev.n + 1
		if frameFirst < start {
			frameFirst = start
		}
		var (
			peak            float64
			hasPeaked       bool
			best            float64
			drawdownDefined bool
		)
		visits := 0
		for k := frameFirst; k <= j; k++ {
			// The frame scan claims one context check per
			// indicatorScanPoll visited frame elements (section 8);
			// checkScanCanceled polls on that 64-element cadence.
			visits++
			if err := checkScanCanceled(proc, visits); err != nil {
				return 0, false, err
			}
			fk, fValid, err := ev.rowFn(k, k-start+1)
			if err != nil {
				return 0, false, err
			}
			if !fValid {
				continue
			}
			if !hasPeaked {
				peak, hasPeaked = fk, true
			} else if fk > peak {
				peak = fk
			}
			// The peak(j) > 0 guard precedes every division: division by
			// zero is unrepresentable for defined ratios.
			if !(peak > 0) {
				continue
			}
			q := fk / peak
			if math.IsInf(q, 0) || math.IsNaN(q) {
				return 0, false, moerr.NewInvalidInputf(proc.Ctx,
					"drawdown ratio is not finite for mmaxdrawdown at row %d", pos)
			}
			dd := 1 - q
			if math.IsInf(dd, 0) || math.IsNaN(dd) {
				return 0, false, moerr.NewInvalidInputf(proc.Ctx,
					"drawdown ratio is not finite for mmaxdrawdown at row %d", pos)
			}
			if !drawdownDefined || dd > best {
				best, drawdownDefined = dd, true
			}
		}
		if !drawdownDefined {
			return 0, false, nil
		}
		return best, true, nil

	default: // kernelCumPositiveStreak: dense output, no warmup NULLs.
		v, valid, err := ev.rowFn(j, pos)
		if err != nil {
			return 0, false, err
		}
		if valid && v > 0 {
			ev.streak++
		} else {
			ev.streak = 0
		}
		return float64(ev.streak), true, nil
	}
}

// advanceTo computes prefix rows [computed, target) idempotently and
// incrementally; target stays inside [0, total].
func (ev *indicatorEvaluator) advanceTo(
	proc *process.Process, target int, ps []int64, total int,
) error {
	for ev.prefix.Length() < target {
		j := ev.prefix.Length()
		if err := checkCanceled(proc, j); err != nil {
			return err
		}
		start := 0
		if ps != nil {
			start, _ = buildPartitionInterval(ps, j, total)
		}
		if j == start {
			ev.resetPartition()
		}
		val, defined, err := ev.computeRow(proc, j, start, j-start+1)
		if err != nil {
			return err
		}
		if ev.kernel == kernelCumPositiveStreak {
			if err := vector.AppendFixed(ev.prefix, int64(val), false, proc.Mp()); err != nil {
				return err
			}
		} else if err := vector.AppendFixed(ev.prefix, val, !defined, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

// newIndicatorEvaluator builds the evaluator of window expression idx. The
// period constant has already been per-row validated for this materialized
// generation by validateIndicatorPeriods.
func (ctr *container) newIndicatorEvaluator(
	idx int, ap *Window, proc *process.Process,
) (*indicatorEvaluator, error) {
	w := ap.WinSpecList[idx].Expr.(*plan.Expr_W).W
	funcName := w.Name
	kernel, ok := indicatorKernelByName[funcName]
	if !ok {
		return nil, moerr.NewInternalErrorf(proc.Ctx, "unknown indicator kernel %s", funcName)
	}
	ev := &indicatorEvaluator{
		kernel:   kernel,
		funcName: funcName,
		retType:  types.MustTypeFromPlan(w.WindowFunc.Typ),
	}
	prefixVec, err := ctr.allocation.NewVector(ev.retType)
	if err != nil {
		return nil, err
	}
	ev.prefix = prefixVec
	srcVec := ctr.aggVecs[idx].Vec[0]
	srcRowFn, err := newIndicatorValueRowFn(funcName, srcVec, proc.Ctx)
	if err != nil {
		ev.freeVectors(proc.Mp())
		return nil, err
	}
	ev.rowFn = srcRowFn
	if kernel != kernelCumPositiveStreak {
		nVal, ok := getInt64FromVec(ctr.aggVecs[idx].Vec[1], 0)
		if !ok || int(nVal) < indicatorMinPeriod || int(nVal) > indicatorMaxPeriod {
			ev.freeVectors(proc.Mp())
			return nil, moerr.NewInvalidInputf(proc.Ctx,
				"period argument of %s must be a constant integer in [1, 1024]", funcName)
		}
		ev.n = int(nVal)
	}
	switch kernel {
	case kernelSMA:
		// Plain mean over the last n values: exact integer denominator n.
		ev.denom = int64(ev.n)
	case kernelWMA:
		// Weight k pairs with the k-th OLDEST value of the block; scan
		// buffers are newest-first, so weight n pairs with buf[0].
		ev.weights = make([]int64, ev.n)
		for i := range ev.weights {
			ev.weights[i] = int64(ev.n) - int64(i)
		}
		ev.denom = int64(ev.n) * int64(ev.n+1) / 2
	case kernelTRIMA:
		// Folded single-pass triangular convolution, equal in exact
		// arithmetic to the documented double-SMA composition.
		ev.weights = make([]int64, ev.n)
		if ev.n%2 == 1 {
			p := int64(ev.n+1) / 2
			for i := range ev.weights { // oldest-order o = n - i
				o := int64(ev.n - i)
				w := o
				if mx := int64(ev.n+1) - o; mx < w {
					w = mx
				}
				ev.weights[i] = w
			}
			ev.denom = p * p
		} else {
			q := int64(ev.n / 2)
			for i := range ev.weights {
				o := int64(ev.n - i)
				w := o
				if q < w {
					w = q
				}
				if mx := int64(ev.n+1) - o; mx < w {
					w = mx
				}
				ev.weights[i] = w
			}
			ev.denom = q * (q + 1)
		}
	case kernelEMA, kernelWILDER:
		ev.aHat, ev.bHat = indicatorAlphaPair(kernel, ev.n)
	case kernelDEMA, kernelTEMA, kernelT3:
		ev.l = indicatorCombinationBound(kernel)
		// Stage feeder rings hold only the seed-count values the next
		// stage consumes (bounded by n, retired at the seed); they replace
		// the full-partition stage prefix vectors of the earlier draft
		// (section 5.2 O(1) member state).
		for s := 0; s < indicatorChainStages(kernel)-1; s++ {
			ev.stageSeed[s] = make([]float64, 0, ev.n)
		}
	}
	return ev, nil
}

// processIndicatorFuncRange advances the per-partition sequence of window
// expression idx up to outputEnd (idempotent, incremental per materialized
// generation) and emits the [outputStart, outputEnd) slice (design 5.2).
func (ctr *container) processIndicatorFuncRange(
	idx int,
	ap *Window,
	proc *process.Process,
	outputStart int,
	outputEnd int,
) (result *vector.Vector, err error) {
	w := ap.WinSpecList[idx].Expr.(*plan.Expr_W).W
	retType := types.MustTypeFromPlan(w.WindowFunc.Typ)
	for len(ctr.indicatorEvals) <= idx {
		ctr.indicatorEvals = append(ctr.indicatorEvals, nil)
	}
	ev := ctr.indicatorEvals[idx]
	if ev == nil {
		ev, err = ctr.newIndicatorEvaluator(idx, ap, proc)
		if err != nil {
			return nil, err
		}
		ctr.indicatorEvals[idx] = ev
	}
	if err = ev.advanceTo(proc, outputEnd, ctr.ps, ctr.bat.RowCount()); err != nil {
		return nil, err
	}
	localResult, err := ctr.allocation.NewVector(retType)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil && localResult != nil {
			localResult.Free(proc.Mp())
			result = nil
		}
	}()
	for j := outputStart; j < outputEnd; j++ {
		if err = checkCanceled(proc, j-outputStart); err != nil {
			return nil, err
		}
		if err = localResult.UnionOne(ev.prefix, int64(j), proc.Mp()); err != nil {
			return nil, err
		}
	}
	return localResult, nil
}

// validateIndicatorPeriods mirrors validateLagLeadOffsets: the period
// argument of every two-argument indicator window expression must evaluate
// to the same integer constant in [1, 1024] on every row of the materialized
// generation (the binder validates constant foldability at bind time; this
// is the executor-side per-row mirroring of that contract).
func (ctr *container) validateIndicatorPeriods(idx int, ap *Window, proc *process.Process) error {
	w := ap.WinSpecList[idx].Expr.(*plan.Expr_W).W
	kernel, ok := indicatorKernelByName[w.Name]
	if !ok || kernel == kernelCumPositiveStreak {
		return nil
	}
	if idx >= len(ctr.aggVecs) || len(ctr.aggVecs[idx].Vec) < 2 {
		return moerr.NewInvalidInputf(proc.Ctx,
			"period argument of %s must be a constant integer in [1, 1024]", w.Name)
	}
	nVec := ctr.aggVecs[idx].Vec[1]
	rows := nVec.Length()
	if nVec.IsConst() && rows > 1 {
		rows = 1
	}
	first, ok := getInt64FromVec(nVec, 0)
	if !ok || first < indicatorMinPeriod || first > indicatorMaxPeriod {
		return moerr.NewInvalidInputf(proc.Ctx,
			"period argument of %s must be a constant integer in [1, 1024]", w.Name)
	}
	for row := 0; row < rows; row++ {
		if err := checkCanceled(proc, row); err != nil {
			return err
		}
		v, ok := getInt64FromVec(nVec, row)
		if !ok || v != first {
			return moerr.NewInvalidInputf(proc.Ctx,
				"period argument of %s must be a constant integer in [1, 1024]", w.Name)
		}
	}
	return nil
}

// freeIndicatorState releases the indicator evaluators of every window
// expression at the three lifecycle reset points (eval entry, Reset, Free).
func (ctr *container) freeIndicatorState(mp *mpool.MPool) {
	for _, ev := range ctr.indicatorEvals {
		if ev != nil {
			ev.freeVectors(mp)
		}
	}
	ctr.indicatorEvals = nil
}
