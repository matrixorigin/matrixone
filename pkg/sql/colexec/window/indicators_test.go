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
	"math/big"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ---- helpers ------------------------------------------------------------

func indSpec(name string, ret types.T) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_W{W: &plan.WindowSpec{
			Name:       name,
			WindowFunc: &plan.Expr{Typ: plan.Type{Id: int32(ret)}},
		}},
	}
}

// runIndicator drives one window expression over a fixed fixture through the
// executor path (period validation + chunked advancing + emission). The
// returned slices contain the kernel values and NULL bits in row order.
func runIndicator(
	t *testing.T,
	name string,
	period int64,
	vals []float64,
	nulls []bool,
	partitionStarts []int64,
	chunkSize int,
) ([]float64, []bool, error) {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	retType := types.T_float64
	if name == "cumpositivestreak" {
		retType = types.T_int64
	}
	xVec := indMakeFloat64Vector(t, proc, vals, nulls)
	defer xVec.Free(proc.Mp())
	return runIndicatorOnWindow(t, proc, name, period, xVec, vals, partitionStarts, chunkSize, retType)
}

// runIndicatorOnWindow binds the spec against a fresh Window evaluator and
// evaluates it in chunks of chunkSize (0 = single pass).
func runIndicatorOnWindow(
	t *testing.T,
	proc *process.Process,
	name string,
	period int64,
	xVec *vector.Vector,
	vals []float64,
	partitionStarts []int64,
	chunkSize int,
	retType types.T,
) ([]float64, []bool, error) {
	t.Helper()
	rows := len(vals)
	arg := &Window{WinSpecList: []*plan.Expr{indSpec(name, retType)}}
	if err := arg.Prepare(proc); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer arg.ctr.freeIndicatorState(proc.Mp())
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = xVec
	bat.SetRowCount(rows)
	arg.ctr.bat = bat
	arg.ctr.ps = partitionStarts
	argVec := colexec.ExprEvalVector{}
	if name != "cumpositivestreak" {
		per := make([]int64, rows)
		for i := range per {
			per[i] = period
		}
		nVec := indMakeInt64Vector(t, proc, per, nil)
		defer nVec.Free(proc.Mp())
		argVec.Vec = []*vector.Vector{xVec, nVec}
	} else {
		argVec.Vec = []*vector.Vector{xVec}
	}
	arg.ctr.aggVecs = []colexec.ExprEvalVector{argVec}
	if err := arg.ctr.validateIndicatorPeriods(0, arg, proc); err != nil {
		return nil, nil, err
	}
	if chunkSize <= 0 {
		chunkSize = rows
	}
	var out []float64
	var outNulls []bool
	for start := 0; start < rows; start += chunkSize {
		end := start + chunkSize
		if end > rows {
			end = rows
		}
		vec, err := arg.ctr.processIndicatorFuncRange(0, arg, proc, start, end)
		if err != nil {
			return nil, nil, err
		}
		if name == "cumpositivestreak" {
			col64 := vector.MustFixedColNoTypeCheck[int64](vec)
			for row := 0; row < vec.Length(); row++ {
				out = append(out, float64(col64[row]))
				outNulls = append(outNulls, vec.IsNull(uint64(row)))
			}
		} else {
			col := vector.MustFixedColNoTypeCheck[float64](vec)
			for row := 0; row < vec.Length(); row++ {
				out = append(out, col[row])
				outNulls = append(outNulls, vec.IsNull(uint64(row)))
			}
		}
		vec.Free(proc.Mp())
	}
	return out, outNulls, nil
}

func assertSeq(
	t *testing.T,
	label string,
	vals []float64,
	want []float64,
	nulls []bool,
	wantNulls []bool,
) {
	t.Helper()
	if len(vals) != len(want) || len(nulls) != len(wantNulls) {
		t.Fatalf("%s: length mismatch got vals=%d nulls=%d want=%d/%d",
			label, len(vals), len(nulls), len(want), len(wantNulls))
	}
	for i := range want {
		if wantNulls[i] {
			if !nulls[i] {
				t.Fatalf("%s row %d: want NULL got %v", label, i+1, vals[i])
			}
			continue
		}
		if nulls[i] {
			t.Fatalf("%s row %d: want %v got NULL", label, i+1, want[i])
		}
		if vals[i] != want[i] && !(math.IsNaN(want[i]) && math.IsNaN(vals[i])) {
			t.Fatalf("%s row %d: want %v got %v", label, i+1, want[i], vals[i])
		}
	}
}

// assertSeqSlack asserts the NULL-bit matrix exactly and the values within a
// per-row slack (the documented accuracy contracts composed over the rows).
func assertSeqSlack(
	t *testing.T,
	label string,
	vals []float64,
	want []float64,
	nulls []bool,
	wantNulls []bool,
	scale float64,
) {
	t.Helper()
	if len(vals) != len(want) || len(nulls) != len(wantNulls) {
		t.Fatalf("%s: length mismatch got vals=%d nulls=%d want=%d/%d",
			label, len(vals), len(nulls), len(want), len(wantNulls))
	}
	for i := range want {
		if wantNulls[i] != nulls[i] {
			t.Fatalf("%s row %d: NULL bit want %v got %v", label, i+1, wantNulls[i], nulls[i])
		}
		if wantNulls[i] {
			continue
		}
		tol := scale * math.Max(math.Abs(want[i]), 1e-300)
		if math.Abs(vals[i]-want[i]) > tol && !(math.IsNaN(want[i]) && math.IsNaN(vals[i])) {
			t.Fatalf("%s row %d: want %v got %v (slack %g)", label, i+1, want[i], vals[i], tol)
		}
	}
}

func indMakeFloat64Vector(tb testing.TB, proc *process.Process, vals []float64, nulls []bool) *vector.Vector {
	vec := vector.NewVec(types.T_float64.ToType())
	for i := range vals {
		isNull := nulls != nil && nulls[i]
		if err := vector.AppendFixed(vec, vals[i], isNull, proc.Mp()); err != nil {
			tb.Fatalf("make float vector: %v", err)
		}
	}
	return vec
}

func indMakeInt64Vector(tb testing.TB, proc *process.Process, vals []int64, nulls []bool) *vector.Vector {
	vec := vector.NewVec(types.T_int64.ToType())
	for i := range vals {
		isNull := nulls != nil && nulls[i]
		if err := vector.AppendFixed(vec, vals[i], isNull, proc.Mp()); err != nil {
			tb.Fatalf("make int vector: %v", err)
		}
	}
	return vec
}

func makeF64Slice(v float64, n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func ratF(v float64) *big.Rat { return new(big.Rat).SetFloat64(v) }

func ratOfPow2(e int) *big.Rat {
	num, den := big.NewInt(1), big.NewInt(1)
	if e >= 0 {
		num.Lsh(num, uint(e))
	} else {
		den.Lsh(den, uint(-e))
	}
	return new(big.Rat).SetFrac(num, den)
}

// ---- independent naive references (no shared code with the kernels) ------

// refMean is a plain floating mean of a window (exact on dyadic fixtures).
func refMean(win []float64) float64 {
	s := 0.0
	for _, v := range win {
		s += v
	}
	return s / float64(len(win))
}

// refMeanExact is the correctly rounded mean (exact rational accumulation,
// one division, one rounding): procedure M's pinned behavior on the
// representable class. The recurrence seeds use it so reference and kernel
// agree bit-for-bit where the design pins seeded values.
func refMeanExact(win []float64) float64 {
	exact := new(big.Rat)
	for _, v := range win {
		exact.Add(exact, ratF(v))
	}
	exact.Quo(exact, new(big.Rat).SetInt64(int64(len(win))))
	f, _ := exact.Float64()
	return f
}

// refMovingMean is a trailing plain moving mean of period p.
func refMovingMean(xs []float64, p int) []float64 {
	var out []float64
	for i := p - 1; i < len(xs); i++ {
		out = append(out, refMean(xs[i-p+1:i+1]))
	}
	return out
}

// kamaUpdate mirrors the Turtle smoothing step in plain float arithmetic; the
// staged constants fold exactly like the runtime expression.
func kamaUpdate(er, u, y float64) (float64, float64) {
	const (
		fastTerm = 2.0 / 3.0
		slowTerm = 2.0 / 31.0
	)
	scBase := mulRounded(er, fastTerm-slowTerm) + slowTerm
	sc := scBase * scBase
	next := mulRounded(sc, u) + mulRounded(1-sc, y)
	return next, next
}

// refDrawdown mirrors the design's mmaxDrawdown closed form in plain float:
// physical frame, NULL rows skip peaks but occupy slots, peak>0 before every
// division, own-row NULL wins, max over the frame's defined dd(j).
func refDrawdown(vals []float64, nulls []bool, n int) ([]float64, []bool) {
	rows := len(vals)
	outV := make([]float64, rows)
	outN := make([]bool, rows)
	for i := 0; i < rows; i++ {
		pos := i + 1
		if (nulls != nil && nulls[i]) || pos < n {
			outN[i] = true
			continue
		}
		frameFirst := i - n + 1
		if frameFirst < 0 {
			frameFirst = 0
		}
		peak := math.NaN()
		hasPeak := false
		best := math.NaN()
		hasBest := false
		for k := frameFirst; k <= i; k++ {
			if nulls != nil && nulls[k] {
				continue
			}
			fk := vals[k]
			if !hasPeak || fk > peak {
				peak = fk
				hasPeak = true
			}
			if !(peak > 0) {
				continue
			}
			dd := 1 - fk/peak
			if !hasBest || dd > best {
				best, hasBest = dd, true
			}
		}
		if !hasBest {
			outN[i] = true
			continue
		}
		outV[i] = best
	}
	return outV, outN
}

// refIndicatorFloat computes each kernel's expected value per row with plain
// float arithmetic over the non-NULL stream (value axis per profile). The
// recurrence coefficient pair mirrors procedure U's input convention: a_hat =
// fl(a) and b_hat = fl(1-a) of the TRUE rationals (2/(n+1), (n-1)/(n+1) for
// the ema family, 1/n, (n-1)/n for wilder), and each update is the two
// rounded products and one rounded add, fl(fl(a*u) + fl(b*y)) - the value
// procedure U produces whenever its hull clamp is a no-op.
func refIndicatorFloat(name string, n int, vals []float64, nulls []bool) ([]float64, []bool) {
	rows := len(vals)
	outV := make([]float64, rows)
	outN := make([]bool, rows)
	switch name {
	case "cumpositivestreak":
		streak := 0
		for i, v := range vals {
			if nulls == nil || !nulls[i] {
				if v > 0 {
					streak++
				} else {
					streak = 0
				}
			} else {
				streak = 0
			}
			outV[i] = float64(streak)
		}
		return outV, outN
	case "mmaxdrawdown":
		return refDrawdown(vals, nulls, n)
	}
	m := 0
	var state float64
	var chains [6]float64
	var stageCnt [6]int
	var seedBuf [6][]float64
	stages := 1
	switch name {
	case "dema":
		stages = 2
	case "tema":
		stages = 3
	case "t3":
		stages = 6
	}
	for i := range vals {
		if nulls != nil && nulls[i] {
			outN[i] = true
			continue
		}
		m++
		if name == "kama" {
			if m <= n {
				if m == n {
					state = vals[i]
				}
				outN[i] = true
				continue
			}
		} else if name == "dema" || name == "tema" || name == "t3" {
			// warmup is defined by the cascade below
		} else if m < n {
			outN[i] = true
			continue
		}
		// window of the last n non-NULL values, oldest first
		var win []float64
		if name != "dema" && name != "tema" && name != "t3" && m >= n {
			cnt := 0
			for k := i; k >= 0 && cnt < n; k-- {
				if nulls == nil || !nulls[k] {
					win = append([]float64{vals[k]}, win...)
					cnt++
				}
			}
		}
		switch name {
		case "sma":
			outV[i] = refMean(win)
		case "wma":
			s := 0.0
			for k := range win {
				s += mulRounded(win[k], float64(k+1)) // oldest gets weight 1
			}
			outV[i] = s / float64(n*(n+1)/2)
		case "trima":
			s1 := append([]float64(nil), win...)
			var firstPeriod int
			if n%2 == 1 {
				firstPeriod = (n + 1) / 2
			} else {
				firstPeriod = n / 2
			}
			e1 := refMovingMean(s1, firstPeriod)
			var e2 []float64
			if n%2 == 1 {
				e2 = refMovingMean(e1, firstPeriod)
			} else {
				e2 = refMovingMean(e1, firstPeriod+1)
			}
			if len(e2) == 0 {
				outN[i] = true
			} else {
				outV[i] = e2[len(e2)-1]
			}
		case "ema", "wilder":
			aHat := 2.0 / float64(n+1)
			bHat := float64(n-1) / float64(n+1)
			if name == "wilder" {
				aHat = 1.0 / float64(n)
				bHat = float64(n-1) / float64(n)
			}
			if m == n {
				state = refMeanExact(win)
			} else {
				p := mulRounded(aHat, vals[i])
				q := mulRounded(bHat, state)
				state = p + q
			}
			outV[i] = state
		case "kama":
			// window of the last n+1 non-NULL values, oldest first
			var w []float64
			cnt := 0
			for k := i; k >= 0 && cnt <= n; k-- {
				if nulls == nil || !nulls[k] {
					w = append([]float64{vals[k]}, w...)
					cnt++
				}
			}
			c := math.Abs(w[0] - w[n])
			var vol float64
			for k := 0; k < n; k++ {
				vol += math.Abs(w[k] - w[k+1])
			}
			var er float64
			if vol != 0 {
				er = c / vol
			}
			outV[i], state = kamaUpdate(er, vals[i], state)
		case "dema", "tema", "t3":
			aHat := 2.0 / float64(n+1)
			bHat := float64(n-1) / float64(n+1)
			feeder := vals[i]
			defined := true
			for s := 0; s < stages; s++ {
				if !defined {
					break
				}
				stageCnt[s]++
				if stageCnt[s] < n {
					seedBuf[s] = append(seedBuf[s], feeder)
					defined = false
					break
				}
				if stageCnt[s] == n {
					seedBuf[s] = append(seedBuf[s], feeder)
					chains[s] = refMeanExact(seedBuf[s])
				} else {
					p := mulRounded(aHat, feeder)
					q := mulRounded(bHat, chains[s])
					chains[s] = p + q
				}
				feeder = chains[s]
			}
			if !defined {
				outN[i] = true
				continue
			}
			switch name {
			case "dema":
				outV[i] = mulRounded(2, chains[0]) - chains[1]
			case "tema":
				outV[i] = (mulRounded(3, chains[0]) - mulRounded(3, chains[1])) + chains[2]
			default:
				const v = 0.7
				c3 := (1 + v) * (1 + v) * (1 + v)
				c4 := -3 * v * (1 + v) * (1 + v)
				c5 := 3 * v * v * (1 + v)
				c6 := -(v * v * v)
				outV[i] = ((mulRounded(c3, chains[2]) + mulRounded(c4, chains[3])) + mulRounded(c5, chains[4])) + mulRounded(c6, chains[5])
			}
		}
	}
	return outV, outN
}

// ---- procedure-M and procedure-U direct pins --------------------------------

func TestIndicatorProcedureMExactnessPins(t *testing.T) {
	MF := math.MaxFloat64
	h := math.Ldexp(1.0, -1074)
	requireValues := map[string]struct {
		got  float64
		want float64
		plus bool // result must be nonnegative zero or positive
		neg  bool // result must be a negative zero
	}{
		"11xMAXFLOAT -> MAXFLOAT":                {procedureM(makeF64Slice(MF, 11), nil, 11), MF, false, false},
		"3xmin-subnormal -> h":                   {procedureM([]float64{h, h, h}, nil, 3), h, false, false},
		"2x1e308 -> 1e308":                       {procedureM([]float64{1e308, 1e308}, nil, 2), 1e308, false, false},
		"MAXFLOAT,-MAXFLOAT -> 0":                {procedureM([]float64{MF, -MF}, nil, 2), 0, true, false},
		"[h,h] -> h":                             {procedureM([]float64{h, h}, nil, 2), h, false, false},
		"[h,2h] -> 2h (even tie)":                {procedureM([]float64{h, 2 * h}, nil, 2), 2 * h, false, false},
		"[0,3h] -> 2h":                           {procedureM([]float64{0, 3 * h}, nil, 2), 2 * h, false, false},
		"[h,-0] -> +0":                           {procedureM([]float64{h, math.Copysign(0, -1)}, nil, 2), 0, true, false},
		"[-0,-0] -> -0":                          {procedureM([]float64{math.Copysign(0, -1), math.Copysign(0, -1)}, nil, 2), 0, false, true},
		"gap [0,h] -> 0":                         {procedureM([]float64{0, h}, nil, 2), 0, true, false},
		"wma [[h,0] w=[2,1] newest-first] -> h":  {procedureM([]float64{h, 0}, []int64{2, 1}, 3), h, false, false},
		"wma gap [[0,h] w=[1(old),2(new)]] -> h": {procedureM([]float64{h, 0}, []int64{2, 1}, 3), h, false, false},
		"wma [[h,h] w=[1,3] -> h]":               {procedureM([]float64{h, h}, []int64{3, 1}, 4), h, false, false},
	}
	for label, tc := range requireValues {
		if tc.got != tc.want {
			t.Fatalf("%s: got %v want %v", label, tc.got, tc.want)
		}
		if tc.plus && math.Signbit(tc.got) {
			t.Fatalf("%s: want nonnegative zero", label)
		}
		if tc.neg && !math.Signbit(tc.got) {
			t.Fatalf("%s: want -0 sign", label)
		}
	}
	// Zero-window rule: all-+0 -> +0, all--0 -> -0 (procedure-M signed-zero).
	if got := procedureM(makeF64Slice(0, 8), nil, 8); got != 0 || math.Signbit(got) {
		t.Fatalf("all +0 window: got %v want +0", got)
	}
	if got := procedureM(makeF64Slice(math.Copysign(0, -1), 8), nil, 8); got != 0 || !math.Signbit(got) {
		t.Fatalf("all -0 window: got %v want -0", got)
	}
	// floor-augmented contract: the gap vector's exact mean is 2^-1075, half
	// the subnormal grid; the correctly rounded result 0 sits exactly at the
	// 2^-1075 floor allowance (r5.review finding 11 pin).
	gapExact := ratOfPow2(-1075)
	got := procedureM([]float64{0, h}, nil, 2)
	errAbs := new(big.Rat).Sub(gapExact, ratF(got))
	errAbs.Abs(errAbs)
	if errAbs.Cmp(ratOfPow2(-1075)) != 0 {
		t.Fatalf("gap pin: |result - exact| = %v, want exactly 2^-1075", errAbs)
	}
}

// TestIndicatorProcedureMOracleFuzz runs fixed-seed vectors against a
// math/big oracle and asserts the tiered accuracy contract (plain
// 2^-40*T + 2^-1075; weighted 2^-51*sum w|u| + 2^-40*T + 2^-1075), the
// floor-augmented r6 tier, including n = 1024 profile extremes and
// bottom-seeded subnormal windows.
func TestIndicatorProcedureMOracleFuzz(t *testing.T) {
	rng := rand.New(rand.NewSource(20261024))
	h := math.Ldexp(1.0, -1074)
	classes := []func(*rand.Rand) float64{
		// general doubles
		func(r *rand.Rand) float64 { return (r.Float64() - 0.5) * math.Ldexp(1, r.Intn(200)-100) },
		// large magnitudes near the top
		func(r *rand.Rand) float64 { return math.MaxFloat64 / (1 + r.Float64()*3) * (r.Float64() - 0.5) * 2 },
		// small integers
		func(r *rand.Rand) float64 { return float64(r.Intn(201) - 100) },
		// subnormal grid multiples
		func(r *rand.Rand) float64 { return float64(r.Intn(4097)-2048) * h },
		// exact halving grid
		func(r *rand.Rand) float64 { return math.Ldexp(float64(r.Intn(1<<20))-(1<<19), r.Intn(100)-50) },
	}
	for v := 0; v < 384; v++ {
		k := rng.Intn(len(classes))
		n := 2 + rng.Intn(1023)
		weighted := v%2 == 0 && n <= 64
		var terms []float64
		var weights []int64
		denominator := int64(n)
		weightedSum := new(big.Rat)
		exact := new(big.Rat)
		T := 0.0
		for i := 0; i < n; i++ {
			u := classes[k](rng)
			terms = append(terms, u)
			if abs := math.Abs(u); abs > T {
				T = abs
			}
			ratU := ratF(u)
			if weighted {
				w := int64(1 + rng.Intn(1024))
				weights = append(weights, w)
				exact.Add(exact, new(big.Rat).Mul(ratU, new(big.Rat).SetInt64(w)))
				absU := new(big.Rat).Abs(ratU)
				absU.Mul(absU, new(big.Rat).SetInt64(w))
				weightedSum.Add(weightedSum, absU)
			} else {
				exact.Add(exact, ratU)
			}
		}
		if weighted {
			denominator = 0
			for _, w := range weights {
				denominator += w
			}
		}
		got := procedureM(append([]float64(nil), terms...), weights, denominator)
		if math.IsInf(got, 0) || math.IsNaN(got) {
			t.Fatalf("vector %d: non-finite mean %v", v, got)
		}
		exact.Quo(exact, new(big.Rat).SetInt64(denominator))
		bound := new(big.Rat).Add(
			new(big.Rat).Mul(ratOfPow2(-40), ratF(T)),
			ratOfPow2(-1075))
		if weighted {
			bound.Add(bound, new(big.Rat).Mul(ratOfPow2(-51), weightedSum))
		}
		errR := new(big.Rat).Sub(ratF(got), exact)
		errR.Abs(errR)
		if errR.Cmp(bound) > 0 {
			t.Fatalf("vector %d (weighted=%v n=%d): |err|=%v bound=%v", v, weighted, n, errR, bound)
		}
	}
	// bottom-seeded subnormal vectors: half-grid-mean targeting and
	// deep-subnormal clusters against the floor-augmented tier.
	for v := 0; v < 128; v++ {
		n := 2 + rng.Intn(63)
		var terms []float64
		exact := new(big.Rat)
		T := 0.0
		for i := 0; i < n; i++ {
			u := float64(rng.Intn(3)-1) * h
			terms = append(terms, u)
			if abs := math.Abs(u); abs > T {
				T = abs
			}
			exact.Add(exact, ratF(u))
		}
		exact.Quo(exact, big.NewRat(int64(n), 1))
		got := procedureM(terms, nil, int64(n))
		bound := new(big.Rat).Add(
			new(big.Rat).Mul(ratOfPow2(-40), ratF(T)),
			ratOfPow2(-1075))
		errR := new(big.Rat).Sub(ratF(got), exact)
		errR.Abs(errR)
		if errR.Cmp(bound) > 0 {
			t.Fatalf("bottom-seed vector %d: |err|=%v bound=%v", v, errR, bound)
		}
	}
}

// TestIndicatorProcedureUAccuracyContract pins the r7 corrected accuracy
// contract on the reviewer's reachable falsifier and keeps the falsified r6
// constant as the must-fail harness.
func TestIndicatorProcedureUAccuracyContract(t *testing.T) {
	seed := procedureM(makeF64Slice(1.12, 12), nil, 12)
	if seed != 1.12 {
		t.Fatalf("seed = %v want 1.12", seed)
	}
	aHat, bHat := 1.0/12.0, 11.0/12.0
	got := procedureU(aHat, bHat, 0.86, seed)
	if got != 1.0983333333333332 {
		t.Fatalf("row 13 = %v want 1.0983333333333332", got)
	}
	// exact-rational oracle over the true coefficient
	exact := new(big.Rat).SetFloat64(0.86)
	exact.Mul(exact, big.NewRat(1, 12))
	tmp := new(big.Rat).SetFloat64(1.12)
	tmp.Mul(tmp, big.NewRat(11, 12))
	exact.Add(exact, tmp)
	errR := new(big.Rat).Sub(ratF(got), exact)
	errR.Abs(errR)
	corrected := new(big.Rat).Add(
		new(big.Rat).Mul(ratOfPow2(-51), ratF(1.12)),
		ratOfPow2(-1074))
	if errR.Cmp(corrected) > 0 {
		t.Fatalf("corrected bound violated: err=%v bound=%v", errR, corrected)
	}
	// must-fail harness: the falsified r6 constant (2^-52) fails here
	oldBound := new(big.Rat).Add(
		new(big.Rat).Mul(ratOfPow2(-52), ratF(1.12)),
		ratOfPow2(-1074))
	if errR.Cmp(oldBound) <= 0 {
		t.Fatalf("must-fail harness broken: r6 old bound unexpectedly holds (err=%v)", errR)
	}
	// hull and constant-carry pins at the domain floor
	h := math.Ldexp(1.0, -1074)
	if u := procedureU(0.5, 0.5, h, h); u != h {
		t.Fatalf("convex update dropped constant h: %v", u)
	}
	if u := procedureU(0.5, 0.5, 3*h, 3*h); u != 3*h {
		t.Fatalf("convex update dropped constant 3h: %v", u)
	}
	// the direct per-row update used before r6 collapses the constant-h
	// stream to 0: the harness must keep failing it
	if p, q := 0.5*h, 0.5*h; p+q != 0 {
		t.Fatalf("harness premise broken: fl(0.5*h)+fl(0.5*h) != 0")
	}
	// hull containment always holds on finite inputs
	for _, tc := range [][4]float64{
		{0.5, 0.5, h, -h},
		{0.75, 0.25, 1e308, -1e308},
		{2.0 / 3.0, 1.0 / 3.0, -5, 3},
	} {
		u := procedureU(tc[0], tc[1], tc[2], tc[3])
		lo, hi := tc[2], tc[3]
		if lo > hi {
			lo, hi = hi, lo
		}
		if u < lo || u > hi {
			t.Fatalf("hull violated: a=%v u=%v y=%v out=%v", tc[0], tc[2], tc[3], u)
		}
		if math.IsInf(u, 0) || math.IsNaN(u) {
			t.Fatalf("non-finite on finite inputs: %v", u)
		}
	}
}

// TestIndicatorProcedureUSubnormalSweep pins the a = 0.5 subnormal-grid family:
// every odd constant X = (2k+1)h >= 3h carries exactly under procedure U and
// fails under the withdrawn direct-update mechanism (r5-review finding 12).
func TestIndicatorProcedureUSubnormalSweep(t *testing.T) {
	h := math.Ldexp(1.0, -1074)
	oldFailures := 0
	for k := 1; k <= 4096; k++ {
		x := float64(2*k+1) * h
		out := procedureU(0.5, 0.5, x, x)
		if out != x {
			t.Fatalf("odd constant %dh: U returned %v", 2*k+1, out)
		}
		// the withdrawn mechanism: fl(0.5x) + fl(0.5y) = 2*floor(x/2) = x - h
		p, q := 0.5*x, 0.5*x
		if p+q == x {
			t.Fatalf("sweep k=%d: old mechanism unexpectedly carried %dh", k, 2*k+1)
		}
		oldFailures++
	}
	if oldFailures == 0 {
		t.Fatal("sweep found no old-mechanism failures")
	}
}

// uCoefficients enumerates the four reachable alpha families (wilder 1/n,
// ema 2/(n+1), KAMA Rational sc values, half-family a = 1/2), each with
// a_hat = fl(a) and b_hat = fl(1-a) taken from the TRUE rational complement
// exactly - the r7 scan-harness convention (b_hat = fl(1-a_hat) is a
// trace-level deviation the pins do not certify).
type uCoefficient struct {
	exact *big.Rat
	bEx   *big.Rat
	aHat  float64
	bHat  float64
}

func newUCoeff(num, den int64) uCoefficient {
	exact := big.NewRat(num, den)
	bEx := new(big.Rat).Sub(new(big.Rat).SetInt64(1), exact)
	aHat, _ := exact.Float64()
	bHat, _ := bEx.Float64()
	return uCoefficient{exact: exact, bEx: bEx, aHat: aHat, bHat: bHat}
}

func uReachableCoefficients() []uCoefficient {
	var out []uCoefficient
	// wilder 1/n across the reachable period domain
	for n := 2; n <= 1024; n += 11 {
		out = append(out, newUCoeff(1, int64(n)))
	}
	// ema 2/(n+1) across the reachable period domain
	for n := 1; n <= 1023; n += 11 {
		out = append(out, newUCoeff(2, int64(n+1)))
	}
	// KAMA Rational sc values over the er grid k/64
	for k := int64(0); k <= 64; k++ {
		scBase := big.NewRat(5*k+32, 496)
		sc := new(big.Rat).Mul(scBase, scBase)
		exact := new(big.Rat).Set(sc)
		bEx := new(big.Rat).Sub(new(big.Rat).SetInt64(1), exact)
		aHat, _ := exact.Float64()
		bHat, _ := bEx.Float64()
		out = append(out, uCoefficient{exact: exact, bEx: bEx, aHat: aHat, bHat: bHat})
	}
	out = append(out, newUCoeff(1, 2)) // half family: ema(3), wilder(2)
	return out
}

// oracle returns |procedureU(aHat,bHat,u,y) - (a*u + (1-a)*y)| as an exact
// rational together with the produced value.
func (c uCoefficient) oracle(u, y float64) (*big.Rat, float64) {
	got := procedureU(c.aHat, c.bHat, u, y)
	exact := new(big.Rat).Add(
		new(big.Rat).Mul(c.exact, ratF(u)),
		new(big.Rat).Mul(c.bEx, ratF(y)))
	errR := new(big.Rat).Sub(ratF(got), exact)
	errR.Abs(errR)
	return errR, got
}

func uCorrectedBound(maxU float64) *big.Rat {
	return new(big.Rat).Add(
		new(big.Rat).Mul(ratOfPow2(-51), ratF(maxU)),
		ratOfPow2(-1074))
}

func uOldBound(maxU float64) *big.Rat {
	return new(big.Rat).Add(
		new(big.Rat).Mul(ratOfPow2(-52), ratF(maxU)),
		ratOfPow2(-1074))
}

// TestIndicatorProcedureUBinadeScan concentrates same-binade grid pairs over
// every reachable coefficient family and asserts the corrected 2^-51
// contract everywhere, with the old r6 2^-52 constant as the must-fail
// harness and the pinned worst vector at its exact error.
func TestIndicatorProcedureUBinadeScan(t *testing.T) {
	for _, c := range uReachableCoefficients() {
		for iu := 0; iu < 17; iu++ {
			for iy := 0; iy < 17; iy++ {
				u := 1 + float64(iu)*2/32
				y := 1 + float64(iy)*2/32
				errR, got := c.oracle(u, y)
				maxU := math.Max(u, y)
				if errR.Cmp(uCorrectedBound(maxU)) > 0 {
					t.Fatalf("corrected bound violated: a=%v u=%v y=%v err=%v got=%v",
						c.aHat, u, y, errR, got)
				}
			}
		}
	}
	// pinned worst vector of the design scan (a = 1/63 wilder, u = 1.015625,
	// y = 1.0625): error 2.749123680024197e-16, corrected bound holds and
	// the old 2^-52 constant fails.
	c := newUCoeff(1, 63)
	errR, _ := c.oracle(1.015625, 1.0625)
	wantWorst := new(big.Rat).SetFloat64(2.749123680024197e-16)
	delta := new(big.Rat).Sub(errR, wantWorst)
	delta.Abs(delta)
	if delta.Cmp(new(big.Rat).SetFloat64(1e-32)) > 0 {
		t.Fatalf("pinned worst vector error = %v want 2.749123680024197e-16", errR)
	}
	if errR.Cmp(uCorrectedBound(1.0625)) > 0 {
		t.Fatal("pinned worst vector violates the corrected bound")
	}
	if errR.Cmp(uOldBound(1.0625)) <= 0 {
		t.Fatal("pinned worst vector must fail the old 2^-52 bound")
	}
}

// TestIndicatorProcedureUOracleFuzz is the nonconstant 100,000-vector fuzz
// across the four alpha families and the subnormal/general/MAXFLOAT value
// classes: hull containment, finiteness and the corrected U accuracy bound
// against exact-rational oracles.
func TestIndicatorProcedureUOracleFuzz(t *testing.T) {
	rng := rand.New(rand.NewSource(29949))
	h := math.Ldexp(1.0, -1074)
	coeffs := uReachableCoefficients()
	classes := []func(*rand.Rand) float64{
		func(r *rand.Rand) float64 {
			k := int64(r.Intn(1<<16) - (1 << 15))
			return float64(k) * h
		},
		func(r *rand.Rand) float64 { return float64(r.Intn(2001) - 1000) },
		func(r *rand.Rand) float64 { return (r.Float64() - 0.5) * math.Ldexp(1, r.Intn(40)-20) },
		func(r *rand.Rand) float64 { return math.MaxFloat64 / (1 + r.Float64()*4) * (r.Float64() - 0.5) * 2 },
	}
	for v := 0; v < 100000; v++ {
		c := coeffs[v%len(coeffs)]
		u := classes[v%len(classes)](rng)
		y := classes[(v+1)%len(classes)](rng)
		errR, got := c.oracle(u, y)
		if math.IsInf(got, 0) || math.IsNaN(got) {
			t.Fatalf("vector %d: non-finite %v", v, got)
		}
		lo, hi := u, y
		if lo > hi {
			lo, hi = hi, lo
		}
		if got < lo || got > hi {
			t.Fatalf("vector %d: hull violated", v)
		}
		if errR.Cmp(uCorrectedBound(math.Max(math.Abs(u), math.Abs(y)))) > 0 {
			t.Fatalf("vector %d: corrected bound violated a=%v u=%v y=%v err=%v",
				v, c.aHat, u, y, errR)
		}
	}
}

// ---- kernel profiles through the executor path -------------------------------

func TestIndicatorMeanKernelProfiles(t *testing.T) {
	fixtures := [][]float64{
		{1, 2, 3, 4, 5, 6, 7},
		{7, 6, 5, 4, 3, 2, 1},
	}
	cases := []struct {
		name string
		n    int64
	}{
		{"sma", 1}, {"sma", 3}, {"wma", 1}, {"wma", 3}, {"trima", 1}, {"trima", 3}, {"trima", 4},
		{"ema", 1}, {"ema", 3}, {"wilder", 1}, {"wilder", 4},
		{"kama", 1}, {"kama", 2},
		{"dema", 1}, {"dema", 2}, {"dema", 3}, {"tema", 1}, {"tema", 2}, {"t3", 1}, {"t3", 2},
	}
	for fi, vals := range fixtures {
		for _, tc := range cases {
			gotV, gotN, err := runIndicator(t, tc.name, tc.n, vals, nil, nil, 2)
			if err != nil {
				t.Fatalf("fixture %d %s(%d): %v", fi, tc.name, tc.n, err)
			}
			wantV, wantN := refIndicatorFloat(tc.name, int(tc.n), vals, nil)
			assertSeq(t, tc.name, gotV, wantV, gotN, wantN)
		}
	}
	// NULL skipping, warmup NULL bits and value-axis partition semantics.
	nvals := []float64{1, 2, 3, 4, 5}
	nnulls := []bool{false, true, false, false, false}
	gotV, gotN, err := runIndicator(t, "ema", 3, nvals, nnulls, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantV, wantN := refIndicatorFloat("ema", 3, nvals, nnulls)
	assertSeq(t, "ema-with-nulls", gotV, wantV, gotN, wantN)
	if !gotN[0] || !gotN[2] {
		t.Fatal("leading NULL warmup bits wrong")
	}
	for _, tc := range []struct {
		name string
		n    int64
	}{{"dema", 2}, {"kama", 2}, {"sma", 2}} {
		// NULL rows skip kernel consumption but the chain-stage prefixes
		// stay row-aligned: naive references match bit-for-bit on dyadic
		// fixtures.
		vals := []float64{1, 2, 3, 4, 5, 6, 7}
		nulls := []bool{false, false, true, false, true, false, false}
		gotV, gotN, err := runIndicator(t, tc.name, tc.n, vals, nulls, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		wantV, wantN := refIndicatorFloat(tc.name, int(tc.n), vals, nulls)
		assertSeq(t, tc.name+"-nulls", gotV, wantV, gotN, wantN)
	}
	// r7 falsifier as a stream through the kernel: wilder(12) 13 rows.
	svals := append(makeF64Slice(1.12, 12), 0.86)
	gotV, gotN, err = runIndicator(t, "wilder", 12, svals, nil, nil, 5)
	if err != nil {
		t.Fatal(err)
	}
	if gotV[len(gotV)-1] != 1.0983333333333332 || gotN[len(gotN)-1] || len(gotV) != 13 {
		t.Fatalf("wilder(12) row 13 = %v want 1.0983333333333332", gotV[len(gotV)-1])
	}
	wantV, wantN = refIndicatorFloat("wilder", 12, svals, nil)
	assertSeq(t, "wilder12-stream", gotV, wantV, gotN, wantN)
}

func TestIndicatorWindowPartitionReset(t *testing.T) {
	vals := []float64{1, 2, 3, 10, 20, 30}
	gotV, gotN, err := runIndicator(t, "ema", 3, vals, nil, []int64{3}, 2)
	if err != nil {
		t.Fatal(err)
	}
	w1, n1 := refIndicatorFloat("ema", 3, vals[:3], nil)
	w2, n2 := refIndicatorFloat("ema", 3, vals[3:], nil)
	assertSeq(t, "partitioned emit: after the boundary reset", gotV, append(w1, w2...), gotN, append(n1, n2...))
	// three partitions through the chain kernel
	vals3 := []float64{1, 2, 3, 4, 5, 2, 8, 9}
	gotV, gotN, err = runIndicator(t, "dema", 1, vals3, nil, []int64{2, 6}, 3)
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]float64{vals3[:2], vals3[2:6], vals3[6:]}
	var wantV []float64
	var wantN []bool
	for _, p := range parts {
		v, n := refIndicatorFloat("dema", 1, p, nil)
		wantV, wantN = append(wantV, v...), append(wantN, n...)
	}
	assertSeq(t, "three partitions dema", gotV, wantV, gotN, wantN)
	// streak reset per partition
	svals := []float64{1, 1, -2, 1, 1, 1}
	gotV, gotN, err = runIndicator(t, "cumpositivestreak", 0, svals, nil, []int64{3}, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertSeq(t, "streak partitions", gotV, []float64{1, 2, 0, 1, 2, 3}, gotN,
		[]bool{false, false, false, false, false, false})
}

func TestIndicatorStreakAndSignedZero(t *testing.T) {
	vals := []float64{
		math.Copysign(0, -1), math.Copysign(0, 1),
		4, math.NaN(), 2, -3, 1,
	}
	nulls := []bool{false, false, false, true, false, false, false}
	gotV, gotN, err := runIndicator(t, "cumpositivestreak", 0, vals, nulls, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	// NULL row value must not enter the predicate; both zeros exit 0.
	assertSeq(t, "streak with NULL", gotV, []float64{0, 0, 1, 0, 1, 0, 1}, gotN,
		[]bool{false, false, false, false, false, false, false})
}

// TestIndicatorConstSourceSeries pins the literal-series contract through the
// executor path: the plan materializes a constant value argument as a const
// vector holding one physical element, every logical row reads row 0, and
// the kernels produce the constant profile with the regular warmup NULLs; a
// const NULL argument keeps every value-kernel output NULL and drops the
// streak to zero.
func TestIndicatorConstSourceSeries(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	rows := 6
	logical := make([]float64, rows)
	for i := range logical {
		logical[i] = 2
	}
	constVec, err := vector.NewConstFixed[float64](types.T_float64.ToType(), 2, rows, proc.Mp())
	if err != nil {
		t.Fatal(err)
	}
	defer constVec.Free(proc.Mp())

	gotV, gotN, err := runIndicatorOnWindow(t, proc, "sma", 3, constVec, logical, nil, 0, types.T_float64)
	if err != nil {
		t.Fatal(err)
	}
	assertSeq(t, "const sma(2,3)", gotV, []float64{0, 0, 2, 2, 2, 2}, gotN,
		[]bool{true, true, false, false, false, false})

	gotV, gotN, err = runIndicatorOnWindow(t, proc, "ema", 2, constVec, logical, nil, 2, types.T_float64)
	if err != nil {
		t.Fatal(err)
	}
	// the m = n seed is the exact mean [2, 2] -> 2, carried exactly by U.
	assertSeq(t, "const ema(2,2) chunked", gotV, []float64{0, 2, 2, 2, 2, 2}, gotN,
		[]bool{true, false, false, false, false, false})

	gotV, gotN, err = runIndicatorOnWindow(t, proc, "mmaxdrawdown", 3, constVec, logical, nil, 0, types.T_float64)
	if err != nil {
		t.Fatal(err)
	}
	assertSeq(t, "const mmaxdrawdown(2,3)", gotV, []float64{0, 0, 0, 0, 0, 0}, gotN,
		[]bool{true, true, false, false, false, false})

	gotV, gotN, err = runIndicatorOnWindow(t, proc, "cumpositivestreak", 0, constVec, logical, nil, 0, types.T_int64)
	if err != nil {
		t.Fatal(err)
	}
	assertSeq(t, "const streak(2)", gotV, []float64{1, 2, 3, 4, 5, 6}, gotN,
		[]bool{false, false, false, false, false, false})

	nullVec := vector.NewConstNull(types.T_float64.ToType(), rows, proc.Mp())
	defer nullVec.Free(proc.Mp())
	gotV, gotN, err = runIndicatorOnWindow(t, proc, "sma", 3, nullVec, logical, nil, 0, types.T_float64)
	if err != nil {
		t.Fatal(err)
	}
	assertSeq(t, "const-null sma", gotV, []float64{0, 0, 0, 0, 0, 0}, gotN,
		[]bool{true, true, true, true, true, true})
	gotV, gotN, err = runIndicatorOnWindow(t, proc, "cumpositivestreak", 0, nullVec, logical, nil, 0, types.T_int64)
	if err != nil {
		t.Fatal(err)
	}
	assertSeq(t, "const-null streak", gotV, []float64{0, 0, 0, 0, 0, 0}, gotN,
		[]bool{false, false, false, false, false, false})
}

func TestIndicatorMMaxDrawdownMatrix(t *testing.T) {
	// the [NULL, NULL, 100, 95] row-4 value is 1 - fl(95/100) exactly as the
	// kernel computes it: two runtime roundings, not the constant-folded
	// once-rounded 0.05 (the shorthand renders differently)
	var ddratio95 float64 = 95.0 / 100.0
	ddratio95 = 1 - ddratio95
	type row struct {
		n      int64
		vals   []float64
		nulls  []bool
		want   []float64
		wnulls []bool
	}
	rows := []row{
		{2, []float64{100, 0}, nil, []float64{math.NaN(), 1.0}, []bool{true, false}},
		{2, []float64{100, -20}, nil, []float64{math.NaN(), 1.2}, []bool{true, false}},
		{2, []float64{1, -10}, nil, []float64{math.NaN(), 11}, []bool{true, false}},
		{2, []float64{0, 0}, nil, []float64{math.NaN(), math.NaN()}, []bool{true, true}},
		{2, []float64{0, -5}, nil, []float64{math.NaN(), math.NaN()}, []bool{true, true}},
		{2, []float64{-10, -20}, nil, []float64{math.NaN(), math.NaN()}, []bool{true, true}},
		{3, []float64{100, 90, 0}, []bool{false, false, true}, []float64{math.NaN(), math.NaN(), math.NaN()}, []bool{true, true, true}},
		{3, []float64{100, 90, 0, 95}, []bool{false, false, true, false}, []float64{math.NaN(), math.NaN(), math.NaN(), 0}, []bool{true, true, true, false}},
		{3, []float64{0, 0, 100, 95}, []bool{true, true, false, false}, []float64{math.NaN(), math.NaN(), 0, ddratio95}, []bool{true, true, false, false}},
		{1, []float64{0, -1, 0, 1}, []bool{true, false, false, false}, []float64{math.NaN(), math.NaN(), math.NaN(), 0}, []bool{true, true, true, false}},
		{2, []float64{1e308, -1e308}, nil, []float64{math.NaN(), 2.0}, []bool{true, false}},
		{2, []float64{1e-308, -1}, nil, []float64{math.NaN(), 1e308}, []bool{true, false}},
		{2, []float64{math.MaxFloat64 / 4, -math.MaxFloat64}, nil, []float64{math.NaN(), 5.0}, []bool{true, false}},
		{2, []float64{1e-308, -1e-308}, nil, []float64{math.NaN(), 2.0}, []bool{true, false}},
		{2, []float64{math.Ldexp(1, -1074), -math.Ldexp(1, -1074)}, nil, []float64{math.NaN(), 2.0}, []bool{true, false}},
	}
	for i, tc := range rows {
		gotV, gotN, err := runIndicator(t, "mmaxdrawdown", tc.n, tc.vals, tc.nulls, nil, 0)
		if err != nil {
			t.Fatalf("row %d: %v", i+1, err)
		}
		assertSeq(t, "drawdown-matrix", gotV, tc.want, gotN, tc.wnulls)
	}
	// frame-entry/exit: peak leaves the frame, so the ratio is recomputed
	// against the frame-local peak (the issue's bounded-frame requirement).
	vals := []float64{100, 120, 90, 110, 80, 70}
	gotV, gotN, err := runIndicator(t, "mmaxdrawdown", 3, vals, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	assertSeq(t, "drawdown-rotation", gotV,
		[]float64{math.NaN(), math.NaN(), 0.25, 0.25, 1 - 80.0/110.0, 1 - 70.0/110.0},
		gotN, []bool{true, true, false, false, false, false})
	// all-NULL partition defines nothing
	gotV, gotN, err = runIndicator(t, "mmaxdrawdown", 2, makeF64Slice(0, 4), []bool{true, true, true, true}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	assertSeq(t, "drawdown all-NULL", gotV, []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		gotN, []bool{true, true, true, true})
}

func TestIndicatorDocumentedErrors(t *testing.T) {
	cases := []struct {
		name  string
		n     int64
		vals  []float64
		nulls []bool
		text  string
	}{
		{"ema", 2, []float64{1, math.Inf(1), 1}, nil,
			"invalid input: input value is not finite for ema at row 2"},
		{"ema", 2, []float64{1, math.NaN(), 1}, nil,
			"invalid input: input value is not finite for ema at row 2"},
		{"mmaxdrawdown", 2, []float64{1e-308, -1e308}, nil,
			"invalid input: drawdown ratio is not finite for mmaxdrawdown at row 2"},
		{"kama", 2, []float64{1e308, -1e308, -1e308}, nil,
			"invalid input: input magnitude exceeds the finite range of kama (period n = 2, bound MAXFLOAT/2)"},
		{"kama", 4, []float64{5e307, -5e307, 5e307, -5e307, 5e307, -5e307, 5e307, -5e307}, nil,
			"invalid input: input magnitude exceeds the finite range of kama (period n = 4, bound MAXFLOAT/2)"},
		{"dema", 2, []float64{math.MaxFloat64, 1, 1}, nil,
			"invalid input: input magnitude exceeds the finite range of dema (period n = 2, bound MAXFLOAT/3)"},
		{"tema", 2, []float64{math.MaxFloat64, 1, 1, 1, 1, 1}, nil,
			"invalid input: input magnitude exceeds the finite range of tema (period n = 2, bound MAXFLOAT/7)"},
		{"t3", 2, []float64{math.MaxFloat64, math.MaxFloat64 / 16, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, nil,
			"invalid input: input magnitude exceeds the finite range of t3 (period n = 2, bound MAXFLOAT/14)"},
		{"ema", 1025, []float64{1, 2}, nil,
			"invalid input: period argument of ema must be a constant integer in [1, 1024]"},
	}
	for i, tc := range cases {
		_, _, err := runIndicator(t, tc.name, tc.n, tc.vals, tc.nulls, nil, 0)
		if err == nil || err.Error() != tc.text {
			t.Fatalf("case %d: error = %v, want %q", i+1, err, tc.text)
		}
	}
	// period 1024 is inside the domain and executes
	if _, _, err := runIndicator(t, "sma", 1024, makeF64Slice(1, 1025), nil, nil, 0); err != nil {
		t.Fatalf("period 1024 rejected: %v", err)
	}
}

// TestIndicatorConstantStreamsAndLifeCycle pins the procedure-U constant
// streams: subnormal and MAXFLOAT-class constants carried exactly through
// every defined row (per-kernel first-defined position), with the
// design-approval advisory correction that ema(5) seeds at 1 ulp below
// MAXFLOAT and wilder(3) seeds at MAXFLOAT itself.
func TestIndicatorConstantStreamsAndLifeCycle(t *testing.T) {
	MF := math.MaxFloat64
	h := math.Ldexp(1.0, -1074)
	esSeed := math.Float64frombits(math.Float64bits(MF) - 1)
	// stream-level MAXFLOAT cases: ema(5) carries the seed; wilder(3) the top.
	gotV, gotN, err := runIndicator(t, "ema", 5, makeF64Slice(MF, 8), nil, nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	assertSeq(t, "ema5-MAXFLOAT", gotV,
		[]float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), esSeed, esSeed, esSeed, esSeed},
		gotN, []bool{true, true, true, true, false, false, false, false})
	gotV, gotN, err = runIndicator(t, "wilder", 3, makeF64Slice(MF, 6), nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	assertSeq(t, "wilder3-MAXFLOAT", gotV,
		[]float64{math.NaN(), math.NaN(), MF, MF, MF, MF},
		gotN, []bool{true, true, false, false, false, false})
	// subnormal constant-stream carry per kernel with the kernel-true
	// first-defined rows: value kernels at m=n, kama at m=n+1,
	// dema at m=2n-1, tema at m=3n-2, t3 at m=6n-5. Constant streams carry
	// the exact constant through every stage (procedure-U hull rule) and
	// through the t3 combination (which lands back on the constant on the
	// subnormal grid).
	for _, tc := range []struct {
		name string
		n    int64
		v    float64
		def  int
	}{
		{"ema", 3, h, 2}, {"ema", 3, 3 * h, 2},
		{"wilder", 2, h, 1}, {"wilder", 2, 3 * h, 1},
		{"dema", 3, h, 4}, {"dema", 3, 3 * h, 4},
		{"tema", 3, h, 6}, {"tema", 3, 3 * h, 6},
		{"t3", 2, h, 6}, {"t3", 2, 3 * h, 6},
		{"kama", 2, h, 2}, {"kama", 2, 3 * h, 2},
		{"sma", 4, h, 3}, {"wma", 4, h, 3}, {"trima", 4, h, 3},
	} {
		gotV, gotN, err = runIndicator(t, tc.name, tc.n, makeF64Slice(tc.v, 10), nil, nil, 4)
		if err != nil {
			t.Fatalf("%s constant %v: %v", tc.name, tc.v, err)
		}
		want := make([]float64, 10)
		wantNul := make([]bool, 10)
		for i := 0; i < tc.def; i++ {
			want[i] = math.NaN()
			wantNul[i] = true
		}
		for i := tc.def; i < 10; i++ {
			want[i] = tc.v
		}
		assertSeq(t, tc.name+"-constant-carry", gotV, want, gotN, wantNul)
	}
	// generation isolation: the same materialized operator window re-driven
	// after a free recomputes from zero (stale prefixes impossible).
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	spec := indSpec("ema", types.T_float64)
	arg := &Window{WinSpecList: []*plan.Expr{spec}}
	if err := arg.Prepare(proc); err != nil {
		t.Fatal(err)
	}
	defer arg.Reset(proc, false, nil)
	arg.ctr.ps = nil
	// first generation
	xVecA := indMakeFloat64Vector(t, proc, []float64{1, 2, 3, 4}, nil)
	batA := batch.NewWithSize(1)
	batA.Vecs[0] = xVecA
	batA.SetRowCount(4)
	nVecA := indMakeInt64Vector(t, proc, []int64{2, 2, 2, 2}, nil)
	arg.ctr.bat = batA
	arg.ctr.aggVecs = []colexec.ExprEvalVector{{Vec: []*vector.Vector{xVecA, nVecA}}}
	if err := arg.ctr.validateIndicatorPeriods(0, arg, proc); err != nil {
		t.Fatal(err)
	}
	first, err := arg.ctr.processIndicatorFuncRange(0, arg, proc, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	gotA := vector.MustFixedColNoTypeCheck[float64](first)
	wantV, wantN := refIndicatorFloat("ema", 2, []float64{1, 2, 3, 4}, nil)
	for i := range wantV {
		if wantN[i] && !first.IsNull(uint64(i)) {
			t.Fatalf("gen-1 row %d: want NULL", i+1)
			continue
		}
		if !wantN[i] {
			if gotA[i] != wantV[i] {
				t.Fatalf("gen-1 row %d: got %v want %v", i+1, gotA[i], wantV[i])
			}
		}
	}
	first.Free(proc.Mp())
	// second generation through the eval-entry free discipline
	arg.ctr.freeIndicatorState(proc.Mp())
	xVecB := indMakeFloat64Vector(t, proc, []float64{10, 10, 10}, nil)
	batB := batch.NewWithSize(1)
	batB.Vecs[0] = xVecB
	batB.SetRowCount(3)
	nVecB := indMakeInt64Vector(t, proc, []int64{2, 2, 2}, nil)
	arg.ctr.bat = batB
	arg.ctr.aggVecs = []colexec.ExprEvalVector{{Vec: []*vector.Vector{xVecB, nVecB}}}
	if err := arg.ctr.validateIndicatorPeriods(0, arg, proc); err != nil {
		t.Fatal(err)
	}
	second, err := arg.ctr.processIndicatorFuncRange(0, arg, proc, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	gotB := vector.MustFixedColNoTypeCheck[float64](second)
	if len(gotB) != 3 || !second.IsNull(uint64(0)) || gotB[1] != 10 || gotB[2] != 10 {
		t.Fatalf("gen-2: got %v (nulls %v %v %v)", gotB,
			second.IsNull(0), second.IsNull(1), second.IsNull(2))
	}
	second.Free(proc.Mp())
	// Reset and Free discipline: both clear the evaluator slot
	if evs := len(arg.ctr.indicatorEvals); evs != 1 {
		t.Fatalf("expected one live evaluator, got %d", evs)
	}
	arg.Reset(proc, false, nil)
	if evs := len(arg.ctr.indicatorEvals); evs != 0 {
		t.Fatalf("Reset left %d evaluators alive", evs)
	}
	arg.Free(proc, false, nil)
	if evs := len(arg.ctr.indicatorEvals); evs != 0 {
		t.Fatalf("Free left %d evaluators alive", evs)
	}
}

// TestIndicatorStabilityVectors pins the review-driven KAMA and mean-kernel
// stability vectors: the in-test efficiency-ratio harness over the fresh
// window (Neumaier vs an uncompensated sequential variant), the derived
// er <= 1 + 2^-50 invariant with the sequential variant as the must-fail
// counterexample harness, the n = 1024 escalation, the nonflat-to-flat
// (er = 0) transition, and magnitude-drop windows for sma/wma/trima
// asserted against exact-rational oracles within the documented slack.
func TestIndicatorStabilityVectors(t *testing.T) {
	erInvariants := func(er float64) bool {
		return er <= 1+math.Ldexp(1, -50)
	}
	// [0, 1e16, 0, 1, 2], n = 2 (r2-review vector): the fresh Neumaier
	// window gives er = 1 exactly, while the withdrawn r2 mechanism - a
	// subtractive RUNNING aggregate - loses the small diffs once the
	// accumulator reaches 1e16 class and computes er = 2 on the same
	// vector. The subtractive replayer is the must-fail counterexample
	// harness; it also flips the carried output sign (~ -3.37e15).
	vals := []float64{0, 1e16, 0, 1, 2}
	gotV, gotN, err := runIndicator(t, "kama", 2, vals, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantV, wantN := refIndicatorFloat("kama", 2, vals, nil)
	assertSeqSlack(t, "kama r2 vector", gotV, wantV, gotN, wantN, 1e-12)
	neu, sub := refERNeumaier(vals, 2, 5), refERSubtractive(vals, 2, 5)
	if neu != 1.0 {
		t.Fatalf("r2 vector: neumaier er = %v want exact 1", neu)
	}
	if !erInvariants(neu) {
		t.Fatal("er invariant violated on neumaier")
	}
	if sub != 2.0 {
		t.Fatalf("r2 vector: subtractive running er = %v want 2", sub)
	}
	if erInvariants(sub) {
		t.Fatal("must-fail harness broken: subtractive variant satisfies the er bound")
	}
	if got := math.Abs(gotV[4]) - 3.074e15; got > 1e12 {
		t.Fatalf("kama r2 row5 = %v, expected about 3.074e15", gotV[4])
	}
	ySub := refSubtractiveKAMA(vals, 2)
	if ySub[len(ySub)-1] > 0 && gotV[4] > 0 {
		t.Fatalf("subtractive harness expected a sign-flipped output (~ -3.37e15), got %v", ySub[len(ySub)-1])
	}
	if math.Abs(ySub[len(ySub)-1]) < 1e15 {
		t.Fatalf("subtractive harness output too small: %v", ySub[len(ySub)-1])
	}
	// monotone 7-rows and reversal match the naive reference
	for _, vec7 := range [][]float64{
		{1, 2, 3, 4, 5, 6, 7},
		{7, 6, 5, 4, 3, 2, 1},
	} {
		gotV, gotN, err = runIndicator(t, "kama", 4, vec7, nil, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		wantV, wantN = refIndicatorFloat("kama", 4, vec7, nil)
		assertSeqSlack(t, "kama monotone", gotV, wantV, gotN, wantN, 1e-12)
	}
	// n = 4 monotone vector and reversal (r3-review vectors): er = 1
	// exactly under Neumaier on both; the uncompensated sequential fresh
	// sum picks up a 4-half-ulp excess on the [-1e16, 0, 1, 2, 3] window,
	// which must fail the withdrawn 1 + 2e-16 bound assertion (the derived
	// 1 + 2^-50 contract absorbs it; the next escalation does not).
	withdrawnInvariants := func(er float64) bool { return er <= 1+2e-16 }
	for _, vec := range [][]float64{
		{-1e16, 0, 1, 2, 3},
		{3, 2, 1, 0, -1e16},
	} {
		gotV, gotN, err = runIndicator(t, "kama", 4, vec, nil, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		wantV, wantN = refIndicatorFloat("kama", 4, vec, nil)
		assertSeqSlack(t, "kama 1e16 vector", gotV, wantV, gotN, wantN, 1e-12)
		neu, seq := refERNeumaier(vec, 4, 5), refERSequential(vec, 4, 5)
		if neu != 1.0 {
			t.Fatalf("1e16 vector %v: neumaier er = %v want exact 1", vec, neu)
		}
		if !erInvariants(neu) {
			t.Fatal("monotone er invariant violated")
		}
		if !erInvariants(seq) {
			t.Fatalf("1e16 vector sequential er = %v: outside the derived bound", seq)
		}
		seqFailsWithdrawn := !withdrawnInvariants(seq)
		if vec[0] == -1e16 && !seqFailsWithdrawn {
			t.Fatalf("sequential er = %v: the 4-half-ulp excess must fail the withdrawn bound", seq)
		}
		if vec[len(vec)-1] == -1e16 && seq != 1.0 {
			t.Fatalf("reversal sequential er = %v want 1", seq)
		}
	}
	// n = 1024 escalation: er = 1 exactly under Neumaier; the sequential
	// variant accumulates 461 ulps and must fail the derived bound.
	esc := make([]float64, 1025)
	esc[0] = -1e16
	for i := 1; i < 1025; i++ {
		esc[i] = float64(i - 1)
	}
	neu, seq := refERNeumaier(esc, 1024, 1025), refERSequential(esc, 1024, 1025)
	if neu != 1.0 {
		t.Fatalf("escalation neumaier er = %v want exact 1", neu)
	}
	if erInvariants(seq) {
		t.Fatalf("escalation sequential er = %v satisfies the bound (461-ulp violation expected)", seq)
	}
	if seq != 1.0000000000001024 {
		t.Fatalf("escalation sequential er = %v want 1.0000000000001024", seq)
	}
	// nonflat-to-flat: vol == 0 exactly, er = 0, smooth carry
	flat := []float64{1, 2, 3, 3, 3, 3}
	gotV, gotN, err = runIndicator(t, "kama", 2, flat, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantV, wantN = refIndicatorFloat("kama", 2, flat, nil)
	assertSeq(t, "kama flat carry", gotV, wantV, gotN, wantN)
	if erFlat := refERNeumaier(flat, 2, 6); erFlat != 0 {
		t.Fatalf("flat window er = %v want exact 0", erFlat)
	}
	// magnitude-drop windows, sma/wma/trima, oracle with documented slack
	for _, tc := range []struct {
		name string
		n    int64
		w    []float64
	}{
		{"sma", 4, []float64{1e16, 1e16, 1e16, 1e16, 1, 2, 3, 4}},
		{"wma", 4, []float64{1e16, -1e16, 1e16, -1e16, 1, 2, 3, 4}},
		{"trima", 4, []float64{1e16, 1e16, 1, 2, 3, 4}},
	} {
		gotV, gotN, err = runIndicator(t, tc.name, tc.n, tc.w, nil, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		wantV, wantN = refIndicatorFloat(tc.name, int(tc.n), tc.w, nil)
		assertSeqSlack(t, tc.name+" magnitude-drop", gotV, wantV, gotN, wantN, 1e-12)
		// exact-oracle assertion on the mean kernels' accuracy contract
		for col, want := range wantV {
			if wantN[col] {
				continue
			}
			if math.Abs(gotV[col]-want) > math.Ldexp(math.Abs(want)+math.Abs(gotV[col]), -38) {
				t.Fatalf("%s magnitude-drop row %d exceeded slack: %v vs %v",
					tc.name, col+1, gotV[col], want)
			}
		}
	}
}

// kamaWindowSource returns the change c and the diff sequence of the fresh
// KAMA window ending at 1-based row end with period n (last n+1 values,
// oldest first).
func kamaWindowSource(vals []float64, n, end int) (c float64, diffs []float64) {
	j := end - 1
	var w []float64
	for j >= 0 && len(w) <= n {
		w = append([]float64{vals[j]}, w...)
		j--
	}
	c = math.Abs(w[0] - w[n])
	for k := 0; k < n; k++ {
		diffs = append(diffs, math.Abs(w[k]-w[k+1]))
	}
	return c, diffs
}

// refERNeumaier is the selected fresh window variant: Neumaier-compensated
// volatility (the kernel's method).
func refERNeumaier(vals []float64, n, end int) float64 {
	c, diffs := kamaWindowSource(vals, n, end)
	var vol, comp float64
	for _, d := range diffs {
		t := vol + d
		if math.Abs(vol) >= math.Abs(d) {
			comp += (vol - t) + d
		} else {
			comp += (d - t) + vol
		}
		vol = t
	}
	vol += comp
	if vol == 0 {
		return 0
	}
	return c / vol
}

// refERSequential is the r3-review counterexample: the same fresh window
// with a plain uncompensated sequential sum.
func refERSequential(vals []float64, n, end int) float64 {
	c, diffs := kamaWindowSource(vals, n, end)
	var vol float64
	for _, d := range diffs {
		vol += d
	}
	if vol == 0 {
		return 0
	}
	return c / vol
}

// refERSubtractive is the withdrawn r2-review mechanism: a running
// volatility initialized once (the n diffs of the first full window) and
// then updated per row by subtracting the diff that leaves the window
// before adding the new one.
func refERSubtractive(vals []float64, n, end int) float64 {
	// diffs[k] enters at 1-based row k+2
	var diffs []float64
	for r := 2; r <= end; r++ {
		diffs = append(diffs, math.Abs(vals[r-1]-vals[r-2]))
	}
	vol := 0.0
	for k := 0; k < n; k++ { // window at 1-based row n+1
		vol += diffs[k]
	}
	for r := n + 2; r <= end; r++ {
		vol = (vol - diffs[r-n-2]) + diffs[r-2]
	}
	c := math.Abs(vals[end-1] - vals[end-1-n])
	if vol == 0 {
		return 0
	}
	return c / vol
}

// refSubtractiveKAMA replays the whole KAMA output stream of the withdrawn
// r2 mechanism (subtractive running volatility: initialize the n-diff sum
// once, then per row subtract the diff that leaves the window before adding
// the new one) for the must-fail counterexample harness.
func refSubtractiveKAMA(vals []float64, n int) []float64 {
	var out []float64
	var state float64
	var diffs []float64
	vol := 0.0
	init := false
	for i, u := range vals {
		if i == 0 {
			out = append(out, math.NaN())
			continue
		}
		diffs = append(diffs, math.Abs(u-vals[i-1]))
		if i == n-1 {
			state = u
		}
		if i < n+1 {
			// warmup: seed at m = n, first output at m = n+1
			out = append(out, math.NaN())
			continue
		}
		if !init {
			for k := 0; k < n; k++ {
				vol += diffs[i-k-1]
			}
			init = true
		} else {
			vol = (vol - diffs[i-n-1]) + diffs[i-1]
		}
		c := math.Abs(u - vals[i-n])
		er := 0.0
		if vol != 0 {
			er = c / vol
		}
		next, _ := kamaUpdate(er, u, state)
		state = next
		out = append(out, next)
	}
	return out
}

// TestIndicatorFuzzVsReference drives fixed-seed random streams (with NULLs
// and multiple partitions) through every value-axis kernel and compares
// against the naive references within the documented accumulated slack,
// with the NULL-bit matrices exact.
func TestIndicatorFuzzVsReference(t *testing.T) {
	rng := rand.New(rand.NewSource(29949))
	for iter := 0; iter < 60; iter++ {
		rows := 20 + rng.Intn(80)
		vals := make([]float64, rows)
		nulls := make([]bool, rows)
		var partitionStarts []int64
		step := 10 + rng.Intn(30)
		for i := 0; i < rows; i++ {
			r := rng.Float64()
			switch {
			case r < 0.12:
				vals[i] = 0
				nulls[i] = true
			case rng.Float64() < 0.3:
				vals[i] = float64(rng.Intn(2001) - 1000)
			default:
				vals[i] = (rng.Float64() - 0.5) * math.Ldexp(1, rng.Intn(10)+2)
			}
			if i > 0 && i%step == 0 {
				partitionStarts = append(partitionStarts, int64(i))
			}
		}
		for _, tc := range []struct {
			name string
			n    int64
			tol  float64
		}{
			{"sma", int64(1 + iter%7), 1e-12},
			{"wma", int64(1 + iter%7), 1e-12},
			{"trima", int64(1 + iter%7), 1e-12},
			{"ema", int64(1 + iter%7), 1e-11},
			{"wilder", int64(1 + iter%7), 1e-11},
			{"kama", int64(1 + iter%7), 1e-11},
			{"dema", int64(1 + iter%5), 1e-11},
			{"tema", int64(1 + iter%4), 1e-10},
			{"t3", int64(1 + iter%3), 1e-10},
			{"mmaxdrawdown", int64(1 + iter%9), 1e-12},
		} {
			gotV, gotN, err := runIndicator(t, tc.name, tc.n, vals, nulls, partitionStarts, 7)
			if err != nil {
				t.Fatalf("iter %d %s(%d): %v", iter, tc.name, tc.n, err)
			}
			var wantV []float64
			var wantN []bool
			if len(partitionStarts) == 0 {
				wantV, wantN = refIndicatorFloat(tc.name, int(tc.n), vals, nulls)
			} else {
				bounds := append([]int64{0}, partitionStarts...)
				bounds = append(bounds, int64(rows))
				var wv []float64
				var wn []bool
				for p := 0; p+1 < len(bounds); p++ {
					lo, hi := int(bounds[p]), int(bounds[p+1])
					a, b := refIndicatorFloat(tc.name, int(tc.n), vals[lo:hi], nulls[lo:hi])
					wv, wn = append(wv, a...), append(wn, b...)
				}
				wantV, wantN = wv, wn
			}
			assertSeqSlack(t, tc.name+" fuzz", gotV, wantV, gotN, wantN, tc.tol)
		}
	}
}

// TestIndicatorOperatorLifecycle covers the section-5.2 business rules at
// the harness level: chunked emission is exactly equal to single-pass
// emission (prefix reuse, no recomputation drift) across the 8192 strategy
// boundary, and a canceled context fails the statement instead of silently
// continuing.
func TestIndicatorOperatorLifecycle(t *testing.T) {
	rows := 12288
	vals := make([]float64, rows)
	for i := range vals {
		vals[i] = float64(i%101) + 0.5
	}
	single, singleN, err := runIndicator(t, "kama", 2, vals, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range []int{8191, 8192, 8193, 4096} {
		got, gotN, err := runIndicator(t, "kama", 2, vals, nil, nil, chunk)
		if err != nil {
			t.Fatalf("chunk %d: %v", chunk, err)
		}
		if len(got) != len(single) {
			t.Fatalf("chunk %d: length %d want %d", chunk, len(got), len(single))
		}
		for i := range single {
			if got[i] != single[i] || gotN[i] != singleN[i] {
				t.Fatalf("chunk %d row %d: %v/%v want %v/%v",
					chunk, i+1, got[i], gotN[i], single[i], singleN[i])
			}
		}
	}
	// cancellation: a canceled context surfaces through the statement
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	proc.Ctx = canceled
	defer func() { proc.Ctx = context.Background() }()
	_, _, err = runIndicatorOnWindow(t, proc, "mmaxdrawdown", 1024, indMakeFloat64Vector(t, proc, makeF64Slice(1, 64), nil), makeF64Slice(1, 64), nil, 0, types.T_float64)
	if err == nil {
		t.Fatal("canceled context did not fail the statement")
	}
}

// BenchmarkIndicatorMMaxDrawdownCapacity is the section-9 capacity probe
// (evidence, not a gate): one recorded run at n = 1024, p = 65,536 rows
// reports ns/op for the budget of section 8, with sma at the same shape
// for the class split. Run with -benchtime=1x to record the evidence.
func BenchmarkIndicatorMMaxDrawdownCapacity(b *testing.B) {
	benchmarkIndicatorKernel(b, "mmaxdrawdown", 1024, 65536)
}

func BenchmarkIndicatorSMAKernelCapacity(b *testing.B) {
	benchmarkIndicatorKernel(b, "sma", 1024, 65536)
}

func benchmarkIndicatorKernel(b *testing.B, name string, n, rows int) {
	proc := testutil.NewProcessWithOwnedMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	vals := make([]float64, rows)
	for i := range vals {
		vals[i] = float64(i%997) + 0.25
	}
	xVec := indMakeFloat64Vector(b, proc, vals, nil)
	defer xVec.Free(proc.Mp())
	per := make([]int64, rows)
	for i := range per {
		per[i] = int64(n)
	}
	nVec := indMakeInt64Vector(b, proc, per, nil)
	arg := &Window{WinSpecList: []*plan.Expr{indSpec(name, types.T_float64)}}
	if err := arg.Prepare(proc); err != nil {
		b.Fatal(err)
	}
	defer arg.Free(proc, false, nil)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = xVec
	bat.SetRowCount(rows)
	arg.ctr.bat = bat
	arg.ctr.aggVecs = []colexec.ExprEvalVector{{Vec: []*vector.Vector{xVec, nVec}}}
	if err := arg.ctr.validateIndicatorPeriods(0, arg, proc); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arg.ctr.freeIndicatorState(proc.Mp())
		vec, err := arg.ctr.processIndicatorFuncRange(0, arg, proc, 0, rows)
		if err != nil {
			b.Fatal(err)
		}
		vec.Free(proc.Mp())
	}
}
