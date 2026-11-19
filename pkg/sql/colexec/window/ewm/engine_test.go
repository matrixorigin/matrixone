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

package ewm

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"strconv"
	"testing"
)

//go:embed testdata/oracle_vectors.json
var oracleVectorFS embed.FS

// vcKey is one serialized ORDER BY key component: {"i":..} integer,
// {"f":bits} float or {"d":"decimal"} rational.
type vcKey struct {
	I *int64  `json:"i"`
	F *string `json:"f"`
	D *string `json:"d"`
}

type vcRow struct {
	K []vcKey `json:"k"`
	X *string `json:"x"`
	Y *string `json:"y"`
}

type vcOut struct {
	M *string `json:"m"`
	V *string `json:"v"`
	S *string `json:"s"`
	C *string `json:"c"`
	R *string `json:"r"`
	W string  `json:"w"`
	N int     `json:"n"`
	P int     `json:"p"`
}

type vcCase struct {
	Name       string  `json:"n"`
	Mode       string  `json:"mode"`
	Alpha      *string `json:"alpha"`
	HalfLife   *string `json:"halflife"`
	Adjust     bool    `json:"adjust"`
	IgnoreNA   bool    `json:"ignore_na"`
	MinPeriods int     `json:"min_periods"`
	Rows       []vcRow `json:"rows"`
	Outs       []vcOut `json:"outs"`
}

type vcFile struct {
	Cases []vcCase `json:"cases"`
}

func fromBits(s string) float64 {
	u, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		panic("ewm test: bad bits " + s + ": " + err.Error())
	}
	return math.Float64frombits(u)
}

func hexParse(p *string) *float64 {
	if p == nil {
		return nil
	}
	v := fromBits(*p)
	return &v
}

func buildRows(raw []vcRow) []Row {
	rows := make([]Row, len(raw))
	for i, r := range raw {
		keys := make([]KeyVal, len(r.K))
		for j, k := range r.K {
			switch {
			case k.I != nil:
				keys[j] = IntKey(*k.I)
			case k.F != nil:
				keys[j] = FloatKey(fromBits(*k.F))
			case k.D != nil:
				rat := new(big.Rat)
				if _, ok := rat.SetString(*k.D); !ok {
					panic("ewm test: bad decimal key " + *k.D)
				}
				keys[j] = DecKey(rat)
			default:
				panic("ewm test: empty key entry")
			}
		}
		rows[i] = Row{Keys: keys, X: hexParse(r.X), Y: hexParse(r.Y)}
	}
	return rows
}

func buildConfig(c vcCase) (Config, error) {
	cfg := Config{
		Adjust:     c.Adjust,
		IgnoreNA:   c.IgnoreNA,
		MinPeriods: c.MinPeriods,
	}
	if c.Mode == "row" {
		cfg.Mode = ModeRow
		if c.Alpha != nil {
			cfg.Alpha = fromBits(*c.Alpha)
		}
	} else {
		cfg.Mode = ModeTime
		if c.HalfLife != nil {
			cfg.HalfLife = fromBits(*c.HalfLife)
		}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func loadVectorFile(t *testing.T) []vcCase {
	t.Helper()
	data, err := oracleVectorFS.ReadFile("testdata/oracle_vectors.json")
	if err != nil {
		t.Fatalf("read vector corpus: %v", err)
	}
	var f vcFile
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("parse vector corpus: %v", err)
	}
	if len(f.Cases) == 0 {
		t.Fatal("empty vector corpus")
	}
	return f.Cases
}

// fieldMismatch reports one bit-exact field failure with enough context
// to identify the family.
func fieldMismatch(t *testing.T, name string, row int, field string,
	wantBits *string, got float64, gotNull bool) {
	t.Helper()
	if gotNull != (wantBits == nil) {
		t.Fatalf("[%s] row %d %s: NULL mismatch (got null=%v, want bits %v)",
			name, row, field, gotNull, wantBits)
	}
	if wantBits == nil {
		return
	}
	want := fromBits(*wantBits)
	if math.Float64bits(want) != math.Float64bits(got) {
		t.Fatalf("[%s] row %d %s: got %v (%08x), want %v (%08x)",
			name, row, field, got, math.Float64bits(got), want, math.Float64bits(want))
	}
}

// TestOracleVectors drives every archived-rev-21 oracle vector through
// the Go engine and asserts bit-exact equality of all five outputs,
// their NULL bits and the account counters.
func TestOracleVectors(t *testing.T) {
	cases := loadVectorFile(t)
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			cfg, err := buildConfig(c)
			if err != nil {
				t.Fatalf("config: %v", err)
			}
			outs, err := Evaluate(buildRows(c.Rows), cfg)
			if err != nil {
				t.Fatalf("evaluate: %v", err)
			}
			if len(outs) != len(c.Outs) {
				t.Fatalf("row count: got %d, want %d", len(outs), len(c.Outs))
			}
			for i, want := range c.Outs {
				got := outs[i]
				fieldMismatch(t, c.Name, i, "mean", want.M, got.Mean, got.MeanNull)
				fieldMismatch(t, c.Name, i, "var", want.V, got.Var, got.VarNull)
				fieldMismatch(t, c.Name, i, "std", want.S, got.Std, got.StdNull)
				fieldMismatch(t, c.Name, i, "cov", want.C, got.Cov, got.CovNull)
				fieldMismatch(t, c.Name, i, "corr", want.R, got.Corr, got.CorrNull)
				if math.Float64bits(fromBits(want.W)) != math.Float64bits(got.W) {
					t.Fatalf("[%s] row %d W: got %x, want %x",
						c.Name, i, math.Float64bits(got.W), math.Float64bits(fromBits(want.W)))
				}
				if want.N != got.C || want.P != got.Cp {
					t.Fatalf("[%s] row %d counters: got C=%d Cp=%d, want C=%d Cp=%d",
						c.Name, i, got.C, got.Cp, want.N, want.P)
				}
			}
		})
	}
}

// TestEvaluatorSliceResumability proves the §9 publication contract at
// the engine level: resumable FillSlice calls over arbitrary slice
// boundaries reproduce Evaluate's whole-partition outputs bit-for-bit,
// including groups straddling the boundaries (merge exactly once).
func TestEvaluatorSliceResumability(t *testing.T) {
	cases := loadVectorFile(t)
	for _, c := range cases {
		cfg, err := buildConfig(c)
		if err != nil {
			t.Fatalf("%s: config: %v", c.Name, err)
		}
		rows := buildRows(c.Rows)
		whole, err := Evaluate(rows, cfg)
		if err != nil {
			t.Fatalf("%s: evaluate: %v", c.Name, err)
		}
		for _, size := range []int{1, 2, 3, 5} {
			ev, err := NewEvaluator(rows, groupRows, cfg)
			if err != nil {
				t.Fatalf("%s: new evaluator: %v", c.Name, err)
			}
			if err := ev.ValidateSamples(); err != nil {
				t.Fatalf("%s: pre-scan: %v", c.Name, err)
			}
			outs := make([]Out, len(rows))
			for start := 0; start < len(rows); start += size {
				end := start + size
				if end > len(rows) {
					end = len(rows)
				}
				err := ev.FillSlice(start, end, &poller{}, func(row int, out Out) error {
					outs[row] = out
					return nil
				})
				if err != nil {
					t.Fatalf("%s: fill [%d,%d): %v", c.Name, start, end, err)
				}
			}
			if len(rows) == 0 {
				continue
			}
			for i := range whole {
				got, want := outs[i], whole[i]
				if math.Float64bits(got.Mean) != math.Float64bits(want.Mean) ||
					math.Float64bits(got.Var) != math.Float64bits(want.Var) ||
					math.Float64bits(got.Std) != math.Float64bits(want.Std) ||
					math.Float64bits(got.Cov) != math.Float64bits(want.Cov) ||
					math.Float64bits(got.Corr) != math.Float64bits(want.Corr) ||
					math.Float64bits(got.W) != math.Float64bits(want.W) ||
					got.C != want.C || got.Cp != want.Cp {
					t.Fatalf("%s size %d row %d: mean %x/%x var %x/%x std %x/%x cov %x/%x corr %x/%x",
						c.Name, size, i,
						math.Float64bits(got.Mean), math.Float64bits(want.Mean),
						math.Float64bits(got.Var), math.Float64bits(want.Var),
						math.Float64bits(got.Std), math.Float64bits(want.Std),
						math.Float64bits(got.Cov), math.Float64bits(want.Cov),
						math.Float64bits(got.Corr), math.Float64bits(want.Corr))
				}
			}
		}
	}
}

// TestNonFiniteSampleRejection pins the §7 public finite-sample
// contract: any non-finite x or y rejects at the pre-scan before any
// state exists, and the error is defined and reusable (rev-20
// Invariant 39).
func TestNonFiniteSampleRejection(t *testing.T) {
	inf := math.Inf(1)
	one := 1.0
	nan := math.NaN()
	cfg := Config{Mode: ModeRow, Alpha: 0.5, Adjust: true}
	mk := func(v float64) *float64 { x := v; return &x }
	cases := []struct {
		name string
		rows []Row
	}{
		{"inf_x_first", []Row{{Keys: []KeyVal{IntKey(1)}, X: mk(inf)}}},
		{"nan_x_first", []Row{{Keys: []KeyVal{IntKey(1)}, X: mk(nan)}}},
		{"inf_y_mid", []Row{
			{Keys: []KeyVal{IntKey(1)}, X: mk(one)},
			{Keys: []KeyVal{IntKey(2)}, X: mk(one), Y: mk(inf)},
		}},
		{"nan_y_later", []Row{
			{Keys: []KeyVal{IntKey(1)}, X: mk(one), Y: mk(one)},
			{Keys: []KeyVal{IntKey(2)}, X: mk(2.0), Y: mk(2.0)},
			{Keys: []KeyVal{IntKey(3)}, X: mk(3.0), Y: mk(nan)},
		}},
	}
	for _, tc := range cases {
		_, err := Evaluate(tc.rows, cfg)
		if err != ErrNonFiniteSample {
			t.Fatalf("%s: got %v, want ErrNonFiniteSample", tc.name, err)
		}
		ev, err := NewEvaluator(tc.rows, groupRows, cfg)
		if err != nil {
			t.Fatalf("%s: new: %v", tc.name, err)
		}
		if err := ev.ValidateSamples(); err != ErrNonFiniteSample {
			t.Fatalf("%s: pre-scan: got %v, want ErrNonFiniteSample", tc.name, err)
		}
	}
	// clean reuse: a finite run right after rejects works (α=.5
	// adjust=true: the second observation's mass 1 against the
	// decayed first mass .5 → mean 5/3).
	good := []Row{{Keys: []KeyVal{IntKey(1)}, X: mk(1.0)},
		{Keys: []KeyVal{IntKey(2)}, X: mk(2.0)}}
	outs, err := Evaluate(good, cfg)
	if err != nil {
		t.Fatalf("clean reuse: %v", err)
	}
	if outs[0].Mean != 1.0 || outs[1].Mean != 5.0/3.0 {
		t.Fatalf("clean reuse means: %v %v", outs[0].Mean, outs[1].Mean)
	}
}

// TestKeyDomainRules pins the §5 key domains: unsigned-magnitude int
// subtraction at the int64 extremes, the float quotient-domain rule
// on the raw-subtraction overflow, and the decimal exact-gap rule.
func TestKeyDomainRules(t *testing.T) {
	// [MinInt64, MaxInt64] h=1 -> 0, 10 (mean); the wrap fault (-1)
	// is impossible by construction.
	x0, x1 := 0.0, 10.0
	outs, err := Evaluate([]Row{
		{Keys: []KeyVal{IntKey(math.MinInt64)}, X: &x0},
		{Keys: []KeyVal{IntKey(math.MaxInt64)}, X: &x1},
	}, Config{Mode: ModeTime, HalfLife: 1.0, Adjust: false})
	if err != nil {
		t.Fatalf("min/max int keys: %v", err)
	}
	if outs[0].Mean != 0.0 || outs[1].Mean != 10.0 {
		t.Fatalf("min/max int keys means: %v %v", outs[0].Mean, outs[1].Mean)
	}
	// float extreme keys: r = b/h - a/h (raw subtraction overflows).
	outs, err = Evaluate([]Row{
		{Keys: []KeyVal{FloatKey(-1e308)}, X: &x0},
		{Keys: []KeyVal{FloatKey(1e308)}, X: &x1},
	}, Config{Mode: ModeTime, HalfLife: 1e308, Adjust: false})
	if err != nil {
		t.Fatalf("float extreme keys: %v", err)
	}
	if outs[0].Mean != 0.0 || outs[1].Mean != 7.5 {
		t.Fatalf("float extreme keys means: %v %v", outs[0].Mean, outs[1].Mean)
	}
	// decimal exact gaps: 1.5 -> 3.5, h=1.
	r1, r3 := big.NewRat(3, 2), new(big.Rat)
	r3.SetString("3.5")
	outs, err = Evaluate([]Row{
		{Keys: []KeyVal{DecKey(r1)}, X: &x1},
		{Keys: []KeyVal{DecKey(r3)}, X: &x0},
	}, Config{Mode: ModeTime, HalfLife: 1.0, Adjust: false})
	if err != nil {
		t.Fatalf("decimal keys: %v", err)
	}
	if outs[0].Mean != 10.0 {
		t.Fatalf("decimal keys first mean: %v", outs[0].Mean)
	}
	// a gap beyond the widest decimal domain rejects (§7).
	huge, tiny := new(big.Rat), new(big.Rat)
	huge.SetString("1e200")
	tiny.SetString("1e-200")
	// gap needs 400+ digits at aligned scale below the radix of the
	// widest decimal domain; the ratio would still be computable, so
	// the domain rule is the only protection.
	bigGap := new(big.Rat).Sub(huge, tiny) // 1e200 - 1e-200
	ev, err := NewEvaluator([]Row{
		{Keys: []KeyVal{DecKey(tiny)}, X: &x0},
		{Keys: []KeyVal{DecKey(bigGap)}, X: &x1},
	}, groupRows, Config{Mode: ModeTime, HalfLife: 1.0, Adjust: false})
	if err != nil {
		t.Fatalf("decimal overflow setup: %v", err)
	}
	if _, err := keyRatio(KeyVal{Kind: KeyDec, Dec: tiny}, KeyVal{Kind: KeyDec, Dec: bigGap}, 1.0); err != ErrDecimalGapOverflow {
		t.Fatalf("decimal overflow: got %v, want ErrDecimalGapOverflow", err)
	}
	_ = ev
}

// cancelAfterN is a direct cancellation hook for the engine's poller:
// the first done-1 calls report a live operator, every later call
// returns the sentinel cause.
func cancelAfterN(done int) (func() error, *int) {
	calls := 0
	return func() error {
		calls++
		if calls > done {
			return context.Canceled
		}
		return nil
	}, &calls
}

// TestMergePhaseCancellation: the batch-statistical passes of a large
// single peer group observe the bounded cancellation contract (§9):
// with k rows whose first two values differ, the poller that flips at
// its fourth done check surfaces the cause from inside batchSide's
// compensated sweep (the first three checks cover FillSlice, the
// observation collection and the xs/ys fill loops), and a fresh
// evaluator over the same rows afterwards publishes the complete
// profile.
func TestMergePhaseCancellation(t *testing.T) {
	const rows = 1500
	mkRows := func() []Row {
		one := IntKey(1)
		out := make([]Row, rows)
		for i := range out {
			x := 1.0
			if i == 1 {
				x = 2.0
			}
			out[i] = Row{Keys: []KeyVal{one}, X: &x}
		}
		return out
	}
	cfg := Config{Mode: ModeRow, Alpha: 0.5, Adjust: true}
	ev, err := NewEvaluator(mkRows(), groupRows, cfg)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	fn, calls := cancelAfterN(3)
	if err := ev.FillSlice(0, rows, NewPoller(fn), func(row int, out Out) error {
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("merge-phase cancellation: got %v (fn calls=%d), want context.Canceled", err, *calls)
	}
	if *calls < 4 {
		t.Fatalf("cancellation surfaced before the merge sweeps (fn calls=%d)", *calls)
	}

	// a fresh evaluator over the same rows publishes complete outputs
	outs, err := Evaluate(mkRows(), cfg)
	if err != nil {
		t.Fatalf("clean reuse: %v", err)
	}
	// one tied batch of 1499 ones and one 2.0: the group-post mean is
	// the batch mean (k+1)/k at every row
	want := 1501.0 / 1500.0
	if outs[0].Mean != want || outs[1].Mean != want || outs[2].Mean != want {
		t.Fatalf("clean reuse: a tied batch of k rows has mean %v/%v/%v, want %.17g",
			outs[0].Mean, outs[1].Mean, outs[2].Mean, want)
	}
}

// TestValidateSamplesPollingCancellation: the finite pre-scan itself
// polls — a poller that flips before the scan reaches a trailing NaN
// row surfaces the cancellation cause, the §7 finite rejection included.
func TestValidateSamplesPollingCancellation(t *testing.T) {
	const rows = 4096
	built := make([]Row, rows)
	one := IntKey(1)
	for i := range built {
		x := 1.0
		built[i] = Row{Keys: []KeyVal{one}, X: &x}
	}
	tail := math.NaN()
	built[rows-1] = Row{Keys: []KeyVal{one}, X: &tail}
	cfg := Config{Mode: ModeRow, Alpha: 0.5, Adjust: true}
	ev, err := NewEvaluator(built, groupRows, cfg)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	fn, _ := cancelAfterN(1)
	if err := ev.ValidateSamplesPolling(NewPoller(fn)); !errors.Is(err, context.Canceled) {
		t.Fatalf("validation cancellation: got %v (%T), want context.Canceled", err, err)
	}
	// without the early cancellation the same scan rejects the NaN row
	fresh, err := NewEvaluator(built, groupRows, cfg)
	if err != nil {
		t.Fatalf("fresh: %v", err)
	}
	if err := fresh.ValidateSamples(); err != ErrNonFiniteSample {
		t.Fatalf("finite pre-scan: got %v, want ErrNonFiniteSample", err)
	}
}

// TestAlphaQuarterSemantics pins the design's alpha convention against
// projections that reverse it: alpha is the ARRIVAL share while
// 1-alpha is the retention factor (§5). At alpha=0.25 the unadjusted
// recurrence y = .75*y_prev + .25*x publishes [1, 1.25, 1.6875] over
// [1,2,3] (a reversed convention would publish [1, 1.75, 2.6875]),
// and the adjusted values match the mass-normalized weights w_i =
// .75^(t-i) within the rounding tolerance of the compensated engine.
func TestAlphaQuarterSemantics(t *testing.T) {
	mk := func(vals ...float64) []Row {
		out := make([]Row, len(vals))
		for i, v := range vals {
			x := v
			out[i] = Row{Keys: []KeyVal{IntKey(int64(i + 1))}, X: &x}
		}
		return out
	}
	rows := mk(1, 2, 3)

	outs, err := Evaluate(rows, Config{Mode: ModeRow, Alpha: 0.25, Adjust: false})
	if err != nil {
		t.Fatalf("unadjusted: %v", err)
	}
	want := []float64{1.0, 1.25, 1.6875}
	for i := range want {
		if outs[i].Mean != want[i] {
			t.Fatalf("unadjusted row %d: got %v, want %v", i, outs[i].Mean, want[i])
		}
	}

	outs, err = Evaluate(rows, Config{Mode: ModeRow, Alpha: 0.25, Adjust: true})
	if err != nil {
		t.Fatalf("adjusted: %v", err)
	}
	// exact mass-normalized expectations: 1, 11/7, 81/37
	type rw struct {
		num, den float64
	}
	exact := []rw{{1, 1}, {11, 7}, {81, 37}}
	for i := range exact {
		e := exact[i].num / exact[i].den
		if math.Abs(outs[i].Mean-e) > 1e-12*math.Abs(e) {
			t.Fatalf("adjusted row %d: got %.17g, want %.17g", i, outs[i].Mean, e)
		}
	}
}

// TestEventTimeNullInstantClock pins §5's event-time clock for a
// NULL-valued instant between two observations (the round-5 black-box
// event-null fixture): every peer group applies its own transition —
// NULL-valued groups always apply their instant's transition, and the
// per-group gaps telescope — so with keys [0,1,2], x = [0,NULL,10] and
// h=1 the prior mass decays twice (1 -> .5 -> .25) while the arrival
// batch contributes the gapped share of its own instant. The merged
// mass-normalized state is (0.25*0 + 0.5*10)/0.75 = 20/3 under
// adjust=false (not the transition-free reading 5), and the arrival
// mass k=1 publishes (0.25*0 + 1*10)/1.25 = 8 under adjust=true;
// ignoreNA changes only the NULL-input row's bit.
func TestEventTimeNullInstantClock(t *testing.T) {
	x0, x10 := 0.0, 10.0
	rows := []Row{
		{Keys: []KeyVal{IntKey(0)}, X: &x0},
		{Keys: []KeyVal{IntKey(1)}, X: nil},
		{Keys: []KeyVal{IntKey(2)}, X: &x10},
	}
	unadjusted, err := Evaluate(rows, Config{Mode: ModeTime, HalfLife: 1.0, Adjust: false})
	if err != nil {
		t.Fatalf("unadjusted: %v", err)
	}
	// first batch publishes itself; the NULL instant carries the live
	// state (0); the arrival merges on the twice-decayed prior mass.
	if unadjusted[0].Mean != 0.0 || unadjusted[1].Mean != 0.0 {
		t.Fatalf("unadjusted carried rows: got %v %v, want 0 0",
			unadjusted[0].Mean, unadjusted[1].Mean)
	}
	want := 20.0 / 3.0
	if math.Abs(unadjusted[2].Mean-want) > 1e-12*math.Abs(want) {
		t.Fatalf("unadjusted arrival: got %.17g, want %.17g",
			unadjusted[2].Mean, want)
	}

	adjusted, err := Evaluate(rows, Config{Mode: ModeTime, HalfLife: 1.0, Adjust: true})
	if err != nil {
		t.Fatalf("adjusted: %v", err)
	}
	// arrival mass k = 1: (0.25*0 + 1*10)/1.25 = 8.
	if adjusted[0].Mean != 0.0 || adjusted[1].Mean != 0.0 {
		t.Fatalf("adjusted carried rows: got %v %v, want 0 0",
			adjusted[0].Mean, adjusted[1].Mean)
	}
	if adjusted[2].Mean != 8.0 {
		t.Fatalf("adjusted arrival: got %.17g, want 8", adjusted[2].Mean)
	}

	// ignoreNA=true keeps the same account (the NULL instant still
	// decays) and flips only the NULL-input row's value bit.
	inaT, err := Evaluate(rows, Config{Mode: ModeTime, HalfLife: 1.0, Adjust: false, IgnoreNA: true})
	if err != nil {
		t.Fatalf("inaT: %v", err)
	}
	if inaT[1].Mean != 0.0 || !inaT[1].XNullBit || inaT[0].XNullBit || inaT[2].XNullBit {
		t.Fatalf("inaT NULL bits: mean %v, bits %v %v %v",
			inaT[1].Mean, inaT[0].XNullBit, inaT[1].XNullBit, inaT[2].XNullBit)
	}
	if math.Abs(inaT[2].Mean-want) > 1e-12*math.Abs(want) {
		t.Fatalf("inaT arrival: got %.17g, want %.17g", inaT[2].Mean, want)
	}
}

// TestRetainedAccountBytesConstant: the retained per-partition account
// is constant-sized (§9 resource model): it equals itself across a
// 64-row and a 4096-row partition and stays a bounded bureaucracy
// budget, proving the incremental-state claim the design pins.
func TestRetainedAccountBytesConstant(t *testing.T) {
	mk := func(n int) []Row {
		one := IntKey(1)
		out := make([]Row, n)
		for i := range out {
			x := float64(i%37) + 1.0
			if i == 1 {
				x = 40.0
			}
			out[i] = Row{Keys: []KeyVal{one}, X: &x}
		}
		return out
	}
	small, err := NewEvaluator(mk(64), groupRows, Config{Mode: ModeRow, Alpha: 0.5, Adjust: true})
	if err != nil {
		t.Fatalf("small: %v", err)
	}
	large, err := NewEvaluator(mk(4096), groupRows, Config{Mode: ModeRow, Alpha: 0.5, Adjust: true})
	if err != nil {
		t.Fatalf("large: %v", err)
	}
	if err := large.ValidateSamples(); err != nil {
		t.Fatalf("large validate: %v", err)
	}
	emitted := 0
	if err := large.FillSlice(0, 4096, NOP(), func(row int, out Out) error {
		emitted++
		return nil
	}); err != nil {
		t.Fatalf("large fill: %v", err)
	}
	if emitted != 4096 {
		t.Fatalf("large fill emitted %d rows, want 4096", emitted)
	}
	if small.RetainedAccountBytes() != large.RetainedAccountBytes() {
		t.Fatalf("retained account grows with the partition: %d vs %d",
			small.RetainedAccountBytes(), large.RetainedAccountBytes())
	}
	if large.RetainedAccountBytes() > 1024 {
		t.Fatalf("retained account %d exceeds the constant-state bound", large.RetainedAccountBytes())
	}
}

// NOP returns the never-cancelling poller for deterministic bulk runs.
func NOP() *Poller { return NewPoller(nil) }

// TestDdsumResidualCancellation pins the residual phase of the bounded
// cancellation contract (§9's fixed ≤1024-step interval covers the
// fold-loss accumulation sweep too, not only ddsumPoll's primary fold):
// folding many tiny arrivals into a one-accumulator is a cancellation-
// packing fixture whose every fold leaves a nonzero exact complement,
// so the losses pile is batch-sized. A live poller leaves the result
// bit-identical to the unpolled fold; a poller already dead by the
// fold leaves the residual unaccumulated (the merge layer's dead check
// receives partial words and discards them); and a poller that flips
// exactly between the two sweeps - the fold completes on live checks -
// still stops the residual mid-way: the unchecked partition-sized
// linear residual sweep is the pre-fix behavior this test excludes.
func TestDdsumResidualCancellation(t *testing.T) {
	const folds = 8192
	items := make([]xnum, folds+1)
	items[0] = xfrexp(scalar(1.0))
	for i := 1; i <= folds; i++ {
		items[i] = xfrexp(scalar(1e-10))
	}

	// the live sweep matches the unpolled fold bit-for-bit
	liveH, liveL := ddsumPoll(items, NewPoller(func() error { return nil }))
	nopH, nopL := ddsum(items)
	if liveH != nopH || liveL != nopL {
		t.Fatalf("live residual differs from the unpolled fold: (%.17g,%v)/(%.17g,%v) vs (%.17g,%v)/(%.17g,%v)",
			xval(liveH), liveH.isPair, xval(liveL), liveL.isPair,
			xval(nopH), nopH.isPair, xval(nopL), nopL.isPair)
	}
	if xval(liveL) == 0.0 {
		t.Fatalf("fixture must accumulate nonzero fold losses (lo=%.17g)", xval(liveL))
	}

	// dead by the fold: the residual sweep sees the dead mark on its
	// first step and accumulates nothing
	fn1, calls1 := cancelAfterN(1)
	_, deadL := ddsumPoll(items, NewPoller(fn1))
	if xval(deadL) != 0.0 {
		t.Fatalf("dead-by-fold poller must leave the residual unaccumulated, got %.17g", xval(deadL))
	}
	if *calls1 < 2 {
		t.Fatalf("the fold sweep must reach its interval checks (calls=%d)", *calls1)
	}

	// flip exactly between the two sweeps: the fold's own interval
	// checks (8 over 8192 steps) stay live and the fold completes, and
	// the residual's first interval check stops it mid-sweep
	fn2, calls2 := cancelAfterN(8)
	flipH, flipL := ddsumPoll(items, NewPoller(fn2))
	if flipH != liveH {
		t.Fatalf("the fully checked fold must match the live result high word")
	}
	if flipL == liveL {
		t.Fatalf("a mid-residual cancellation must not return the fully accumulated losses")
	}
	if *calls2 < 9 {
		t.Fatalf("the residual sweep must reach its own interval check (calls=%d)", *calls2)
	}
}
