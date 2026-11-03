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
	"math"
	"math/big"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// The engine's defined rejections are package-level moerr singletons
// (the repository's static contract requires moerr for constructed
// errors in pkg/ and its err-check gate enforces it): each singleton
// keeps pointer identity for the tests' equality checks, and
// ErrNonFiniteSample and ErrDecimalGapOverflow render byte-for-byte as
// the contract texts of docs/design/ewm_window_statistics.md (§7).

// ErrNonFiniteSample is the public finite-sample contract (§7): a row
// sample whose float64 value is non-finite is a defined execution
// rejection raised by the pre-scan BEFORE any group is processed and
// any state is published (rev-20 Invariant 39 / Decision 63).
var ErrNonFiniteSample = moerr.NewInvalidInputNoCtx(
	"ewm sample values must be finite")

// ErrDecimalGapOverflow mirrors the DECIMAL key-gap domain rule (§5):
// a gap that overflows the widest decimal domain is a defined
// execution rejection (§7), never a wrapped two's-complement value.
var ErrDecimalGapOverflow = moerr.NewInvalidInputNoCtx(
	"ewm decimal key gaps exceed the decimal domain")

// ErrBadConfig mirrors the resolved-policy validation families (§7):
// a decoded configuration with an out-of-range alpha or half-life is
// an execution-boundary error in moerr's bad-configuration family.
var ErrBadConfig = moerr.NewBadConfigNoCtx(
	"ewm statistics require alpha in (0,1] or a strictly positive finite half-life")

// Mode selects the decay axis (§5): row batches (λ^s or λ per
// observation batch) or physical event-time (exp(−ln2·Δt/h)). It
// changes only the mass generator, never the evaluation formulas.
type Mode uint8

const (
	// ModeRow is the row-decay form (alpha declared).
	ModeRow Mode = iota
	// ModeTime is the event-time half-life form ('halflife' tag).
	ModeTime
)

// Config is the fully resolved per-execution configuration carried by
// the encoded config bytes (§9): the registry/binder validated the
// plan-level shapes; the execution boundary re-validates resolved
// values so a bad EXECUTE fails before any state exists.
type Config struct {
	Mode       Mode
	Alpha      float64 // row mode
	HalfLife   float64 // time mode
	Adjust     bool
	IgnoreNA   bool
	MinPeriods int
}

// Validate applies the §7 resolved-value families: alpha ∈ (0,1] in
// row mode, a strictly positive finite half-life in event-time mode,
// nothing else constrains the engine.
func (c Config) Validate() error {
	switch c.Mode {
	case ModeRow:
		if !isFinite(c.Alpha) || c.Alpha <= 0.0 || c.Alpha > 1.0 {
			return ErrBadConfig
		}
	default:
		if !isFinite(c.HalfLife) || c.HalfLife <= 0.0 {
			return ErrBadConfig
		}
	}
	return nil
}

// KeyKind is the native comparison domain of one ORDER BY key (§5).
type KeyKind uint8

const (
	// KeyInt is the signed/unsigned integer and temporal (int64 µs)
	// domain: unsigned-magnitude subtraction, one float64 conversion of
	// the ≤64-bit magnitude.
	KeyInt KeyKind = iota
	// KeyFloat is the FLOAT/DOUBLE domain: the quotient-domain ratio.
	KeyFloat
	// KeyDec is the DECIMAL domain: exact widened subtraction, one
	// float64 conversion of the gap.
	KeyDec
)

// KeyVal carries one ORDER BY key component in its native comparison
// domain (§5 domain table). Rows of one peer group compare equal on
// every component (exact word/rational equality); the event-time gap
// is computed in the key's own domain before any float conversion.
type KeyVal struct {
	Kind KeyKind
	I    int64    // KeyInt
	F    float64  // KeyFloat
	Dec  *big.Rat // KeyDec (exact)
}

// IntKey builds an integer-domain key.
func IntKey(v int64) KeyVal {
	return KeyVal{Kind: KeyInt, I: v}
}

// FloatKey builds a float-domain key.
func FloatKey(v float64) KeyVal {
	return KeyVal{Kind: KeyFloat, F: v}
}

// DecKey builds a decimal-domain key.
func DecKey(r *big.Rat) KeyVal {
	return KeyVal{Kind: KeyDec, Dec: r}
}

// EqualX reports exact key-equality across any pair of domains; two
// keys in different domains are never equal (an OVER clause has a
// single key type, so the mixed comparison is unreachable in SQL and
// reports false).
func (a KeyVal) EqualX(b KeyVal) bool {
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case KeyInt:
		return a.I == b.I
	case KeyFloat:
		return a.F == b.F
	default:
		if a.Dec == nil || b.Dec == nil {
			return a.Dec == nil && b.Dec == nil
		}
		return a.Dec.Cmp(b.Dec) == 0
	}
}

// RowsEqual reports peer-group equality of the ORDER BY tuples.
func RowsEqual(a, b []KeyVal) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].EqualX(b[i]) {
			return false
		}
	}
	return true
}

// keyValFloatView widens any single-domain key to the float64 view of
// its exact value (integer keys round once; an OVER clause carries
// one single key type, so mixed kinds appear only off the SQL path).
func keyValFloatView(v KeyVal) float64 {
	switch v.Kind {
	case KeyInt:
		return float64(v.I)
	case KeyFloat:
		return v.F
	default:
		if v.Dec == nil {
			return 0.0
		}
		f, _ := v.Dec.Float64()
		return f
	}
}

// keyRatio computes r = Δt/h in the key's native domain (§5 domain
// table): unsigned-magnitude integer subtraction with one float64
// conversion of the ≤64-bit magnitude; the quotient-domain rule for
// float keys (Δt/h formed directly; the overflow fraction b/h − a/h
// when the raw subtraction overflows); exact widened decimal
// subtraction with the widest-domain overflow surfaced (§7) and one
// float64 conversion of the gap; mixed kinds fall to the float
// domain exactly like the sequential reference.
func keyRatio(prev, cur KeyVal, h float64) (float64, error) {
	switch {
	case prev.Kind == KeyInt && cur.Kind == KeyInt:
		d := uint64(cur.I) - uint64(prev.I)
		return float64(d) / h, nil
	case prev.Kind == KeyDec && cur.Kind == KeyDec:
		if prev.Dec == nil || cur.Dec == nil {
			return 0.0, ErrDecimalGapOverflow
		}
		gap := new(big.Rat).Sub(cur.Dec, prev.Dec)
		// The widest decimal domain (decimal256) carries at most 77
		// significant decimal digits at the fixed scales; a gap whose
		// exact significand exceeds that width is the §7 overflow.
		num := gap.Num()
		if num.Sign() != 0 && len(num.Text(10)) > 77+len(gap.Denom().Text(10)) {
			return 0.0, ErrDecimalGapOverflow
		}
		f, _ := gap.Float64()
		return f / h, nil
	default:
		a, b := keyValFloatView(prev), keyValFloatView(cur)
		d := b - a
		if math.IsInf(d, 0) {
			return b/h - a/h, nil
		}
		return d / h, nil
	}
}

// Row is one input row: the ORDER BY key tuple (peers are maximal
// equal-key runs) and the two statistical samples with their NULL
// bits (nil = NULL).
type Row struct {
	Keys []KeyVal
	X    *float64
	Y    *float64
}

// Out carries one published row's five outputs (§5 Outputs) with the
// C-gate NULL bits. The shared group-post value and the per-row value
// NULL bits (event-time ignoreNA=true) combine in the caller (§6).
type Out struct {
	Mean, Var, Std, Cov, Corr  float64
	MeanNull, VarNull, StdNull bool
	CovNull, CorrNull          bool
	XNullBit, PNullBit         bool
	W                          float64
	C, Cp                      int
}

const cancelPollInterval = 1024

// Poller is the exported form of the engine's bounded cancellation
// poller: the operator supplies its shared checkCanceled-shaped hook.
type Poller = poller

// NewPoller builds a poller whose long loops poll the caller's
// cancellation hook at the fixed 1024-step interval.
func NewPoller(fn func() error) *Poller { return &poller{fn: fn} }

// Step is the operator-side form of the per-element poll: the window
// dispatch polls its staging loops through this exported wrapper while
// every internal sweep keeps the same sticky-dead semantics.
func (p *Poller) Step() error { return p.step() }

// poller is the operator's bounded in-loop cancellation hook (§9):
// every long EWM loop polls the shared helper at this fixed interval.
// The first canceled check marks the poller dead — well-behaved sweep
// helpers abort their remaining loop on that mark and return partial
// values, which the merge layer discards when its immediate dead check
// fails (a canceled generation is torn down by the caller, so a
// partially merged account is observable nowhere; the §7 error and
// cancellation catalog contract holds).
type poller struct {
	fn   func() error
	iter int
	dead bool
	err  error
}

func (p *poller) step() error {
	if p == nil {
		return nil
	}
	p.iter++
	if p.dead {
		return p.err
	}
	if p.iter%cancelPollInterval != 0 {
		return nil
	}
	if p.fn == nil {
		return nil
	}
	if err := p.fn(); err != nil {
		p.dead, p.err = true, err
	}
	return p.ok()
}

// ok reports the poller's current cause: nil while no check canceled.
func (p *poller) ok() error {
	if p == nil || !p.dead {
		return nil
	}
	return p.err
}

// canceled reports whether any check flagged the operator canceled. The
// merge layer calls it after every abortable sweep so a partially
// accumulated value never commits (§9 bounded cancellation).
func (p *poller) canceled() bool { return p != nil && p.dead }

// Evaluator is the resumable per-partition EWM engine (§9 lifecycle):
// whole peer groups at a time, exactly-once merges, monotonic
// publication cursor, the in-flight straddling-group cache and the
// per-account unseen flags/decay clocks. The caller owns the row
// storage; the evaluator owns the accounts.
type Evaluator struct {
	cfg Config
	// λ = 1 − α (row mode) and the event-time half-life.
	lam float64
	h   float64

	// the two stream accounts (§5): the x-observation stream drives
	// mean/var/std, the full-pair stream drives cov/corr.
	xs, ps             Stream
	seenX, seenP       bool
	lastRowX, lastRowP int

	// groupRows returns the peer-group row range [start, end) that
	// contains row r, or the empty range when r is out of the
	// partition. Supplied once per partition at Reset.
	rows      []Row
	groupRows func(rows []Row, r int) (int, int)

	// cursor: the first row whose output is not yet filled.
	filled int
	// the in-flight group cache (the group straddling the last slice
	// boundary: merged exactly once, values emitted across slices).
	cacheStart, cacheEnd int
	cacheVal             Out
	cacheActive          bool
}

// NewEvaluator builds an unseen evaluator for one partition; the row
// slice and the peer-group walker are bound at Reset time (the
// operator rebinds them per partition; accounts follow the partition
// reset contract).
func NewEvaluator(rows []Row, group func(rows []Row, r int) (int, int), cfg Config) (*Evaluator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	e := &Evaluator{cfg: cfg}
	if cfg.Mode == ModeRow {
		e.lam = 1.0 - cfg.Alpha
	} else {
		e.h = cfg.HalfLife
	}
	e.Reset(rows, group)
	return e, nil
}

// Reset restores the unseen partition state: account statistics and
// clocks (W, means, the centered moments in either plain or extended
// form, the C gates and the per-account decay clocks) reset to their
// unseen values; the absolute row cursor and the straddling-group
// cache reset with the bound partition rows (§9 partition boundary).
func (e *Evaluator) Reset(rows []Row, group func(rows []Row, r int) (int, int)) {
	e.xs.Reset()
	e.ps.Reset()
	e.seenX, e.seenP = false, false
	e.lastRowX, e.lastRowP = 0, 0
	e.rows = rows
	e.groupRows = group
	e.filled = 0
	e.cacheActive = false
}

// ValidateSamples runs the finite-sample pre-scan over the bound rows
// (§7): non-finite x or y values reject BEFORE any group is processed
// or state is published.
func (e *Evaluator) ValidateSamples() error {
	return e.ValidateSamplesPolling(nil)
}

// ValidateSamplesPolling is the finite-sample pre-scan of §7 with the
// bounded cancellation contract: the partition-sized scan polls the
// operator's hook at the fixed interval and a canceled scan rejects
// with the cancellation cause before any group is processed or state
// is published.
func (e *Evaluator) ValidateSamplesPolling(poll *poller) error {
	for i := range e.rows {
		if e.rows[i].X != nil && !isFinite(*e.rows[i].X) {
			return ErrNonFiniteSample
		}
		if e.rows[i].Y != nil && !isFinite(*e.rows[i].Y) {
			return ErrNonFiniteSample
		}
		if err := poll.step(); err != nil {
			return err
		}
	}
	return nil
}

// groupLenBounds finds the rows of the group [start, end) containing
// r using the bound walker; also returns the group's first-past-end.
func (e *Evaluator) groupAt(r int) (int, int) {
	return e.groupRows(e.rows, r)
}

// processGroupFrom merges one complete group; prevKey carries the
// previous group's first key tuple for event-time gaps (nil at the
// partition start). The row-mode span anchors at the GROUP start row
// (the oracle's gs), so leading NULL rows of a mixed group count
// once — the batch's first row, not its first observation.
func (e *Evaluator) processGroupFrom(gs, ge int, prevKeys []KeyVal, poll *poller) (Out, error) {
	cfg := e.cfg
	// Event-time transition mass: Δt in the key's native domain.
	var gt, at float64
	if cfg.Mode == ModeTime && prevKeys != nil {
		r, err := keyRatio(prevKeys[0], e.rows[gs].Keys[0], e.h)
		if err != nil {
			return Out{}, err
		}
		c := LN2 * r
		if math.IsInf(c, 0) {
			gt, at = 0.0, 1.0
		} else {
			gt = expNegCR(c)
			at = expm1NegCR(c)
		}
	}

	// The observation batches of each account (§5): x-observations for
	// the x account, full pairs for the pair account.
	xobs := make([]int, 0, ge-gs)
	pobs := make([]int, 0, ge-gs)
	for r := gs; r < ge; r++ {
		row := e.rows[r]
		if row.X != nil {
			xobs = append(xobs, r)
			if row.Y != nil {
				pobs = append(pobs, r)
			}
		}
		if err := poll.step(); err != nil {
			return Out{}, err
		}
	}

	// Per-account step (§5 clocks): transitions scale only W; arrival
	// masses follow the adjust/mode table; the merge runs on the
	// observation values with the pair stream's y defaulted to 0.
	if err := e.stepAccount(&e.xs, true, xobs, gs, ge, gt, at, poll); err != nil {
		return Out{}, err
	}
	if err := e.stepAccount(&e.ps, false, pobs, gs, ge, gt, at, poll); err != nil {
		return Out{}, err
	}

	// Group-post publication (§6): one shared value for every row of
	// the group; NULL bits per row follow the event-time ignoreNA
	// rule and combine in the caller.
	return e.publish(poll)
}

// stepAccount applies the clock and arrival rules of one account
// (§5): the row clock's absolute/span or per-batch λ; event-time's
// group-to-group transition BEFORE the empty-batch check; the arrival
// mass table (first/key-adjusted/adjust-false shapes); then the
// layered merge of the observation batch.
func (e *Evaluator) stepAccount(st *Stream, xSide bool, obs []int, gs, ge int,
	gt, at float64, poll *poller) error {
	cfg := e.cfg
	seen := e.seenX
	last := e.lastRowX
	if !xSide {
		seen = e.seenP
		last = e.lastRowP
	}
	if cfg.Mode == ModeTime {
		if seen {
			st.Transition(gt)
		}
		if len(obs) == 0 {
			return nil
		}
	} else {
		if len(obs) == 0 {
			return nil
		}
		if seen {
			if cfg.IgnoreNA {
				st.Transition(e.lam)
			} else {
				st.Transition(intPowCR(e.lam, int64(gs-last)))
			}
		}
	}
	k := float64(len(obs))
	var A float64
	if !seen {
		A = k
		if !cfg.Adjust {
			A = 1.0
		}
	} else if cfg.Adjust {
		A = k
	} else if cfg.Mode == ModeRow {
		A = k * cfg.Alpha
	} else {
		A = at
	}
	xs := make([]float64, len(obs))
	ys := make([]float64, len(obs))
	for i, r := range obs {
		if err := poll.step(); err != nil {
			return err
		}
		xs[i] = *e.rows[r].X
		if e.rows[r].Y != nil {
			ys[i] = *e.rows[r].Y
		}
	}
	if err := st.merge(xs, ys, A, poll); err != nil {
		return err
	}
	if xSide {
		e.seenX = true
		if len(obs) > 0 {
			e.lastRowX = ge - 1
		}
	} else {
		e.seenP = true
		if len(obs) > 0 {
			e.lastRowP = ge - 1
		}
	}
	return nil
}

// RetainedAccountBytes reports the retained per-partition account
// footprint: the two streams' scalar statistics, the decay/clock
// scalars and the straddling-group cache inputs. It is constant in the
// partition's row count — the incremental state the design's resource
// model pins (the batch scratch piles are per-group transients the
// operator admits through the statement budget, not retained state).
func (e *Evaluator) RetainedAccountBytes() int {
	// The evaluator's own value (cfg/lam/h scalars, the two stream
	// accounts, clocks and caches) plus the straddling-group outputs:
	// the row view and the Walker are fixed-size operator-owned
	// references (borrowed storage), so the sum is constant in the
	// partition's row count. The layout sizes come from reflect (this
	// file keeps the repository's no-unsafe rule for files outside the
	// checker's allocation-owner allowlist) in nil-pointer form, so no
	// Evaluator value is ever copied here.
	return int(evaluatorValueSize) + int(outValueSize)
}

// evaluatorValueSize and outValueSize are the reflect equivalents of the
// two value layouts this file used to size with the unsafe package; they
// are computed once at package init.
var (
	evaluatorValueSize = reflect.TypeOf((*Evaluator)(nil)).Elem().Size()
	outValueSize       = reflect.TypeOf((*Out)(nil)).Elem().Size()
)

// publish reads the group-post outputs of both accounts.
func (e *Evaluator) publish(poll *poller) (Out, error) {
	mp := e.cfg.MinPeriods
	out := Out{W: e.xs.W, C: e.xs.C, Cp: e.ps.C}
	m, ok := e.xs.OutMean(mp)
	out.Mean, out.MeanNull = m, !ok
	if err := poll.step(); err != nil {
		return Out{}, err
	}
	v, ok := e.xs.OutVar(mp)
	out.Var, out.VarNull = v, !ok
	s, ok := e.xs.OutStd(mp)
	out.Std, out.StdNull = s, !ok
	c, ok := e.ps.OutCov(mp)
	out.Cov, out.CovNull = c, !ok
	r, ok := e.ps.OutCorr(mp)
	out.Corr, out.CorrNull = r, !ok
	return out, nil
}

// FillSlice advances the exactly-once cursor through whole peer
// groups and emits every row of [start, end) with the group-post
// value of its group (§9 publication contract): the group containing
// end−1 is completed by look-ahead and cached in flight, so a group
// straddling the slice boundary merges exactly once and every one of
// its rows — in whichever slices they fall — emits the same value.
// emit receives (row, group-post outputs); the caller combines the
// per-row value-NULL bits. An error leaves the cursor untouched.
func (e *Evaluator) FillSlice(start, end int, poll *poller,
	emit func(row int, out Out) error) error {
	if start < e.filled {
		return moerr.NewInternalErrorNoCtx("ewm: non-sequential output request")
	}
	row := start
	for row < end {
		if err := poll.step(); err != nil {
			return err
		}
		if e.cacheActive && row < e.cacheEnd {
			hi := e.cacheEnd
			if hi > end {
				hi = end
			}
			for r := row; r < hi; r++ {
				if err := emit(r, e.cacheVal); err != nil {
					return err
				}
				if err := poll.step(); err != nil {
					return err
				}
			}
			row = hi
			if e.cacheEnd <= end {
				e.cacheActive = false
			}
			continue
		}
		gs, ge := e.groupAt(row)
		if gs != row {
			return moerr.NewInternalErrorNoCtx("ewm: peer-group walker desynchronized")
		}
		if gs >= ge {
			break
		}
		var prevKeys []KeyVal
		if gs > 0 {
			prevKeys = e.rows[gs-1].Keys
		} else {
			prevKeys = nil
		}
		out, err := e.processGroupFrom(gs, ge, prevKeys, poll)
		if err != nil {
			return err
		}
		if ge <= end {
			hi := ge
			if hi > end {
				hi = end
			}
			for r := row; r < hi; r++ {
				if err := emit(r, out); err != nil {
					return err
				}
				if err := poll.step(); err != nil {
					return err
				}
			}
			row = ge
			continue
		}
		// The group straddles the slice end: merge once, cache, emit
		// this slice's part; the next slice continues from the cache.
		e.cacheStart, e.cacheEnd, e.cacheVal, e.cacheActive = gs, ge, out, true
		for r := row; r < end; r++ {
			if err := emit(r, out); err != nil {
				return err
			}
			if err := poll.step(); err != nil {
				return err
			}
		}
		row = end
	}
	e.filled = end
	return nil
}

// Evaluate runs the complete engine over one partition and returns
// every row's outputs (the oracle engine's convenience form; the
// operator uses FillSlice for resumable slices).
func Evaluate(rows []Row, cfg Config) ([]Out, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	for i := range rows {
		if rows[i].X != nil && !isFinite(*rows[i].X) {
			return nil, ErrNonFiniteSample
		}
		if rows[i].Y != nil && !isFinite(*rows[i].Y) {
			return nil, ErrNonFiniteSample
		}
	}
	e := &Evaluator{cfg: cfg}
	if cfg.Mode == ModeRow {
		e.lam = 1.0 - cfg.Alpha
	} else {
		e.h = cfg.HalfLife
	}
	e.Reset(rows, groupRows)
	outs := make([]Out, len(rows))
	nop := &poller{}
	err := e.FillSlice(0, len(rows), nop, func(row int, out Out) error {
		outs[row] = out
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i := range e.rows {
		if e.cfg.Mode == ModeTime && e.cfg.IgnoreNA {
			outs[i].XNullBit = e.rows[i].X == nil
			outs[i].PNullBit = e.rows[i].X == nil || e.rows[i].Y == nil
		}
	}
	return outs, nil
}

// GroupRows is the exported group walker: the maximal equal-key run
// that contains row r.
func GroupRows(rows []Row, r int) (int, int) {
	return groupRows(rows, r)
}

// groupRows discovers the maximal equal-key run containing r.
func groupRows(rows []Row, r int) (int, int) {
	if r < 0 || r >= len(rows) {
		return 0, 0
	}
	i := r
	for i > 0 && RowsEqual(rows[i-1].Keys, rows[r].Keys) {
		i--
	}
	j := r
	for j < len(rows) && RowsEqual(rows[j].Keys, rows[r].Keys) {
		j++
	}
	return i, j
}
