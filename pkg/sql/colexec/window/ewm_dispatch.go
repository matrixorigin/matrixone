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
	"math"
	"math/big"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/window/ewm"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// This file is the EWM window-statistics dispatch (issue #29945, §9
// executor): one resumable evaluator per window slot, whole peer groups
// at a time with an exactly-once cursor, per-partition staging of one
// materialized row view, the in-loop bounded cancellation contract and
// the value/NULL publication rules of §5–§7.

// The staging-budget sizing factors (bytes): the per-group engine
// factors cover the observation batch slices, the xs/ys fill and the
// merge's worst-route deviation piles (a 2k-term xnum pile rides
// several lanes at once; 512 bytes per group row covers the widest
// route with margin), and the rational backing is the widest big.Rat
// payload a decimal key lowers to. The staged Row's and its event-time
// KeyVal's value layouts are computed through unsafe.Sizeof in
// window.go, this package's allocation-owner file covered by the
// repository's unsafe allowlist while this dispatch file stays
// unsafe-free.
const (
	ewmObsBatchBytes     = 16
	ewmFillBytes         = 16
	ewmMergePileBytes    = 512
	ewmKeyRationalBackup = 128
)

// ewmAggState carries one window slot's EWM dispatch state: the decoded
// per-execution configuration (decode once per container generation),
// the staged row view of the partition currently being emitted, the
// partition-local peer-group directory driving the evaluator's group
// walker, the per-partition evaluator with its account statistics, the
// generation's absolute publication cursor, the current partition index
// and the statement-budget charge admitting the staged rows, keys and
// the worst-case per-group scratch.
type ewmAggState struct {
	cfg      ewm.Config
	cfgReady bool

	// the staged partition row view (absolute range [partStart, partEnd))
	partStart, partEnd int
	rows               []ewm.Row

	// spans holds the partition-local starts of the framework's full-tuple
	// peer groups (sorted, spans[0]==0): the evaluator's group walker and
	// the tie semantics of §6 come from the window framework's sorted
	// peer directory, never from re-reading key components.
	spans []int64

	// charge is the admitted worst-case byte estimate of everything the
	// staged partition retains or scratches (released on partition
	// completion, generation teardown and every error path).
	charge uint64

	// per-partition accounts (a fresh evaluator is exactly an unseen
	// partition's state)
	eval *ewm.Evaluator

	// generation-scoped cursor: the first row whose output is not filled
	// and the partition containing it
	next      int
	partition int
}

// processEwmFuncRange publishes one output slice of one ewm window
// statistic. The dispatch owns its own resumable cursor: it walks
// partitions (staging one partition's row view at a time), drives the
// evaluator's whole-group exactly-once FillSlice across the slice
// boundary, applies the §NULL-bit matrix per row and appends the
// float64 outputs in strict row order.
func (ctr *container) processEwmFuncRange(
	idx int,
	ap *Window,
	proc *process.Process,
	funcName string,
	outputStart int,
	outputEnd int,
) (_ *vector.Vector, retErr error) {
	if ctr.ewmAggs == nil {
		ctr.ewmAggs = make([]*ewmAggState, len(ap.Aggs))
	}
	if ctr.ewmAggs[idx] == nil {
		ctr.ewmAggs[idx] = &ewmAggState{}
	}
	st := ctr.ewmAggs[idx]

	if !st.cfgReady {
		// First call of the generation: decode this aggregate's own
		// config bytes ONCE and open the cursor at the slice start.
		if err := st.loadConfig(ap.Aggs[idx]); err != nil {
			return nil, err
		}
		st.next = outputStart
	}
	if outputStart != st.next {
		ctr.freeEwmState()
		return nil, moerr.NewInternalErrorNoCtx("ewm window output is not sequential")
	}
	defer func() {
		if retErr != nil {
			ctr.freeEwmState()
		}
	}()

	stat := ewmStatOf(funcName)
	values := make([]float64, 0, outputEnd-outputStart)
	nullList := make([]bool, 0, outputEnd-outputStart)

	// The bounded in-loop cancellation hook: every long EWM loop inside
	// the evaluator polls this through its 1024-step interval; the
	// publication stays atomic per group (§9).
	cancelFn := func() error {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return err
		}
		return nil
	}

	n := ctr.bat.RowCount()
	for st.next < outputEnd {
		// stage the partition containing the cursor when it changes
		pStart := 0
		if len(ctr.ps) > 0 {
			pStart = int(ctr.ps[st.partition])
		}
		pEnd := partitionEnd(ctr.ps, st.partition, n)
		if pEnd <= st.next {
			// partition consumed (its slice ended exactly at pEnd)
			st.partition++
			continue
		}
		if st.eval == nil || st.partStart != pStart {
			keyIdx := 0
			// PartitionTopN coalesces the identity partition keys into
			// the order-vector prefix; the event-time key component sits
			// at len(PartitionBy) there and at 0 on the ordinary path.
			w := ap.WinSpecList[idx].Expr.(*plan.Expr_W).W
			if ap.PartitionTopN && len(w.PartitionBy) > 0 {
				keyIdx = len(w.PartitionBy)
			}
			if err := st.stagePartition(ctr, proc, pStart, pEnd, idx, keyIdx, cancelFn); err != nil {
				return nil, err
			}
		}
		sliceEnd := outputEnd
		if sliceEnd > pEnd {
			sliceEnd = pEnd
		}
		emit := func(row int, out ewm.Out) error {
			// the callback row is partition-local (FillSlice bounds are
			// relative to the partition start), and the staged row view is
			// indexed partition-locally as well: index it with the
			// callback row directly — subtracting the absolute partition
			// start indexed before the slice on every later partition.
			v, isNull := ewmValueOf(stat, out), ewmNullOf(stat, out)
			if st.cfg.Mode == ewm.ModeTime && st.cfg.IgnoreNA {
				// event-time ignoreNA: a NULL-input row emits the NULL
				// bit; the shared value stays the group's (§6)
				if stat == ewmStatCov || stat == ewmStatCorr {
					isNull = isNull || st.rows[row].X == nil ||
						st.rows[row].Y == nil
				} else {
					isNull = isNull || st.rows[row].X == nil
				}
			}
			values = append(values, v)
			nullList = append(nullList, isNull)
			return nil
		}
		err := st.eval.FillSlice(st.next-pStart, sliceEnd-pStart,
			st.poll(cancelFn), emit)
		if err != nil {
			return nil, err
		}
		st.next = sliceEnd
		if st.next == pEnd {
			// partition complete: release the staged view and its
			// charge; the next loop iteration stages the next partition
			// with fresh account state
			st.eval = nil
			st.rows = nil
			st.spans = nil
			ctr.allocation.ReleaseMetadata(st.charge)
			st.charge = 0
			st.partStart = -1
			st.partition++
		}
	}

	vec, err := ctr.allocation.NewVector(types.T_float64.ToType())
	if err != nil {
		return nil, err
	}
	if err := vector.AppendFixedList(vec, values, nullList, proc.Mp()); err != nil {
		vec.Free(proc.Mp())
		return nil, err
	}
	return vec, nil
}

// loadConfig decodes this aggregate expression's config bytes ONCE per
// container build (§9 config channel): the bytes are immutable inputs
// of the operator arguments, produced by constructEwmConfig at this
// execution's compile boundary and never mutated here.
func (st *ewmAggState) loadConfig(ag aggexec.AggFuncExecExpression) error {
	raw := ag.GetExtraConfig()
	cfg, err := aggexec.DecodeEwmConfig(raw)
	if err != nil {
		return err
	}
	st.cfg = ewm.Config{
		Adjust:     cfg.Adjust,
		IgnoreNA:   cfg.IgnoreNA,
		MinPeriods: int(cfg.MinPeriods),
	}
	if cfg.Mode == aggexec.EwmModeTime {
		st.cfg.Mode = ewm.ModeTime
		st.cfg.HalfLife = cfg.HalfLife
	} else {
		st.cfg.Mode = ewm.ModeRow
		st.cfg.Alpha = cfg.Alpha
	}
	if err := st.cfg.Validate(); err != nil {
		return moerr.NewInternalErrorNoCtx(err.Error())
	}
	st.cfgReady = true
	return nil
}

// releaseCharge unwinds an uncommitted staging's admission charge: a
// staging that failed after its reservation leaves no charge behind
// (§9 lifecycle — the next call or the teardown starts from zero).
func (st *ewmAggState) releaseCharge(ctr *container, err error) error {
	ctr.allocation.ReleaseMetadata(st.charge)
	st.charge = 0
	return err
}

// stagePartition materializes one partition's row view and its peer-group
// directory: the staged rows borrow the sample columns' own backing
// (a constant vector keeps one physical element, so every read goes
// through a constant-aware index), the keys of event-time mode come from
// the ORDER BY component in its native comparison domain, and the tie
// groups the evaluator consumes are the window framework's full-tuple
// peer boundaries (§6) — never re-reads of order key components, whose
// earlier entries are unshuffled on the in-memory sort path. The staged
// rows, keys, the span directory and the worst-case per-group scratch
// are admitted through the statement allocation budget before
// allocation and released on every terminal path. A non-finite sample
// rejects here — the row pre-scan polls the bounded cancellation hook
// and runs before any group is processed (§7).
func (st *ewmAggState) stagePartition(
	ctr *container,
	proc *process.Process,
	pStart, pEnd, idx, keyIdx int,
	cancel func() error,
) error {
	st.partStart, st.partEnd = pStart, pEnd
	poll := ewm.NewPoller(cancel)
	// admission precedes construction (§9 lifecycle): the sizing
	// phase walks the same ledger entries the construction walk uses,
	// allocates nothing, polls the bounded hook at the fixed interval
	// and hands the exact span count and worst-case peer group to the
	// pure estimate - only after the budget admits the charge does the
	// directory materialize with its exact charged backing
	spanCount, maxGroup, szErr := st.sizeSpans(ctr, pStart, pEnd, poll)
	if szErr != nil {
		return st.releaseCharge(ctr, szErr)
	}
	est := st.estimateCharge(spanCount, maxGroup)
	if err := ctr.allocation.ReserveMetadata(proc, est); err != nil {
		return err
	}
	st.charge = est
	if err := st.buildSpans(ctr, pStart, pEnd, spanCount, poll); err != nil {
		return st.releaseCharge(ctr, err)
	}

	xVec := ctr.aggVecs[idx].Vec[0]
	if xVec.GetType().Oid != types.T_float64 {
		return st.releaseCharge(ctr, moerr.NewInternalErrorNoCtx(
			"ewm sample column must be float64 at the window dispatch"))
	}
	hasY := len(ctr.aggVecs[idx].Vec) > 1 && ctr.aggVecs[idx].Vec[1] != nil
	var yVec *vector.Vector
	if hasY {
		yVec = ctr.aggVecs[idx].Vec[1]
	}
	xCol := vector.MustFixedColWithTypeCheck[float64](xVec)
	var yCol []float64
	if hasY {
		yCol = vector.MustFixedColWithTypeCheck[float64](yVec)
	}
	st.rows = make([]ewm.Row, pEnd-pStart)
	for r := pStart; r < pEnd; r++ {
		if err := poll.Step(); err != nil {
			return st.releaseCharge(ctr, err)
		}
		row := &st.rows[r-pStart]
		// constant vectors carry one physical element: the sample
		// columns read it for every logical row (§5 constants)
		px := ewmPhysIndex(xVec, r)
		if !xVec.IsNull(uint64(r)) {
			row.X = &xCol[px]
		}
		if hasY && !yVec.IsNull(uint64(r)) {
			row.Y = &yCol[ewmPhysIndex(yVec, r)]
		}
		if st.cfg.Mode == ewm.ModeTime {
			if keyIdx >= len(ctr.orderVecs) || len(ctr.orderVecs[keyIdx].Vec) == 0 ||
				ctr.orderVecs[keyIdx].Vec[0] == nil {
				return st.releaseCharge(ctr, moerr.NewInvalidInputNoCtx(
					"invalid input: event-time ewm statistics require exactly one ORDER BY expression"))
			}
			keyVec := ctr.orderVecs[keyIdx].Vec[0]
			kv, kerr := ewmKeyAt(keyVec, ewmPhysIndex(keyVec, r))
			if kerr != nil {
				return st.releaseCharge(ctr, kerr)
			}
			row.Keys = []ewm.KeyVal{kv}
		}
	}
	eval, err := ewm.NewEvaluator(st.rows,
		ewmSpanWalker(st.spans, pEnd-pStart), st.cfg)
	if err != nil {
		return st.releaseCharge(ctr, err)
	}
	if err := eval.ValidateSamplesPolling(poll); err != nil {
		return st.releaseCharge(ctr, err)
	}
	st.eval = eval
	return nil
}

// sizeSpans is the allocation-free sizing phase of the peer directory:
// it walks the ledger's sorted entries once and yields the exact span
// count (the partition start plus every peer-group start inside
// (pStart, pEnd)) and the largest peer group. The walk polls the
// bounded cancellation hook at the fixed interval, so a canceled
// sizing aborts before any allocation exists and the partition stays
// at zero bytes (§9 admission lifecycle).
func (st *ewmAggState) sizeSpans(
	ctr *container, pStart, pEnd int, poll *ewm.Poller,
) (uint64, uint64, error) {
	n := uint64(pEnd - pStart)
	if n == 0 {
		return 0, 0, nil
	}
	spanCount := uint64(1)
	maxGroup := uint64(0)
	prev := uint64(0)
	for i := 0; i < len(ctr.os); i++ {
		if err := poll.Step(); err != nil {
			return 0, 0, err
		}
		entry := ctr.os[i]
		if entry >= int64(pEnd) {
			break
		}
		if entry > int64(pStart) {
			local := uint64(entry) - uint64(pStart)
			if local-prev > maxGroup {
				maxGroup = local - prev
			}
			prev = local
			spanCount++
		}
	}
	if n-prev > maxGroup {
		maxGroup = n - prev
	}
	return spanCount, maxGroup, nil
}

// buildSpans copies the partition-local starts of the framework's
// full-tuple peer groups: the entries of the sorted peer directory in
// [pStart, pEnd), with the partition start itself always present (a
// peer group never crosses a partition boundary, and the framework's
// accumulated diff directory includes the partition key changes). It
// runs only after admission, with the sizing phase's exact span count
// as its capacity - the backing is the charged span count exactly -
// and it polls the same shared cancellation hook per entry.
func (st *ewmAggState) buildSpans(
	ctr *container, pStart, pEnd int, count uint64, poll *ewm.Poller,
) error {
	spans := make([]int64, 0, count)
	spans = append(spans, 0)
	for i := 0; i < len(ctr.os); i++ {
		if err := poll.Step(); err != nil {
			return err
		}
		entry := ctr.os[i]
		if entry >= int64(pEnd) {
			break
		}
		if entry > int64(pStart) {
			spans = append(spans, entry-int64(pStart))
		}
	}
	st.spans = spans
	return nil
}

// estimateCharge admits the staged rows, the event-time keys, the span
// directory and the worst-case per-group scratch of the engine through
// the statement allocation budget: the per-row factors are the staged
// Row, a KeyVal with its widest rational backing and the span entry;
// the per-group factors (bounded by the largest peer group, which the
// degenerate single-tie partition makes equal to the partition) cover
// the engine's observation batches, its xs/ys fill and the merge's
// worst-route deviation piles. It is pure arithmetic over the sizing
// phase's snapshot - no unpolled scan remains between the sizing walk
// and the construction walk.
func (st *ewmAggState) estimateCharge(spanCount, maxGroup uint64) uint64 {
	n := uint64(st.partEnd - st.partStart)
	if n == 0 {
		return 0
	}
	rowBytes := uint64(ewmRowSize)
	keyBytes := uint64(0)
	if st.cfg.Mode == ewm.ModeTime {
		keyBytes = uint64(ewmKeySize + ewmKeyRationalBackup)
	}
	spanBytes := spanCount * 8
	groupScratch := uint64(ewmObsBatchBytes + ewmFillBytes + ewmMergePileBytes)
	return n*(rowBytes+keyBytes) + spanBytes + maxGroup*groupScratch
}

// ewmPhysIndex maps a logical row to the physical element of a possibly
// constant vector (constants keep one physical element).
func ewmPhysIndex(v *vector.Vector, row int) int {
	if v.IsConst() || v.IsConstNull() {
		return 0
	}
	return row
}

// ewmSpanWalker returns the peer-group walker the engine consumes for
// one staged partition: binary-search the local span start containing
// the row and clamp the group to the partition's local length.
func ewmSpanWalker(spans []int64, length int) func(rows []ewm.Row, r int) (int, int) {
	return func(rows []ewm.Row, r int) (int, int) {
		if r < 0 || r >= length {
			return 0, 0
		}
		lo, hi := 0, len(spans)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if spans[mid] <= int64(r) {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		start := 0
		if lo > 0 {
			start = int(spans[lo-1])
		}
		end := length
		if lo < len(spans) {
			end = int(spans[lo])
		}
		return start, end
	}
}

// poll adapts the operator's cancellation checker to the engine's
// poller interface (the shared bounded helper, window.go:57).
func (st *ewmAggState) poll(cancel func() error) *ewm.Poller {
	return ewm.NewPoller(cancel)
}

// ewmStat identifies which of the five statistics this slot publishes.
type ewmStat uint8

const (
	ewmStatMean ewmStat = iota
	ewmStatVar
	ewmStatStd
	ewmStatCov
	ewmStatCorr
)

func ewmStatOf(funcName string) ewmStat {
	switch funcName {
	case "ewmvar":
		return ewmStatVar
	case "ewmstd":
		return ewmStatStd
	case "ewmcov":
		return ewmStatCov
	case "ewmcorr":
		return ewmStatCorr
	default:
		return ewmStatMean
	}
}

func ewmValueOf(stat ewmStat, out ewm.Out) float64 {
	switch stat {
	case ewmStatVar:
		return out.Var
	case ewmStatStd:
		return out.Std
	case ewmStatCov:
		return out.Cov
	case ewmStatCorr:
		return out.Corr
	default:
		return out.Mean
	}
}

func ewmNullOf(stat ewmStat, out ewm.Out) bool {
	switch stat {
	case ewmStatVar:
		return out.VarNull
	case ewmStatStd:
		return out.StdNull
	case ewmStatCov:
		return out.CovNull
	case ewmStatCorr:
		return out.CorrNull
	default:
		return out.MeanNull
	}
}

// ewmKeyAt converts one ORDER BY key element into its native comparison
// domain (§5 domain table): integer and temporal keys as exact int64
// magnitudes (timestamp/datetime microseconds, dates scaled to
// microseconds), float keys as float64, decimal keys as exact rationals
// (the significand words with the column's scale).
func ewmKeyAt(keyVec *vector.Vector, row int) (ewm.KeyVal, error) {
	// constant ORDER BY keys carry one physical element read at index 0
	// through every logical row (§5 constants)
	if keyVec == nil || keyVec.Length() <= ewmPhysIndex(keyVec, row) {
		return ewm.KeyVal{}, moerr.NewInternalErrorNoCtx(
			"ewm dispatch: missing ORDER BY key vector")
	}
	if keyVec.IsNull(uint64(row)) {
		return ewm.KeyVal{}, moerr.NewInvalidInputNoCtx(
			"invalid input: ewm event-time ORDER BY keys must be non-NULL")
	}
	switch keyVec.GetType().Oid {
	case types.T_int8:
		return ewm.IntKey(int64(vector.MustFixedColWithTypeCheck[int8](keyVec)[ewmPhysIndex(keyVec, row)])), nil
	case types.T_int16:
		return ewm.IntKey(int64(vector.MustFixedColWithTypeCheck[int16](keyVec)[ewmPhysIndex(keyVec, row)])), nil
	case types.T_int32:
		return ewm.IntKey(int64(vector.MustFixedColWithTypeCheck[int32](keyVec)[ewmPhysIndex(keyVec, row)])), nil
	case types.T_int64:
		return ewm.IntKey(vector.MustFixedColWithTypeCheck[int64](keyVec)[ewmPhysIndex(keyVec, row)]), nil
	case types.T_uint8:
		return ewm.IntKey(int64(vector.MustFixedColWithTypeCheck[uint8](keyVec)[ewmPhysIndex(keyVec, row)])), nil
	case types.T_uint16:
		return ewm.IntKey(int64(vector.MustFixedColWithTypeCheck[uint16](keyVec)[ewmPhysIndex(keyVec, row)])), nil
	case types.T_uint32:
		return ewm.IntKey(int64(vector.MustFixedColWithTypeCheck[uint32](keyVec)[ewmPhysIndex(keyVec, row)])), nil
	case types.T_uint64:
		v := vector.MustFixedColWithTypeCheck[uint64](keyVec)[ewmPhysIndex(keyVec, row)]
		// the full [0, MaxUint64] domain: the preserved bit pattern rides
		// KeyInt and the engine's wrap-aware unsigned subtraction
		// computes every gap exactly, including keys beyond the signed
		// fence (§5 unsigned-magnitude acceptance vector)
		return ewm.IntKey(int64(v)), nil
	case types.T_float32:
		f := float64(vector.MustFixedColWithTypeCheck[float32](keyVec)[ewmPhysIndex(keyVec, row)])
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return ewm.KeyVal{}, moerr.NewInvalidInputNoCtx(
				"invalid input: ewm event-time ORDER BY keys must be finite")
		}
		return ewm.FloatKey(f), nil
	case types.T_float64:
		f := vector.MustFixedColWithTypeCheck[float64](keyVec)[ewmPhysIndex(keyVec, row)]
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return ewm.KeyVal{}, moerr.NewInvalidInputNoCtx(
				"invalid input: ewm event-time ORDER BY keys must be finite")
		}
		return ewm.FloatKey(f), nil
	case types.T_timestamp:
		return ewm.IntKey(int64(vector.MustFixedColWithTypeCheck[types.Timestamp](keyVec)[ewmPhysIndex(keyVec, row)])), nil
	case types.T_datetime:
		return ewm.IntKey(int64(vector.MustFixedColWithTypeCheck[types.Datetime](keyVec)[ewmPhysIndex(keyVec, row)])), nil
	case types.T_date:
		d := vector.MustFixedColWithTypeCheck[types.Date](keyVec)[ewmPhysIndex(keyVec, row)]
		return ewm.IntKey(int64(uint32(d)) * 86400 * 1000000), nil
	case types.T_decimal64:
		v := vector.MustFixedColWithTypeCheck[types.Decimal64](keyVec)[ewmPhysIndex(keyVec, row)]
		return ewm.DecKey(ewmDecimalRat(
			big.NewInt(int64(uint64(v))), keyVec.GetType().Scale)), nil
	case types.T_decimal128:
		v := vector.MustFixedColWithTypeCheck[types.Decimal128](keyVec)[ewmPhysIndex(keyVec, row)]
		hi := big.NewInt(int64(v.B64_127))
		hi.Lsh(hi, 64)
		lo := new(big.Int).SetUint64(v.B0_63)
		sig := hi.Add(hi, lo)
		return ewm.DecKey(ewmDecimalRat(sig, keyVec.GetType().Scale)), nil
	case types.T_decimal256:
		v := vector.MustFixedColWithTypeCheck[types.Decimal256](keyVec)[ewmPhysIndex(keyVec, row)]
		hi := big.NewInt(int64(v.B192_255))
		hi.Lsh(hi, 192)
		m2 := new(big.Int).SetUint64(v.B128_191)
		m2.Lsh(m2, 128)
		m1 := new(big.Int).SetUint64(v.B64_127)
		m1.Lsh(m1, 64)
		lo := new(big.Int).SetUint64(v.B0_63)
		sig := hi.Add(hi, m2)
		sig.Add(sig, m1)
		sig.Add(sig, lo)
		return ewm.DecKey(ewmDecimalRat(sig, keyVec.GetType().Scale)), nil
	default:
		return ewm.KeyVal{}, moerr.NewInvalidInputNoCtx(
			"event-time ewm statistics require a numeric or temporal ORDER BY key")
	}
}

// ewmDecimalRat forms the exact rational of a decimal significand at
// the given binary scale.
// ewmDecimalRat forms the exact rational of a decimal significand at
// its base-10^scale: 1.5 at scale 1 is significand 15 over 10^1, never
// over 2^1 (a binary-side scale would shrink every decimal key gap and
// over-decay the event-time weights).
func ewmDecimalRat(sig *big.Int, scale int32) *big.Rat {
	if scale <= 0 {
		scale = 0
	}
	den := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	r := new(big.Rat)
	r.SetFrac(sig, den)
	return r
}
