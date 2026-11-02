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

import "math"

// Stream carries one account's normalized state
// (W, mu_x, mu_y, vxx, vyy, vxy, C). Transitions scale only W; peers
// merge through the layered scale- and range-safe paths. Account
// slots hold one float64 or one extended pair (mx/mxLo/my/myLo words
// plus the centered moments, which may retain out-of-range magnitude
// or subnormal-lattice content).
type Stream struct {
	W    float64
	mx   float64
	my   float64
	mxLo xnum
	myLo xnum
	vxx  xnum
	vyy  xnum
	vxy  xnum
	C    int
}

// Reset restores the unseen state (exactly a fresh partition).
func (s *Stream) Reset() {
	s.W = 0.0
	s.mx = 0.0
	s.my = 0.0
	s.mxLo = scalar(0.0)
	s.myLo = scalar(0.0)
	s.vxx = scalar(0.0)
	s.vyy = scalar(0.0)
	s.vxy = scalar(0.0)
	s.C = 0
}

// Transition scales the whole mass by g (the decay between groups).
func (s *Stream) Transition(g float64) { s.W *= g }

// merge folds one arrival batch (k observations sharing the batch
// total mass A): compensated batch statistics with the identity fast
// path, the zero-mass early branch, and the layered merge — the
// scaled mass-form primary, the gated share layers, then the
// absolute-mass layer with weighted-difference identities. Every
// batch-sized pass polls the shared cancellation hook; a cancellation
// aborts the remaining sweep and returns its cause with the account
// left uncommitted (the generation tearing down on error is the
// observable cleanup contract).
func (s *Stream) merge(xs, ys []float64, A float64, poll *poller) error {
	k := len(xs)
	if k == 0 {
		return nil
	}
	s.C += k
	if A == 0.0 {
		return nil // zero-weight arrival: state carried
	}
	var mbx, mby float64
	var lox, loy xnum
	var sxx, syy, sxy xnum
	var devfinX, devfinY, latx, laty bool

	identX := true
	for _, v := range xs {
		if err := poll.step(); err != nil {
			return err
		}
		if v != xs[0] {
			identX = false
			break
		}
	}
	if identX {
		mbx = xs[0]
		sxx = scalar(0.0)
		lox = scalar(0.0)
		devfinX, latx = true, false
	} else {
		mbx, lox, sxx, devfinX, latx = batchSide(xs, k, poll)
		if err := poll.ok(); err != nil {
			return err
		}
	}
	identY := true
	for _, v := range ys {
		if err := poll.step(); err != nil {
			return err
		}
		if v != ys[0] {
			identY = false
			break
		}
	}
	if identY {
		mby = ys[0]
		syy = scalar(0.0)
		loy = scalar(0.0)
		devfinY, laty = true, false
	} else {
		mby, loy, syy, devfinY, laty = batchSide(ys, k, poll)
		if err := poll.ok(); err != nil {
			return err
		}
	}

	latxy := latx || laty
	if devfinX && devfinY && !latxy {
		// The common batch-cross contract: exact twosum deviations,
		// error-free products over the sub-parts, the mean-tail
		// correction (-k*taux*tay) folded as error-free pieces gated
		// by VALUE, the xsum4 register cascade (full cancellation
		// admitted), and ONE quotient fold; out-of-range quotients
		// keep their retained shape.
		items := make([]xnum, 0, 2*len(xs)+4)
		for i := range xs {
			if err := poll.step(); err != nil {
				return err
			}
			dxh, dxl := twosum(xs[i], -mbx)
			dyh, dyl := twosum(ys[i], -mby)
			xdevItems(dxh, dxl, dyh, dyl, &items)
		}
		if tailNonZero(lox) && tailNonZero(loy) {
			appendTailCorrection(lox, loy, k, &items)
		}
		sf := xdiv(xsum4E(items, poll), xfrexp(scalar(float64(k))))
		if err := poll.ok(); err != nil {
			return err
		}
		sfv := xval(sf)
		if isFinite(sfv) {
			sxy = scalar(sfv)
		} else {
			sxy = sf
		}
	} else if devfinX && devfinY && latxy {
		// The lattice route keeps the extended channel that retains
		// the subnormal-lattice covariance; the same error-free
		// contract, and the signed cross cascades through the
		// registers with the remainder lane keeping the second word.
		items := make([]xnum, 0, 2*len(xs)+2)
		for i := range xs {
			if err := poll.step(); err != nil {
				return err
			}
			dxh, dxl := twosum(xs[i], -mbx)
			dyh, dyl := twosum(ys[i], -mby)
			xdevItems(dxh, dxl, dyh, dyl, &items)
		}
		xanchorKItems([]xnum{lox}, []xnum{loy}, float64(k), &items)
		sxy = xqCascade(items, xfrexp(scalar(float64(k))), poll)
		if err := poll.ok(); err != nil {
			return err
		}
	} else {
		// At least one side's raw mean-deviation overflowed — anchor
		// both sides (the narrowed mean on a finite-deviation side,
		// the exact range midpoint on an overflowing side); no raw
		// deviation and no infinite operand is ever formed.
		anchx := mbx
		if !devfinX {
			anchx = minOf(xs, poll)/2 + maxOf(xs, poll)/2
		}
		anchy := mby
		if !devfinY {
			anchy = minOf(ys, poll)/2 + maxOf(ys, poll)/2
		}
		if err := poll.ok(); err != nil {
			return err
		}
		items := make([]xnum, 0, 2*len(xs)+4)
		for i := range xs {
			if err := poll.step(); err != nil {
				return err
			}
			dxh, dxl := twosum(xs[i], -anchx)
			dyh, dyl := twosum(ys[i], -anchy)
			xdevItems(dxh, dxl, dyh, dyl, &items)
		}
		var fxp, fyp []xnum
		if devfinX {
			fxp = []xnum{lox}
		} else {
			hx, hl := twosum(mbx, -anchx)
			fxp = []xnum{scalar(hx), scalar(hl), lox}
		}
		if devfinY {
			fyp = []xnum{loy}
		} else {
			hy, hly := twosum(mby, -anchy)
			fyp = []xnum{scalar(hy), scalar(hly), loy}
		}
		xanchorKItems(fxp, fyp, float64(k), &items)
		sxy = xqCascade(items, xfrexp(scalar(float64(k))), poll)
	}

	if err := poll.ok(); err != nil {
		return err
	}
	if s.W == 0.0 {
		// degenerate branch: unseen or fully decayed mass
		s.W = A
		s.mx, s.my = mbx, mby
		s.mxLo, s.myLo = lox, loy
		s.vxx, s.vyy, s.vxy = sxx, syy, sxy
		return nil
	}
	extState := s.vxx.isPair || s.vyy.isPair || s.vxy.isPair ||
		sxx.isPair || syy.isPair || sxy.isPair
	if s.mxLo.isPair || s.myLo.isPair || lox.isPair || loy.isPair {
		extState = true
	}
	committed := false
	if !extState {
		committed = s.mergeScaled(mbx, lox, mby, loy, sxx, syy, sxy, A, poll)
	}
	if !committed {
		s.mergeAbs(mbx, lox, mby, loy, sxx, syy, sxy, A, poll)
	}
	if err := poll.ok(); err != nil {
		return err
	}
	s.W = s.W + A
	return nil
}

func minOf(xs []float64, poll *poller) float64 {
	m := xs[0]
	for _, v := range xs[1:] {
		if poll.step() != nil {
			break
		}
		if v < m {
			m = v
		}
	}
	return m
}

func maxOf(xs []float64, poll *poller) float64 {
	m := xs[0]
	for _, v := range xs[1:] {
		if poll.step() != nil {
			break
		}
		if v > m {
			m = v
		}
	}
	return m
}

// appendTailCorrection folds the -k*taux*tay cross/center correction
// pieces for two nonzero tails (each scaled once and negated).
func appendTailCorrection(lox, loy xnum, k int, items *[]xnum) {
	t2h, t2l := xtwoprod(lox, loy)
	if t2h.m != 0.0 {
		for _, pr := range [][2]xnum{
			{t2h, xfrexp(scalar(float64(k)))},
			{t2l, xfrexp(scalar(float64(k)))},
		} {
			p2h, p2l := xtwoprod(pr[0], pr[1])
			if p2h.m != 0.0 {
				*items = append(*items, xneg(p2h))
			}
			if p2l.m != 0.0 {
				*items = append(*items, xneg(p2l))
			}
		}
	}
}

// appendTailSquare folds the -k*taux^2 (or -k*tauy^2) mean-tail
// correction pieces for one nonzero tail.
func appendTailSquare(lo xnum, k int, items *[]xnum) {
	t2h, t2l := xtwoprod(lo, lo)
	if t2h.m != 0.0 {
		for _, pr := range [][2]xnum{
			{t2h, xfrexp(scalar(float64(k)))},
			{t2l, xfrexp(scalar(float64(k)))},
		} {
			p2h, p2l := xtwoprod(pr[0], pr[1])
			if p2h.m != 0.0 {
				*items = append(*items, xneg(p2h))
			}
			if p2l.m != 0.0 {
				*items = append(*items, xneg(p2l))
			}
		}
	}
}

// batchSide computes one side's narrowed mean, compensated tail,
// centered moment and route flags for one observation batch. Every
// k-sized sweep polls the shared cancellation hook and aborts on a
// dead poller; the merge layer discards partial results on its dead
// check afterwards.
func batchSide(xs []float64, k int, poll *poller) (float64, xnum, xnum, bool, bool) {
	var mbx float64
	var lox xnum
	var sxx xnum
	var devfinX, latx bool
	sx, cx, cxl := neumaierPair(xs, poll)
	if poll.canceled() {
		return 0, zeroPair, zeroPair, false, false
	}
	kf := float64(k)
	mbx = (sx + cx + cxl) / kf
	if !isFinite(mbx) {
		// The shifted recurrence still overflows at the subtraction
		// for opposite-sign extremes — anchor at the exact range
		// midpoint (|a| and every |x_i - a| bounded by max float64)
		// and accumulate the per-observation (x_i - a)/k terms.
		a := minOf(xs, poll)/2 + maxOf(xs, poll)/2
		if poll.canceled() {
			return 0, zeroPair, zeroPair, false, false
		}
		shifted := make([]float64, k)
		for i, x := range xs {
			if poll.step() != nil {
				return 0, zeroPair, zeroPair, false, false
			}
			shifted[i] = (x - a) / kf
		}
		sa, ca, caLo := neumaierPair(shifted, poll)
		if poll.canceled() {
			return 0, zeroPair, zeroPair, false, false
		}
		mbx = a + (sa + ca + caLo)
		// The tail is the GENUINE complete batch residual rebuilt by
		// the ONE shared genuine_tail reconstruction from the
		// ORIGINAL observations; the narrowed lead commits only when
		// it carries the pair's full content.
		tta := genuineTail(xs, kf, mbx, poll)
		if poll.canceled() {
			return 0, zeroPair, zeroPair, false, false
		}
		lox = scalar(xval(tta))
		if laneRetains(tta) {
			lox = tta
		}
	} else {
		// The narrowed float64 mean is bit-identical; the compensated
		// tail keeps the exact batch residual — rebuilt from ALL
		// compensated registers and re-sourced to the shared
		// genuine_tail reconstruction, committed in the retained
		// pair channel whenever the float64 tail cannot carry it.
		p, pe := twoprod(kf, mbx)
		h, r := twosum(sx, -p)
		lox = scalar(neumaier([]float64{h, r, cx, cxl, -pe}) / kf)
		tta := genuineTail(xs, kf, mbx, poll)
		if poll.canceled() {
			return 0, zeroPair, zeroPair, false, false
		}
		if !isFinite(p) || laneRetains(tta) {
			lox = tta
		}
	}
	dxs := make([]float64, k)
	devfinX = true
	latx = false
	for i, x := range xs {
		if poll.step() != nil {
			return 0, zeroPair, zeroPair, false, false
		}
		d := x - mbx
		dxs[i] = d
		if !isFinite(d) {
			devfinX = false
		} else if d != 0.0 {
			_, e := math.Frexp(abs64(d))
			if e <= -511 {
				latx = true
			}
		}
	}
	if devfinX && !latx {
		// The common batch-moment contract: each deviation forms
		// exactly (twosum against the narrowed mean) and each square
		// contributes its error-free pieces; the tail identity
		// correction joins the numerator gated by VALUE, the pile
		// cascades through ddsum, dddiv keeps the quotient's exact
		// remainder, and a single final fold narrows once.
		items := make([]xnum, 0, 2*k+4)
		for _, xv := range xs {
			if poll.step() != nil {
				return 0, zeroPair, zeroPair, false, false
			}
			dh, dl := twosum(xv, -mbx)
			xdevItems(dh, dl, dh, dl, &items)
		}
		if tailNonZero(lox) {
			appendTailSquare(lox, k, &items)
		}
		nh, nl := ddsumPoll(items, poll)
		if poll.canceled() {
			return 0, zeroPair, zeroPair, false, false
		}
		qh, ql := dddiv(nh, nl, xfrexp(scalar(kf)))
		sf := xadd(qh, ql)
		sfv := xval(sf)
		if isFinite(sfv) {
			sxx = scalar(sfv)
		} else {
			sxx = sf
		}
	} else if devfinX && latx {
		// The lattice route keeps the extended channel that retains
		// the subnormal-lattice moment (std/corr stay computable).
		items := make([]xnum, 0, 2*k+2)
		for _, xv := range xs {
			if poll.step() != nil {
				return 0, zeroPair, zeroPair, false, false
			}
			dh, dl := twosum(xv, -mbx)
			xdevItems(dh, dl, dh, dl, &items)
		}
		xanchorKItems([]xnum{lox}, []xnum{lox}, kf, &items)
		nh, nl := ddsumPoll(items, poll)
		if poll.canceled() {
			return 0, zeroPair, zeroPair, false, false
		}
		qh, ql := dddiv(nh, nl, xfrexp(scalar(kf)))
		sxx = xadd(qh, ql)
	} else {
		// A raw mean-deviation can overflow although the mean is
		// finite — this path forms NO raw deviation: every term
		// subtracts from the exact range midpoint first and
		// evaluates sum (x_i - mu)^2 = sum (x_i - a)^2 - k*(mu-a)^2
		// in the extended compensated form with the tail-carrying
		// mean, all error-free.
		a := minOf(xs, poll)/2 + maxOf(xs, poll)/2
		if poll.canceled() {
			return 0, zeroPair, zeroPair, false, false
		}
		items := make([]xnum, 0, 2*k+8)
		for _, xv := range xs {
			if poll.step() != nil {
				return 0, zeroPair, zeroPair, false, false
			}
			dh, dl := twosum(xv, -a)
			xdevItems(dh, dl, dh, dl, &items)
		}
		hx, hl := twosum(mbx, -a)
		xanchorKItems([]xnum{scalar(hx), scalar(hl), lox},
			[]xnum{scalar(hx), scalar(hl), lox}, kf, &items)
		nh, nl := ddsumPoll(items, poll)
		if poll.canceled() {
			return 0, zeroPair, zeroPair, false, false
		}
		qh, ql := dddiv(nh, nl, xfrexp(scalar(kf)))
		sxx = xadd(qh, ql)
	}
	return mbx, lox, sxx, devfinX, latx
}

// mergeScaled is the primary power-of-two-scaled mass form, then the
// direct/convex-share paths; deviations re-center on the complete
// carried means and the retention gates decide on the WEIGHTED
// products. It returns true when a layer committed; otherwise the
// caller recomputes with absolute masses.
func (s *Stream) mergeScaled(mbx float64, lox xnum, mby float64, loy xnum,
	sxx, syy, sxy xnum, A float64, poll *poller) bool {
	for _, v := range []xnum{s.vxx, s.vyy, s.vxy, sxx, syy, sxy} {
		if v.isPair || v.m == 0.0 {
			continue
		}
		if abs64(v.m) < minNormal {
			return false
		}
		if _, e := Frexpf(abs64(v.m)); e <= -511 {
			return false
		}
	}
	m := s.W
	if A > m {
		m = A
	}
	_, e := Frexpf(m) // m = mant * 2^e, mant in [0.5, 1)
	w1 := Ldexpf(s.W, -e)
	w2 := Ldexpf(A, -e)
	degenerateScaling := (w2 == 0.0 && A > 0.0) || (w1 == 0.0 && s.W > 0.0)
	exactScaling := Ldexpf(w1, e) == s.W && Ldexpf(w2, e) == A
	eqx := mbx == s.mx && xeq(lox, s.mxLo)
	eqy := mby == s.my && xeq(loy, s.myLo)
	inputsFinite := isFinite(s.W) && isFinite(A) &&
		isFinite(s.mx) && isFinite(s.my) &&
		isFinite(mbx) && isFinite(mby) &&
		isFinite(s.vxx.m) && isFinite(s.vyy.m) && isFinite(s.vxy.m) &&
		isFinite(sxx.m) && isFinite(syy.m) && isFinite(sxy.m)
	committed := false
	if !degenerateScaling && exactScaling {
		wtot := w1 + w2
		// Merge-level equality fast path: a state and arrival whose
		// COMPLETE means coincide are preserved exactly (constant
		// streams keep variance 0 / corr NULL at any magnitude).
		var nmx, nmy float64
		var nloX, nloY xnum
		if eqx {
			nmx = s.mx
			nloX = s.mxLo
		} else {
			nmx = (mul64(w1, s.mx) + mul64(w2, mbx)) / wtot
			p1, e1 := twoprod(w1, s.mx)
			p2, e2 := twoprod(w2, mbx)
			p3, e3 := twoprod(wtot, nmx)
			nloX = scalar(neumaier([]float64{
				p1, e1, w1 * s.mxLo.m,
				p2, e2, w2 * lox.m,
				-p3, -e3}) / wtot)
		}
		if eqy {
			nmy = s.my
			nloY = s.myLo
		} else {
			nmy = (mul64(w1, s.my) + mul64(w2, mby)) / wtot
			p1, e1 := twoprod(w1, s.my)
			p2, e2 := twoprod(w2, mby)
			p3, e3 := twoprod(wtot, nmy)
			nloY = scalar(neumaier([]float64{
				p1, e1, w1 * s.myLo.m,
				p2, e2, w2 * loy.m,
				-p3, -e3}) / wtot)
		}
		dx := (s.mx - nmx) + (s.mxLo.m - nloX.m)
		dy := (s.my - nmy) + (s.myLo.m - nloY.m)
		dbx := (mbx - nmx) + (lox.m - nloX.m)
		dby := (mby - nmy) + (loy.m - nloY.m)
		for _, dv := range []float64{dx, dy, dbx, dby} {
			if dv != 0.0 {
				if _, de := Frexpf(abs64(dv)); de <= -511 {
					return false
				}
			}
		}
		// Retention is decided on the WEIGHTED PRODUCTS: a nonzero
		// moment or deviation whose product with its side's scaled
		// weight would land on the subnormal lattice rebinds to the
		// absolute-mass layer.
		for _, gw := range [][2]xnum{
			{s.vxx, scalar(w1)}, {s.vyy, scalar(w1)}, {s.vxy, scalar(w1)},
			{sxx, scalar(w2)}, {syy, scalar(w2)}, {sxy, scalar(w2)},
		} {
			val, wgt := gw[0], gw[1]
			if val.isPair || val.m == 0.0 {
				continue
			}
			_, ve := Frexpf(val.m)
			_, we := Frexpf(wgt.m)
			if ve+we <= -1021 {
				return false
			}
		}
		for _, dd := range [][3]float64{
			{dx, dx, w1}, {dbx, dbx, w2},
			{dy, dy, w1}, {dby, dby, w2},
			{dx, dy, w1}, {dbx, dby, w2},
		} {
			if dd[0] == 0.0 || dd[1] == 0.0 {
				continue
			}
			_, a1 := Frexpf(abs64(dd[0]))
			_, b1 := Frexpf(abs64(dd[1]))
			_, g1 := Frexpf(dd[2])
			if a1+b1+g1 <= -1021 {
				return false
			}
		}
		nvxx := (mul64(w1, s.vxx.m+sq(dx)) + mul64(w2, sxx.m+sq(dbx))) / wtot
		nvyy := (mul64(w1, s.vyy.m+sq(dy)) + mul64(w2, syy.m+sq(dby))) / wtot
		nvxy := (mul64(w1, s.vxy.m+mul64(dx, dy)) + mul64(w2, sxy.m+mul64(dbx, dby))) / wtot
		if !inputsFinite ||
			(isFinite(nmx) && isFinite(nmy) && isFinite(nvxx) &&
				isFinite(nvyy) && isFinite(nvxy)) {
			// Finite inputs, finite results: commit the scaled form
			// with the canonical complete-mean normalization before
			// the store.
			wa, wb := meanPairNorm(scalar(nmx), nloX)
			nmx = wa.m
			nloX = wb
			ya, yb := meanPairNorm(scalar(nmy), nloY)
			nmy = ya.m
			nloY = yb
			s.vxx = scalar(nvxx)
			s.vyy = scalar(nvyy)
			s.vxy = scalar(nvxy)
			s.mx = nmx
			s.my = nmy
			s.mxLo = nloX
			s.myLo = nloY
			committed = true
		}
	}
	if !committed {
		// Overflow / extreme-ratio path: convex shares with each
		// moment term computed as d*(d*s): the weight sits INSIDE the
		// product, so every term equals its true weighted share and
		// cannot overflow while the result is representable. For the
		// degenerate extreme-ratio case the shares are formed
		// directly from the raw masses down to the smallest
		// subnormal.
		var s1, s2, denom, ma1, ma2 float64
		if degenerateScaling || !exactScaling {
			nsum := s.W + A
			if isFinite(nsum) && nsum > 0.0 {
				s1 = s.W / nsum
				s2 = A / nsum
			} else {
				s1, s2 = 1.0, 0.0 // cannot happen for positive finite masses
			}
			denom = nsum
			ma1, ma2 = s.W, A
		} else {
			wtot := w1 + w2
			s1 = w1 / wtot
			s2 = w2 / wtot
			denom = wtot
			ma1, ma2 = w1, w2
		}
		var nmx, nmy float64
		var nloX, nloY xnum
		if eqx {
			nmx = s.mx
			nloX = s.mxLo
		} else {
			nmx = mul64(s1, s.mx) + mul64(s2, mbx)
			p1, e1 := twoprod(ma1, s.mx)
			p2, e2 := twoprod(ma2, mbx)
			p3, e3 := twoprod(denom, nmx)
			nloX = scalar(neumaier([]float64{
				p1, e1, ma1 * s.mxLo.m,
				p2, e2, ma2 * lox.m,
				-p3, -e3}) / denom)
		}
		if eqy {
			nmy = s.my
			nloY = s.myLo
		} else {
			nmy = mul64(s1, s.my) + mul64(s2, mby)
			p1, e1 := twoprod(ma1, s.my)
			p2, e2 := twoprod(ma2, mby)
			p3, e3 := twoprod(denom, nmy)
			nloY = scalar(neumaier([]float64{
				p1, e1, ma1 * s.myLo.m,
				p2, e2, ma2 * loy.m,
				-p3, -e3}) / denom)
		}
		dx := (s.mx - nmx) + (s.mxLo.m - nloX.m)
		dy := (s.my - nmy) + (s.myLo.m - nloY.m)
		dbx := (mbx - nmx) + (lox.m - nloX.m)
		dby := (mby - nmy) + (loy.m - nloY.m)
		for _, dv := range []float64{dx, dy, dbx, dby} {
			if dv != 0.0 {
				if _, de := Frexpf(abs64(dv)); de <= -511 {
					return false
				}
			}
		}
		for _, gw := range [][2]xnum{
			{s.vxx, scalar(s1)}, {s.vyy, scalar(s1)}, {s.vxy, scalar(s1)},
			{sxx, scalar(s2)}, {syy, scalar(s2)}, {sxy, scalar(s2)},
		} {
			val, wgt := gw[0], gw[1]
			if val.isPair || val.m == 0.0 {
				continue
			}
			_, ve := Frexpf(val.m)
			_, we := Frexpf(wgt.m)
			if ve+we <= -1021 {
				return false
			}
		}
		for _, dd := range [][3]float64{
			{dx, dx, s1}, {dbx, dbx, s2},
			{dy, dy, s1}, {dby, dby, s2},
			{dx, dy, s1}, {dbx, dby, s2},
		} {
			if dd[0] == 0.0 || dd[1] == 0.0 {
				continue
			}
			_, a1 := Frexpf(abs64(dd[0]))
			_, b1 := Frexpf(abs64(dd[1]))
			_, g1 := Frexpf(dd[2])
			if a1+b1+g1 <= -1021 {
				return false
			}
		}
		tvxx := mul64(s1, s.vxx.m) + mul64(dx, mul64(dx, s1)) + mul64(s2, sxx.m) + mul64(dbx, mul64(dbx, s2))
		tvyy := mul64(s1, s.vyy.m) + mul64(dy, mul64(dy, s1)) + mul64(s2, syy.m) + mul64(dby, mul64(dby, s2))
		tvxy := mul64(s1, s.vxy.m) + mul64(dx, mul64(dy, s1)) + mul64(s2, sxy.m) + mul64(dbx, mul64(dby, s2))
		// A share that rounds a POSITIVE side's mass to zero drops a
		// representable contribution, and a subnormal share can
		// understate its mass — both rebind to the absolute-mass
		// layer.
		sideDrop := (s1 == 0.0 && s.W > 0.0) || (s2 == 0.0 && A > 0.0)
		shareLossy := (0.0 < s1 && s1 < minNormal) || (0.0 < s2 && s2 < minNormal)
		safeOk := isFinite(nmx) && isFinite(nmy) &&
			isFinite(tvxx) && isFinite(tvyy) && isFinite(tvxy)
		if !sideDrop && !shareLossy && (safeOk || !inputsFinite) {
			wa, wb := meanPairNorm(scalar(nmx), nloX)
			nmx = wa.m
			nloX = wb
			ya, yb := meanPairNorm(scalar(nmy), nloY)
			nmy = ya.m
			nloY = yb
			s.vxx = scalar(tvxx)
			s.vyy = scalar(tvyy)
			s.vxy = scalar(tvxy)
			s.mx = nmx
			s.my = nmy
			s.mxLo = nloX
			s.myLo = nloY
			committed = true
		}
	}
	return committed
}

// mergeAbs is the layer (c) absolute-mass merge with weighted-
// difference identities: no raw difference and no materialized share
// is ever evaluated; operands may be float64 or extended pairs. The
// complete means are true double-double pairs at every step; the
// products form error-free (ddsum_e/dddiv); merged tails pack through
// the shared pack_remainder and store through the shared lane_retains
// rule (Invariant 40 / Decision 64).
func (s *Stream) mergeAbs(mbx float64, lox xnum, mby float64, loy xnum,
	sxx, syy, sxy xnum, A float64, poll *poller) {
	W := s.W
	T := xadd(scalar(W), scalar(A))
	eqx := mbx == s.mx && xeq(lox, s.mxLo)
	eqy := mby == s.my && xeq(loy, s.myLo)
	var nmx, nmy float64
	var nulx, nuly [2]xnum
	haveNulx, haveNuly := false, false
	if eqx {
		nmx = s.mx // exact fast path (constants)
	} else {
		nh, nl := ddsumE([]eterm{
			{scalar(W), scalar(s.mx), false},
			{scalar(A), scalar(mbx), false},
			{scalar(W), s.mxLo, false},
			{scalar(A), lox, false},
		})
		qh, ql := dddiv(nh, nl, T)
		// The merged dd pair folds through the shared complete-mean
		// normalization — the narrow word is fl of the COMPLETE
		// lead+tail value and the exact fold residue stays in the
		// tail slot.
		fh, fl, flhas := xfold(qh, ql)
		if flhas && fl.m != 0.0 {
			nulx = [2]xnum{fh, fl}
		} else {
			nulx = [2]xnum{fh, zeroPair}
		}
		haveNulx = true
		nmx = xval(nulx[0])
	}
	if eqy {
		nmy = s.my
	} else {
		nh, nl := ddsumE([]eterm{
			{scalar(W), scalar(s.my), false},
			{scalar(A), scalar(mby), false},
			{scalar(W), s.myLo, false},
			{scalar(A), loy, false},
		})
		qh, ql := dddiv(nh, nl, T)
		fh, fl, flhas := xfold(qh, ql)
		if flhas && fl.m != 0.0 {
			nuly = [2]xnum{fh, fl}
		} else {
			nuly = [2]xnum{fh, zeroPair}
		}
		haveNuly = true
		nmy = xval(nuly[0])
	}
	// The COMPLETE anchors: on an equality fast path the target is the
	// complete state mean itself, so every deviation is exactly zero.
	tx0, tx1 := scalar(s.mx), s.mxLo
	if haveNulx {
		tx0, tx1 = nulx[0], nulx[1]
	}
	ty0, ty1 := scalar(s.my), s.myLo
	if haveNuly {
		ty0, ty1 = nuly[0], nuly[1]
	}
	sqTerm := func(sv xnum, slo xnum, t0, t1 xnum, mass float64) xnum {
		// mass * ((s + slo) - t)^2 — the weighted difference and the
		// product both survive as double-double; the correction
		// products replace any representable-difference fast paths.
		if mass <= 0.0 {
			return zeroPair
		}
		dh, dl := ddsub(sv, slo, t0, t1)
		if dh.m == 0.0 && dl.m == 0.0 {
			return zeroPair
		}
		qh, ql := ddsumE([]eterm{
			{scalar(mass), sv, false},
			{scalar(mass), slo, false},
			{scalar(mass), t0, true},
			{scalar(mass), t1, true},
		})
		return ddMul(qh, ql, dh, dl)
	}
	crossTerm := func(sa, salo xnum, ta0, ta1 xnum, sb, sblo xnum, tb0, tb1 xnum, mass float64) xnum {
		// mass * ((sa + salo) - ta) * ((sb + sblo) - tb) with BOTH
		// differences carried as double-double pairs through the
		// weighted products (each side anchors its own dd target).
		if mass <= 0.0 {
			return zeroPair
		}
		dah, dal := ddsub(sa, salo, ta0, ta1)
		if dah.m == 0.0 && dal.m == 0.0 {
			return zeroPair
		}
		dbh, dbl := ddsub(sb, sblo, tb0, tb1)
		if dbh.m == 0.0 && dbl.m == 0.0 {
			return zeroPair
		}
		qah, qal := ddsumE([]eterm{
			{scalar(mass), sa, false},
			{scalar(mass), salo, false},
			{scalar(mass), ta0, true},
			{scalar(mass), ta1, true},
		})
		return ddMul(qah, qal, dbh, dbl)
	}
	nvxx := xdiv(xsum4([]xnum{
		sqTerm(scalar(s.mx), s.mxLo, tx0, tx1, W),
		sqTerm(scalar(mbx), lox, tx0, tx1, A),
		xmul(scalar(W), s.vxx),
		xmul(scalar(A), sxx),
	}), T)
	nvyy := xdiv(xsum4([]xnum{
		sqTerm(scalar(s.my), s.myLo, ty0, ty1, W),
		sqTerm(scalar(mby), loy, ty0, ty1, A),
		xmul(scalar(W), s.vyy),
		xmul(scalar(A), syy),
	}), T)
	nvxy := xdiv(xsum4([]xnum{
		crossTerm(scalar(s.mx), s.mxLo, tx0, tx1,
			scalar(s.my), s.myLo, ty0, ty1, W),
		crossTerm(scalar(mbx), lox, tx0, tx1,
			scalar(mby), loy, ty0, ty1, A),
		xmul(scalar(W), s.vxy),
		xmul(scalar(A), sxy),
	}), T)
	s.mx = nmx
	s.my = nmy
	// The merged narrowed-mean tails stay carried: the shared packing
	// plus the shared retention rule own this tail-storage boundary
	// (rev-21, twentieth-review P2).
	if haveNulx {
		items := []xnum{}
		packRemainder(nulx[0], xneg(scalar(nmx)), &items)
		if nulx[1].m != 0.0 {
			items = append(items, nulx[1])
		}
		var tv xnum
		if len(items) > 0 {
			tv = xsum4(items)
		} else {
			tv = zeroPair
		}
		s.mxLo = scalar(xval(tv))
		if laneRetains(tv) {
			s.mxLo = tv
		}
	}
	if haveNuly {
		items := []xnum{}
		packRemainder(nuly[0], xneg(scalar(nmy)), &items)
		if nuly[1].m != 0.0 {
			items = append(items, nuly[1])
		}
		var tv xnum
		if len(items) > 0 {
			tv = xsum4(items)
		} else {
			tv = zeroPair
		}
		s.myLo = scalar(xval(tv))
		if laneRetains(tv) {
			s.myLo = tv
		}
	}
	// In-range NORMAL results resume the plain float64 state; out-of-
	// range magnitude stays extended until weighting brings it back.
	s.vxx = store(nvxx)
	s.vyy = store(nvyy)
	s.vxy = store(nvxy)
}

// store narrows a merged moment only when in-range and NORMAL.
func store(xv xnum) xnum {
	m := xfrexp(xv)
	if m.m == 0.0 {
		return xv
	}
	v := xval(xv)
	if v == 0.0 || abs64(v) < minNormal {
		return xv
	}
	if isFinite(v) {
		return scalar(v)
	}
	return xv
}

func (s *Stream) f(v xnum) float64 { return xval(v) }

func (s *Stream) active(mp int) bool {
	if mp < 1 {
		mp = 1
	}
	return s.C >= mp
}

// OutMean emits the current mean or the NULL pre-gate.
func (s *Stream) OutMean(mp int) (float64, bool) {
	if !s.active(mp) {
		return 0.0, false
	}
	return s.mx, true
}

// OutVar emits the current variance (honest signed +Inf here, after
// weighting had its chance; the round-off guard clamps tiny negatives).
func (s *Stream) OutVar(mp int) (float64, bool) {
	if !s.active(mp) {
		return 0.0, false
	}
	v := s.f(s.vxx)
	if math.IsNaN(v) {
		return 0.0, false // never occurs: non-negative sums
	}
	if math.IsInf(v, 0) {
		return v, true
	}
	// Python's max(v, 0.0) keeps the FIRST argument on ties, so a
	// negative-zero variance passes through with its sign bit.
	if v >= 0.0 {
		return v, true
	}
	return 0.0, true
}

// OutStd takes the square root on the RETAINED moment before the
// float64 narrowing: a stored extended variance whose sqrt is
// representable emits that finite standard deviation while ewmVar
// honestly emits +Inf on the same row.
func (s *Stream) OutStd(mp int) (float64, bool) {
	if !s.active(mp) {
		return 0.0, false
	}
	if s.vxx.isPair {
		return xval(xsqrt(s.vxx)), true
	}
	v := s.f(s.vxx)
	if math.IsNaN(v) {
		return 0.0, false
	}
	if math.IsInf(v, 0) {
		return v, true
	}
	if v < 0.0 {
		v = 0.0
	}
	return math.Sqrt(v), true
}

// OutCov emits the current covariance (signed; honest signed
// infinity).
func (s *Stream) OutCov(mp int) (float64, bool) {
	if !s.active(mp) {
		return 0.0, false
	}
	return s.f(s.vxy), true
}

// OutCorr emits the correlation: NULL when either normalized variance
// is non-positive or non-finite (a variance beyond float64 makes the
// geometric normalization Inf/Inf = NaN). The normalization computes
// from the RETAINED moments so subnormal quantization cannot corrupt
// the verdict; only the final verdict narrows.
func (s *Stream) OutCorr(mp int) (float64, bool) {
	if !s.active(mp) {
		return 0.0, false
	}
	vxx := s.f(s.vxx)
	vyy := s.f(s.vyy)
	if !(isFinite(vxx) && isFinite(vyy)) {
		return 0.0, false
	}
	if vxx <= 0.0 || vyy <= 0.0 {
		return 0.0, false
	}
	sx := xsqrt(s.vxx)
	sy := xsqrt(s.vyy)
	r := s.f(xdiv(s.vxy, xmul(sx, sy)))
	if math.IsNaN(r) {
		return 0.0, false
	}
	if r == 0.0 {
		return math.Copysign(0.0, s.f(s.vxy)), true
	}
	return r, true
}
