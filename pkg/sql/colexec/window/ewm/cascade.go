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
	"sort"
)

// Ldexpf rescales a mantissa by an exact power of two (math.Ldexp on a
// plain float; the honest ±Inf / 0.0 limit shapes match Python's
// OverflowError branch).
func Ldexpf(v float64, e int) float64 { return math.Ldexp(v, e) }

// Frexpf mirrors math.Frexp.
func Frexpf(v float64) (float64, int) { return math.Frexp(v) }

// alignSum is the exact two-sum of two extended pairs by exponent
// difference, returning (out, loss, has): loss is the EXACT complement
// (has=false when the whole smaller operand went in — including the
// whole-operand residual when the gap exceeds the representable
// window). An ldexp that would land on the subnormal mantissa lattice
// never quantizes first: for gaps in [1022, 2043] the two-sum runs at
// a scale where BOTH mantissa views are normal; gaps beyond 2043
// cascade the small operand intact.
func alignSum(big, sml xnum) (xnum, xnum, bool) {
	mb, eb := big.m, big.e
	ms, es := sml.m, sml.e
	g := eb - es
	var two, err float64
	var scale int
	if g <= 1021 {
		ta := Ldexpf(ms, -g)
		two = mb + ta
		if abs64(mb) >= abs64(ta) {
			err = (mb - two) + ta
		} else {
			err = (ta - two) + mb
		}
		scale = eb
	} else if g <= 2043 {
		sbig := Ldexpf(mb, g-1021)
		ssml := Ldexpf(ms, -1021)
		two = sbig + ssml
		if abs64(sbig) >= abs64(ssml) {
			err = (sbig - two) + ssml
		} else {
			err = (ssml - two) + sbig
		}
		scale = es + 1021
	} else {
		return big, sml, true
	}
	var out xnum
	if two == 0.0 {
		out = zeroPair
	} else {
		m, e := Frexpf(two)
		out = pair(m, e+scale)
	}
	if err == 0.0 {
		return out, zeroPair, false
	}
	return out, pair(err, scale), true
}

// xfold folds q into register r with xadd's kept bits, returning the
// fold's EXACT discarded part (has=false when the whole q went in).
func xfold(r, q xnum) (xnum, xnum, bool) {
	ma := xfrexp(r)
	mb := xfrexp(q)
	if ma.m == 0.0 {
		return mb, zeroPair, false
	}
	if mb.m == 0.0 {
		return ma, zeroPair, false
	}
	var big, sml xnum
	if ma.e >= mb.e {
		big, sml = ma, mb
	} else {
		big, sml = mb, ma
	}
	return alignSum(big, sml)
}

const qmax = 128

// xsum4 is the cascaded extended accumulation: the level-0 loop and
// first residual register are the plain two-sum compensation
// bit-identically; every bit a register fold discards cascades into
// the next register instead of vanishing. The restoration keeps every
// fold loss: one shared queue processes items in DESCENDING magnitude,
// mirror pairs cancel exactly before small content folds, a fold whose
// loss returns the WHOLE item holds it back and retries only after
// the running total changed, and the remainder flushes through xadd
// (the float64 information limit). qmax (128) covers the supported
// term-grammar span with margin.
func xsum4(terms []xnum) xnum {
	return xsum4E(terms, nil)
}

// xsum4E is the cancellation-aware accumulation entry: the k-sized
// level-0 loop polls the operator's hook at the fixed interval and a
// canceled sweep stops mid-way with a partial value that the merge
// layer discards on its dead check (bounded callers pass nil).
func xsum4E(terms []xnum, poll *poller) xnum {
	acc := zeroPair
	regs := []xnum{zeroPair}
	for _, t := range terms {
		if poll.step() != nil {
			break
		}
		tm := xfrexp(t)
		if tm.m == 0.0 {
			continue
		}
		if acc.m == 0.0 {
			acc = tm
			continue
		}
		newacc, loss, has := xfold(acc, t)
		acc = newacc
		if !has {
			continue
		}
		q := loss
		j := 0
		for {
			r, l, lhas := xfold(regs[j], q)
			regs[j] = r
			if !lhas || l.m == 0.0 {
				break
			}
			j++
			if j == len(regs) {
				if j >= qmax {
					regs[j-1] = xadd(regs[j-1], q)
					break
				}
				regs = append(regs, zeroPair)
			}
			q = l
		}
	}
	onlyZero := true
	for i := 1; i < len(regs); i++ {
		if regs[i].m != 0.0 {
			onlyZero = false
			break
		}
	}
	if onlyZero {
		return xadd(acc, regs[0])
	}
	// The shared bounded reconstruction.
	head, l0, l0has := xfold(acc, regs[0])
	var items []xnum
	if l0has && l0.m != 0.0 {
		items = append(items, l0)
	}
	for i := 1; i < len(regs); i++ {
		if regs[i].m != 0.0 {
			items = append(items, regs[i])
		}
	}
	if len(items) == 0 {
		return head
	}
	itemsSorted := make([]xnum, len(items))
	copy(itemsSorted, items)
	sort.SliceStable(itemsSorted, func(i, j int) bool {
		pi, pj := itemsSorted[i], itemsSorted[j]
		if pi.e != pj.e {
			return pi.e > pj.e
		}
		return abs64(pi.m) > abs64(pj.m)
	})
	total := head
	pend := itemsSorted
	var hold []xnum
	for {
		changed := false
		for len(pend) > 0 {
			it := pend[0]
			pend = pend[1:]
			ntot, ls, lshas := xfold(total, it)
			if ntot.m != total.m || ntot.e != total.e {
				changed = true
			}
			total = ntot
			if !lshas || ls.m == 0.0 {
				continue
			}
			if ls.m == it.m && ls.e == it.e {
				hold = append(hold, it)
				continue
			}
			pend = append(pend, ls)
		}
		if len(hold) == 0 || !changed {
			break
		}
		pend = hold
		hold = nil
	}
	for _, rest := range hold {
		total = xadd(total, rest)
	}
	return total
}

// neumaier annotations: the batch-level compensated sums are cascaded
// through neumaierPair and collapse once here.
func neumaier(values []float64) float64 {
	s, comp, compLo := neumaierPair(values, nil)
	return s + comp + compLo
}

// neumaierPair is the Neumaier sum with the (sum, first-residual
// register, residual-tail) triple returned separately; the residual
// registers are themselves compensated — each residual's fold loss
// cascades into the next bounded register. A NON-FINITE residual (an
// overflowing partial sum in a rejecting branch) never enters the
// cascading loop: it folds straight into the first register and
// propagates exactly like the single compensation register.
// neumaierPair polls the k-sized sum sweep; a dead poller stops the
// sweep and the partial sums are discarded by the merge layer's dead
// check.
func neumaierPair(values []float64, poll *poller) (float64, float64, float64) {
	s := 0.0
	var regs []float64
	for _, x := range values {
		if poll.step() != nil {
			break
		}
		t := s + x
		var err float64
		if abs64(s) >= abs64(x) {
			err = (s - t) + x
		} else {
			err = (x - t) + s
		}
		s = t
		if err == 0.0 {
			continue
		}
		if !isFinite(err) {
			if len(regs) > 0 {
				regs[0] += err
			} else {
				regs = []float64{err}
			}
			continue
		}
		q := err
		j := 0
		for {
			if j == len(regs) {
				regs = append(regs, 0.0)
			}
			r := regs[j]
			t2 := r + q
			var e2 float64
			if abs64(r) >= abs64(q) {
				e2 = (r - t2) + q
			} else {
				e2 = (q - t2) + r
			}
			regs[j] = t2
			if e2 == 0.0 {
				break
			}
			q = e2
			j++
		}
	}
	if len(regs) == 0 {
		return s, 0.0, 0.0
	}
	if !isFinite(regs[0]) {
		// The non-finite fold keeps the naive tail verbatim (the
		// rejecting branches' single-compensation propagation).
		compLo := 0.0
		for _, r := range regs[1:] {
			compLo += r
		}
		return s, regs[0], compLo
	}
	if len(regs) == 1 {
		return s, regs[0], 0.0
	}
	// The registers join the SHARED bounded reconstruction (the ddsum
	// exact-alignment chain with every fold loss kept) instead of a
	// naive comp_lo pass; registers >= 53 binary orders apart fold
	// losslessly and benign walks are bit-identical.
	parts := make([]xnum, len(regs))
	for i, r := range regs {
		parts[i] = xfrexp(scalar(r))
	}
	cp, clp := ddsum(parts)
	return s, xval(cp), xval(clp)
}

// ddsum is the double-double sum of extended pairs: the leading word
// folds through the exact-alignment chain and the fold losses
// accumulate as the second word.
func ddsum(items []xnum) (xnum, xnum) {
	return ddsumPoll(items, nil)
}

// ddsumPoll is the cancellation-aware exact-alignment sum: the items
// sweep polls at the fixed interval; a canceled sweep stops with
// partial words the merge layer discards on its dead check.
func ddsumPoll(items []xnum, poll *poller) (xnum, xnum) {
	if len(items) == 0 {
		return zeroPair, zeroPair
	}
	acc := items[0]
	var losses []xnum
	for _, t := range items[1:] {
		if poll.step() != nil {
			break
		}
		var l xnum
		var has bool
		acc, l, has = xfold(acc, t)
		if has && l.m != 0.0 {
			losses = append(losses, l)
		}
	}
	if len(losses) == 0 {
		return acc, zeroPair
	}
	// the residual fold-loss sweep runs through the same shared polled
	// helper as the items sweep: a cancellation inside it stops the
	// accumulation and the merge layer discards the partial words
	return acc, xsum4E(losses, poll)
}

// eterm is one ddsum_e item: an error-free weighted product with an
// optional negation (the sign slot).
type eterm struct {
	u   xnum
	v   xnum
	neg bool
}

// ddsumE is the complete-mean weighted-product sum: every term is an
// ERROR-FREE product — the rounded leading word is the same pair xmul
// returned and its EXACT residual joins the fold losses in the second
// word — so a mass product can never round before its compensation
// acts. With exact products the result is bit-identical to ddsum.
func ddsumE(items []eterm) (xnum, xnum) {
	var acc xnum
	accSet := false
	var loTerms []xnum
	for _, it := range items {
		ph, pl := xtwoprod(it.u, it.v)
		if it.neg {
			ph, pl = xneg(ph), xneg(pl)
		}
		if !accSet {
			acc = ph
			accSet = true
		} else {
			var l xnum
			var has bool
			acc, l, has = xfold(acc, ph)
			if has && l.m != 0.0 {
				loTerms = append(loTerms, l)
			}
		}
		if pl.m != 0.0 {
			loTerms = append(loTerms, pl)
		}
	}
	if !accSet {
		return zeroPair, zeroPair
	}
	if len(loTerms) == 0 {
		return acc, zeroPair
	}
	return acc, xsum4(loTerms)
}

// ddsub is the double-double difference of hi+lo operands; the
// leading sum's exact fold loss joins the lo operands in the second
// word.
func ddsub(ah, al, bh, bl xnum) (xnum, xnum) {
	ahp := xfrexp(ah)
	alp := xfrexp(al)
	bhp := xfrexp(bh)
	blr := xfrexp(bl)
	dh, dl, dlhas := xfold(ahp, xneg(bhp))
	var items []xnum
	if dlhas && dl.m != 0.0 {
		items = append(items, dl)
	}
	if alp.m != 0.0 {
		items = append(items, alp)
	}
	if blr.m != 0.0 {
		items = append(items, xneg(blr))
	}
	if len(items) == 0 {
		return dh, zeroPair
	}
	return dh, xsum4(items)
}

// dddiv is the double-double division: the leading quotient, then the
// exact hi residual (error-free qh*d product, its exact residual pair
// and the second word in the remainder) divided for the tail quotient.
func dddiv(nh, nl, d xnum) (xnum, xnum) {
	qh := xdiv(nh, d)
	ph, pl := xtwoprod(qh, d)
	items := []xnum{nh, xneg(ph)}
	if pl.m != 0.0 {
		items = append(items, xneg(pl))
	}
	if nl.m != 0.0 {
		items = append(items, nl)
	}
	return qh, xdiv(xsum4(items), d)
}

// xqCascade is the cancellation-preserving quotient of the signed
// batch-cross pile: the terms cascade through xsum4's registers (each
// magnitude scale in its own register, mirror pairs cancelling
// exactly before any restore), the first quotient word divides once,
// and the EXACT -q1*d residual re-cascades WITH the pile so the
// second word carries the quotient remainder. The uncancelled pile is
// never compressed into a two-word window.
// xqCascade polls through its two k-sized pile passes; a dead poller
// stops the second pass with a partial value the merge layer discards
// on its dead check.
func xqCascade(items []xnum, d xnum, poll *poller) xnum {
	if len(items) == 0 {
		return zeroPair
	}
	q1 := xdiv(xsum4E(items, poll), d)
	ph, pl := xtwoprod(q1, d)
	rest := []xnum{xneg(ph)}
	if pl.m != 0.0 {
		rest = append(rest, xneg(pl))
	}
	return xadd(q1, xdiv(xsum4E(append(items, rest...), poll), d))
}

// ddMul is the double-double product rounded to one extended pair:
// the leading product with the three correction products folded by the
// bounded accumulator.
func ddMul(qh, ql, dh, dl xnum) xnum {
	p := xmul(qh, dh)
	var items []xnum
	for _, c := range [][2]xnum{{qh, dl}, {ql, dh}, {ql, dl}} {
		if c[0].isPair {
			if c[0].m == 0.0 {
				continue
			}
		} else if c[0].m == 0.0 {
			continue
		}
		if c[1].isPair {
			if c[1].m == 0.0 {
				continue
			}
		} else if c[1].m == 0.0 {
			continue
		}
		items = append(items, xmul(c[0], c[1]))
	}
	if len(items) > 0 {
		return xadd(p, xsum4(items))
	}
	return p
}

// meanPairNorm is the shared complete-mean normalization at every
// commit boundary: the normalized store carries fl of the COMPLETE
// hi+tail value in the narrowed word and the EXACT residual (plus the
// full narrowing remainder) in the tail slot. A float64 tail folds
// through twosum; a retained (pair) tail folds through the exact pair
// two-sum with the fold loss preserved via the shared packing. A
// retained-magnitude lead passes through unchanged; a non-finite
// fl(hi+tail) keeps the pair raw for the honest emission.
func meanPairNorm(hi, tail xnum) (xnum, xnum) {
	if hi.isPair {
		return hi, tail
	}
	if tail.isPair {
		lead, loss, lhas := xfold(xfrexp(hi), tail)
		h2 := xval(lead)
		if !isFinite(h2) {
			return hi, tail
		}
		items := []xnum{}
		if lhas && loss.m != 0.0 {
			items = append(items, loss)
		}
		packRemainder(lead, xneg(xfrexp(scalar(h2))), &items)
		if len(items) == 0 {
			return scalar(h2), zeroPair
		}
		return scalar(h2), xsum4(items)
	}
	if tail.m == 0.0 {
		return hi, tail
	}
	s, err := twosum(hi.m, tail.m)
	if !isFinite(s) {
		return hi, tail
	}
	return scalar(s), scalar(err)
}

// xdevItems appends the exact batch-moment term from one exact
// deviation pair (xh + xl) times another (yh + yl): the leading
// product pair plus its exact residual pair, then the three low-part
// pieces (each passed by value only when a factor is nonzero).
func xdevItems(xh, xl, yh, yl float64, items *[]xnum) {
	ph, pl := xtwoprod(scalar(xh), scalar(yh))
	*items = append(*items, ph)
	if pl.m != 0.0 {
		*items = append(*items, pl)
	}
	for _, c := range [][2]float64{{xh, yl}, {xl, yh}, {xl, yl}} {
		if c[0] == 0.0 || c[1] == 0.0 {
			continue
		}
		ph, pl := xtwoprod(scalar(c[0]), scalar(c[1]))
		*items = append(*items, ph)
		if pl.m != 0.0 {
			*items = append(*items, pl)
		}
	}
}

// xanchorKItems appends the error-free anchor/mean-tail correction
// -k*deltax*delty of the batch identities, with delta supplied as its
// EXACT additive parts (a part may be a retained pair). Every
// sub-product forms error-free in the extended channel; the k scaling
// and negation fold each piece once; zero parts contribute nothing.
func xanchorKItems(fxParts, fyParts []xnum, k float64, items *[]xnum) {
	ks := xfrexp(scalar(k))
	for _, fxp := range fxParts {
		if xfrexp(fxp).m == 0.0 {
			continue
		}
		for _, fyp := range fyParts {
			if xfrexp(fyp).m == 0.0 {
				continue
			}
			ph, pl := xtwoprod(fxp, fyp)
			for _, pp := range [2]xnum{ph, pl} {
				if pp.m == 0.0 {
					continue
				}
				kh, kl := xtwoprod(pp, ks)
				if kh.m != 0.0 {
					*items = append(*items, xneg(kh))
				}
				if kl.m != 0.0 {
					*items = append(*items, xneg(kl))
				}
			}
		}
	}
}
