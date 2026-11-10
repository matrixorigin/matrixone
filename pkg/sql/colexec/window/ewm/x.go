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

// Package ewm implements the exponential-weighted window statistics
// engine specified by docs/design/ewm_window_statistics.md (revision
// 21) for ewmMean/ewmVar/ewmStd/ewmCov/ewmCorr: one float64 value or
// an extended (mantissa, exponent) pair per account slot, layered
// batch statistics and merges with the shared lane_retains retention
// rule and the shared pack_remainder packing (§5).
package ewm

import "math"

// xnum carries one account slot: a plain float64 value or an extended
// (mantissa, exponent) pair whose value is m*2^e with the mantissa
// normalized into [0.5, 1). The extended form retains out-of-range and
// subnormal-lattice magnitude exactly (power-of-two scalings only).
type xnum struct {
	isPair bool
	m      float64
	e      int
}

// LN2 is the correctly rounded ln(2) (the event-time half-life base).
const LN2 = 0.6931471805599453

// minNormal is the smallest normal float64; results that would narrow
// below it stay in the extended channel (narrowing is an
// emission-time event).
const minNormal = 2.2250738585072014e-308

func scalar(v float64) xnum { return xnum{m: v} }

func pair(m float64, e int) xnum { return xnum{isPair: true, m: m, e: e} }

var zeroPair = pair(0.0, 0)

// xeq mirrors Python tuple/float comparison: two extended values are
// equal exactly (their words match); two scalars compare by value; a
// pair NEVER equals a scalar.
func xeq(a, b xnum) bool {
	if a.isPair != b.isPair {
		return false
	}
	if a.isPair {
		return a.m == b.m && a.e == b.e
	}
	return a.m == b.m
}

// tailNonZero mirrors Python `lox != 0.0` where lox may be a float or
// a pair: a pair is never equal to the scalar zero (downstream
// zero-value gates neutralize an all-zero pair), a scalar compares.
func tailNonZero(a xnum) bool {
	if a.isPair {
		return true
	}
	return a.m != 0.0
}

// xfrexp canonicalizes a scalar into the extended pair form (exact for
// every finite float64); a pair is re-normalized with the non-finite
// mantissa guard, which fails closed (never loops).
func xfrexp(v xnum) xnum {
	if v.isPair {
		m, e := v.m, v.e
		if m == 0.0 {
			return zeroPair
		}
		if !isFinite(m) {
			// A non-finite normalized mantissa means an upstream
			// formation bug — fail closed, never loop.
			panic("non-finite mantissa reached the extended normalization")
		}
		for abs64(m) >= 1.0 {
			m *= 0.5
			e++
		}
		for abs64(m) < 0.5 {
			m *= 2.0
			e--
		}
		return pair(m, e)
	}
	if !isFinite(v.m) {
		panic("non-finite value reached the extended normalization")
	}
	mm, me := math.Frexp(v.m)
	return pair(mm, me)
}

// xval emits the float64 view of a value: honest signed Inf when the
// extended value is out of float64 range after all weighting.
func xval(x xnum) float64 {
	if !x.isPair {
		return x.m
	}
	if x.m == 0.0 {
		return 0.0
	}
	return math.Ldexp(x.m, x.e)
}

func abs64(v float64) float64 {
	if v < 0.0 {
		return -v
	}
	return v
}

func isFinite(v float64) bool { return !math.IsInf(v, 0) && !math.IsNaN(v) }

// xmul multiplies two extended values (mantissa product in [0.25, 1):
// one rounding, exact power-of-two re-normalization).
func xmul(a, b xnum) xnum {
	ma := xfrexp(a)
	mb := xfrexp(b)
	if ma.m == 0.0 || mb.m == 0.0 {
		return zeroPair
	}
	m := ma.m * mb.m
	e := ma.e + mb.e
	if abs64(m) >= 0.5 {
		return pair(m, e)
	}
	return pair(m*2.0, e-1)
}

// xadd adds two extended values, aligning to the larger exponent. Terms
// more than ~1070 binary orders below the partial sum round away here —
// the float64 information limit.
func xadd(a, b xnum) xnum {
	ma := xfrexp(a)
	mb := xfrexp(b)
	if ma.m == 0.0 {
		return mb
	}
	if mb.m == 0.0 {
		return ma
	}
	if ma.e < mb.e { // align to the larger exponent
		ma, mb = mb, ma
	}
	t := ma.m + math.Ldexp(mb.m, mb.e-ma.e)
	if t == 0.0 {
		return zeroPair // exact cancellation
	}
	m, e := math.Frexp(t)
	return pair(m, e+ma.e)
}

// xneg negates a scalar or extended value.
func xneg(v xnum) xnum {
	if v.isPair {
		return pair(-v.m, v.e)
	}
	return scalar(-v.m)
}

// xdiv divides two extended values (mantissa quotient in (0.5, 2):
// one rounding, exact power-of-two re-normalization).
func xdiv(a, b xnum) xnum {
	ma := xfrexp(a)
	mb := xfrexp(b)
	if mb.m == 0.0 {
		panic("ewm: division by zero in the extended domain")
	}
	if ma.m == 0.0 {
		return zeroPair
	}
	m := ma.m / mb.m
	e := ma.e - mb.e
	if abs64(m) >= 1.0 {
		return pair(m*0.5, e+1)
	}
	if abs64(m) < 0.5 {
		return pair(m*2.0, e-1)
	}
	return pair(m, e)
}

// xsqrt takes the square root of an extended value in extended form:
// odd exponents borrow one so the mantissa sqrt is the only rounding;
// the standard deviation narrows to float64 only at emission.
func xsqrt(a xnum) xnum {
	m, e := xfrexp(a).m, xfrexp(a).e
	if m == 0.0 {
		return zeroPair
	}
	if e%2 != 0 {
		m *= 2.0 // exact: keeps scalings out of the sqrt
		e--
	}
	return pair(math.Sqrt(m), e/2)
}

// mul64 rounds a float64 product ONCE. The non-inlined call boundary
// inhibits the compiler's FMA fusion (Go on arm64 fuses a*b±c into a
// single fused multiply-add, which produces -- for this engine's
// sequential-rounding contract -- one-ulp deviations): every product
// consumed by an addition/subtraction in the §5 formulas must round
// in isolation, exactly like the sequential reference.
//
//go:noinline
func mul64(a, b float64) float64 { return a * b }

// sq is the overflow-tolerant float64 square, rounded once through
// the non-inlined product so its result can never fuse into a
// surrounding addition (v*v + z stays two roundings).
//
//go:noinline
func sq(v float64) float64 { return mul64(v, v) }

// twosum is the exact float two-sum: (fl(a+b), residual).
func twosum(a, b float64) (float64, float64) {
	s := a + b
	if abs64(a) >= abs64(b) {
		return s, (a - s) + b
	}
	return s, (b - s) + a
}

// split is the Dekker split in the MANTISSA domain: the 2^27+1 scaling
// multiplies only the normalized mantissa, so the top-26-bit part is
// exact at every representable magnitude and the pieces reassemble
// with ldexp (exact on the subnormal lattice too).
func split(v float64) (float64, float64) {
	if !isFinite(v) || v == 0.0 {
		return v, 0.0
	}
	mm, me := math.Frexp(v)
	t := mul64(134217729.0, mm) // in [2^26, 2^28): never overflows
	if !isFinite(t) {
		return v, 0.0
	}
	hi := t - (t - mm)
	return math.Ldexp(hi, me), math.Ldexp(mm-hi, me)
}

// twoprod is the Dekker two-product: (p, e) with a*b == p + e exactly,
// valid whenever fl(a*b) is finite. Overflowing or zero products keep
// (p, 0.0), the honest rejecting-branch shape.
func twoprod(a, b float64) (float64, float64) {
	p := a * b
	if !isFinite(p) {
		return p, 0.0
	}
	ma, ea := math.Frexp(a)
	mb, eb := math.Frexp(b)
	if ma == 0.0 || mb == 0.0 {
		return p, 0.0
	}
	ah, al := split(ma)
	bh, bl := split(mb)
	pm := mul64(ma, mb)
	em := ((mul64(ah, bh) - pm) + mul64(ah, bl)) + mul64(al, bh) + mul64(al, bl)
	if em == 0.0 {
		return p, 0.0
	}
	return p, math.Ldexp(em, ea+eb)
}

// xtwoprod is the error-free product of two extended values: (p, e)
// with a*b == p + e EXACTLY across the declared extended range. p is
// the same rounded pair xmul returned; e carries the product's lost
// tail.
func xtwoprod(a, b xnum) (xnum, xnum) {
	ma := xfrexp(a)
	mb := xfrexp(b)
	if ma.m == 0.0 || mb.m == 0.0 {
		return zeroPair, zeroPair
	}
	ah, al := split(ma.m)
	bh, bl := split(mb.m)
	p := mul64(ma.m, mb.m) // in [0.25, 1): one rounding
	e := ((mul64(ah, bh) - p) + mul64(ah, bl)) + mul64(al, bh) + mul64(al, bl)
	pp := xfrexp(pair(p, ma.e+mb.e))
	if e == 0.0 {
		return pp, zeroPair
	}
	return pp, xfrexp(pair(e, ma.e+mb.e))
}
