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

import "math/big"

// This file pins the engine's transcendental evaluations to exact,
// platform-independent arithmetic. The Go math library's Exp/Expm1/Pow
// are honest, ulp-accurate implementations — but their last-ulp
// choices differ between platforms (arm64 math.Exp measures one ulp
// above the correctly rounded value on this corpus's exp(-ln2/4)),
// and the engine's outputs must reproduce bit-exactly everywhere.
// Half-life transitions and row-decay powers therefore go through
// big.Float/big.Rat evaluation with the rounding done once at the
// end: correctly rounded results under a fixed, documented rule.

// crPrec carries enough guard digits that the final rounding decides
// every value with certainty: the series' own truncation error sits
// hundreds of bits below the float64 rounding boundary.
const crPrec = 256

// expNegCR computes the correctly rounded exp(-c) for c ≥ 0 (the
// event-time transition factor g = exp(−ln2·Δt/h)): the argument is
// halved down into the well-conditioned range (exact binary scalings),
// evaluated by the alternating Taylor series at 256-bit precision,
// squared back up, and rounded once.
func expNegCR(c float64) float64 {
	if c == 0.0 {
		return 1.0
	}
	if !(c > 0.0) {
		return 1.0 // called with c = fl(ln2·r), r ≥ 0; keep the door closed
	}
	if c >= 746.0 {
		return 0.0 // exp(-746) is below half the smallest subnormal
	}
	m := 0
	y := c
	for y > 0.5 {
		y = Ldexpf(y, -1) // exact halving
		m++
	}
	v := taylorExpNeg(y)
	sq := new(big.Float).SetPrec(crPrec)
	for i := 0; i < m; i++ {
		sq.Mul(v, v)
		v, sq = sq, v
	}
	f, _ := v.Float64()
	return f
}

// expm1NegCR computes the correctly rounded −expm1(−c) — the arrival
// mass A = 1 − exp(−c) under adjust=false (never the literal 1−g).
// Small c uses the direct expm1 series around c (no cancellation at
// the one); larger c forms exp(−c) by halving and subtracts it from
// one, where no relative cancellation remains.
func expm1NegCR(c float64) float64 {
	if !(c >= 0.0) || c == 0.0 {
		return 0.0
	}
	if c >= 746.0 {
		return 1.0 // 1 - exp(-746) rounds to exactly one
	}
	if c <= 1.0 {
		// at = c - c²/2 + c³/6 - …: exact series around c.
		neg := new(big.Float).SetPrec(crPrec).SetFloat64(-c)
		term := new(big.Float).SetPrec(crPrec).SetFloat64(c)
		acc := new(big.Float).SetPrec(crPrec).SetFloat64(c)
		kf := new(big.Float).SetPrec(crPrec)
		for i := int64(2); i <= 80; i++ {
			term.Mul(term, neg)
			kf.SetInt64(i)
			term.Quo(term, kf)
			acc.Add(acc, term)
			if term.Sign() == 0 {
				break
			}
		}
		f, _ := acc.Float64()
		return f
	}
	m := 0
	y := c
	for y > 0.5 {
		y = Ldexpf(y, -1)
		m++
	}
	v := taylorExpNeg(y)
	sq := new(big.Float).SetPrec(crPrec)
	for i := 0; i < m; i++ {
		sq.Mul(v, v)
		v, sq = sq, v
	}
	one := new(big.Float).SetPrec(crPrec).SetInt64(1)
	at := new(big.Float).SetPrec(crPrec)
	at.Sub(one, v)
	f, _ := at.Float64()
	return f
}

// taylorExpNeg evaluates exp(-y) for 0 < y ≤ 0.5 at crPrec bits (the
// truncation error sits below 2^-400 there).
func taylorExpNeg(y float64) *big.Float {
	neg := new(big.Float).SetPrec(crPrec).SetFloat64(-y)
	term := new(big.Float).SetPrec(crPrec).SetInt64(1)
	acc := new(big.Float).SetPrec(crPrec).SetInt64(1)
	kf := new(big.Float).SetPrec(crPrec)
	for i := int64(1); i <= 80; i++ {
		kf.SetInt64(i)
		term.Mul(term, neg)
		term.Quo(term, kf)
		acc.Add(acc, term)
		if term.Sign() == 0 {
			break
		}
	}
	return acc
}

// intPowCR computes the row-decay transition power λ^span for an
// integer span ≥ 1 exactly: λ is an exact rational, so λ^span is
// formed with exact big integers (or, for spans beyond the exact
// budget, by fixed-precision repeated squaring — a deterministic,
// platform-independent rule) and rounded once.
func intPowCR(lam float64, span int64) float64 {
	if span == 1 {
		return lam
	}
	if span <= 0 {
		return 1.0 // unreachable by the clock rules; the door stays closed
	}
	if span > 4096 {
		base := new(big.Float).SetPrec(160).SetFloat64(lam)
		acc := new(big.Float).SetPrec(160).SetInt64(1)
		sq := new(big.Float).SetPrec(160)
		for span > 0 {
			if span&1 == 1 {
				acc.Mul(acc, base)
			}
			span >>= 1
			if span > 0 {
				sq.Mul(base, base)
				base, sq = sq, base
			}
		}
		f, _ := acc.Float64()
		return f
	}
	r := new(big.Rat).SetFloat64(lam) // exact
	num := new(big.Int).Exp(r.Num(), big.NewInt(span), nil)
	den := new(big.Int).Exp(r.Denom(), big.NewInt(span), nil)
	q := new(big.Rat).SetFrac(num, den) // exact: odd numerator, power-of-two denominator
	f, _ := new(big.Float).SetPrec(0).SetRat(q).Float64()
	return f
}
