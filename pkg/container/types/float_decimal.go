// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in LICENSES/Golang-license.

package types

import (
	"math"
	"math/bits"
)

// Adapted from Go 1.27.1 internal/strconv/{ftoa,uscale}.go, using its float64
// shortest-number selection before digit formatting. Only numerical shortest
// conversion is retained: no parsing, fixed precision, float32 or unsafe code.
// See "Floating-Point Printing and Parsing Can Be Simple And Fast",
// https://research.swtch.com/fp.
//
// float64ShortestDecimal returns the magnitude of finite x as c * 10^p, exactly
// matching strconv's shortest decimal value. Zero returns (0,0); otherwise
// 1 <= c < 10^17 and c has no trailing decimal zeros. Selection of the shortest
// value uses ties-to-even; rounding that value to DECIMAL is a separate step.
func float64ShortestDecimal(x float64) (c uint64, p int32) {
	value := math.Float64bits(x)
	mantissa := value & ((uint64(1) << 52) - 1)
	exponent := int(value>>52) & 0x7ff
	if exponent == 0 {
		if mantissa == 0 {
			return 0, 0
		}
		exponent = 1
	} else {
		mantissa |= uint64(1) << 52
	}
	s := 64 - bits.Len64(mantissa)
	d, power := decimalFloatShortest(mantissa<<s, exponent-1023-s-52)
	// strconv removes these zeros while emitting digits. Normalize the numeric
	// pair instead, preserving both its value and the <=17 significant digits.
	for d%10 == 0 {
		d /= 10
		power++
	}
	return d, int32(power)
}

func decimalFloatBool(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// decimalFloatUnrounded keeps guard and sticky bits below the integer part.
type decimalFloatUnrounded uint64

func (u decimalFloatUnrounded) floor() uint64 { return uint64((u + 0) >> 2) }
func (u decimalFloatUnrounded) round() uint64 { return uint64((u + 1 + (u>>2)&1) >> 2) }
func (u decimalFloatUnrounded) ceil() uint64  { return uint64((u + 3) >> 2) }
func (u decimalFloatUnrounded) nudge(δ int) decimalFloatUnrounded {
	return u + decimalFloatUnrounded(δ)
}

// decimalFloatLog10Pow2(x) returns ⌊log₁₀ 2**x⌋ = ⌊x * log₁₀ 2⌋.
func decimalFloatLog10Pow2(x int) int {
	// log₁₀ 2 ≈ 0.30102999566 ≈ 78913 / 2^18
	return (x * 78913) >> 18
}

// decimalFloatLog2Pow10(x) returns ⌊log₂ 10**x⌋ = ⌊x * log₂ 10⌋.
func decimalFloatLog2Pow10(x int) int {
	// log₂ 10 ≈ 3.32192809489 ≈ 108853 / 2^15
	return (x * 108853) >> 15
}

// decimalFloatShortest computes the shortest formatting of f,
// using as few digits as possible that will still round trip
// back to the original float.
func decimalFloatShortest(m uint64, e int) (d uint64, p int) {
	const mantBits, minExp = 52, -1085

	// Note: these cases could be factored a little more,
	// but in the first two branches, z is a constant,
	// allowing the compiler to greatly simplify the code.
	var min, max uint64
	var odd int
	z := 63 - mantBits
	if m == 1<<63 && e > minExp {
		p = -decimalFloatSkewed(e + z)
		min = m - 1<<(z-2) // min = m - 1/4 * 2**(e+z)
		max = m + 1<<(z-1) // max = m + 1/2 * 2**(e+z)
		odd = int(m>>z) & 1
	} else if e >= minExp {
		p = -decimalFloatLog10Pow2(e + z)
		min = m - 1<<(z-1) // min = m - 1/2 * 2**(e+z)
		max = m + 1<<(z-1) // max = m + 1/2 * 2**(e+z)
		odd = int(m>>z) & 1
	} else {
		z = z + (minExp - e)
		p = -decimalFloatLog10Pow2(e + z)
		min = m - 1<<(z-1) // min = m - 1/2 * 2**(e+z)
		max = m + 1<<(z-1) // max = m + 1/2 * 2**(e+z)
		odd = int(m>>z) & 1
	}

	var pre decimalFloatScaler
	decimalFloatPrescale(&pre, e, p, decimalFloatLog2Pow10(p))
	dmin := decimalFloatScale(min, &pre).nudge(+odd).ceil()
	dmax := decimalFloatScale(max, &pre).nudge(-odd).floor()

	if d = dmax / 10; d*10 >= dmin {
		return d, -(p - 1)
	}
	if d = dmin; d < dmax {
		d = decimalFloatScale(m, &pre).round()
	}
	return d, -p
}

// decimalFloatSkewed computes the skewed footprint of m * 2**e,
// which is ⌊log₁₀ 3/4 * 2**e⌋ = ⌊e*(log₁₀ 2)-(log₁₀ 4/3)⌋.
func decimalFloatSkewed(e int) int {
	return (e*631305 - 261663) >> 21
}

// A decimalFloatHiLo represents hi<<64 - lo.
type decimalFloatHiLo struct {
	hi uint64
	lo uint64
}

// A decimalFloatScaler holds derived scaling constants for a given e, p pair.
type decimalFloatScaler struct {
	// Note: using pm decimalFloatHiLo here nudges decimalFloatScale just over the inlining boundary. Don't.
	pmHi uint64
	pmLo uint64
	s    int
}

// decimalFloatPrescale returns the scaling constants for e, p.
// lp must be decimalFloatLog2Pow10(p).
// For finite binary64 values the shortest algorithm uses table exponents
// [-292,324] and shifts [5,58]; tests enumerate the exponent classes.
func decimalFloatPrescale(pre *decimalFloatScaler, e, p, lp int) {
	pre.pmHi = decimalFloatPow10[p-decimalFloatPow10Min].hi
	pre.pmLo = decimalFloatPow10[p-decimalFloatPow10Min].lo
	pre.s = -(e + lp + 3)
}

// decimalFloatScale returns unround(x * 2**e * 10**p).
// The caller should pass &pre for decimalFloatPrescale(&pre, e, p, decimalFloatLog2Pow10(p))
// and should have left-justified x so its high bit is set.
// The shortest algorithm guarantees 0 <= c.s < 64 before the shift mask.
func decimalFloatScale(x uint64, c *decimalFloatScaler) decimalFloatUnrounded {
	hi, mid := bits.Mul64(x, c.pmHi)
	s := c.s & 63 // make shifts cheaper
	if hi>>s<<s != hi {
		return decimalFloatUnrounded(hi>>s | 1)
	}
	mid2, _ := bits.Mul64(x, c.pmLo)
	hi -= decimalFloatBool(mid < mid2)
	return decimalFloatUnrounded(hi>>s | decimalFloatBool(mid-mid2 > 1))
}
