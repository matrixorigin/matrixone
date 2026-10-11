// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"math/big"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// exactDecimalRound rounds the decimal text s (optionally with an exponent)
// half away from zero to scale digits and formats it like Decimal256.Format.
func exactDecimalRound(t *testing.T, s string, scale int32) string {
	t.Helper()
	r, ok := new(big.Rat).SetString(s)
	require.True(t, ok, s)
	neg := r.Sign() < 0
	r.Abs(r)
	r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)))
	q, m := new(big.Int).QuoRem(r.Num(), r.Denom(), new(big.Int))
	if new(big.Int).Mul(m, big.NewInt(2)).Cmp(r.Denom()) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	digits := q.String()
	if int32(len(digits)) <= scale {
		digits = strings.Repeat("0", int(scale)-len(digits)+1) + digits
	}
	out := digits
	if scale > 0 {
		out = digits[:len(digits)-int(scale)] + "." + digits[len(digits)-int(scale):]
	}
	if neg && strings.Trim(digits, "0") != "" {
		out = "-" + out
	}
	return out
}

// Text longer than 76 digits must be rounded once, to the column scale, not
// first at the 76-digit parser limit and again to the scale (double rounding).
func TestParseDecimal256RoundsOnceWhenDownscaling(t *testing.T) {
	tiny := "0." + strings.Repeat("0", 63) + "14" + strings.Repeat("9", 12) + "5"
	for _, tc := range []struct {
		in    string
		scale int32
	}{
		{tiny, 64},
		{"-" + tiny, 64},
		{tiny + "e0", 64},
		{"0." + strings.Repeat("0", 63) + "15" + strings.Repeat("0", 20), 64},
		{"0." + strings.Repeat("0", 63) + "1449" + strings.Repeat("9", 20), 64},
		{"1." + strings.Repeat("4", 64) + strings.Repeat("9", 20), 64},
		{"0." + strings.Repeat("0", 10) + strings.Repeat("7", 80) + "e2", 30},
		{"0." + strings.Repeat("0", 76) + "5e76", 0},
	} {
		got, err := ParseDecimal256(tc.in, 65, tc.scale)
		require.NoError(t, err, tc.in)
		require.Equal(t, exactDecimalRound(t, tc.in, tc.scale), got.Format(tc.scale), tc.in)
	}
}

func TestParseDecimal256MatchesExactRoundingRandomized(t *testing.T) {
	rnd := rand.New(rand.NewSource(29853))
	digit := func(n int, biased bool) string {
		var sb strings.Builder
		for i := 0; i < n; i++ {
			if biased && rnd.Intn(3) == 0 {
				sb.WriteByte("0459"[rnd.Intn(4)])
			} else {
				sb.WriteByte(byte('0' + rnd.Intn(10)))
			}
		}
		return sb.String()
	}
	for i := 0; i < 20000; i++ {
		scale := int32(rnd.Intn(66))
		intDigits := rnd.Intn(int(65-scale) + 1)
		in := "0"
		if intDigits > 0 {
			in = string(byte('1'+rnd.Intn(9))) + digit(intDigits-1, false)
		}
		frac := strings.Repeat("0", rnd.Intn(5)) + digit(rnd.Intn(80), true)
		if rnd.Intn(3) == 0 {
			// a 4 followed by a run of 9s: the carry pattern that exposes
			// rounding at the parser limit followed by rounding to the scale
			frac = digit(int(scale), false)[:max(0, int(scale)-1)] + "4" + strings.Repeat("9", 1+rnd.Intn(90)) + digit(rnd.Intn(3), false)
		}
		in += "." + frac
		if rnd.Intn(4) == 0 {
			in += "e0"
		}
		if rnd.Intn(2) == 0 {
			in = "-" + in
		}
		want := exactDecimalRound(t, in, scale)
		if len(strings.TrimLeft(strings.ReplaceAll(strings.TrimPrefix(want, "-"), ".", ""), "0")) > 65 {
			continue // rounding carried past the column width
		}
		got, err := ParseDecimal256(in, 65, scale)
		require.NoError(t, err, in)
		require.Equal(t, want, got.Format(scale), "input %s scale %d", in, scale)
	}
}

// Leading zeros of 0.xxx are not significant digits.
func TestParse256LeadingZerosDoNotConsumePrecision(t *testing.T) {
	in := "0." + strings.Repeat("1", 76)
	got, scale, err := Parse256(in)
	require.NoError(t, err)
	require.Equal(t, int32(76), scale)
	require.Equal(t, in, got.Format(scale))
}
