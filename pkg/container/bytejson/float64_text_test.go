// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bytejson

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFloat64TextRoundTripKeepsIntegralDoubleType pins #30782: rendering an
// integral-valued DOUBLE without a fractional marker lets the exported text
// re-parse as INTEGER through display, JSON_PRETTY and materialized text.
func TestFloat64TextRoundTripKeepsIntegralDoubleType(t *testing.T) {
	cases := []struct {
		source   string
		expected string
	}{
		{"1.0", "1.0"},
		{"1.000", "1.0"},
		{"-0.0", "-0.0"},
		{"0.0", "0.0"},
		{"10.0", "10.0"},
		{"1.5", "1.5"},
		{"-2.25", "-2.25"},
	}
	for _, c := range cases {
		bj, err := ParseFromString(c.source)
		require.NoError(t, err, c.source)
		require.Equal(t, TpCodeFloat64, bj.Type, "source %s must keep DOUBLE binary kind", c.source)
		text, err := bj.MarshalJSON()
		require.NoError(t, err, c.source)
		require.Equal(t, c.expected, string(text), "rendered text of %s", c.source)

		// The rendered text must re-parse as DOUBLE, closing the round trip.
		reparsed, err := ParseFromString(string(text))
		require.NoError(t, err, c.source)
		require.Equal(t, TpCodeFloat64, reparsed.Type, "reparse of %s", c.source)
		f, err := strconv.ParseFloat(c.source, 64)
		require.NoError(t, err)
		require.Equal(t, f, reparsed.GetFloat64(), "reparse value of %s", c.source)
	}
}

// TestFloat64TextRoundTripNestedValue covers the nested document form; the
// fractional marker must appear inside containers, not only at the root.
func TestFloat64TextRoundTripNestedValue(t *testing.T) {
	bj, err := ParseFromString(`{"a":1.0,"b":2.5,"c":3}`)
	require.NoError(t, err)
	text, err := bj.MarshalJSON()
	require.NoError(t, err)
	require.Contains(t, string(text), `"a": 1.0`)
	require.Contains(t, string(text), `"b": 2.5`)
	require.Contains(t, string(text), `"c": 3`)

	nested, err := ParseFromString(string(text))
	require.NoError(t, err)
	require.Equal(t, TpCodeFloat64, nested.GetObjectVal(0).Type)
	require.Equal(t, float64(1), nested.GetObjectVal(0).GetFloat64())
}

// TestFloat64TextExponentSpellingFollowsUpstream records that the exponent
// spelling belongs to upstream #30316's MySQL-format serializer, which this
// change set reuses wholesale after the base update; only the integral
// fractional marker is issue #31121's own contract. The 'e' format already
// carries an exponent, so no marker may be appended there.
func TestFloat64TextExponentSpellingFollowsUpstream(t *testing.T) {
	bj, err := ParseFromString("1e30")
	require.NoError(t, err)
	text, err := bj.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, "1e30", string(text))
}

// TestFloat64WideBoundaryValuesRoundTrip pins the wide-number value
// assertions the local review retained: the rendered spelling follows the
// upstream MySQL-format serializer (adjudicated against MySQL 8.0.44
// receipts), so the DOUBLE kind and the numeric VALUE are checked
// independently of the exact text. 1e18 keeps its magnitude and the 2^53
// boundary 9007199254740992 stays exactly representable through reparse.
func TestFloat64WideBoundaryValuesRoundTrip(t *testing.T) {
	for _, c := range []struct {
		source  string
		textual string
		value   float64
	}{
		{"1000000000000000000.0", "1e18", 1e18},
		{"1e18", "1e18", 1e18},
		{"9007199254740992.0", "9.007199254740992e15", 9007199254740992.0},
	} {
		bj, err := ParseFromString(c.source)
		require.NoError(t, err, c.source)
		require.Equal(t, TpCodeFloat64, bj.Type, "source %s must keep DOUBLE binary kind", c.source)
		text, err := bj.MarshalJSON()
		require.NoError(t, err, c.source)
		require.Equal(t, c.textual, string(text), "rendered text of %s", c.source)
		reparsed, err := ParseFromString(string(text))
		require.NoError(t, err, c.source)
		require.Equal(t, TpCodeFloat64, reparsed.Type, "reparse kind of %s", c.source)
		require.Equal(t, c.value, reparsed.GetFloat64(), "reparse value of %s", c.source)
	}
}

// TestIntegersKeepPlainIntegerText guards the counterexample: appending ".0"
// must only apply to DOUBLE-typed values, never to int64/uint64 scalars.
func TestIntegersKeepPlainIntegerText(t *testing.T) {
	for _, c := range []string{"1", "-1", "0", "9223372036854775807"} {
		bj, err := ParseFromString(c)
		require.NoError(t, err)
		require.NotEqual(t, TpCodeFloat64, bj.Type, c)
		text, err := bj.MarshalJSON()
		require.NoError(t, err)
		require.Equal(t, c, string(text), "integer text of %s", c)
	}
}
