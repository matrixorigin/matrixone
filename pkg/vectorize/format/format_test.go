// Copyright 2022 Matrix Origin
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

package format

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFormatNormalizesNumericPrefixesAndRoundsSafely(t *testing.T) {
	tests := []struct {
		name   string
		number string
		scale  string
		want   string
	}{
		{name: "leading whitespace", number: " 1234.56", scale: "2", want: "1,234.56"},
		{name: "leading plus", number: "+1234.56", scale: "2", want: "1,234.56"},
		{name: "leading zeroes", number: "000123.4500", scale: "2", want: "123.45"},
		{name: "round zero decimals", number: "1.5", scale: "0", want: "2"},
		{name: "round negative zero decimals", number: "-1.5", scale: "0", want: "-2"},
		{name: "round negative fraction approximate", number: "-1.25", scale: "1", want: "-1.2"},
		{name: "scale whitespace", number: "1.2", scale: " 2", want: "1.20"},
		{name: "scale plus", number: "1.2", scale: "+2", want: "1.20"},
		{name: "empty scale", number: "1.2", scale: "", want: "1"},
		{name: "lone minus", number: "-", scale: "2", want: "0.00"},
		{name: "lone dot", number: ".", scale: "2", want: "0.00"},
		{name: "negative dot", number: "-.", scale: "2", want: "0.00"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := formatENUS(test.number, test.scale)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}

	got, err := formatENUSWithMode("-1.25", "1", formatRoundHalfUp)
	require.NoError(t, err)
	require.Equal(t, "-1.3", got)
	for _, tc := range []struct {
		number string
		scale  string
		want   string
	}{
		{number: "1.25", scale: "1", want: "1.2"},
		{number: "1.35", scale: "1", want: "1.4"},
		{number: "2.5", scale: "0", want: "2"},
	} {
		got, err := formatENUS(tc.number, tc.scale)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
	for _, tc := range []struct {
		number string
		want   string
	}{
		{number: "0.545", want: "0.55"},
		{number: "1.015", want: "1.01"},
		{number: "-0.545", want: "-0.55"},
		{number: "-1.015", want: "-1.01"},
	} {
		got, err := GetNumberFormat(tc.number, "2", "en_US")
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}

func TestFormatCapsScaleAndHandlesScientificOverflow(t *testing.T) {
	got, err := formatENUS("1.5", "100")
	require.NoError(t, err)
	require.Equal(t, "1.500000000000000200000000000000", got)

	got, err = formatENUS("1e309", "2")
	require.NoError(t, err)
	require.Contains(t, got, "179,769,313")
	require.True(t, strings.HasSuffix(got, ".00"))
}

func TestFormatLocaleLookupIsCaseInsensitiveAndUsesIndianGrouping(t *testing.T) {
	for _, locale := range []string{"de_de", "DE_DE", "De_dE"} {
		got, err := GetNumberFormat("1234.56", "2", locale)
		require.NoError(t, err)
		require.Equal(t, "1.234,56", got)
	}
	for _, locale := range []string{"en_IN", "EN_in", "Ta_In", "te_IN"} {
		got, err := GetNumberFormat("123456789.12", "2", locale)
		require.NoError(t, err)
		require.Equal(t, "12,34,56,789.12", got)
	}
	for _, tc := range []struct {
		locale string
		want   string
	}{
		{locale: "fr_FR", want: "1234,56"},
		{locale: "IT_ch", want: "1'234,56"},
	} {
		got, err := GetNumberFormat("1234.56", "2", tc.locale)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}

func TestExactFormatHandlesScientificAndRoundingBoundaries(t *testing.T) {
	for _, tc := range []struct {
		number string
		scale  string
		want   string
	}{
		{number: "not-a-number", scale: "2", want: "0.00"},
		{number: "1.25e1", scale: "1", want: "12.5"},
		{number: "1e-400", scale: "2", want: "0.00"},
	} {
		got, err := GetNumberFormatExact(tc.number, tc.scale, "en_US")
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}

	positiveOverflow, err := GetNumberFormatExact("1e309", "0", "en_US")
	require.NoError(t, err)
	require.Contains(t, positiveOverflow, "179,769,313")
	negativeOverflow, err := GetNumberFormatExact("-1e309", "0", "en_US")
	require.NoError(t, err)
	require.Contains(t, negativeOverflow, "-179,769,313")

	for _, tc := range []struct {
		number string
		want   string
	}{
		{number: "1.25", want: "1.2"},
		{number: "1.35", want: "1.4"},
		{number: "1.251", want: "1.3"},
	} {
		got, err := formatENUSWithMode(tc.number, "1", formatRoundHalfEven)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}

func TestApproximateFormatConvertsThroughFloat64(t *testing.T) {
	tests := []struct {
		name   string
		number string
		scale  string
		want   string
	}{
		{name: "binary tie from long decimal", number: "1.25000000000000001", scale: "1", want: "1.2"},
		{name: "integer beyond float precision", number: "9007199254740993", scale: "0", want: "9,007,199,254,740,992"},
		{name: "double rounding", number: "2.675", scale: "2", want: "2.68"},
		{name: "binary scale up", number: "0.545", scale: "2", want: "0.55"},
		{name: "binary scale down", number: "1.015", scale: "2", want: "1.01"},
		{name: "subnormal scale up", number: "0.00000000000000009", scale: "16", want: "0.0000000000000001"},
		{name: "negative subnormal scale up", number: "-0.00000000000000009", scale: "16", want: "-0.0000000000000001"},
		{name: "post-division double at high scale", number: "1.5", scale: "30", want: "1.500000000000000200000000000000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GetNumberFormat(tc.number, tc.scale, "en_US")
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	plain, err := GetNumberFormat(strings.Repeat("9", 400), "2", "en_US")
	require.NoError(t, err)
	scientific, err := GetNumberFormat("1e309", "2", "en_US")
	require.NoError(t, err)
	require.Equal(t, scientific, plain)
}

func TestForamtENUS(t *testing.T) {
	cases := []struct {
		name   string
		number string
		scale  string
		want   string
	}{
		{
			name:   "TEST01",
			number: "12332.2",
			scale:  "2",
			want:   "12,332.20",
		},
		{
			name:   "TEST02",
			number: "12332.123456",
			scale:  "4",
			want:   "12,332.1235",
		},
		{
			name:   "TEST03",
			number: "12332.1",
			scale:  "4",
			want:   "12,332.1000",
		},
		{
			name:   "TEST04",
			number: "12332.2",
			scale:  "0",
			want:   "12,332",
		},
		{
			name:   "TEST05",
			number: "-.12334.2",
			scale:  "2",
			want:   "-0.12",
		},
		{
			name:   "TEST06",
			number: "19999999.999999999",
			scale:  "4",
			want:   "20,000,000.0000",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := formatENUS(c.number, c.scale)
			require.Equal(t, c.want, got)
		})
	}
}

func TestForamtARSA(t *testing.T) {
	cases := []struct {
		name   string
		number string
		scale  string
		want   string
	}{
		{
			name:   "TEST01",
			number: "12332.2",
			scale:  "2",
			want:   "12332.20",
		},
		{
			name:   "TEST02",
			number: "12332.123456",
			scale:  "4",
			want:   "12332.1235",
		},
		{
			name:   "TEST03",
			number: "12332.1",
			scale:  "4",
			want:   "12332.1000",
		},
		{
			name:   "TEST04",
			number: "12332.2",
			scale:  "0",
			want:   "12332",
		},
		{
			name:   "TEST05",
			number: "-.12334.2",
			scale:  "2",
			want:   "-0.12",
		},
		{
			name:   "TEST06",
			number: "19999999.999999999",
			scale:  "4",
			want:   "20000000.0000",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := formatARSA(c.number, c.scale)
			require.Equal(t, c.want, got)
		})
	}
}

func TestForamtBEBY(t *testing.T) {
	cases := []struct {
		name   string
		number string
		scale  string
		want   string
	}{
		{
			name:   "TEST01",
			number: "12332.2",
			scale:  "2",
			want:   "12.332,20",
		},
		{
			name:   "TEST02",
			number: "12332.123456",
			scale:  "4",
			want:   "12.332,1235",
		},
		{
			name:   "TEST03",
			number: "12332.1",
			scale:  "4",
			want:   "12.332,1000",
		},
		{
			name:   "TEST04",
			number: "12332.2",
			scale:  "0",
			want:   "12.332",
		},
		{
			name:   "TEST05",
			number: "-.12334.2",
			scale:  "2",
			want:   "-0,12",
		},
		{
			name:   "TEST06",
			number: "19999999.999999999",
			scale:  "4",
			want:   "20.000.000,0000",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := formatBEBY(c.number, c.scale)
			require.Equal(t, c.want, got)
		})
	}
}

func TestForamtBGBG(t *testing.T) {
	cases := []struct {
		name   string
		number string
		scale  string
		want   string
	}{
		{
			name:   "TEST01",
			number: "12332.2",
			scale:  "2",
			want:   "12 332,20",
		},
		{
			name:   "TEST02",
			number: "12332.123456",
			scale:  "4",
			want:   "12 332,1235",
		},
		{
			name:   "TEST03",
			number: "12332.1",
			scale:  "4",
			want:   "12 332,1000",
		},
		{
			name:   "TEST04",
			number: "12332.2",
			scale:  "0",
			want:   "12 332",
		},
		{
			name:   "TEST05",
			number: "-.12334.2",
			scale:  "2",
			want:   "-0,12",
		},
		{
			name:   "TEST06",
			number: "19999999.999999999",
			scale:  "4",
			want:   "20 000 000,0000",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := formatBGBG(c.number, c.scale)
			require.Equal(t, c.want, got)
		})
	}
}

func TestForamtDECH(t *testing.T) {
	cases := []struct {
		name   string
		number string
		scale  string
		want   string
	}{
		{
			name:   "TEST01",
			number: "12332.2",
			scale:  "2",
			want:   "12'332.20",
		},
		{
			name:   "TEST02",
			number: "12332.123456",
			scale:  "4",
			want:   "12'332.1235",
		},
		{
			name:   "TEST03",
			number: "12332.1",
			scale:  "4",
			want:   "12'332.1000",
		},
		{
			name:   "TEST04",
			number: "12332.2",
			scale:  "0",
			want:   "12'332",
		},
		{
			name:   "TEST05",
			number: "-.12334.2",
			scale:  "2",
			want:   "-0.12",
		},
		{
			name:   "TEST06",
			number: "19999999.999999999",
			scale:  "4",
			want:   "20'000'000.0000",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := formatDECH(c.number, c.scale)
			require.Equal(t, c.want, got)
		})
	}
}
