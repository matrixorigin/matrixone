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
		{name: "round negative fraction", number: "-1.25", scale: "1", want: "-1.3"},
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
}

func TestFormatCapsScaleAndHandlesScientificOverflow(t *testing.T) {
	got, err := formatENUS("1.5", "100")
	require.NoError(t, err)
	require.Equal(t, "1.5"+strings.Repeat("0", maxFormatDecimals-1), got)

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
