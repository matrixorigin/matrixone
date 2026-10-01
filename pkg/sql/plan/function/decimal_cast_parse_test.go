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

package function

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecimalCastParseWrappers(t *testing.T) {
	d64, err := ParseDecimal64CastString("0b10", 10, 2)
	require.NoError(t, err)
	require.Equal(t, "2.00", d64.Format(2))
	d128, err := ParseDecimal128CastString("1.25", 30, 2)
	require.NoError(t, err)
	require.Equal(t, "1.25", d128.Format(2))
	d256, err := ParseDecimal256CastString("1.25", 40, 2)
	require.NoError(t, err)
	require.Equal(t, "1.25", d256.Format(2))

	d64, err = ParseExplicitDecimal64CastString("9999999999999999999", 10, 2)
	require.NoError(t, err)
	require.Equal(t, "99999999.99", d64.Format(2))
	d128, err = ParseExplicitDecimal128CastString("999999999999999999999999999999999999999", 30, 2)
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("9", 28)+".99", d128.Format(2))
	d256, err = ParseExplicitDecimal256CastString("999999999999999999999999999999999999999999999", 40, 2)
	require.NoError(t, err)
	require.Equal(t, "99999999999999999999999999999999999999.99", d256.Format(2))

	_, err = ParseExplicitDecimal64CastString("not-a-decimal", 10, 2)
	require.Error(t, err)
	_, err = ParseExplicitDecimal128CastString("not-a-decimal", 30, 2)
	require.Error(t, err)
	_, err = ParseExplicitDecimal256CastString("not-a-decimal", 40, 2)
	require.Error(t, err)
}

func TestDecimalScientificCastContracts(t *testing.T) {
	parsers := []struct {
		name  string
		parse func(string) (string, error)
	}{
		{"64", func(s string) (string, error) {
			v, e := ParseExplicitDecimal64CastString(s, 5, 2)
			return v.Format(2), e
		}},
		{"128", func(s string) (string, error) {
			v, e := ParseExplicitDecimal128CastString(s, 5, 2)
			return v.Format(2), e
		}},
		{"256", func(s string) (string, error) {
			v, e := ParseExplicitDecimal256CastString(s, 5, 2)
			return v.Format(2), e
		}},
	}
	for _, p := range parsers {
		t.Run(p.name, func(t *testing.T) {
			for _, tc := range []struct{ input, want string }{
				{"1E-2", "0.01"}, {"1e-2", "0.01"}, {"-1E-2", "-0.01"}, {"+1E+2", "100.00"},
				{"0E2", "0.00"}, {"0E2147483647", "0.00"}, {"1E-2147483647", "0.00"},
				{"999.99", "999.99"}, {"999.994", "999.99"}, {"999.995", "999.99"},
				{"-1E10", "-999.99"}, {"1E10", "999.99"}, {"0xFFFFFF", "999.99"},
				{"1" + strings.Repeat("0", 80) + "E-80", "1.00"},
			} {
				got, err := p.parse(tc.input)
				require.NoError(t, err, tc.input)
				require.Equal(t, tc.want, got, tc.input)
			}
			for _, input := range []string{"1p2", "1P2", "1E", "1E2tail", "0Ebad", "0xGG", "1E2147483647", "0." + strings.Repeat("0", 80) + "123E81", "0." + strings.Repeat("0", 80) + "123Ebad"} {
				_, err := p.parse(input)
				require.Error(t, err, input)
			}
		})
	}
	// An in-range storage-parser capacity failure is never evidence of overflow.
	_, err := clampDecimal64CastString("1"+strings.Repeat("0", 80)+"E-80", 5, 2)
	require.Error(t, err)
	_, err = clampDecimal64CastString("0E2", 5, 2)
	require.Error(t, err)
}
