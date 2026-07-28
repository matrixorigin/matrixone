// Copyright 2021 Matrix Origin
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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTYearTypeDefinition(t *testing.T) {
	// Test T_year constant value
	require.Equal(t, T(55), T_year)

	// Test T_year.ToType()
	typ := T_year.ToType()
	require.Equal(t, T_year, typ.Oid)
	require.Equal(t, int32(2), typ.Size)
	require.Equal(t, int32(4), typ.Width)

	// Test T_year.String()
	require.Equal(t, "YEAR", T_year.String())

	// Test T_year.OidString()
	require.Equal(t, "T_year", T_year.OidString())

	// Test T_year.TypeLen()
	require.Equal(t, 2, T_year.TypeLen())

	// Test T_year.FixedLength()
	require.Equal(t, 2, T_year.FixedLength())

	// Test T_year.IsDateRelate()
	require.True(t, T_year.IsDateRelate())

	// Test T_year.IsOrdered()
	require.True(t, T_year.IsOrdered())

	// Test T_year.IsFixedLen()
	require.True(t, T_year.IsFixedLen())

	// Test Type.IsTemporal()
	require.True(t, typ.IsTemporal())
}

func TestMoYearConstants(t *testing.T) {
	require.Equal(t, MoYear(1901), MinYearValue)
	require.Equal(t, MoYear(2155), MaxYearValue)
}

func TestParseMoYear(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    MoYear
		wantErr bool
	}{
		// 4-digit string tests
		{"4-digit min", "1901", 1901, false},
		{"4-digit max", "2155", 2155, false},
		{"4-digit mid", "2024", 2024, false},

		// 2-digit string tests (0-69 -> 2000-2069)
		{"2-digit 00", "00", 2000, false},
		{"2-digit 0", "0", 2000, false},
		{"2-digit 01", "01", 2001, false},
		{"2-digit 69", "69", 2069, false},

		// 2-digit string tests (70-99 -> 1970-1999)
		{"2-digit 70", "70", 1970, false},
		{"2-digit 99", "99", 1999, false},

		// Error cases
		{"empty string", "", 0, true},
		{"invalid string", "abc", 0, true},
		{"out of range low", "1900", 0, true},
		{"out of range high", "2156", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMoYear(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestParseMoYearFromInt(t *testing.T) {
	tests := []struct {
		name    string
		input   int64
		want    MoYear
		wantErr bool
	}{
		// 4-digit number tests
		{"4-digit min", 1901, 1901, false},
		{"4-digit max", 2155, 2155, false},
		{"4-digit mid", 2024, 2024, false},

		// Numeric 0 -> 0000
		{"numeric 0", 0, 0, false},

		// 2-digit number tests (1-69 -> 2001-2069)
		{"2-digit 1", 1, 2001, false},
		{"2-digit 69", 69, 2069, false},

		// 2-digit number tests (70-99 -> 1970-1999)
		{"2-digit 70", 70, 1970, false},
		{"2-digit 99", 99, 1999, false},

		// Error cases
		{"out of range low", 1900, 0, true},
		{"out of range high", 2156, 0, true},
		{"negative", -1, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMoYearFromInt(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestValidateMoYear(t *testing.T) {
	tests := []struct {
		name    string
		input   MoYear
		wantErr bool
	}{
		{"zero", 0, false},
		{"min", 1901, false},
		{"max", 2155, false},
		{"mid", 2024, false},
		{"below min", 1900, true},
		{"above max", 2156, true},
		{"negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMoYear(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMoYearString(t *testing.T) {
	tests := []struct {
		input MoYear
		want  string
	}{
		{0, "0000"},
		{1901, "1901"},
		{2024, "2024"},
		{2155, "2155"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			require.Equal(t, tt.want, tt.input.String())
		})
	}
}

func TestMoYearToInt64(t *testing.T) {
	tests := []struct {
		input MoYear
		want  int64
	}{
		{0, 0},
		{1901, 1901},
		{2024, 2024},
		{2155, 2155},
	}

	for _, tt := range tests {
		t.Run(tt.input.String(), func(t *testing.T) {
			require.Equal(t, tt.want, tt.input.ToInt64())
		})
	}
}

// Property tests
func TestMoYearRoundTrip(t *testing.T) {
	// Property 5: MoYear-Int Round-Trip
	// For any valid MoYear y, casting to int64 and back produces same value
	validYears := []MoYear{0, 1901, 1970, 2000, 2024, 2069, 2155}
	for _, y := range validYears {
		i := y.ToInt64()
		var y2 MoYear
		if i == 0 {
			y2 = 0
		} else {
			var err error
			y2, err = ParseMoYearFromInt(i)
			require.NoError(t, err)
		}
		require.Equal(t, y, y2)
	}
}

func TestMoYearStringRoundTrip(t *testing.T) {
	// Property 6: MoYear-String Round-Trip
	// For any valid MoYear y, String() then ParseMoYear() produces same value
	validYears := []MoYear{0, 1901, 1970, 2000, 2024, 2069, 2155}
	for _, y := range validYears {
		s := y.String()
		y2, err := ParseMoYear(s)
		require.NoError(t, err)
		require.Equal(t, y, y2)
	}
}

func TestYearTypesMapEntry(t *testing.T) {
	// Verify "year" is in the Types map
	ty, ok := Types["year"]
	require.True(t, ok)
	require.Equal(t, T_year, ty)
}
