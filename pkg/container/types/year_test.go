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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMoYearTypeDefinition(t *testing.T) {
	typ := T_year.ToType()
	require.Equal(t, T_year, typ.Oid)
	require.Equal(t, int32(2), typ.Size)
	require.Equal(t, int32(4), typ.Width)
	require.Equal(t, "YEAR", T_year.String())
	require.Equal(t, "T_year", T_year.OidString())
	require.Equal(t, 2, T_year.TypeLen())
	require.Equal(t, 2, T_year.FixedLength())
	require.True(t, T_year.IsDateRelate())
	require.True(t, T_year.IsOrdered())
	require.True(t, T_year.IsFixedLen())
	require.True(t, typ.IsTemporal())
}

func TestParseMoYear(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    MoYear
		wantErr bool
	}{
		{"zero string", "0000", 0, false},
		{"four digit min", "1901", 1901, false},
		{"four digit max", "2155", 2155, false},
		{"two digit zero", "0", 2000, false},
		{"two digit low", "69", 2069, false},
		{"two digit high", "70", 1970, false},
		{"empty string", "", 0, true},
		{"invalid string", "abc", 0, true},
		{"below range", "1900", 0, true},
		{"above range", "2156", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMoYear(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseMoYearFromInt(t *testing.T) {
	tests := []struct {
		input   int64
		want    MoYear
		wantErr bool
	}{
		{0, 0, false},
		{1, 2001, false},
		{69, 2069, false},
		{70, 1970, false},
		{99, 1999, false},
		{1901, 1901, false},
		{2155, 2155, false},
		{-1, 0, true},
		{1900, 0, true},
		{2156, 0, true},
	}
	for _, tt := range tests {
		got, err := ParseMoYearFromInt(tt.input)
		if tt.wantErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, tt.want, got)
	}
}

func TestMoYearString(t *testing.T) {
	require.Equal(t, "0000", MoYear(0).String())
	require.Equal(t, "1901", MoYear(1901).String())
	require.Equal(t, int64(2155), MoYear(2155).ToInt64())
	require.NoError(t, ValidateMoYear(2024))
	require.Error(t, ValidateMoYear(2156))
}
