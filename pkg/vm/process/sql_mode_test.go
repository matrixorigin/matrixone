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

package process

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// The strict predicates share token interpretation, but require different flags.
func TestStrictSQLModePredicates(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		mode                       any
		strict, zeroDate, division bool
	}{
		{"traditional", "TRADITIONAL", true, true, true},
		{"traditional combined", "ERROR_FOR_DIVISION_BY_ZERO, traditional ,TRADITIONAL", true, true, true},
		{"strict trans", "STRICT_TRANS_TABLES", true, false, false},
		{"strict all", "STRICT_ALL_TABLES", true, false, false},
		{"division trans", "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO", true, false, true},
		{"division all", " error_for_division_by_zero , strict_all_tables ", true, false, true},
		{"date trans", "NO_ZERO_DATE,STRICT_TRANS_TABLES", true, true, false},
		{"date all", "STRICT_ALL_TABLES,NO_ZERO_DATE", true, true, false},
		{"all explicit", "STRICT_ALL_TABLES,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO", true, true, true},
		{"no strict", "ERROR_FOR_DIVISION_BY_ZERO,NO_ZERO_DATE", false, false, false},
		{"traditional exact token", "TRADITIONAL_EXTRA", false, false, false},
		{"strict exact token", "NOT_STRICT_TRANS_TABLES,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO", false, false, false},
		{"component exact tokens", "STRICT_ALL_TABLES,NO_ZERO_DATE_EXTRA,ERROR_FOR_DIVISION_BY_ZERO_EXTRA", true, false, false},
		{"unrelated", "ANSI,NO_ZERO_IN_DATE", false, false, false},
		{"empty tokens", " , , ", false, false, false},
		{"nil", nil, false, false, false},
		{"non string", 1, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.strict, IsStrictMode(tc.mode))
			require.Equal(t, tc.zeroDate, IsStrictNoZeroDateMode(tc.mode))
			require.Equal(t, tc.division, IsStrictDivisionByZeroMode(tc.mode))
		})
	}
}

func TestIsPadCharToFullLengthMode(t *testing.T) {
	tests := []struct {
		name string
		mode any
		want bool
	}{
		{name: "enabled", mode: "PAD_CHAR_TO_FULL_LENGTH", want: true},
		{name: "case and whitespace", mode: "STRICT_TRANS_TABLES, pad_char_to_full_length ", want: true},
		{name: "exact token", mode: "PAD_CHAR_TO_FULL_LENGTH_EXTRA", want: false},
		{name: "disabled", mode: "STRICT_TRANS_TABLES", want: false},
		{name: "non string", mode: int64(1), want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, IsPadCharToFullLengthMode(test.mode))
		})
	}
}

func TestResolvePadCharToFullLength(t *testing.T) {
	t.Run("nil process", func(t *testing.T) {
		enabled, err := ResolvePadCharToFullLength(nil)
		require.NoError(t, err)
		require.False(t, enabled)
	})

	t.Run("local resolver", func(t *testing.T) {
		proc := &Process{Base: &BaseProcess{}}
		proc.SetResolveVariableFunc(func(name string, system, global bool) (any, error) {
			require.Equal(t, "sql_mode", name)
			require.True(t, system)
			require.False(t, global)
			return "PAD_CHAR_TO_FULL_LENGTH", nil
		})
		enabled, err := ResolvePadCharToFullLength(proc)
		require.NoError(t, err)
		require.True(t, enabled)
	})

	t.Run("remote session snapshot", func(t *testing.T) {
		proc := &Process{Base: &BaseProcess{SessionInfo: SessionInfo{SqlMode: "ANSI,PAD_CHAR_TO_FULL_LENGTH"}}}
		enabled, err := ResolvePadCharToFullLength(proc)
		require.NoError(t, err)
		require.True(t, enabled)
	})

	t.Run("resolver error", func(t *testing.T) {
		wantErr := errors.New("resolve failed")
		proc := &Process{Base: &BaseProcess{}}
		proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
			return nil, wantErr
		})
		_, err := ResolvePadCharToFullLength(proc)
		require.ErrorIs(t, err, wantErr)
	})
}
