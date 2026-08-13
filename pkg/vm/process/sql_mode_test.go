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
