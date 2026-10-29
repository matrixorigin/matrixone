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

package frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.starlark.net/starlark"
)

func TestConvertToStarlarkValueNull(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input any
		want  starlark.Value
	}{
		{"null", nil, starlark.None},
		{"empty string", "", starlark.String("")},
		{"string", "text", starlark.String("text")},
		{"zero", int64(0), starlark.MakeInt(0)},
		{"signed", int32(-7), starlark.MakeInt(-7)},
		{"unsigned", uint64(7), starlark.MakeUint64(7)},
		{"float", 1.5, starlark.Float(1.5)},
		{"bool", false, starlark.False},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertToStarlarkValue(context.Background(), tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	// byte is the Go uint8 alias, so non-nil bytes use the SQL UINT8 vector path.
	bytes, err := convertToStarlarkValue(context.Background(), []byte("text"))
	require.NoError(t, err)
	require.Equal(t, starlark.String("[116, 101, 120, 116]"), bytes)
	_, err = convertToStarlarkValue(context.Background(), []int16{1, 2})
	require.ErrorContains(t, err, "type slice is not a supported starlark type")
	got, err := convertFromStarlarkValue(context.Background(), starlark.None)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestStarlarkVariablesNull(t *testing.T) {
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	si := &starlarkInterpreter{interp: &Interpreter{ctx: context.Background(), ses: ses}}
	for _, value := range []starlark.Value{starlark.None, starlark.String("")} {
		t.Run(value.Type(), func(t *testing.T) {
			// First fail, then ensure the successful NULL/empty round trip clears diagnostics.
			_, err := si.moGetVar(nil, nil, starlark.Tuple{starlark.String("missing")}, nil)
			require.NoError(t, err)
			set, err := si.moSetVar(nil, nil, starlark.Tuple{starlark.String("value"), value}, nil)
			require.NoError(t, err)
			require.Equal(t, starlark.None, set.(*starlark.List).Index(1))
			stored, err := ses.GetUserDefinedVar("value")
			require.NoError(t, err)
			if value == starlark.None {
				require.Nil(t, stored.Value)
			} else {
				require.Equal(t, "", stored.Value)
			}
			got, err := si.moGetVar(nil, nil, starlark.Tuple{starlark.String("value")}, nil)
			require.NoError(t, err)
			require.Equal(t, value, got.(*starlark.List).Index(0))
			require.Equal(t, starlark.None, got.(*starlark.List).Index(1))
			errno, err := si.moErrno(nil, nil, nil, nil)
			require.NoError(t, err)
			require.Equal(t, starlark.MakeInt(0), errno)
		})
	}
}

type starlarkNullErrorResult struct {
	ExecResult
}

func (starlarkNullErrorResult) ColumnIsNull(context.Context, uint64, uint64) (bool, error) {
	return false, errors.New("cannot read SQL NULL")
}

type starlarkStringErrorResult struct {
	ExecResult
}

func (starlarkStringErrorResult) GetString(context.Context, uint64, uint64) (string, error) {
	return "", errors.New("cannot read SQL string")
}

func TestStarlarkSQLNullCells(t *testing.T) {
	rs := &MysqlResultSet{}
	for range 4 {
		rs.AddColumn(&MysqlColumn{})
	}
	rs.AddRow([]any{nil, "", int64(0), "text"})
	rs.AddRow([]any{"text", int64(7), "", nil})
	for _, tc := range []struct {
		name    string
		result  ExecResult
		wantErr string
	}{
		{"mixed cells", rs, ""},
		{"NULL inspection error", starlarkNullErrorResult{ExecResult: rs}, "cannot read SQL NULL"},
		{"string extraction error", starlarkStringErrorResult{ExecResult: rs}, "cannot read SQL string"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			back := &evalCondBackgroundExec{execResult: []interface{}{tc.result}}
			si := &starlarkInterpreter{interp: &Interpreter{ctx: context.Background(), bh: back}}
			got, err := si.moSql(nil, nil, starlark.Tuple{starlark.String("select null, '', 0, 'text'")}, nil)
			require.NoError(t, err)
			ret := got.(*starlark.List)
			if tc.wantErr != "" {
				require.Equal(t, starlark.None, ret.Index(0))
				require.Contains(t, ret.Index(1).String(), tc.wantErr)
				return
			}
			require.Equal(t, starlark.None, ret.Index(1))
			rows := ret.Index(0).(*starlark.List)
			require.Equal(t, 2, rows.Len())
			require.Equal(t, starlark.Tuple{starlark.None, starlark.String(""), starlark.String("0"), starlark.String("text")}, rows.Index(0))
			require.Equal(t, starlark.Tuple{starlark.String("text"), starlark.String("7"), starlark.String(""), starlark.None}, rows.Index(1))
		})
	}
}
