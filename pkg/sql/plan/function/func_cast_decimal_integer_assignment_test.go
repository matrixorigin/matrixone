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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimalIntegerAssignmentClipsOnlyWhenAllowed(t *testing.T) {
	sources := []struct {
		name  string
		typ   types.Type
		parse func(*testing.T, string) any
	}{
		{"decimal64", types.New(types.T_decimal64, 18, 2), func(t *testing.T, s string) any {
			v, err := types.ParseDecimal64(s, 18, 2)
			require.NoError(t, err)
			return []types.Decimal64{v}
		}},
		{"decimal128", types.New(types.T_decimal128, 38, 2), func(t *testing.T, s string) any {
			v, err := types.ParseDecimal128(s, 38, 2)
			require.NoError(t, err)
			return []types.Decimal128{v}
		}},
		{"decimal256", types.New(types.T_decimal256, 65, 2), func(t *testing.T, s string) any {
			v, err := types.ParseDecimal256(s, 65, 2)
			require.NoError(t, err)
			return []types.Decimal256{v}
		}},
	}
	cases := []struct {
		name, input string
		target      types.Type
		want        any
		outOfRange  bool
	}{
		{"unsigned high", "300.00", types.T_uint8.ToType(), []uint8{255}, true},
		{"unsigned negative", "-1.00", types.T_uint8.ToType(), []uint8{0}, true},
		{"signed high", "300.00", types.T_int8.ToType(), []int8{127}, true},
		{"signed low", "-300.00", types.T_int8.ToType(), []int8{-128}, true},
		{"rounds into range", "255.49", types.T_uint8.ToType(), []uint8{255}, false},
		{"rounds out of range", "255.50", types.T_uint8.ToType(), []uint8{255}, true},
		{"signed rounds out", "127.50", types.T_int8.ToType(), []int8{127}, true},
	}
	modes := []struct {
		name, sqlMode string
		ignore        bool
		strict        bool
	}{
		{"strict", "STRICT_TRANS_TABLES", false, true},
		{"nonstrict", "", false, false},
		{"ignore", "STRICT_TRANS_TABLES", true, false},
	}
	for _, source := range sources {
		for _, tc := range cases {
			for _, mode := range modes {
				t.Run(source.name+"/"+tc.name+"/"+mode.name, func(t *testing.T) {
					proc := testutil.NewProcess(t)
					proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
						if name == "sql_mode" {
							return mode.sqlMode, nil
						}
						return nil, nil
					})
					session := &numericWarningSession{}
					proc.Session = session
					proc.WarningSink = session
					fn := NewAssignCast
					if mode.ignore {
						fn = NewAssignIgnoreCast
					}
					input := []FunctionTestInput{
						NewFunctionTestInput(source.typ, source.parse(t, tc.input), nil),
						NewFunctionTestInput(tc.target, tc.want, nil),
					}
					expect := NewFunctionTestResult(tc.target, tc.outOfRange && mode.strict, tc.want, nil)
					fc := NewFunctionTestCase(proc, input, expect, fn)
					ok, info := fc.RunAndFree()
					require.True(t, ok, info)
					if tc.outOfRange && !mode.strict {
						require.Len(t, session.warnings, 1)
						require.Equal(t, moerr.ER_WARN_DATA_OUT_OF_RANGE, session.warnings[0].code)
					} else {
						require.Empty(t, session.warnings)
					}
				})
			}
		}
	}
}
