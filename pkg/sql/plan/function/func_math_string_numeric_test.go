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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// T_any is used for both a bare NULL and an unbound prepare marker.  It must
// stay in the arithmetic/metadata domain until execution rebinding proves that
// a concrete character value is present; otherwise MOD(NULL, bigint) widens
// CTAS/UNION/COM_STMT metadata to DOUBLE for no semantic reason.
func TestModUntypedNullKeepsNumericDomain(t *testing.T) {
	ctx := context.Background()
	decimal := types.New(types.T_decimal64, 10, 2)
	for _, tc := range []struct {
		name        string
		args        []types.Type
		wantReturn  types.T
		wantTargets []types.T
	}{
		{name: "left null", args: []types.Type{types.T_any.ToType(), types.T_int64.ToType()}, wantReturn: types.T_int64, wantTargets: []types.T{types.T_int64, types.T_int64}},
		{name: "right null", args: []types.Type{types.T_int64.ToType(), types.T_any.ToType()}, wantReturn: types.T_int64, wantTargets: []types.T{types.T_int64, types.T_int64}},
		{name: "decimal right null", args: []types.Type{decimal, types.T_any.ToType()}, wantReturn: types.T_decimal64, wantTargets: []types.T{types.T_decimal64, types.T_decimal64}},
		{name: "both null", args: []types.Type{types.T_any.ToType(), types.T_any.ToType()}, wantReturn: types.T_int64, wantTargets: []types.T{types.T_int64, types.T_int64}},
		{name: "known string remains double", args: []types.Type{types.T_varchar.ToType(), types.T_any.ToType()}, wantReturn: types.T_float64, wantTargets: []types.T{types.T_float64, types.T_float64}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(ctx, "mod", tc.args)
			require.NoError(t, err)
			require.Equal(t, tc.wantReturn, resolved.GetReturnType().Oid)
			targets, needsCast := resolved.ShouldDoImplicitTypeCast()
			require.True(t, needsCast)
			require.Len(t, targets, len(tc.wantTargets))
			for i := range targets {
				require.Equal(t, tc.wantTargets[i], targets[i].Oid)
			}
		})
	}
}

func TestHistoricalCeilFloorVarcharOverloadCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name string
		fid  int32
		want []float64
	}{
		{name: "ceil", fid: CEIL, want: []float64{2, 0, 0}},
		{name: "floor", fid: FLOOR, want: []float64{1, 0, 0}},
	} {
		t.Run(tc.name+"/mysql_numeric_prefix", func(t *testing.T) {
			session := &numericWarningSession{}
			proc := testutil.NewProcess(t)
			proc.Session = session
			proc.GetSessionInfo().MySQLNumericCompatibilityMode = true
			input := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
				[]byte("1.5tail"), []byte("abc"), []byte(""),
			}, nil)
			defer input.Free(proc.Mp())

			output, err := RunFunctionDirectly(proc, EncodeOverloadID(tc.fid, 12),
				[]*vector.Vector{input}, input.Length())
			require.NoError(t, err)
			require.Equal(t, tc.want, vector.MustFixedColWithTypeCheck[float64](output))
			require.Len(t, session.warnings, 2)
			for _, warning := range session.warnings {
				require.Equal(t, moerr.ER_TRUNCATED_WRONG_VALUE, warning.code)
				require.Contains(t, warning.msg, "DOUBLE")
			}
			output.Free(proc.Mp())
		})

		t.Run(tc.name+"/native_rejects_prefixes", func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.GetSessionInfo().MatrixOneNativeMode = true
			proc.GetSessionInfo().MySQLNumericCompatibilityMode = true
			for _, value := range []string{"1.5tail", "abc", ""} {
				input := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(),
					[][]byte{[]byte(value)}, nil)
				output, err := RunFunctionDirectly(proc, EncodeOverloadID(tc.fid, 12),
					[]*vector.Vector{input}, input.Length())
				require.Error(t, err, value)
				if output != nil {
					output.Free(proc.Mp())
				}
				input.Free(proc.Mp())
			}
		})

		t.Run(tc.name+"/native_unicode_whitespace", func(t *testing.T) {
			session := &numericWarningSession{}
			proc := testutil.NewProcess(t)
			proc.GetSessionInfo().MatrixOneNativeMode = true
			proc.Session = session
			input := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
				[]byte("\u00a01.5\u00a0"),
			}, nil)
			defer input.Free(proc.Mp())

			output, err := RunFunctionDirectly(proc, EncodeOverloadID(tc.fid, 12),
				[]*vector.Vector{input}, input.Length())
			require.NoError(t, err)
			require.Equal(t, []float64{tc.want[0]}, vector.MustFixedColWithTypeCheck[float64](output))
			require.Empty(t, session.warnings)
			output.Free(proc.Mp())
		})

		for _, native := range []bool{false, true} {
			modeName := "mysql"
			if native {
				modeName = "native"
			}
			t.Run(tc.name+"/"+modeName+"_binary_literal", func(t *testing.T) {
				proc := testutil.NewProcess(t)
				proc.GetSessionInfo().MatrixOneNativeMode = native
				proc.GetSessionInfo().MySQLNumericCompatibilityMode = true
				session := &numericWarningSession{}
				proc.Session = session
				input := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(),
					[][]byte{{0x31}}, nil)
				input.SetIsBin(true)
				defer input.Free(proc.Mp())

				output, err := RunFunctionDirectly(proc, EncodeOverloadID(tc.fid, 12),
					[]*vector.Vector{input}, input.Length())
				require.NoError(t, err)
				require.Equal(t, []float64{49}, vector.MustFixedColWithTypeCheck[float64](output))
				require.Empty(t, session.warnings)
				output.Free(proc.Mp())
			})
		}
	}
}

func TestExactMathStringNumericPrefixTypeMatching(t *testing.T) {
	ctx := context.Background()
	expectedOverloads := map[string]int64{
		"abs string":      EncodeOverloadID(ABS, 2),
		"sign string":     EncodeOverloadID(SIGN, 2),
		"ceil string":     EncodeOverloadID(CEIL, 4),
		"floor string":    EncodeOverloadID(FLOOR, 4),
		"round string":    EncodeOverloadID(ROUND, 4),
		"truncate string": EncodeOverloadID(TRUNCATE, 5),
	}
	for _, test := range []struct {
		name        string
		args        []types.Type
		returnType  types.T
		shouldCast  bool
		targetTypes []types.T
	}{
		{"abs string", []types.Type{types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64}},
		{"abs char", []types.Type{types.T_char.ToType()}, types.T_float64, true, []types.T{types.T_float64}},
		{"round text", []types.Type{types.T_text.ToType()}, types.T_float64, true, []types.T{types.T_float64}},
		{"sign string", []types.Type{types.T_varchar.ToType()}, types.T_int64, true, []types.T{types.T_float64}},
		{"ceil string", []types.Type{types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64}},
		{"floor string", []types.Type{types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64}},
		{"round string", []types.Type{types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64}},
		{"truncate string", []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}, types.T_float64, true, []types.T{types.T_float64, types.T_int64}},
		{"round string digits string", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64, types.T_int64}},
		{"truncate string digits string", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64, types.T_int64}},
		{"mod string", []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}, types.T_float64, true, []types.T{types.T_float64, types.T_float64}},
		{"mod right string", []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64, types.T_float64}},
		{"mod char", []types.Type{types.T_char.ToType(), types.T_int64.ToType()}, types.T_float64, true, []types.T{types.T_float64, types.T_float64}},
		{"abs int control", []types.Type{types.T_int64.ToType()}, types.T_int64, false, nil},
		{"mod int control", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, types.T_int64, false, nil},
		{"round decimal control", []types.Type{types.New(types.T_decimal64, 10, 2)}, types.T_decimal64, false, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := GetFunctionByName(ctx, test.name[:indexOfSpace(test.name)], test.args)
			require.NoError(t, err)
			if want, ok := expectedOverloads[test.name]; ok {
				require.Equal(t, want, got.GetEncodedOverloadID())
			}
			require.Equal(t, test.returnType, got.GetReturnType().Oid)
			targets, shouldCast := got.ShouldDoImplicitTypeCast()
			require.Equal(t, test.shouldCast, shouldCast)
			if test.targetTypes != nil {
				require.Len(t, targets, len(test.targetTypes))
				for i := range targets {
					require.Equal(t, test.targetTypes[i], targets[i].Oid)
				}
			}
		})
	}
}

func indexOfSpace(s string) int {
	for i := range s {
		if s[i] == ' ' {
			return i
		}
	}
	return len(s)
}
