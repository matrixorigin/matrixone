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

func TestHistoricalCeilVarcharOverloadCompatibility(t *testing.T) {
	t.Run("strict rejects incomplete token", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		input := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{[]byte("1.5tail")}, nil)
		defer input.Free(proc.Mp())
		_, err := RunFunctionDirectly(proc, EncodeOverloadID(CEIL, 12), []*vector.Vector{input}, input.Length())
		require.Error(t, err)
	})

	t.Run("compatibility consumes prefix and warns", func(t *testing.T) {
		session := &numericWarningSession{}
		proc := testutil.NewProcess(t)
		proc.Session = session
		proc.GetSessionInfo().MySQLNumericCompatibilityMode = true
		input := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{[]byte("1.5tail")}, nil)
		defer input.Free(proc.Mp())
		output, err := RunFunctionDirectly(proc, EncodeOverloadID(CEIL, 12), []*vector.Vector{input}, input.Length())
		require.NoError(t, err)
		defer output.Free(proc.Mp())
		require.Equal(t, []float64{2}, vector.MustFixedColWithTypeCheck[float64](output))
		require.Len(t, session.warnings, 1)
		require.Equal(t, moerr.ER_TRUNCATED_WRONG_VALUE, session.warnings[0].code)
		require.Contains(t, session.warnings[0].msg, "DOUBLE")
	})

	t.Run("native mode wins", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.GetSessionInfo().MatrixOneNativeMode = true
		proc.GetSessionInfo().MySQLNumericCompatibilityMode = true
		input := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{[]byte("1.5tail")}, nil)
		defer input.Free(proc.Mp())
		_, err := RunFunctionDirectly(proc, EncodeOverloadID(CEIL, 12), []*vector.Vector{input}, input.Length())
		require.Error(t, err)
	})

	t.Run("marked HEX is numeric", func(t *testing.T) {
		session := &numericWarningSession{}
		proc := testutil.NewProcess(t)
		proc.Session = session
		input := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{{0x31}}, nil)
		input.SetIsBin(true)
		defer input.Free(proc.Mp())
		output, err := RunFunctionDirectly(proc, EncodeOverloadID(CEIL, 12), []*vector.Vector{input}, input.Length())
		require.NoError(t, err)
		defer output.Free(proc.Mp())
		require.Equal(t, []float64{49}, vector.MustFixedColWithTypeCheck[float64](output))
		require.Empty(t, session.warnings)
	})
}
