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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestPreparedGetLockTimeoutPreservesRuntimeType(t *testing.T) {
	ctx := context.Background()
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_get_lock_timeout from 'select get_lock(''prepared_lock'', ?)'")
	require.NoError(t, err)
	preparePlan := prepared.GetDcl().GetPrepare().Plan
	fn := findPlanFunctionExpr(preparePlan, "get_lock")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_float64), fn.GetF().Args[1].Typ.Id,
		"a bare marker has a stable prepare-time DOUBLE domain")
	require.True(t, PreparedPlanNeedsRuntimeSpecialization(preparePlan),
		"the registered GET_LOCK overload changes with the timeout marker's runtime numeric type")

	for _, test := range []struct {
		name         string
		timeout      ParamValue
		wantArgType  types.T
		wantOverload int32
	}{
		{
			name:         "decimal half-up domain",
			timeout:      ParamValue{Value: "0.5", PrepareParamKind: vector.PrepareParamDecimal},
			wantArgType:  types.T_decimal64,
			wantOverload: 1,
		},
		{
			name:         "double ties-to-even domain",
			timeout:      ParamValue{Value: "0.5", PrepareParamKind: vector.PrepareParamFloat},
			wantArgType:  types.T_float64,
			wantOverload: 0,
		},
		{
			name:         "integer keeps the historical double coercion",
			timeout:      ParamValue{Value: "2", PrepareParamKind: vector.PrepareParamInteger},
			wantArgType:  types.T_float64,
			wantOverload: 0,
		},
		{
			name:         "decimal after double does not reuse stale metadata",
			timeout:      ParamValue{Value: "1.25", PrepareParamKind: vector.PrepareParamDecimal},
			wantArgType:  types.T_decimal64,
			wantOverload: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := []any{test.timeout}
			runtimePlan, specialized, err := FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(
				ctx, preparePlan, values,
			)
			require.NoError(t, err)
			require.True(t, specialized)
			runtimeFn := findPlanFunctionExpr(runtimePlan, "get_lock")
			require.NotNil(t, runtimeFn)
			_, overload := function.DecodeOverloadID(runtimeFn.GetF().GetFunc().GetObj())
			require.Equal(t, test.wantOverload, overload)
			require.Equal(t, int32(test.wantArgType), runtimeFn.GetF().Args[1].Typ.Id)
		})
	}
}

func TestPreparedGetLockExplicitTimeoutCastKeepsItsDomain(t *testing.T) {
	for _, test := range []struct {
		name         string
		typeName     string
		wantArgType  types.T
		wantOverload int32
	}{
		{name: "decimal", typeName: "DECIMAL(20,5)", wantArgType: types.T_decimal128, wantOverload: 2},
		{name: "double", typeName: "DOUBLE", wantArgType: types.T_float64, wantOverload: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t,
				"prepare stmt_get_lock_explicit from 'select get_lock(''prepared_lock'', cast(? as "+test.typeName+"))'")
			require.NoError(t, err)
			preparePlan := prepared.GetDcl().GetPrepare().Plan
			require.False(t, PreparedPlanNeedsRuntimeSpecialization(preparePlan),
				"an explicit cast owns the timeout conversion domain")
			fn := findPlanFunctionExpr(preparePlan, "get_lock")
			require.NotNil(t, fn)
			_, overload := function.DecodeOverloadID(fn.GetF().GetFunc().GetObj())
			require.Equal(t, test.wantOverload, overload)
			require.Equal(t, int32(test.wantArgType), fn.GetF().Args[1].Typ.Id)
		})
	}
}
