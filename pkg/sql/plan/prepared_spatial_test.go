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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestPreparedSpatialDistanceUnitOverload(t *testing.T) {
	ctx := t.Context()
	geometryArgs := "st_geomfromtext(?,4326), st_geomfromtext(?,4326)"

	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_spatial_distance from 'select st_distance("+geometryArgs+", ?)'")
	require.NoError(t, err)
	preparePlan := prepared.GetDcl().GetPrepare().Plan
	prepareFn := findPlanFunctionExpr(preparePlan, "st_distance")
	require.NotNil(t, prepareFn)
	_, prepareOverload := function.DecodeOverloadID(prepareFn.GetF().GetFunc().GetObj())
	require.Equal(t, int32(4), prepareOverload,
		"the marker uses the VARCHAR unit overload as the provisional prepare-time domain")
	require.True(t, PreparedPlanNeedsRuntimeSpecialization(preparePlan))

	unitValues := []any{
		ParamValue{Value: "POINT(0 0)", SourceType: types.T_varchar.ToType(), HasSourceType: true},
		ParamValue{Value: "POINT(1 0)", SourceType: types.T_varchar.ToType(), HasSourceType: true},
		ParamValue{Value: "kilometre", SourceType: types.T_varchar.ToType(), HasSourceType: true},
	}
	runtimePlan, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan, unitValues)
	require.NoError(t, err)
	require.True(t, specialized)
	runtimeFn := findPlanFunctionExpr(runtimePlan, "st_distance")
	require.NotNil(t, runtimeFn)
	_, runtimeOverload := function.DecodeOverloadID(runtimeFn.GetF().GetFunc().GetObj())
	require.Equal(t, int32(4), runtimeOverload,
		"a string-backed EXECUTE marker must select the length-unit overload")
	require.Equal(t, int32(types.T_varchar), runtimeFn.GetF().Args[2].Typ.Id)

	numericValues := []any{
		unitValues[0], unitValues[1],
		ParamValue{
			Value:            int64(4326),
			SourceType:       types.T_varchar.ToType(),
			HasSourceType:    true,
			PrepareParamKind: vector.PrepareParamInteger,
		},
	}
	runtimePlan, _, err = FillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan, numericValues)
	require.NoError(t, err)
	runtimeFn = findPlanFunctionExpr(runtimePlan, "st_distance")
	require.NotNil(t, runtimeFn)
	_, runtimeOverload = function.DecodeOverloadID(runtimeFn.GetF().GetFunc().GetObj())
	require.Equal(t, int32(1), runtimeOverload,
		"a numeric EXECUTE marker must preserve MatrixOne's explicit-SRID overload")

	for _, test := range []struct {
		name        string
		function    string
		wantPrepare int32
		wantRuntime int32
	}{
		{name: "frechet", function: "st_frechetdistance", wantPrepare: 2, wantRuntime: 2},
		{name: "hausdorff", function: "st_hausdorffdistance", wantPrepare: 2, wantRuntime: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t,
				"prepare stmt_spatial_"+test.name+" from 'select "+test.function+"("+geometryArgs+", ?)'")
			require.NoError(t, err)
			preparePlan := prepared.GetDcl().GetPrepare().Plan
			prepareFn := findPlanFunctionExpr(preparePlan, test.function)
			require.NotNil(t, prepareFn)
			_, overload := function.DecodeOverloadID(prepareFn.GetF().GetFunc().GetObj())
			require.Equal(t, test.wantPrepare, overload)

			runtimePlan, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan, unitValues)
			require.NoError(t, err)
			runtimeFn := findPlanFunctionExpr(runtimePlan, test.function)
			require.NotNil(t, runtimeFn)
			_, overload = function.DecodeOverloadID(runtimeFn.GetF().GetFunc().GetObj())
			require.Equal(t, test.wantRuntime, overload)
			// These functions have no legacy third-argument SRID overload, so the
			// unit overload is already selected at PREPARE and need not specialize.
			_ = specialized
		})
	}
}
