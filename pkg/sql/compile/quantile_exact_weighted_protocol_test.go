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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestPreflightQuantileExactWeightedConfigsReturnsPreparedValueError checks
// the prepared-level preflight and the config construction boundary: the
// level is evaluated exactly once, its [0,1] contract rejects bad EXECUTE
// values, and the configuration extraction keeps the two data arguments in
// their original types.
func TestPreflightQuantileExactWeightedConfigsReturnsPreparedValueError(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	value := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}
	weight := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}}
	level := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	// The function registry is keyed by the lower-case SQL name, the same
	// form the SQL parser hands the binder.
	const registryName = "quantileexactweighted"
	bound, err := plan2.BindFuncExprImplByPlanExpr(
		context.Background(), registryName,
		[]*planpb.Expr{value, weight, level})
	require.NoError(t, err)
	node := &planpb.Node{AggList: []*planpb.Expr{bound}}

	check := func(param []byte, isNull bool) error {
		params := vector.NewVec(types.T_text.ToType(), proc.Mp())
		require.NoError(t, vector.AppendBytes(params, param, isNull, proc.Mp()))
		proc.SetPrepareParams(params)
		baseline := proc.Mp().CurrNB()
		err := preflightPercentileConfigs(node, proc)
		require.Equal(t, baseline, proc.Mp().CurrNB(),
			"preflight evaluation must release its temporary vector")
		proc.SetPrepareParams(nil)
		params.Free(proc.Mp())
		return err
	}

	err = check([]byte("1.5"), false)
	require.ErrorContains(t, err,
		"percentile argument of quantileexactweighted must be finite and in [0,1]")
	err = check(nil, true)
	require.ErrorContains(t, err,
		"percentile argument of quantileexactweighted cannot be NULL")

	params := vector.NewVec(types.T_text.ToType(), proc.Mp())
	require.NoError(t, vector.AppendBytes(params, []byte("0.25"), false, proc.Mp()))
	proc.SetPrepareParams(params)
	require.NoError(t, preflightPercentileConfigs(node, proc),
		"the same prepared aggregate must remain reusable after rejected executions")
	args, config, err := constructQuantileExactWeightedConfig(bound.GetF(), proc)
	require.NoError(t, err)
	require.Len(t, args, 2,
		"only the level is stripped; value and weight stay as data arguments")
	require.Equal(t, int32(types.T_int64), args[0].Typ.Id)
	require.Equal(t, int32(types.T_uint64), args[1].Typ.Id)
	require.Equal(t, aggexec.EncodeQuantileExactWeightedConfig([]byte("0.25")), config)
	proc.SetPrepareParams(nil)
	params.Free(proc.Mp())

	// A constant level does not need the prepared parameter path and reaches
	// the same configuration through the ordinary construction boundary.
	constLevel := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_float64)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Dval{Dval: 0.5}}},
	}
	constBound, err := plan2.BindFuncExprImplByPlanExpr(
		context.Background(), registryName,
		[]*planpb.Expr{value, weight, constLevel})
	require.NoError(t, err)
	args, config, err = constructQuantileExactWeightedConfig(constBound.GetF(), proc)
	require.NoError(t, err)
	require.Len(t, args, 2)
	require.Equal(t, aggexec.EncodeQuantileExactWeightedConfig([]byte("0.5")), config)

	// Wrong arity and a non-configuration level are rejected with the
	// aggregate's own diagnostic.
	_, _, err = constructQuantileExactWeightedConfig(&planpb.Function{
		Func: &planpb.ObjectRef{ObjName: plan2.NameQuantileExactWeighted},
		Args: []*planpb.Expr{value, level},
	}, proc)
	require.ErrorContains(t, err,
		"quantileExactWeighted requires a value, a weight, and a level argument")

	columnLevel := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 3}},
	}
	_, err = plan2.BindFuncExprImplByPlanExpr(
		context.Background(), registryName,
		[]*planpb.Expr{value, weight, columnLevel})
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"level argument of quantileexactweighted must be a non-null constant or parameter")
}
