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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestIssue28579PreparedIntegerDivKeepsPrecision(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare issue28579 from 'select cast(9223372036854775807 as signed) div ?'")
	require.NoError(t, err)

	queryPlan := prepared.GetDcl().GetPrepare().Plan
	fn := findPlanFunctionExpr(queryPlan, "div")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_int64), fn.GetF().Args[0].Typ.Id)
	require.Equal(t, int32(types.T_int64), fn.GetF().Args[1].Typ.Id)

	filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), queryPlan, []any{
		ParamValue{Value: "3", PrepareParamKind: vector.PrepareParamInteger},
	})
	require.NoError(t, err)
	got, err := evalPreparedDivExpr(t, filled)
	require.NoError(t, err)
	require.Equal(t, int64(3074457345618258602), got)
}

func TestIssue28578UnsignedDividendRejectsNegativeQuotient(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		param ParamValue
	}{
		{
			name:  "signed divisor",
			sql:   "prepare issue28578_signed from 'select cast(10 as unsigned) div cast(? as signed)'",
			param: ParamValue{Value: "-3", PrepareParamKind: vector.PrepareParamInteger},
		},
		{
			name:  "decimal divisor",
			sql:   "prepare issue28578_decimal from 'select cast(10 as unsigned) div cast(? as decimal(10, 2))'",
			param: ParamValue{Value: "-3.00", PrepareParamKind: vector.PrepareParamDecimal},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			queryPlan := prepared.GetDcl().GetPrepare().Plan
			fn := findPlanFunctionExpr(queryPlan, "div")
			require.NotNil(t, fn)
			require.Equal(t, int32(types.T_uint64), fn.GetF().Args[0].Typ.Id)
			require.NotEqual(t, int32(types.T_float64), fn.GetF().Args[1].Typ.Id)

			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), queryPlan, []any{test.param})
			require.NoError(t, err)
			_, err = evalPreparedDivExpr(t, filled)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
		})
	}
}

func evalPreparedDivExpr(t *testing.T, pl *Plan) (int64, error) {
	t.Helper()
	proc := testutil.NewProc(t)
	defer proc.Free()
	expr := pl.GetQuery().Nodes[len(pl.GetQuery().Nodes)-1].ProjectList[0]
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	if err != nil {
		return 0, err
	}
	defer executor.Free()
	result, err := executor.Eval(proc, nil, nil)
	if err != nil {
		return 0, err
	}
	return vector.GetFixedAtNoTypeCheck[int64](result, 0), nil
}
