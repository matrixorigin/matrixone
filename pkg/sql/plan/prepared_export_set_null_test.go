// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestPreparedExportSetParamPositionOwnership(t *testing.T) {
	require.Nil(t, PreparedPlanExportSetParamPositions(nil))
	require.Nil(t, PreparedPlanExportSetParamPositions(&Plan{}))
	for _, tc := range []struct {
		sql       string
		positions []int32
	}{
		{`select export_set(coalesce((select ?),1.5),?,?,'',4)`, []int32{0}},
		{`select n_nationkey from nation where export_set((select max(?) from nation),'Y','N','',4)=?`, []int32{0}},
		{`select export_set(cast(? as decimal(4,1)),'Y','N','',4)`, []int32{}},
		{`select ?,?`, []int32{}},
		{`select export_set(?,'Y','N','',4)`, []int32{0}},
		{`select export_set(coalesce(?+0,1.5),'Y','N','',4)`, []int32{0}},
	} {
		opt := NewMockOptimizer(false)
		prepared, err := runOneStmt(opt, t, `prepare s from "`+tc.sql+`"`)
		require.NoError(t, err)
		cached := prepared.GetDcl().GetPrepare().Plan
		before := cached.String()
		require.Equal(t, tc.positions, PreparedPlanExportSetParamPositions(cached), tc.sql)
		require.Equal(t, before, cached.String())
	}
}

func TestPreparedExportSetFoldedProducerIsNotBare(t *testing.T) {
	for _, tc := range []struct {
		sql      string
		wantBare bool
		wantType types.T
	}{
		{`select export_set(?,'Y','N','',4)`, true, types.T_int64},
		{`select export_set((select ?),'Y','N','',4)`, true, types.T_text},
		{`select export_set(x,'Y','N','',4) from (select ? as x) d`, false, types.T_text},
	} {
		prepared, err := runOneStmt(NewMockOptimizer(false), t, `prepare s from "`+tc.sql+`"`)
		require.NoError(t, err)
		positions, domains, bare := PreparedPlanExportSetParameters(prepared.GetDcl().GetPrepare().Plan)
		require.Equal(t, []int32{0}, positions)
		require.Equal(t, tc.wantBare, bare[0], tc.sql)
		require.Equal(t, tc.wantType, domains[0].Oid, tc.sql)
	}
}

func TestPreparedExportSetTypedNullDomain(t *testing.T) {
	opt := NewMockOptimizer(false)
	prepared, err := runOneStmt(opt, t, `prepare s from "select export_set(coalesce((select ?),2.5),'Y','N','',4)"`)
	require.NoError(t, err)
	cached := prepared.GetDcl().GetPrepare().Plan
	before := cached.String()
	for _, tc := range []struct {
		typ  types.Type
		want string
	}{
		{types.T_float64.ToType(), "NYNN"},
		{types.New(types.T_decimal64, 4, 1), "YYNN"},
		{types.New(types.T_decimal256, 65, 1), "YYNN"},
	} {
		for _, binary := range []bool{false, true} {
			param := ParamValue{Value: nil, RuntimeType: tc.typ, HasRuntimeType: true, IsBinaryProtocol: binary, EnableNumericPrefix: true, RetainParamRef: true}
			filled, err := FillValuesOfParamsInPlan(context.Background(), cached, []any{param})
			require.NoError(t, err)
			query := filled.GetQuery()
			expr := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
			result, free, err := colexec.GetReadonlyResultFromExpression(opt.CurrentContext().GetProcess(), expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			func() { defer free(); require.Equal(t, tc.want, result.GetStringAt(0), expr.String()) }()
			require.Equal(t, before, cached.String())
		}
	}
}

func TestPreparedExportSetFirstNullDomain(t *testing.T) {
	for _, tc := range []struct{ value, want string }{
		{`coalesce(?,1.5)`, "NYNN"},
		{`coalesce((select ?),1.5)`, "YNNN"},
		{`cast(coalesce((select ?),1.5) as decimal(4,1))`, "NYNN"},
		{`coalesce((select ?),1.5)+0`, "NYNN"},
	} {
		t.Run(tc.value, func(t *testing.T) {
			opt := NewMockOptimizer(false)
			prepared, err := runOneStmt(opt, t, `prepare s from "select export_set(`+tc.value+`,'Y','N','',4)"`)
			require.NoError(t, err)
			cached := prepared.GetDcl().GetPrepare().Plan
			before := cached.String()
			for _, param := range []any{nil, ParamValue{Value: nil, IsBinaryProtocol: true}, ParamValue{Value: nil, IsBinaryProtocol: true, EnableNumericPrefix: true, RetainParamRef: true}} {
				filled, err := FillValuesOfParamsInPlan(context.Background(), cached, []any{param})
				require.NoError(t, err)
				query := filled.GetQuery()
				expr := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
				result, free, err := colexec.GetReadonlyResultFromExpression(opt.CurrentContext().GetProcess(), expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				func() { defer free(); require.Equal(t, tc.want, result.GetStringAt(0), expr.String()) }()
				require.Equal(t, before, cached.String())
			}
		})
	}
}
