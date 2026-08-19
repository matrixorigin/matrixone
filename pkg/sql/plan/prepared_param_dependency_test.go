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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func TestPreparedParamCommonTypeDependencies(t *testing.T) {
	param := func(pos int32, dependent bool) *Expr {
		expr := &Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
		if dependent {
			expr.Typ.Enumvalues = "mo_decimal_common_type_dependency"
		}
		return expr
	}
	call := func(functionID int32, args ...*Expr) *Expr {
		return &Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(functionID, 0)},
			Args: args,
		}}}
	}
	cast := func(arg *Expr, internal bool) *Expr {
		charset := uint32(0)
		if internal {
			charset = 255
		}
		target := &Expr{Typ: planpb.Type{Charset: charset}, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}}
		return call(function.CAST, arg, target)
	}
	query := &Query{
		Steps: []int32{0},
		Nodes: []*Node{{
			NodeType: planpb.Node_PROJECT,
			ProjectList: []*Expr{
				call(function.COALESCE, cast(param(0, true), true), cast(param(1, false), false), call(function.PLUS, param(2, false), param(3, false))),
				call(function.SUM, param(4, false)),
				call(function.GREATEST, param(5, true), param(6, true)),
			},
		}},
	}
	dependencies := PreparedParamCommonTypeDependencies(
		&Plan{Plan: &planpb.Plan_Query{Query: query}}, 7)
	require.Equal(t, []bool{true, false, false, false, false, true, true}, dependencies)
}

func TestPreparedParamCommonTypeDependenciesCoverPlanAndWindowShapes(t *testing.T) {
	param := func(pos int32) *Expr {
		return &Expr{
			Typ:  planpb.Type{Enumvalues: "mo_decimal_common_type_dependency"},
			Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
		}
	}
	call := func(functionID int32, args ...*Expr) *Expr {
		return &Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(functionID, 0)},
			Args: args,
		}}}
	}
	dependent := func(pos int32) *Expr {
		return call(function.COALESCE, param(pos))
	}
	queryWithProject := func(expr *Expr) *Query {
		return &Query{Steps: []int32{0}, Nodes: []*Node{{
			NodeType:    planpb.Node_PROJECT,
			ProjectList: []*Expr{expr},
		}}}
	}

	window := &Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
		WindowFunc:  dependent(0),
		PartitionBy: []*Expr{dependent(1)},
		OrderBy:     []*planpb.OrderBySpec{{Expr: dependent(2)}},
		Frame: &planpb.FrameClause{
			Start: &planpb.FrameBound{Val: dependent(3)},
			End:   &planpb.FrameBound{Val: dependent(4)},
		},
	}}}
	tests := []struct {
		name string
		plan *Plan
		want []bool
	}{
		{
			name: "window closure",
			plan: &Plan{Plan: &planpb.Plan_Query{Query: queryWithProject(window)}},
			want: []bool{true, true, true, true, true},
		},
		{
			name: "dcl set expressions",
			plan: &Plan{Plan: &planpb.Plan_Dcl{Dcl: &planpb.DataControl{
				Control: &planpb.DataControl_SetVariables{SetVariables: &planpb.SetVariables{Items: []*planpb.SetVariablesItem{{
					Value: dependent(0), Reserved: dependent(1),
				}}}},
			}}},
			want: []bool{true, true},
		},
		{
			name: "dcl set scalar subquery graph",
			plan: &Plan{Plan: &planpb.Plan_Dcl{Dcl: &planpb.DataControl{
				Control: &planpb.DataControl_SetVariables{SetVariables: &planpb.SetVariables{
					Query: queryWithProject(dependent(0)),
				}},
			}}},
			want: []bool{true},
		},
		{
			name: "ddl embedded query",
			plan: &Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{
				Query: queryWithProject(dependent(0)),
			}}},
			want: []bool{true},
		},
		{
			name: "nested prepare plan",
			plan: &Plan{Plan: &planpb.Plan_Dcl{Dcl: &planpb.DataControl{
				Control: &planpb.DataControl_Prepare{Prepare: &planpb.Prepare{
					Plan: &Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{Query: queryWithProject(dependent(0))}}},
				}},
			}}},
			want: []bool{true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, PreparedParamCommonTypeDependencies(test.plan, len(test.want)))
		})
	}
}

func TestPreparedParamCommonTypeDependenciesFromPublicSQLShapes(t *testing.T) {
	mock := NewMockOptimizer(false)
	decimalType := types.New(types.T_decimal128, 20, 4)
	mock.ctxt.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "dcl set",
			sql:  "prepare p from 'set @out = coalesce(?, cast(2 as decimal(10,2)))'",
		},
		{
			name: "window function",
			sql:  "prepare p from 'select sum(coalesce(?, p_retailprice)) over () from part'",
		},
		{
			name: "ctas",
			sql:  "prepare p from 'create table dst as select coalesce(?, p_retailprice) x from part'",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)
			prepare := logicPlan.GetDcl().GetPrepare()
			require.NotNil(t, prepare)
			require.Equal(t, []bool{true}, PreparedParamCommonTypeDependencies(prepare.Plan, 1))
		})
	}
}

func TestPreparedParamResultMetadataDependencies(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, "prepare p from 'select ?, cast(? as decimal(10,2)), ? + 1'")
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	require.Equal(t, []bool{true, false, false},
		PreparedParamResultMetadataDependencies(prepare.Plan, 3))
	require.Equal(t, [][]bool{{true, false, false}, nil, nil},
		PreparedParamResultMetadataDependencyColumns(prepare.Plan, 3))
	require.Empty(t, PreparedParamCommonTypeDependencies(prepare.Plan, 3))
}

func TestPreparedParamResultMetadataDependenciesFollowUnionOutputs(t *testing.T) {
	for _, test := range []struct {
		name   string
		sql    string
		params int
		want   []bool
	}{
		{
			name:   "bare params",
			sql:    "prepare p from 'select ? union all select ?'",
			params: 2,
			want:   []bool{true, true},
		},
		{
			name:   "set operation implicit cast",
			sql:    "prepare p from 'select ? union all select cast(0 as decimal(1,0))'",
			params: 1,
			want:   []bool{true},
		},
		{
			name:   "user explicit cast remains fixed",
			sql:    "prepare p from 'select cast(? as text) union all select cast(0 as decimal(1,0))'",
			params: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)
			prepare := logicPlan.GetDcl().GetPrepare()
			require.NotNil(t, prepare)
			require.Equal(t, test.want,
				PreparedParamResultMetadataDependencies(prepare.Plan, test.params))
		})
	}
}

func TestPreparedParamResultMetadataDependenciesFollowControlFlowValues(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		want []bool
	}{
		{"if value", "prepare p from 'select if(true, ?, cast(0 as decimal(1,0)))'", []bool{true}},
		{"case value", "prepare p from 'select case when true then ? else cast(0 as decimal(1,0)) end'", []bool{true}},
		{"if condition excluded", "prepare p from 'select if(?, 1, 0)'", nil},
		{"explicit cast terminates", "prepare p from 'select if(true, cast(? as text), cast(0 as decimal(1,0)))'", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)
			prepare := logicPlan.GetDcl().GetPrepare()
			require.NotNil(t, prepare)
			require.Equal(t, test.want,
				PreparedParamResultMetadataDependencies(prepare.Plan, 1))
			if test.name == "explicit cast terminates" {
				require.Equal(t, []bool{true},
					PreparedParamCommonTypeDependencies(prepare.Plan, 1))
			}
		})
	}
}
