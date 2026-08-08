// Copyright 2021 - 2026 Matrix Origin
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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func regexpTestExpr(oid types.T, value string) *Expr {
	return &Expr{
		Typ: planpb.Type{Id: int32(oid)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Sval{Sval: value},
		}},
	}
}

func TestBindRegexpRejectsStaticBinaryOperands(t *testing.T) {
	text := regexpTestExpr(types.T_varchar, "abc")
	binary := regexpTestExpr(types.T_binary, "abc")

	for _, tc := range []struct {
		name string
		args []*Expr
	}{
		{name: "reg_match", args: []*Expr{binary, text}},
		{name: "not_reg_match", args: []*Expr{text, binary}},
		{name: "regexp_like", args: []*Expr{binary, text}},
		{name: "regexp_instr", args: []*Expr{text, binary}},
		{name: "regexp_substr", args: []*Expr{binary, text}},
		{name: "regexp_replace", args: []*Expr{text, text, binary}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BindFuncExprImplByPlanExpr(context.Background(), tc.name, tc.args)
			require.Error(t, err)
			var moErr *moerr.Error
			require.ErrorAs(t, err, &moErr)
			require.Equal(t, uint16(moerr.ER_CHARACTER_SET_MISMATCH), moErr.MySQLCode())
			require.Equal(t, "HY000", moErr.SqlState())
		})
	}
}

func TestBindRegexpAllowsParamsAndBinaryNull(t *testing.T) {
	text := regexpTestExpr(types.T_varchar, "a")
	param := &Expr{
		Typ:  planpb.Type{Id: int32(types.T_blob)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	binaryNull := &Expr{
		Typ: planpb.Type{Id: int32(types.T_binary)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Isnull: true,
		}},
	}

	_, err := BindFuncExprImplByPlanExpr(context.Background(), "regexp_like", []*Expr{param, text})
	require.NoError(t, err)
	_, err = BindFuncExprImplByPlanExpr(context.Background(), "regexp_like", []*Expr{binaryNull, text})
	require.NoError(t, err)
}

func TestBindRegexpUsesMySQLBinaryCompatibilityPairs(t *testing.T) {
	text := regexpTestExpr(types.T_varchar, "a")
	binary := regexpTestExpr(types.T_binary, "a")
	number := regexpTestExpr(types.T_int64, "1")
	typedBinaryNull := &Expr{
		Typ: planpb.Type{Id: int32(types.T_binary)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "cast"},
			Args: []*planpb.Expr{{
				Typ:  planpb.Type{Id: int32(types.T_any)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}},
			}},
		}},
	}

	for _, args := range [][]*Expr{{binary, binary}, {number, binary}} {
		_, err := BindFuncExprImplByPlanExpr(context.Background(), "regexp_like", args)
		require.NoError(t, err)
	}

	_, err := BindFuncExprImplByPlanExpr(context.Background(), "regexp_like", []*Expr{typedBinaryNull, text})
	require.Error(t, err)
	var moErr *moerr.Error
	require.ErrorAs(t, err, &moErr)
	require.Equal(t, uint16(moerr.ER_CHARACTER_SET_MISMATCH), moErr.MySQLCode())

	_, err = BindFuncExprImplByPlanExpr(
		context.Background(), "regexp_replace", []*Expr{binary, binary, binary})
	require.NoError(t, err)
}

func TestBuildRegexpStringResultKeepsBinaryMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{name: "substr binary pattern", sql: "select regexp_substr(123, _binary '.')"},
		{name: "replace binary pattern and replacement", sql: "select regexp_replace(123, _binary '.', _binary 0xff)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)

			built, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)

			query := built.GetQuery()
			require.NotNil(t, query)
			require.NotEmpty(t, query.Nodes)
			require.NotEmpty(t, query.Nodes[query.Steps[0]].ProjectList)
			require.Equal(t, int32(types.T_varbinary), query.Nodes[query.Steps[0]].ProjectList[0].Typ.Id)
		})
	}

	for _, sql := range []string{
		"select charset(regexp_substr(123, _binary '.'))",
		"select charset(regexp_replace(123, _binary '.', _binary 0xff))",
	} {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)

		built, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
		require.NoError(t, err)

		query := built.GetQuery()
		project := query.Nodes[query.Steps[0]].ProjectList[0]
		require.Equal(t, "charset", project.GetF().GetFunc().GetObjName())
		require.Len(t, project.GetF().Args, 1)
		require.Equal(t, int32(types.T_varbinary), project.GetF().Args[0].Typ.Id)
	}
}
