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

package frontend

import (
	"github.com/gogo/protobuf/proto"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedGeometrySRIDFrontendCacheIsValueSpecific(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 28795, "select st_srid(st_geomfromtext('POINT(1 2)'), ?)")
	t.Cleanup(func() {
		cw.releaseRuntimeCacheRetiredCompiles()
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	})
	preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan
	require.True(t, plan2.PreparedPlanNeedsRuntimeSpecialization(preparePlan))
	originalPlan := proto.Clone(preparePlan).(*plan.Plan)

	find := func(queryPlan *plan.Plan) *plan.Expr {
		var found *plan.Expr
		require.NoError(t, plan.VisitExpressionsInOwner(queryPlan, func(expr *plan.Expr) error {
			if found == nil && expr.GetF() != nil && expr.GetF().GetFunc().GetObjName() == "st_srid" {
				found = expr
			}
			return nil
		}))
		return found
	}
	install := func(value string, mysqlType defines.MysqlType, isNull bool) {
		if prepareStmt.params != nil {
			if cw.proc.GetPrepareParams() == prepareStmt.params {
				cw.proc.SetPrepareParams(nil)
			}
			prepareStmt.params.Free(cw.proc.Mp())
		}
		prepareStmt.params = vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte(value), isNull, cw.proc.Mp()))
		prepareStmt.ParamTypes = []byte{byte(mysqlType), 0}
	}
	execute := func(value string, mysqlType defines.MysqlType, isNull bool) (*compile.Compile, *plan.Plan, error) {
		install(value, mysqlType, isNull)
		retComp, runtimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
		return retComp, runtimePlan, err
	}
	installCandidate := func() *compile.Compile {
		require.NotNil(t, cw.runtimeCachePlan)
		runtimeCompile := compile.NewCompile(
			"", "", prepareStmt.Sql, "", "", nil,
			cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
		require.True(t, cw.installRuntimeCacheCandidate(runtimeCompile))
		return runtimeCompile
	}

	retComp, firstPlan, err := execute("4326", defines.MYSQL_TYPE_LONG, false)
	require.NoError(t, err)
	require.Nil(t, retComp)
	require.Equal(t, int32(4327), find(firstPlan).Typ.Width)
	firstCompile := installCandidate()
	firstKey := prepareStmt.runtimeSpecializationKey

	retComp, reusedPlan, err := execute("4326", defines.MYSQL_TYPE_LONG, false)
	require.NoError(t, err)
	require.Same(t, firstCompile, retComp)
	require.Same(t, firstPlan, reusedPlan)
	require.Equal(t, int32(4327), find(reusedPlan).Typ.Width)

	_, secondPlan, err := execute("3857", defines.MYSQL_TYPE_LONG, false)
	require.NoError(t, err)
	require.NotSame(t, firstPlan, secondPlan)
	require.Equal(t, int32(3858), find(secondPlan).Typ.Width)
	secondCompile := installCandidate()
	secondKey := prepareStmt.runtimeSpecializationKey
	require.NotEqual(t, firstKey, secondKey, "SRID values must not share one cached plan")

	retComp, reusedPlan, err = execute("3857", defines.MYSQL_TYPE_LONG, false)
	require.NoError(t, err)
	require.Same(t, secondCompile, retComp)
	require.Same(t, secondPlan, reusedPlan)

	_, nullPlan, err := execute("", defines.MYSQL_TYPE_NULL, true)
	require.NoError(t, err)
	require.Equal(t, int32(0), find(nullPlan).Typ.Width,
		"NULL must not inherit the previous 3857 metadata")
	require.Equal(t, secondKey, prepareStmt.runtimeSpecializationKey,
		"an untyped NULL must not publish a cache category")
	require.Same(t, secondCompile, prepareStmt.runtimeCompile)

	_, _, err = execute("-1", defines.MYSQL_TYPE_LONG, false)
	require.Error(t, err)
	require.Equal(t, secondKey, prepareStmt.runtimeSpecializationKey,
		"invalid SRID must not replace the last valid cached category")
	require.Same(t, secondCompile, prepareStmt.runtimeCompile)

	_, zeroPlan, err := execute("0", defines.MYSQL_TYPE_LONG, false)
	require.NoError(t, err)
	require.Equal(t, int32(1), find(zeroPlan).Typ.Width,
		"defined SRID 0 must remain distinct from undefined SRID")
	zeroCompile := installCandidate()
	zeroKey := prepareStmt.runtimeSpecializationKey
	require.NotEqual(t, secondKey, zeroKey)
	retComp, reusedPlan, err = execute("0", defines.MYSQL_TYPE_LONG, false)
	require.NoError(t, err)
	require.Same(t, zeroCompile, retComp)
	require.Same(t, zeroPlan, reusedPlan)
	require.True(t, proto.Equal(originalPlan, preparePlan),
		"value specialization must not mutate the cached prepare-time plan")
}

func TestPreparedGeometrySRIDNullThroughCast(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 28796, "select st_srid(cast(? as geometry), ?)")
	t.Cleanup(func() {
		cw.proc.SetPrepareParams(nil)
		prepareStmt.Close()
	})
	prepareStmt.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(prepareStmt.params, nil, true, cw.proc.Mp()))
	require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte("-1"), false, cw.proc.Mp()))
	prepareStmt.ParamTypes = []byte{
		byte(defines.MYSQL_TYPE_NULL), 0,
		byte(defines.MYSQL_TYPE_LONG), 0,
	}

	_, runtimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
	require.NoError(t, err,
		"a NULL source must short-circuit an invalid execute-time SRID through CAST")
	var sridExpr *plan.Expr
	require.NoError(t, plan.VisitExpressionsInOwner(runtimePlan, func(expr *plan.Expr) error {
		if sridExpr == nil && expr.GetF() != nil && expr.GetF().GetFunc().GetObjName() == "st_srid" {
			sridExpr = expr
		}
		return nil
	}))
	require.NotNil(t, sridExpr)
	require.Equal(t, int32(0), sridExpr.Typ.Width)
}
