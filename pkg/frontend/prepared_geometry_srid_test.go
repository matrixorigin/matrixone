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
	"github.com/matrixorigin/matrixone/pkg/geo"
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
	_, typedNullPlan, err := execute("", defines.MYSQL_TYPE_BLOB, true)
	require.NoError(t, err)
	require.Equal(t, int32(0), find(typedNullPlan).Typ.Width)
	typedNullCompile := installCandidate()
	typedNullKey := prepareStmt.runtimeSpecializationKey
	_, _, err = execute("<null>", defines.MYSQL_TYPE_BLOB, false)
	require.Error(t, err,
		"a user value equal to the old NULL sentinel must not reuse a typed-NULL plan")
	require.Equal(t, typedNullKey, prepareStmt.runtimeSpecializationKey)
	require.Same(t, typedNullCompile, prepareStmt.runtimeCompile)
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

func TestPreparedGeometrySRIDCacheSeparatesTypedSourceNull(t *testing.T) {
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, 28798, "select st_srid(st_geomfromwkb(?, ?))")
	t.Cleanup(func() {
		cw.releaseRuntimeCacheRetiredCompiles()
		cw.proc.SetPrepareParams(nil)
		if prepareStmt.params != nil {
			prepareStmt.params.Free(cw.proc.Mp())
			prepareStmt.params = nil
		}
		prepareStmt.Close()
	})

	findConstructor := func(queryPlan *plan.Plan) *plan.Expr {
		var found *plan.Expr
		require.NoError(t, plan.VisitExpressionsInOwner(queryPlan, func(expr *plan.Expr) error {
			if found != nil || expr.GetF() == nil {
				return nil
			}
			if expr.GetF().GetFunc().GetObjName() == "st_geomfromwkb" {
				found = expr
			} else if expr.GetF().GetFunc().GetObjName() == "st_srid" && len(expr.GetF().Args) > 0 &&
				expr.GetF().Args[0].GetF() != nil && expr.GetF().Args[0].GetF().GetFunc().GetObjName() == "st_geomfromwkb" {
				found = expr.GetF().Args[0]
			}
			return nil
		}))
		return found
	}
	install := func(source []byte, sourceNull bool, srid string, sridNull bool) {
		if prepareStmt.params != nil {
			if cw.proc.GetPrepareParams() == prepareStmt.params {
				cw.proc.SetPrepareParams(nil)
			}
			prepareStmt.params.Free(cw.proc.Mp())
		}
		prepareStmt.params = vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(prepareStmt.params, source, sourceNull, cw.proc.Mp()))
		require.NoError(t, vector.AppendBytes(prepareStmt.params, []byte(srid), sridNull, cw.proc.Mp()))
		prepareStmt.ParamTypes = []byte{
			byte(defines.MYSQL_TYPE_BLOB), 0,
			byte(defines.MYSQL_TYPE_LONG), 0,
		}
	}
	execute := func(source []byte, sourceNull bool, srid string, sridNull bool) (*plan.Plan, error) {
		install(source, sourceNull, srid, sridNull)
		_, runtimePlan, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepareStmt.Name)
		return runtimePlan, err
	}
	installCandidate := func() *compile.Compile {
		require.NotNil(t, cw.runtimeCachePlan)
		runtimeCompile := compile.NewCompile(
			"", "", prepareStmt.Sql, "", "", nil,
			cw.proc, prepareStmt.PrepareStmt, false, nil, time.Now())
		require.True(t, cw.installRuntimeCacheCandidate(runtimeCompile))
		return runtimeCompile
	}

	firstPlan, err := execute(nil, true, "4326", false)
	require.NoError(t, err)
	firstConstructor := findConstructor(firstPlan)
	require.NotNil(t, firstConstructor)
	require.Zero(t, firstConstructor.Typ.Width, "typed BLOB NULL produces undefined SRID metadata")
	installCandidate()
	firstKey := prepareStmt.runtimeSpecializationKey

	validWKB := geo.WriteWKB(geo.Point{X: 1, Y: 2})
	secondPlan, err := execute(validWKB, false, "4326", false)
	require.NoError(t, err)
	secondConstructor := findConstructor(secondPlan)
	require.NotNil(t, secondConstructor)
	require.Equal(t, int32(4327), secondConstructor.Typ.Width)
	require.NotSame(t, firstPlan, secondPlan,
		"typed BLOB NULL and valid WKB must not reuse one SRID-specialized plan")
	secondCompile := installCandidate()
	secondKey := prepareStmt.runtimeSpecializationKey
	require.NotEqual(t, firstKey, secondKey)

	_, err = execute(validWKB, false, "-1", false)
	require.Error(t, err, "an invalid SRID must be revalidated after the source becomes non-NULL")
	require.Equal(t, secondKey, prepareStmt.runtimeSpecializationKey,
		"a rejected execution must not replace the last valid cache category")
	require.Same(t, secondCompile, prepareStmt.runtimeCompile)

	thirdPlan, err := execute(nil, true, "4326", false)
	require.NoError(t, err)
	thirdConstructor := findConstructor(thirdPlan)
	require.NotNil(t, thirdConstructor)
	require.Zero(t, thirdConstructor.Typ.Width)
	require.NotSame(t, secondPlan, thirdPlan,
		"the cache must also separate valid WKB and typed NULL in the reverse direction")
}

func TestPreparedGeometrySRIDCacheSeparatesTypedSourceNullWithFixedSRID(t *testing.T) {
	runPreparedGeometrySRIDFixedSourceCacheTest(t, 28799,
		"select st_geomfromwkb(?, 4326)", geo.WriteWKB(geo.Point{X: 1, Y: 2}))
	runPreparedGeometrySRIDFixedSourceCacheTest(t, 28800,
		"select st_geomfromtext(?, 4326)", []byte("POINT(1 2)"))
}

func runPreparedGeometrySRIDFixedSourceCacheTest(
	t *testing.T, statementID uint32, query string, validPayload []byte,
) {
	t.Helper()
	ses, prepareStmt, cw, execCtx := newPreparedExecuteEnvForSQL(
		t, statementID, query)
	t.Cleanup(func() {
		cw.releaseRuntimeCacheRetiredCompiles()
		cw.proc.SetPrepareParams(nil)
		if prepareStmt.params != nil {
			prepareStmt.params.Free(cw.proc.Mp())
			prepareStmt.params = nil
		}
		prepareStmt.Close()
	})

	findConstructor := func(queryPlan *plan.Plan) *plan.Expr {
		var found *plan.Expr
		require.NoError(t, plan.VisitExpressionsInOwner(queryPlan, func(expr *plan.Expr) error {
			if found == nil && expr.GetF() != nil &&
				(expr.GetF().GetFunc().GetObjName() == "st_geomfromwkb" ||
					expr.GetF().GetFunc().GetObjName() == "st_geomfromtext") {
				found = expr
			}
			return nil
		}))
		return found
	}
	install := func(source []byte, sourceNull bool) {
		if prepareStmt.params != nil {
			if cw.proc.GetPrepareParams() == prepareStmt.params {
				cw.proc.SetPrepareParams(nil)
			}
			prepareStmt.params.Free(cw.proc.Mp())
		}
		prepareStmt.params = vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(prepareStmt.params, source, sourceNull, cw.proc.Mp()))
		prepareStmt.ParamTypes = []byte{byte(defines.MYSQL_TYPE_BLOB), 0}
	}
	execute := func(source []byte, sourceNull bool) (*compile.Compile, *plan.Plan, error) {
		install(source, sourceNull)
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

	_, firstPlan, err := execute(nil, true)
	require.NoError(t, err)
	firstConstructor := findConstructor(firstPlan)
	require.NotNil(t, firstConstructor)
	require.Zero(t, firstConstructor.Typ.Width)
	installCandidate()
	firstKey := prepareStmt.runtimeSpecializationKey

	_, secondPlan, err := execute(validPayload, false)
	require.NoError(t, err)
	secondConstructor := findConstructor(secondPlan)
	require.NotNil(t, secondConstructor)
	require.Equal(t, int32(4327), secondConstructor.Typ.Width)
	require.NotSame(t, firstPlan, secondPlan,
		"a fixed SRID must still distinguish typed source NULL from valid geometry")
	secondCompile := installCandidate()
	require.NotEqual(t, firstKey, prepareStmt.runtimeSpecializationKey)

	retComp, reusedPlan, err := execute(validPayload, false)
	require.NoError(t, err)
	require.Same(t, secondCompile, retComp)
	require.Same(t, secondPlan, reusedPlan)
}
