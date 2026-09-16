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
	"fmt"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPersistedIPFunctionProtocolAdmission(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	statements := []string{
		"create table t(a bigint, b varchar(32) default (inet_ntoa(a)))",
		"create table t(a bigint, b varchar(32) generated always as (inet_ntoa(a)) stored)",
		"create table t(a bigint, check (inet_ntoa(a) <> ''))",
	}
	for _, version := range []int64{defines.MORPCVersion70, defines.MORPCVersion71, defines.MORPCVersion72} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		for _, sql := range statements {
			t.Run(fmt.Sprintf("%s/v%d", sql, version), func(t *testing.T) {
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				_, err = BuildPlan(ctx, stmt, false)
				if version < defines.MORPCVersion72 {
					require.ErrorContains(t, err, "protocol version 72")
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestPersistedIPFunctionProtocolAdmissionAcrossOwners(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select inet_ntoa(a)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	binder := NewGeneratedColBinder(proc.Ctx, []string{"a"}, []planpb.Type{{Id: int32(types.T_int64), Width: 64}})
	expr, err := binder.BindExpr(ast, 0, false)
	require.NoError(t, err)

	for _, version := range []int64{defines.MORPCVersion70, defines.MORPCVersion71, defines.MORPCVersion72} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		if version < defines.MORPCVersion72 {
			require.ErrorContains(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, expr), "protocol version 72")
		} else {
			require.NoError(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, expr))
		}
	}

	plain := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}
	require.NoError(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, plain))
	table := &planpb.TableDef{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion70)
	require.ErrorContains(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, table), "protocol version 72")
}

func TestPersistedProtocolVersionAdmissionRejectsFutureReader(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	require.NoError(t, RequirePersistedProtocolVersion(
		proc.Ctx, proc, defines.MORPCLatestVersion))
	require.ErrorContains(t, RequirePersistedProtocolVersion(
		proc.Ctx, proc, defines.MORPCLatestVersion+1), "protocol version")

	// A lower local runtime cannot bind an expression whose marker was already
	// persisted by a newer writer, even when the marker is not IP-specific.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion71)
	require.ErrorContains(t, RequirePersistedProtocolVersion(
		proc.Ctx, proc, defines.MORPCVersion72), "protocol version 72")
}

func TestPersistedProtocolVersionAdmissionHonorsCommittedFloor(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldFloor, hadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldFloor)
		} else {
			if value, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
				rt.CompareAndDeleteGlobalVariables(
					moruntime.PersistedExpressionProtocolFloor, value)
			}
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(0))
	require.ErrorContains(t, RequirePersistedProtocolVersion(
		proc.Ctx, proc, defines.MORPCVersion72), "protocol version 72")
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion72))
	require.NoError(t, RequirePersistedProtocolVersion(
		proc.Ctx, proc, defines.MORPCVersion72))
}

func TestPersistedProtocolVersionAdmissionSeparatesReadAndAuthoringFloors(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadReadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	// Phase one has committed the durable read floor but has not completed the
	// routing/catalog barrier. Existing marked views can be rebound; new
	// protocol-bearing catalog metadata must still be rejected.
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion72))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(0))
	require.NoError(t, RequirePersistedProtocolVersion(
		proc.Ctx, proc, defines.MORPCVersion72))
	require.ErrorContains(t, RequirePersistedProtocolVersionForAuthoring(
		proc.Ctx, proc, defines.MORPCVersion72), "protocol version 72")

	// After the enabled/admitted/catalog-fenced snapshot, the write gate opens.
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor,
		int64(defines.MORPCVersion72))
	require.NoError(t, RequirePersistedProtocolVersionForAuthoring(
		proc.Ctx, proc, defines.MORPCVersion72))
}

func TestPersistedIPFunctionProtocolAdmissionForCatalogBuilders(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	parseColumn := func(t *testing.T, sql string, index int) *tree.ColumnTableDef {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		return stmt.(*tree.CreateTable).Defs[index].(*tree.ColumnTableDef)
	}
	defaultCol := parseColumn(t, "create table t(a bigint, b varchar(32) default (inet_ntoa(a)))", 1)
	onUpdateCol := parseColumn(t, "create table t(a bigint, b varchar(32))", 1)
	selectStmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select inet_ntoa(1)", 1)
	require.NoError(t, err)
	t.Cleanup(selectStmt.Free)
	updateExpr := selectStmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	onUpdateCol.Attributes = append(onUpdateCol.Attributes, tree.NewAttributeOnUpdate(updateExpr))
	generatedCol := parseColumn(t,
		"create table t(a bigint, b varchar(32) generated always as (inet_ntoa(a)) stored)", 1)
	columns := []*ColDef{{Name: "a", Typ: planpb.Type{Id: int32(types.T_int64), Width: 64}}}

	for _, version := range []int64{defines.MORPCVersion70, defines.MORPCVersion71, defines.MORPCVersion72} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			_, err := buildDefaultExprWithColumns(defaultCol,
				planpb.Type{Id: int32(types.T_varchar), Width: 32}, proc, columns)
			checkAdmissionResult(t, version, err)

			_, err = buildOnUpdate(onUpdateCol, planpb.Type{Id: int32(types.T_varchar), Width: 32}, proc)
			checkAdmissionResult(t, version, err)

			_, err = buildGeneratedExpr(generatedCol,
				planpb.Type{Id: int32(types.T_varchar), Width: 32}, columns, proc)
			checkAdmissionResult(t, version, err)

			_, err = buildCTASDefaultFromOrigin(ctx,
				planpb.Type{Id: int32(types.T_varchar), Width: 32}, true,
				"inet_ntoa(1)", columns...)
			checkAdmissionResult(t, version, err)
		})
	}
}

func TestPersistedStringNumericResultProtocolAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	stmt, err := parsers.ParseOne(
		context.Background(), dialect.MYSQL, "select strcmp('a', 'b')", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	binder := NewGeneratedColBinder(proc.Ctx, nil, nil)
	expr, err := binder.BindExpr(ast, 0, false)
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.StringNumericResultContracts)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion74)
	require.ErrorContains(t,
		RequirePersistedExpressionProtocol(proc.Ctx, proc, expr), "protocol version 80")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion80)
	require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, expr))
	require.NoError(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, expr),
		"legacy catalog-builder wrapper must use the shared maximum-version gate")
}

func TestPersistedBoundedConditionalStringProtocolAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	binaryType := types.New(types.T_binary, 4, 0)
	varbinaryType := types.New(types.T_varbinary, 12, 0)
	expr, err := BindFuncExprImplByPlanExpr(proc.Ctx, "coalesce", []*planpb.Expr{
		{Typ: makePlan2Type(&binaryType), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		{Typ: makePlan2Type(&varbinaryType), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
	})
	require.NoError(t, err)
	functionID, overloadID := function.DecodeOverloadID(expr.GetF().Func.Obj)
	require.Equal(t, int32(function.COALESCE), functionID)
	require.Equal(t, int32(31), overloadID)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.BoundedConditionalStringDomains)

	required, err := RequiredPersistedExpressionProtocolVersion(expr)
	require.NoError(t, err)
	require.Equal(t, int64(defines.MORPCVersion83), required)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion80)
	require.ErrorContains(t,
		RequirePersistedExpressionProtocol(proc.Ctx, proc, expr), "protocol version 83")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion83)
	require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, expr))
}

func checkAdmissionResult(t *testing.T, version int64, err error) {
	t.Helper()
	if version < defines.MORPCVersion72 {
		require.ErrorContains(t, err, "protocol version 72")
	} else {
		require.NoError(t, err)
	}
}
