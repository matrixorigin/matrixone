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
	for _, version := range []int64{defines.MORPCVersion70, defines.MORPCVersion71} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		for _, sql := range statements {
			t.Run(fmt.Sprintf("%s/v%d", sql, version), func(t *testing.T) {
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				_, err = BuildPlan(ctx, stmt, false)
				if version < defines.MORPCVersion71 {
					require.ErrorContains(t, err, "protocol version 71")
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

	for _, version := range []int64{defines.MORPCVersion70, defines.MORPCVersion71} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		if version < defines.MORPCVersion71 {
			require.ErrorContains(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, expr), "protocol version 71")
		} else {
			require.NoError(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, expr))
		}
	}

	plain := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}
	require.NoError(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, plain))
	table := &planpb.TableDef{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion70)
	require.ErrorContains(t, RequirePersistedIPFunctionProtocol(proc.Ctx, proc, table), "protocol version 71")
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

	for _, version := range []int64{defines.MORPCVersion70, defines.MORPCVersion71} {
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

func checkAdmissionResult(t *testing.T, version int64, err error) {
	t.Helper()
	if version < defines.MORPCVersion71 {
		require.ErrorContains(t, err, "protocol version 71")
	} else {
		require.NoError(t, err)
	}
}
