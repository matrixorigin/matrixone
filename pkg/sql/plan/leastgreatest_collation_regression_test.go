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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestLeastGreatestCollationSQLNull(t *testing.T) {
	for _, name := range []string{"least", "greatest"} {
		for _, domain := range []string{"cast('a' as char(1))", "'a' collate utf8mb4_unicode_ci"} {
			calls := []string{
				fmt.Sprintf("NULL, %s", domain),
				fmt.Sprintf("%s, NULL", domain),
				fmt.Sprintf("%s, NULL, %s", domain, domain),
				fmt.Sprintf("cast(NULL as char(1)), %s", domain),
			}
			if domain == "cast('a' as char(1))" {
				calls = append(calls, "NULL, NULL", "cast(NULL as char(1)), cast(NULL as char(1))")
			}
			for _, call := range calls {
				t.Run(name+"/"+call, func(t *testing.T) {
					proc := newPlanTestProcess(t)
					ctx := NewMockCompilerContext(false, proc)
					stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select "+name+"("+call+")", 1)
					require.NoError(t, err)
					defer stmt.Free()
					built, err := BuildPlan(ctx, stmt, false)
					require.NoError(t, err)
					qry := built.GetQuery()
					expr := qry.Nodes[qry.Steps[0]].ProjectList[0]
					data, err := expr.Marshal()
					require.NoError(t, err)
					decoded := &planpb.Expr{}
					require.NoError(t, decoded.Unmarshal(data))
					executor, err := colexec.NewExpressionExecutor(proc, decoded)
					require.NoError(t, err)
					defer executor.Free()
					result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
					require.NoError(t, err)
					require.True(t, result.IsNull(0))
				})
			}
		}
	}
}

func TestLeastGreatestCollationPersistedAdmissionAndViewRead(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		old, present := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if present {
				rt.SetGlobalVariables(key, old)
			} else if value, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, value)
			}
		})
	}
	setVersions := func(version, readFloor, authoringFloor int64) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, readFloor)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, authoringFloor)
	}
	build := func(sql string) (*Plan, error) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: sql}, stmt, false)
	}
	typ := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
	typ.CollationVersion = 1
	for _, name := range []string{"least", "greatest"} {
		t.Run(name+"/owner", func(t *testing.T) {
			col := &Expr{Typ: typ.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
			expr, err := BindFuncExprImplByPlanExpr(proc.Ctx, name, []*Expr{col, DeepCopyExpr(col)})
			require.NoError(t, err)
			required, err := RequiredPersistedExpressionProtocolVersion(expr)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion117, required)
			dateExpr := &Expr{Typ: Type{Id: int32(types.T_int32)}, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &ObjectRef{Obj: function.EncodeOverloadID(function.DAYOFYEAR, 0)},
				Args: []*Expr{{Typ: Type{Id: int32(types.T_date)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}}},
			}}}
			statisticalExpr := &Expr{Typ: Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &ObjectRef{Obj: function.EncodeOverloadID(function.VAR_POP, 0)},
				Args: []*Expr{{Typ: Type{Id: int32(types.T_decimal64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 2}}}},
			}}}
			calendarExpr := &Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &ObjectRef{Obj: function.EncodeOverloadID(function.TO_CALENDAR_INTERVAL, 0)},
			}}}
			owner := &planpb.TableDef{Cols: []*planpb.ColDef{
				{GeneratedCol: &planpb.GeneratedCol{Expr: expr}},
				{GeneratedCol: &planpb.GeneratedCol{Expr: dateExpr}},
				{GeneratedCol: &planpb.GeneratedCol{Expr: statisticalExpr}},
				{GeneratedCol: &planpb.GeneratedCol{Expr: calendarExpr}},
			}}
			required, err = RequiredPersistedExpressionProtocolVersion(owner)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion117, required, "mixed owner takes the maximum floor")
			for _, oldVersion := range []int64{defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116} {
				setVersions(oldVersion, defines.MORPCVersion117, defines.MORPCVersion117)
				require.ErrorContains(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, owner), "protocol version 117")
				require.ErrorContains(t, RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, owner), "protocol version 117")
				setVersions(defines.MORPCVersion117, oldVersion, oldVersion)
				require.ErrorContains(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, owner), "protocol version 117")
				require.ErrorContains(t, RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, owner), "protocol version 117")
			}
			setVersions(defines.MORPCVersion117, defines.MORPCVersion117, defines.MORPCVersion117)
			require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, owner))
			require.NoError(t, RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, owner))
			id, _ := function.DecodeOverloadID(expr.GetF().Func.Obj)
			expr.GetF().Func.Obj = function.EncodeOverloadID(id, 0)
			required, err = RequiredPersistedExpressionProtocolVersion(owner)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion115, required, "historical ordering keeps main's independent calendar and decimal contracts")
		})
		for _, selection := range []string{
			name + "(cast('a' as char(1)),cast('B' as char(1))) as v",
			name + "('a' collate utf8mb4_unicode_ci,'B' collate utf8mb4_unicode_ci) as v",
			name + "(cast(n_name as char(20)),cast('B' as char(20))) as v from nation",
		} {
			t.Run(selection, func(t *testing.T) {
				sql := "create view v_least_protocol as select " + selection
				setVersions(defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion117)
				_, err := build(sql)
				require.ErrorContains(t, err, "protocol version 117", "main-116 deployment cannot publish a view")
				setVersions(defines.MORPCVersion117, defines.MORPCVersion117, defines.MORPCVersion116)
				_, err = build(sql)
				require.ErrorContains(t, err, "protocol version 117", "read floor alone does not admit authoring")
				setVersions(defines.MORPCVersion117, defines.MORPCVersion117, defines.MORPCVersion117)
				created, err := build(sql)
				require.NoError(t, err)
				table := created.GetDdl().GetCreateView().GetTableDef()
				var view ViewData
				require.NoError(t, json.Unmarshal([]byte(table.GetViewSql().GetView()), &view))
				require.NotNil(t, view.RequiredProtocolVersion)
				require.Equal(t, defines.MORPCVersion117, *view.RequiredProtocolVersion, "even constant-folded views record the pre-fold contract")
				// Catalog resolution supplies the relation kind, separately from
				// the CREATE VIEW definition's properties.
				table.TableType = catalog.SystemViewRel
				ctx.tables["v_least_protocol"] = table
				ctx.objects["v_least_protocol"] = &ObjectRef{SchemaName: "tpch", ObjName: "v_least_protocol"}
				defer delete(ctx.tables, "v_least_protocol")
				defer delete(ctx.objects, "v_least_protocol")
				setVersions(defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion117)
				_, err = build("select * from v_least_protocol")
				require.ErrorContains(t, err, "protocol version 117")
				setVersions(defines.MORPCVersion117, defines.MORPCVersion116, defines.MORPCVersion117)
				_, err = build("select * from v_least_protocol")
				require.ErrorContains(t, err, "protocol version 117", "unfenced reader rejects saved metadata")
				setVersions(defines.MORPCVersion117, defines.MORPCVersion117, defines.MORPCVersion117)
				_, err = build("select * from v_least_protocol")
				require.NoError(t, err)
			})
		}
	}
}
