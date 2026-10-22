// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestFunctionalJSONTextEligibility(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	rt := runtime.ServiceRuntime(ctx.GetProcess().GetService())
	old, had := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	build := func(sql string) (*Plan, error) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(ctx, stmt, false)
	}
	for _, expression := range []string{
		"cast(json_unquote(json_extract(doc,'$.kind')) as char(20))",
		"cast(doc ->> '$.kind' as char(20))",
		"cast(doc ->> '$[4294967295]' as char(20))",
		"id + 1", "id - 1", "lower(name)",
	} {
		t.Run(expression, func(t *testing.T) {
			p, err := build("create table docs(id int primary key,doc json,name varchar(20),index k((" + expression + ")))")
			require.NoError(t, err)
			table := p.GetDdl().GetCreateTable().TableDef
			col := functionalIndexColumn(table, table.Indexes[0])
			require.NotNil(t, col)
			require.NotEqual(t, int32(types.T_json), col.Typ.Id, "JSON is never a physical index key")
			require.NoError(t, validateFunctionalTable(t.Context(), table))
		})
	}
	for _, expression := range []string{
		"doc", "json_extract(doc,'$.kind')", "doc ->> '$.kind'",
		"cast(doc ->> '$.*' as char(20))",
		"cast(doc ->> '$[4294967296]' as char(20))",
		"cast(doc ->> '$[999999999999999999999999999999999999999999]' as char(20))",
		"cast(json_unquote(json_extract(doc,name)) as char(20))",
		"cast(json_unquote(json_extract(doc,NULL)) as char(20))",
		"cast(ts as char(20))",
		"cast(json_unquote(json_extract(cast(ts as json),'$')) as char(20))",
	} {
		t.Run("reject/"+expression, func(t *testing.T) {
			_, err := build("create table docs(id int primary key,doc json,name varchar(20),ts timestamp,index k((" + expression + ")))")
			require.Error(t, err)
		})
	}
	p, err := build("create table docs(id int primary key,doc json,index k((cast(doc ->> '$.kind' as char(20)))))")
	require.NoError(t, err)
	table := p.GetDdl().GetCreateTable().TableDef
	key := functionalIndexColumn(table, table.Indexes[0]).GeneratedCol.Expr
	badTable := DeepCopyTableDef(table, true)
	badColumn := functionalIndexColumn(badTable, badTable.Indexes[0])
	badColumn.GeneratedCol.Expr = DeepCopyExpr(key.GetF().Args[0].GetF().Args[0].GetF().Args[0])
	badColumn.Typ = badColumn.GeneratedCol.Expr.Typ
	require.Error(t, validateFunctionalTable(t.Context(), badTable), "restored metadata cannot introduce a JSON key")
	for _, corrupt := range []func(*Expr){
		func(e *Expr) { e.Typ.Id = int32(types.T_json) },
		func(e *Expr) { e.GetF().Func.Obj++ },
		func(e *Expr) { e.GetF().Args[0].GetF().Func.Obj++ },
		func(e *Expr) { e.GetF().Args[0].GetF().Args[0].GetF().Func.Obj++ },
		func(e *Expr) { e.GetF().Args[1].Typ.Width++ },
		func(e *Expr) {
			e.GetF().Args[0].GetF().Args[0].GetF().Args[1].GetLit().Value = &pb.Literal_Sval{Sval: "$[4294967296]"}
		},
	} {
		bad := DeepCopyExpr(key)
		corrupt(bad)
		require.False(t, functionalExpressionEligible(bad, table.Cols, make(map[int32]bool)))
	}
	// Query binding must use the existing physical key while preserving the
	// original expression as a base-row residual, rather than index JSON itself.
	ctx.tables["docs"] = table
	ctx.objects["docs"] = &ObjectRef{ObjName: "docs", SchemaName: "tpch"}
	for _, indexTable := range p.GetDdl().GetCreateTable().IndexTables {
		ctx.tables[indexTable.Name] = indexTable
		ctx.objects[indexTable.Name] = &ObjectRef{ObjName: indexTable.Name, SchemaName: "tpch"}
	}
	query, err := build("select id from docs where cast(doc ->> '$.kind' as char(20))='alpha'")
	require.NoError(t, err)
	physical, residual := false, false
	for _, node := range query.GetQuery().Nodes {
		if node.NodeType != pb.Node_TABLE_SCAN || node.TableDef == nil {
			continue
		}
		physical = physical || node.TableDef.Name == table.Indexes[0].IndexTableName
		residual = residual || node.TableDef.Name == "docs" && len(node.FilterList) > 0
	}
	require.True(t, physical)
	require.True(t, residual)
}
