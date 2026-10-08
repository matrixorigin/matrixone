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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestFunctionalIndexOwnershipAfterDropColumn(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	rt := runtime.ServiceRuntime(ctx.GetProcess().GetService())
	old, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion107)
	t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, old) })
	for _, order := range [][]string{{"tenant", "spare"}, {"spare", "tenant"}} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
			"create table t(id int primary key,tenant int,name varchar(40),spare int,index ix(tenant,(lower(name)),spare,(upper(name)),(lower(name))),index iy((lower(name)),tenant))", 1)
		require.NoError(t, err)
		p, err := BuildPlan(ctx, stmt, false)
		stmt.Free()
		require.NoError(t, err)
		table := p.GetDdl().GetCreateTable().TableDef
		ensureName2ColIndexForReplace(table)
		owners := functionalIndexColumns(table, table.Indexes[0])
		require.Len(t, owners, 3)
		origins := []string{owners[0].GeneratedCol.OriginString, owners[1].GeneratedCol.OriginString, owners[2].GeneratedCol.OriginString}
		alias := table.Indexes[0].Parts[len(table.Indexes[0].Parts)-1]
		for _, name := range order {
			col := FindColumn(table.Cols, name)
			require.NoError(t, handleDropColumnWithIndex(t.Context(), name, table))
			for _, owner := range owners {
				require.Same(t, owner, table.Cols[table.Name2ColIndex[owner.Name]])
			}
			require.NoError(t, handleDropColumnPosition(t.Context(), table, col))
			table.Name2ColIndex = nil // COPY rebuilds physical column positions.
			ensureName2ColIndexForReplace(table)
			require.NoError(t, validateFunctionalTable(t.Context(), table))
		}
		for i, col := range owners {
			require.Same(t, col, functionalIndexPartColumn(table, table.Indexes[0], i))
			require.Equal(t, functionalColumnName("ix", i), col.Name)
			require.Equal(t, col.Name, col.OriginName)
			require.Equal(t, origins[i], col.GeneratedCol.OriginString)
			require.Same(t, col, table.Cols[table.Name2ColIndex[col.Name]])
		}
		require.Equal(t, alias, table.Indexes[0].Parts[3])
		require.Len(t, functionalIndexColumns(table, table.Indexes[1]), 1)
	}
}

func TestFunctionalIndexLegacyVisibleColumn(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table legacy(id int primary key,user_value int,index ordinary(user_value))", 1)
	require.NoError(t, err)
	p, err := BuildPlan(ctx, stmt, false)
	stmt.Free()
	require.NoError(t, err)
	// Model catalog metadata written before the functional-index feature.
	table := p.GetDdl().GetCreateTable().TableDef
	col := FindColumn(table.Cols, "user_value")
	col.Name, col.OriginName = "__mo_fi_user", "__mo_fi_user"
	table.Indexes[0].Parts[0] = col.Name
	table.Cols = append(table.Cols, &ColDef{Name: catalog.Row_ID, Hidden: true, Typ: Type{Id: int32(types.T_Rowid)}})
	table.Name2ColIndex = nil
	ensureName2ColIndexForReplace(table)
	require.NoError(t, validateTableIndexDefinitions(table))
	require.Nil(t, functionalIndexColumn(table, table.Indexes[0]))
	ctx.tables["legacy"] = table
	ctx.objects["legacy"] = &ObjectRef{ObjName: "legacy", SchemaName: "tpch"}
	ctx.isDml = true
	for _, indexTable := range p.GetDdl().GetCreateTable().IndexTables {
		indexTable.Cols = append(indexTable.Cols, &ColDef{Name: catalog.Row_ID, Hidden: true, Typ: Type{Id: int32(types.T_Rowid)}})
		ctx.tables[indexTable.Name] = indexTable
		ctx.objects[indexTable.Name] = &ObjectRef{ObjName: indexTable.Name, SchemaName: "tpch"}
	}
	for _, sql := range []string{
		"select __mo_fi_user from legacy", "insert into legacy(id,__mo_fi_user) values(1,7)",
		"insert into legacy(id,__mo_fi_user) values(1,default)",
		"update legacy set __mo_fi_user=8 where id=1", "delete from legacy where __mo_fi_user=8",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
		})
	}
	bad := DeepCopyTableDef(table, true)
	FindColumn(bad.Cols, "__mo_fi_user").Hidden = true
	require.Error(t, validateFunctionalTable(t.Context(), bad), "hidden backing metadata cannot lose its generated expression")
	bad = DeepCopyTableDef(table, true)
	bad.Indexes[0].Parts[0] = "__mo_fi_missing"
	require.Error(t, validateFunctionalTable(t.Context(), bad), "missing internal key metadata must fail closed")
	rt := runtime.ServiceRuntime(ctx.GetProcess().GetService())
	old, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion107)
	t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, old) })
	stmt, err = parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table collision(name varchar(40),"+functionalColumnName("ix")+" int,index ix((lower(name))))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	_, err = BuildPlan(ctx, stmt, false)
	require.ErrorContains(t, err, "backing column already exists")
}

func TestFunctionalIndexSyntheticKeyLayout(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	rt := runtime.ServiceRuntime(ctx.GetProcess().GetService())
	old, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion107)
	t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, old) })
	for _, sql := range []string{
		"create table t(a int,b int,primary key(a,b),index ix((a+1)),index iy(b,(a+2)))",
		"create table t(a int,b int,index ix((a+1)),index iy(b,(a+2))) cluster by(a,b)",
	} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		p, err := BuildPlan(ctx, stmt, false)
		stmt.Free()
		require.NoError(t, err)
		table := p.GetDdl().GetCreateTable().TableDef
		synthetic := table.Pkey.PkeyColName
		if table.ClusterBy != nil {
			synthetic = table.ClusterBy.Name
		} else {
			require.Equal(t, catalog.CPrimaryKeyColName, synthetic)
		}
		require.Equal(t, synthetic, table.Cols[len(table.Cols)-1].Name)
		require.NoError(t, validateFunctionalTable(t.Context(), table))
	}
}

func TestFunctionalIndexDDL(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	rt := runtime.ServiceRuntime(ctx.GetProcess().GetService())
	old, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion107)
	t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, old) })
	build := func(sql string) (*Plan, error) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		return BuildPlan(ctx, stmt, false)
	}
	for _, sql := range []string{
		"create table fi (id int primary key, name varchar(40), index idx_lower ((lower(name))))",
		"create table fi (id int primary key, value int, index idx_value ((value + 1)))",
	} {
		t.Run(sql, func(t *testing.T) {
			p, err := build(sql)
			require.NoError(t, err)
			table := p.GetDdl().GetCreateTable().TableDef
			ctx.tables["fi"] = table
			ctx.objects["fi"] = &ObjectRef{ObjName: "fi", SchemaName: "tpch"}
			for _, indexTable := range p.GetDdl().GetCreateTable().IndexTables {
				ctx.tables[indexTable.Name] = indexTable
				ctx.objects[indexTable.Name] = &ObjectRef{ObjName: indexTable.Name, SchemaName: "tpch"}
			}
			require.Len(t, table.Indexes, 1)
			col := functionalIndexColumn(table, table.Indexes[0])
			require.NotNil(t, col)
			query := "select * from fi where lower(name)='abc'"
			if table.Cols[1].Name == "value" {
				query = "select * from fi where value+1=2"
			}
			qp, queryErr := build(query)
			require.NoError(t, queryErr)
			indexScan := false
			residual := false
			for _, n := range qp.GetQuery().Nodes {
				if n.NodeType == pb.Node_TABLE_SCAN && n.TableDef != nil && n.TableDef.Name == "fi" {
					residual = len(n.FilterList) > 0
				}
				if n.NodeType == pb.Node_TABLE_SCAN && n.TableDef != nil && n.TableDef.Name == table.Indexes[0].IndexTableName {
					indexScan = true
				}
			}
			require.True(t, indexScan, "functional equality must use an index")
			require.True(t, residual, "functional equality must retain the base-row predicate")
			require.NoError(t, validateFunctionalTable(t.Context(), table))
			bad := DeepCopyTableDef(table, true)
			functionalIndexColumn(bad, bad.Indexes[0]).Typ.Width++
			require.Error(t, validateFunctionalTable(t.Context(), bad), "physical and expression types must agree")
			bad = DeepCopyTableDef(table, true)
			bad.Indexes = nil
			require.Error(t, validateFunctionalTable(t.Context(), bad), "backing columns must have an owner")
			ddl, _, err := constructCreateTableSQL(ctx, table, nil, false, nil, false, nil)
			require.NoError(t, err)
			require.NotContains(t, ddl, functionalColumnPrefix)
			rebuilt, err := build(ddl)
			require.NoError(t, err)
			require.NotNil(t, functionalIndexColumn(rebuilt.GetDdl().GetCreateTable().TableDef, rebuilt.GetDdl().GetCreateTable().TableDef.Indexes[0]))
		})
	}
	for _, sql := range []string{
		"create table fi (id int, index bad ((rand())))",
		"create table fi (ts timestamp, index bad ((cast(ts as char(19)))))",
		"create table fi (name varchar(40), unique index bad ((lower(name))))",
		"create table fi (name varchar(40), index bad ((lower(name)), (rand())))",
		"create table fi (name varchar(40), index bad ((lower(name)) desc))",
		"create table fi (name varchar(40), index bad ((lower(name))) include(name))",
		"create table fi (id int auto_increment primary key, index bad ((id+1)))",
		"create table fi (name varchar(40), g varchar(40) as (cast(now() as char(40))), index bad ((lower(g))))",
	} {
		t.Run(sql, func(t *testing.T) { _, err := build(sql); require.Error(t, err) })
	}
	for _, sql := range []string{
		"create index idx_fi on nation ((n_nationkey + 1))",
		"alter table nation add index idx_fi ((n_nationkey + 1))",
	} {
		p, err := build(sql)
		require.NoError(t, err)
		require.Equal(t, pb.AlterTable_COPY, p.GetDdl().GetAlterTable().AlgorithmType)
		require.Contains(t, p.GetDdl().GetAlterTable().CreateTmpTableSql, "idx_fi")
		require.NotContains(t, p.GetDdl().GetAlterTable().CreateTmpTableSql, functionalColumnPrefix)
	}
	_, err := build("alter table nation add index idx_fi ((n_nationkey + 1)), algorithm=inplace")
	require.Error(t, err)
	// The current main predecessor (MORPC v106) must not be able to author
	// functional-index metadata before all CNs understand the new contract.
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion106)
	_, err = build("create table fi (id int, index idx ((id + 1)))")
	require.ErrorContains(t, err, "protocol version 107")
}

func TestFunctionalCompositeIndexDDL(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	rt := runtime.ServiceRuntime(ctx.GetProcess().GetService())
	old, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion105)
	t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, old) })
	build := func(sql string) (*Plan, error) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		return BuildPlan(ctx, stmt, false)
	}
	for _, parts := range []string{
		"(lower(name)), (id+1)", "id, (lower(name))", "(lower(name)), id",
		"(lower(name)), (lower(name))", "id, (lower(name)), (upper(name))",
	} {
		t.Run(parts, func(t *testing.T) {
			p, err := build("create table fi (id int primary key, name varchar(40), index ix (" + parts + "))")
			require.NoError(t, err)
			table := p.GetDdl().GetCreateTable().TableDef
			ctx.tables["fi"] = table
			ctx.objects["fi"] = &ObjectRef{ObjName: "fi", SchemaName: "tpch"}
			for _, it := range p.GetDdl().GetCreateTable().IndexTables {
				ctx.tables[it.Name] = it
				ctx.objects[it.Name] = &ObjectRef{ObjName: it.Name, SchemaName: "tpch"}
			}
			require.NoError(t, validateFunctionalTable(t.Context(), table))
			cols := functionalIndexColumns(table, table.Indexes[0])
			require.NotEmpty(t, cols)
			seen := map[string]bool{}
			for _, col := range cols {
				require.False(t, seen[col.Name])
				seen[col.Name] = true
			}
			ddl, _, err := ConstructCreateTableSQL(ctx, table, nil, false, nil)
			require.NoError(t, err)
			require.NotContains(t, ddl, functionalColumnPrefix)
			rebuilt, err := build(ddl)
			require.NoError(t, err)
			require.Len(t, functionalIndexColumns(rebuilt.GetDdl().GetCreateTable().TableDef, rebuilt.GetDdl().GetCreateTable().TableDef.Indexes[0]), len(cols))
			bad := DeepCopyTableDef(table, true)
			bad.Indexes[0].Parts[0], bad.Indexes[0].Parts[1] = bad.Indexes[0].Parts[1], bad.Indexes[0].Parts[0]
			require.Error(t, validateFunctionalTable(t.Context(), bad), "backing ownership includes key position")
			if len(cols) > 1 {
				bad = DeepCopyTableDef(table, true)
				functionalIndexColumns(bad, bad.Indexes[0])[1].Typ.Width++
				require.Error(t, validateFunctionalTable(t.Context(), bad), "validate later expression parts too")
			}
		})
	}
}
