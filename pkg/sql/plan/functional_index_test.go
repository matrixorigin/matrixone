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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// Mutated planner metadata is a defensive typed oracle, not a SQL reproduction.
func TestFunctionalIndexResolvedOverloadMetadata(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	rt := runtime.ServiceRuntime(ctx.GetProcess().GetService())
	old, had := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion120)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion120)
		}
	})
	build := func(sql string) *TableDef {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		p, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		return p.GetDdl().GetCreateTable().TableDef
	}
	for _, expression := range []string{
		"lower(name)", "upper(name)", "to_lower(name)", "ucase(name)",
		"id+1", "id-1", "id*2", "abs(id)", "abs(u)", "id+u", "+id", "-id", "+u",
		"cast(id as signed)", "cast(name as varchar(40))",
		"lower('a\\\\b')", "cast(doc ->> '$.kind' as char(20))",
	} {
		t.Run("valid/"+expression, func(t *testing.T) {
			table := build("create table fi(id int,u bigint unsigned,name varchar(40),doc json,index idx((" + expression + ")))")
			require.NoError(t, ValidateFunctionalIndexMetadata(t.Context(), table))
			_, _, err := constructCreateTableSQL(ctx, table, nil, false, nil, false, nil)
			require.NoError(t, err)
		})
	}
	mutations := []struct {
		name   string
		change func(*Expr)
	}{
		{"unknown_function", func(e *Expr) { e.GetF().Func.Obj = function.EncodeOverloadID(0x7fffffff, 0) }},
		{"negative_identity", func(e *Expr) { e.GetF().Func.Obj = -1 }},
		{"out_of_range_overload", func(e *Expr) { e.GetF().Func.Obj = function.EncodeOverloadID(function.LOWER, 1) }},
		{"wrong_function_identity", func(e *Expr) { e.GetF().Func.Obj = function.EncodeOverloadID(function.UPPER, 0) }},
		{"wrong_function_name", func(e *Expr) { e.GetF().Func.ObjName = "upper" }},
		{"unsupported_name", func(e *Expr) { e.GetF().Func.ObjName = "case" }},
		{"missing_function_ref", func(e *Expr) { e.GetF().Func = nil }},
		{"missing_argument", func(e *Expr) { e.GetF().Args = nil }},
		{"extra_argument", func(e *Expr) { e.GetF().Args = append(e.GetF().Args, DeepCopyExpr(e.GetF().Args[0])) }},
		{"nil_argument", func(e *Expr) { e.GetF().Args[0] = nil }},
		{"nil_column", func(e *Expr) { e.GetF().Args[0].Expr = &pb.Expr_Col{} }},
		{"nil_literal", func(e *Expr) { e.GetF().Args[0].Expr = &pb.Expr_Lit{} }},
		{"nil_function", func(e *Expr) { e.GetF().Args[0].Expr = &pb.Expr_F{} }},
		{"nested_nil_cast_argument", func(e *Expr) {
			e.GetF().Args[0].Expr = &pb.Expr_F{F: &pb.Function{
				Func: &pb.ObjectRef{Obj: function.EncodeOverloadID(function.CAST, 0), ObjName: "cast"},
				Args: []*Expr{nil, {Typ: e.GetF().Args[0].Typ, Expr: &pb.Expr_T{T: &pb.TargetType{}}}},
			}}
		}},
		{"wrong_column_type", func(e *Expr) { e.GetF().Args[0].Typ.Id = int32(types.T_int64) }},
		{"out_of_range_type", func(e *Expr) { e.GetF().Args[0].Typ.Id = 256 }},
		{"unknown_type", func(e *Expr) { e.GetF().Args[0].Typ.Id = 251 }},
		{"wrong_result_width", func(e *Expr) { e.Typ.Width++ }},
		{"invalid_collation", func(e *Expr) { e.GetF().Args[0].Typ.Charset = 256 }},
	}
	for _, dependency := range []bool{false, true} {
		sql := "create table fi(name varchar(40),index idx((lower(name))))"
		label := "direct"
		if dependency {
			sql = "create table fi(name varchar(40),g varchar(40) as (lower(name)) virtual,index idx((lower(g))))"
			label = "transitive"
		}
		table := build(sql)
		for _, mutation := range mutations {
			t.Run(label+"/"+mutation.name, func(t *testing.T) {
				bad := DeepCopyTableDef(table, true)
				expr := functionalIndexColumn(bad, bad.Indexes[0]).GeneratedCol.Expr
				if dependency {
					expr = FindColumn(bad.Cols, "g").GeneratedCol.Expr
				}
				for expr.GetF().Func.ObjName == "cast" {
					expr = expr.GetF().Args[0]
				}
				mutation.change(expr)
				require.False(t, functionalExpressionEligible(functionalIndexColumn(bad, bad.Indexes[0]).GeneratedCol.Expr, bad.Cols, make(map[int32]bool)))
				require.Error(t, ValidateFunctionalIndexMetadata(t.Context(), bad))
				_, _, err := constructCreateTableSQL(ctx, bad, nil, false, nil, false, nil)
				require.Error(t, err, "the real SQL reconstruction consumer must reject damaged metadata")
			})
		}
	}
	castTable := build("create table fi(id int,index idx((cast(id as signed))))")
	for _, kind := range []string{"wrong_target", "missing_target", "nil_target", "unrelated_overload"} {
		t.Run("cast/"+kind, func(t *testing.T) {
			bad := DeepCopyTableDef(castTable, true)
			fn := functionalIndexColumn(bad, bad.Indexes[0]).GeneratedCol.Expr.GetF()
			require.NotNil(t, fn)
			switch kind {
			case "wrong_target":
				fn.Args[1].Typ.Id = int32(types.T_varchar)
			case "missing_target":
				fn.Args[1].Expr = &pb.Expr_Lit{Lit: &pb.Literal{}}
			case "nil_target":
				fn.Args[1] = nil
			case "unrelated_overload":
				fn.Func.Obj = function.EncodeOverloadID(function.CAST, function.TemporalIntegerArgumentCastOverload)
			}
			require.Error(t, ValidateFunctionalIndexMetadata(t.Context(), bad))
		})
	}
	jsonTable := build("create table fi(doc json,index idx((cast(doc ->> '$.kind' as char(20)))))")
	for _, kind := range []string{"mismatched_nested_identity", "unquote_result_width", "unquote_result_charset", "extract_result_width"} {
		t.Run("json/"+kind, func(t *testing.T) {
			bad := DeepCopyTableDef(jsonTable, true)
			unquote := functionalIndexColumn(bad, bad.Indexes[0]).GeneratedCol.Expr.GetF().Args[0]
			switch kind {
			case "mismatched_nested_identity":
				unquote.GetF().Func.ObjName = "lower"
			case "unquote_result_width":
				unquote.Typ.Width++
			case "unquote_result_charset":
				unquote.Typ.Charset = uint32(types.CharsetUTF8)
			case "extract_result_width":
				unquote.GetF().Args[0].Typ.Width++
			}
			require.Error(t, ValidateFunctionalIndexMetadata(t.Context(), bad))
			_, _, err := constructCreateTableSQL(ctx, bad, nil, false, nil, false, nil)
			require.Error(t, err, "the real SQL reconstruction consumer must reject damaged JSON result metadata")
		})
	}
}

func TestFunctionalIndexCopyColumnTypeRefresh(t *testing.T) {
	for _, change := range []string{"modify name varchar(60)", "modify name varchar(80) first"} {
		t.Run(change, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table fi(id int, name varchar(40), index ix((lower(name)),(upper(name))))", 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			original := p.GetDdl().GetCreateTable().TableDef
			copyTable := DeepCopyTableDef(original, true)
			alter, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "alter table fi "+change, 1)
			require.NoError(t, err)
			defer alter.Free()
			option := alter.(*tree.AlterTable).Options[0].(*tree.AlterTableModifyColumnClause)
			_, err = updateNewColumnInTableDef(ctx, copyTable, FindColumn(copyTable.Cols, "name"), option.NewColumn, option.Position)
			require.NoError(t, err)
			require.NoError(t, validateFunctionalTable(t.Context(), copyTable))
			for _, col := range functionalIndexColumns(copyTable, copyTable.Indexes[0]) {
				require.Equal(t, FindColumn(copyTable.Cols, "name").Typ.Width, col.Typ.Width)
				require.True(t, functionalExpressionEligible(col.GeneratedCol.Expr, copyTable.Cols, make(map[int32]bool)))
			}
			_, _, err = constructCreateTableSQL(ctx, copyTable, nil, false, nil, false, nil)
			require.NoError(t, err)
			require.NoError(t, validateFunctionalTable(t.Context(), original))
			require.Equal(t, int32(40), FindColumn(original.Cols, "name").Typ.Width)
		})
	}
}

func TestFunctionalIndexCopyGeneratedDependencyRefresh(t *testing.T) {
	for _, change := range []string{"modify a varchar(60)", "modify a varchar(60) first"} {
		t.Run(change, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table fi(id int primary key,a varchar(40),g varchar(40) as (lower(a)) virtual,h varchar(40) as (upper(g)) virtual,index ix((lower(h))))", 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			original := p.GetDdl().GetCreateTable().TableDef
			copyTable := DeepCopyTableDef(original, true)
			alter, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "alter table fi "+change, 1)
			require.NoError(t, err)
			defer alter.Free()
			option := alter.(*tree.AlterTable).Options[0].(*tree.AlterTableModifyColumnClause)
			_, err = updateNewColumnInTableDef(ctx, copyTable, FindColumn(copyTable.Cols, "a"), option.NewColumn, option.Position)
			require.NoError(t, err)
			require.Equal(t, int32(60), FindColumn(copyTable.Cols, "a").Typ.Width)
			for _, name := range []string{"g", "h"} {
				col := FindColumn(copyTable.Cols, name)
				require.Equal(t, int32(40), col.Typ.Width, "visible assignment type must be preserved")
				require.True(t, sameFunctionalValueType(col.Typ, col.GeneratedCol.Expr.Typ))
			}
			require.NoError(t, validateFunctionalTable(t.Context(), copyTable))
			for _, damage := range []string{"overload", "charset", "target_width"} {
				t.Run(damage, func(t *testing.T) {
					bad := DeepCopyTableDef(copyTable, true)
					assignment := FindColumn(bad.Cols, "g").GeneratedCol.Expr.GetF()
					require.Equal(t, "cast_strict", assignment.Func.ObjName)
					switch damage {
					case "overload":
						assignment.Func.Obj++
					case "charset":
						assignment.Args[0].Typ.Charset ^= 1
					case "target_width":
						assignment.Args[1].Typ.Width++
					}
					require.Error(t, validateFunctionalTable(t.Context(), bad))
				})
			}
			_, _, err = constructCreateTableSQL(ctx, copyTable, nil, false, nil, false, nil)
			require.NoError(t, err)
			require.NoError(t, validateFunctionalTable(t.Context(), original))
			require.Equal(t, int32(40), FindColumn(original.Cols, "a").Typ.Width)
		})
	}
}

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
	// #29898: expression UNIQUE indexes must be rejected before index construction.
	probe, err := build("CREATE TABLE mo_index_probe (id INT PRIMARY KEY, email VARCHAR(254))")
	require.NoError(t, err)
	ctx.tables["mo_index_probe"] = probe.GetDdl().GetCreateTable().TableDef
	ctx.objects["mo_index_probe"] = &ObjectRef{ObjName: "mo_index_probe", SchemaName: "tpch"}
	for _, tc := range []struct {
		sql  string
		code uint16
	}{
		{"CREATE UNIQUE INDEX mo_unique_email_expression ON mo_index_probe ((LOWER(email)))", moerr.ErrNotSupported},
		{"CREATE TABLE unique_expression_probe (email VARCHAR(254), UNIQUE INDEX bad ((LOWER(email))))", moerr.ErrInternal},
		{"ALTER TABLE mo_index_probe ADD UNIQUE INDEX bad ((LOWER(email)))", moerr.ErrInternal},
		{"ALTER TABLE mo_index_probe ADD UNIQUE INDEX bad ((LOWER(email))), ALGORITHM=COPY", moerr.ErrInternal},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			p, err := build(tc.sql)
			require.Nil(t, p)
			require.True(t, moerr.IsMoErrCode(err, tc.code), err)
		})
	}
	ordinary, err := build("CREATE UNIQUE INDEX mo_unique_email ON mo_index_probe (email)")
	require.NoError(t, err)
	index := ordinary.GetDdl().GetCreateIndex().GetIndex().GetTableDef().GetIndexes()
	require.Len(t, index, 1)
	require.True(t, index[0].Unique)
	require.Equal(t, []string{"email"}, index[0].Parts)
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
	_, err = build("alter table nation add index idx_fi ((n_nationkey + 1)), algorithm=inplace")
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
	// Functional-index metadata is a new persisted contract.  The current main
	// predecessor stops at v106; successful composite authoring must run with
	// the newly allocated cumulative capability v107.
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion107)
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

func TestFunctionalIndexOriginStringQuotesLiterals(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	rt := runtime.ServiceRuntime(ctx.GetProcess().GetService())
	old, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion107)
	t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, old) })

	build := func(sql string) *TableDef {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		plan, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		return plan.GetDdl().GetCreateTable().TableDef
	}

	for _, tc := range []struct {
		name, expression, want, value string
	}{
		{name: "same named column", expression: "lower('abc')", want: "lower('abc')", value: "abc"},
		{name: "quoted literal", expression: "lower('a''b')", want: "lower('a''b')", value: "a'b"},
		{name: "backslash literal", expression: `lower('a\\b')`, want: "lower(cast(0x615c62 as varchar))", value: "a\\b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := build("create table fi (abc varchar(40), index ix ((" + tc.expression + ")))")
			owner := functionalIndexColumn(table, table.Indexes[0])
			require.NotNil(t, owner)
			require.Equal(t, tc.want, owner.GeneratedCol.OriginString)
			require.Equal(t, tc.value, functionalIndexLiteralValue(owner.GeneratedCol.Expr))

			ddl, _, err := constructCreateTableSQL(ctx, table, nil, false, nil, false, nil)
			require.NoError(t, err)
			rebuilt := build(ddl)
			rebuiltOwner := functionalIndexColumn(rebuilt, rebuilt.Indexes[0])
			require.Equal(t, tc.want, rebuiltOwner.GeneratedCol.OriginString)
			require.Equal(t, tc.value, functionalIndexLiteralValue(rebuiltOwner.GeneratedCol.Expr))
		})
	}
}

func functionalIndexLiteralValue(expr *Expr) string {
	if fn := expr.GetF(); fn != nil && len(fn.Args) == 1 {
		expr = fn.Args[0]
	}
	for {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) != 2 {
			break
		}
		expr = fn.Args[0]
	}
	if lit := expr.GetLit(); lit != nil {
		return lit.GetSval()
	}
	return ""
}
