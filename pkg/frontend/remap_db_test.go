// Copyright 2024 Matrix Origin
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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// applyRemapDbToSQL parses sql, runs applyRemapDb with the given remap, and
// returns the re-stringified statement.
func applyRemapDbToSQL(t *testing.T, sql string, remap map[string]string) string {
	ctx := context.Background()
	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.NoError(t, applyRemapDb(ctx, stmts, remap, 1))
	return tree.StringWithOpts(stmts[0], dialect.MYSQL, tree.WithSingleQuoteString())
}

func TestApplyRemapDb(t *testing.T) {
	remap := map[string]string{"dbxxx": "dbyyy"}

	t.Run("analyze all qualified entries", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "analyze table dbxxx.a(x), dbxxx.b", remap)
		require.Contains(t, out, "dbyyy.a(x)")
		require.Contains(t, out, "dbyyy.b")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("analyze unqualified entry untouched", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "analyze table a(x)", remap)
		require.Contains(t, out, "analyze table a(x)")
		require.NotContains(t, out, "dbyyy")
	})

	t.Run("prepared statement body", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "prepare s from select * from dbxxx.t", remap)
		require.Equal(t, "prepare s from select * from dbyyy.t", out)
	})

	t.Run("use preserves one-shot remap semantics", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "use dbxxx", remap)
		require.Equal(t, "use dbxxx", out)
	})

	t.Run("qualified ref", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from dbxxx.t", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("join both sides", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from dbxxx.a join dbxxx.b on a.id = b.id", remap)
		require.Contains(t, out, "dbyyy.a")
		require.Contains(t, out, "dbyyy.b")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("from subquery", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from (select * from dbxxx.t) x", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("union", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select id from dbxxx.a union select id from dbxxx.b", remap)
		require.Contains(t, out, "dbyyy.a")
		require.Contains(t, out, "dbyyy.b")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("cte name is not remapped, body is", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "with c as (select * from dbxxx.t) select * from c", remap)
		require.Contains(t, out, "dbyyy.t") // body remapped
		require.Contains(t, out, "from c")  // CTE reference untouched
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("unqualified ref untouched", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from t", remap)
		require.Contains(t, out, "from t")
		require.NotContains(t, out, "dbyyy")
	})

	t.Run("non-mapped db untouched", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from other.t", remap)
		require.Contains(t, out, "other.t")
	})

	t.Run("insert target and select source", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "insert into dbxxx.t select * from dbxxx.u", remap)
		require.Contains(t, out, "dbyyy.t")
		require.Contains(t, out, "dbyyy.u")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("insert values", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "insert into dbxxx.t values (1)", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("insert logical target and qualified columns", func(t *testing.T) {
		ctx := context.Background()
		stmts, err := parsers.Parse(ctx, dialect.MYSQL,
			"insert into dbxxx.t(dbxxx.t.id, dbxxx.t.v) values (1, 2) on duplicate key update v = values(dbxxx.t.v)", 1)
		require.NoError(t, err)
		insert := stmts[0].(*tree.Insert)
		require.NoError(t, applyRemapDb(context.Background(), stmts, remap, 1))

		require.Equal(t, tree.Identifier("dbyyy"), insert.TargetDatabaseName)
		require.Equal(t, "dbyyy", insert.ColumnNames[0].DbNameOrigin())
		valuesExpr := insert.OnDuplicateUpdate[0].Expr.(*tree.FuncExpr)
		require.Equal(t, "dbyyy", valuesExpr.Exprs[0].(*tree.UnresolvedName).DbNameOrigin())
		require.NotContains(t, tree.String(insert, dialect.MYSQL), "dbxxx")
	})

	t.Run("multi-table insert targets, values, conditions and source", func(t *testing.T) {
		out := applyRemapDbToSQL(t,
			"insert first when dbxxx.u.k > 1 then into dbxxx.t (id) values (dbxxx.u.k)"+
				" else into dbxxx.t2 (dbxxx.t2.id) values (k) select k from dbxxx.u", remap)
		require.Contains(t, out, "dbyyy.t ")
		require.Contains(t, out, "dbyyy.t2")
		require.Contains(t, out, "dbyyy.u")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("replace logical target and qualified columns", func(t *testing.T) {
		ctx := context.Background()
		stmts, err := parsers.Parse(ctx, dialect.MYSQL,
			"replace into dbxxx.t(dbxxx.t.id, dbxxx.t.v) values (1, 2)", 1)
		require.NoError(t, err)
		replace := stmts[0].(*tree.Replace)
		require.NoError(t, applyRemapDb(context.Background(), stmts, remap, 1))

		require.Equal(t, tree.Identifier("dbyyy"), replace.TargetDatabaseName)
		require.Equal(t, "dbyyy", replace.ColumnNames[0].DbNameOrigin())
		require.NotContains(t, tree.String(replace, dialect.MYSQL), "dbxxx")
	})

	t.Run("update", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "update dbxxx.t set v = 1 where id = 2", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("delete", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "delete from dbxxx.t where id = 2", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("where IN subquery", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from dbxxx.a where id in (select id from dbxxx.b)", remap)
		require.Contains(t, out, "dbyyy.a")
		require.Contains(t, out, "dbyyy.b")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("where EXISTS subquery", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from dbxxx.a where exists (select 1 from dbxxx.b where b.id = a.id)", remap)
		require.Contains(t, out, "dbyyy.a")
		require.Contains(t, out, "dbyyy.b")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("having subquery", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select id from dbxxx.a group by id having count(*) > (select count(*) from dbxxx.b)", remap)
		require.Contains(t, out, "dbyyy.a")
		require.Contains(t, out, "dbyyy.b")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("named window subquery", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select 1 from dbxxx.a window w as (partition by (select id from dbxxx.b))", remap)
		require.Contains(t, out, "dbyyy.a")
		require.Contains(t, out, "dbyyy.b")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("projection scalar subquery", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select (select max(id) from dbxxx.b) from dbxxx.a", remap)
		require.Contains(t, out, "dbyyy.a")
		require.Contains(t, out, "dbyyy.b")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("join ON subquery", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "select * from dbxxx.a join dbxxx.c on a.id in (select id from dbxxx.b)", remap)
		require.Contains(t, out, "dbyyy.a")
		require.Contains(t, out, "dbyyy.b")
		require.Contains(t, out, "dbyyy.c")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("delete with IN subquery source", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "delete from dbxxx.t where id in (select id from dbxxx.s)", remap)
		require.Contains(t, out, "dbyyy.t")
		require.Contains(t, out, "dbyyy.s")
		require.NotContains(t, out, "dbxxx")
	})

	// table-level DDL: the qualified target is remapped
	t.Run("create table", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "create table dbxxx.t(id int)", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})
	t.Run("create table as select", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "create table dbxxx.t as select * from dbxxx.s", remap)
		require.Contains(t, out, "dbyyy.t")
		require.Contains(t, out, "dbyyy.s")
		require.NotContains(t, out, "dbxxx")
	})
	t.Run("create view", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "create view dbxxx.v as select * from dbxxx.t", remap)
		require.Contains(t, out, "dbyyy.v")
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("create view with qualified column", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "create view dbxxx.v as select dbxxx.t.id from dbxxx.t", remap)
		require.Contains(t, out, "dbyyy.t.id")
		require.Contains(t, out, "from dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})
	t.Run("create index", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "create index ix on dbxxx.t(id)", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})
	t.Run("alter table", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "alter table dbxxx.t add column c int", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})
	t.Run("alter table rename remaps source and destination", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "alter table dbxxx.t rename to dbxxx.t2", remap)
		require.Contains(t, out, "dbyyy.t")
		require.Contains(t, out, "dbyyy.t2")
		require.NotContains(t, out, "dbxxx")
	})
	t.Run("drop table multi", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "drop table dbxxx.t, dbxxx.s", remap)
		require.Contains(t, out, "dbyyy.t")
		require.Contains(t, out, "dbyyy.s")
		require.NotContains(t, out, "dbxxx")
	})
	t.Run("drop view", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "drop view dbxxx.v", remap)
		require.Contains(t, out, "dbyyy.v")
		require.NotContains(t, out, "dbxxx")
	})
	t.Run("drop index", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "drop index ix on dbxxx.t", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("truncate table", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "truncate table dbxxx.t", remap)
		require.Contains(t, out, "dbyyy.t")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("rename table remaps source and destination", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "rename table dbxxx.a to dbxxx.b", remap)
		require.Contains(t, out, "dbyyy.a")
		require.Contains(t, out, "dbyyy.b")
		require.NotContains(t, out, "dbxxx")
	})

	t.Run("rename table unqualified untouched", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "rename table a to b", remap)
		require.NotContains(t, out, "dbyyy")
	})

	// database-level DDL is NOT remapped
	t.Run("create database is not remapped", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "create database dbxxx", map[string]string{"dbxxx": "dbyyy"})
		require.Contains(t, out, "dbxxx")
		require.NotContains(t, out, "dbyyy")
	})
	t.Run("drop database is not remapped", func(t *testing.T) {
		out := applyRemapDbToSQL(t, "drop database dbxxx", map[string]string{"dbxxx": "dbyyy"})
		require.Contains(t, out, "dbxxx")
		require.NotContains(t, out, "dbyyy")
	})
}

func TestApplyRemapDbPreservesIdentifierComparisonMode(t *testing.T) {
	tests := []struct {
		name        string
		lower       int64
		remapSource string
		wantTarget  string
		wantCompare string
	}{
		{name: "case sensitive", lower: 0, remapSource: "SrcMix27190", wantTarget: "DstMix27190", wantCompare: "DstMix27190"},
		{name: "lowercase names", lower: 1, remapSource: "SrcMix27190", wantTarget: "dstmix27190", wantCompare: "dstmix27190"},
		{name: "preserve names and compare lowercase", lower: 2, remapSource: "SrcMix27190", wantTarget: "DstMix27190", wantCompare: "dstmix27190"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmts, err := parsers.Parse(context.Background(), dialect.MYSQL,
				"prepare s from insert into SrcMix27190.t(SrcMix27190.t.id, SrcMix27190.t.v) "+
					"values (1, 10) on duplicate key update v = values(SrcMix27190.t.v)", test.lower)
			require.NoError(t, err)
			require.NoError(t, applyRemapDb(context.Background(), stmts,
				map[string]string{test.remapSource: "DstMix27190"}, test.lower))

			insert := stmts[0].(*tree.PrepareStmt).Stmt.(*tree.Insert)
			require.Equal(t, tree.Identifier(test.wantTarget), insert.TargetDatabaseName)
			require.Equal(t, test.wantTarget, insert.ColumnNames[0].DbNameOrigin())
			require.Equal(t, test.wantCompare, insert.ColumnNames[0].DbName())
			valuesExpr := insert.OnDuplicateUpdate[0].Expr.(*tree.FuncExpr)
			valuesColumn := valuesExpr.Exprs[0].(*tree.UnresolvedName)
			require.Equal(t, test.wantTarget, valuesColumn.DbNameOrigin())
			require.Equal(t, test.wantCompare, valuesColumn.DbName())
		})
	}
}

func TestApplyRemapDbExpressionContainers(t *testing.T) {
	remap := map[string]string{"src": "dst"}
	tests := []struct {
		name     string
		sql      string
		contains []string
		absent   []string
	}{
		{
			name:     "qualified column in view body",
			sql:      "create view src.v as select src.t.a from src.t",
			contains: []string{"create view dst.v", "dst.t.a", "from dst.t"},
			absent:   []string{"src.t.a", "from src.t"},
		},
		{
			name:     "top level order by subquery",
			sql:      "create view src.v as select a from src.t order by (select max(b) from src.u)",
			contains: []string{"from dst.t", "from dst.u"},
			absent:   []string{"from src.t", "from src.u"},
		},
		{
			name:     "aggregate order by subquery",
			sql:      "create view src.v as select group_concat(a order by (select max(b) from src.u)) from src.t",
			contains: []string{"from dst.t", "from dst.u"},
			absent:   []string{"from src.t", "from src.u"},
		},
		{
			name:     "window order by subquery",
			sql:      "create view src.v as select row_number() over (order by (select max(b) from src.u)) from src.t",
			contains: []string{"from dst.t", "from dst.u"},
			absent:   []string{"from src.t", "from src.u"},
		},
		{
			name:     "cte union nested select and join",
			sql:      "create view src.v as with c as (select src.cte_t.a from src.cte_t) select a from c union select x.a from (select src.nested_t.a from src.nested_t) x join src.join_t j on x.a = j.a",
			contains: []string{"dst.cte_t.a", "from dst.cte_t", "dst.nested_t.a", "from dst.nested_t", "join dst.join_t"},
			absent:   []string{"src.cte_t.a", "from src.cte_t", "src.nested_t.a", "from src.nested_t", "join src.join_t"},
		},
		{
			name:     "aliases and string literals are unchanged",
			sql:      "create view src.v as select src.a, 'src.t.a' as literal from src.t as src",
			contains: []string{"src.a", "'src.t.a'", "from dst.t as src"},
			absent:   []string{"from src.t"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := applyRemapDbToSQL(t, tt.sql, remap)
			for _, expected := range tt.contains {
				require.Contains(t, out, expected)
			}
			for _, unexpected := range tt.absent {
				require.NotContains(t, out, unexpected)
			}
		})
	}
}

func TestRemapDbInFullTextMatchPattern(t *testing.T) {
	pattern := tree.NewUnresolvedName(
		tree.NewCStr("src", 1),
		tree.NewCStr("docs", 1),
		tree.NewCStr("pattern", 1),
	)
	match := &tree.FullTextMatchExpr{Pattern: pattern}

	remapDbInExpr(match, remapDbContext{
		databases: map[string]string{"src": "dst"}, lowerCaseTableNames: 1,
	})

	require.Equal(t, "dst.docs.pattern", tree.String(pattern, dialect.MYSQL))
}

func TestApplyRemapDbDMLExpressionContainers(t *testing.T) {
	remap := map[string]string{"src": "dst"}
	tests := []struct {
		name     string
		sql      string
		contains []string
		absent   []string
	}{
		{
			name:     "update order by subquery",
			sql:      "update src.t set a = (select max(b) from src.u) order by (select max(c) from src.o) limit 1",
			contains: []string{"update dst.t", "from dst.u", "from dst.o"},
			absent:   []string{"update src.t", "from src.u", "from src.o"},
		},
		{
			name:     "delete order by subquery",
			sql:      "delete from src.t order by (select max(b) from src.u) limit 1",
			contains: []string{"delete from dst.t", "from dst.u"},
			absent:   []string{"delete from src.t", "from src.u"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := applyRemapDbToSQL(t, tt.sql, remap)
			for _, expected := range tt.contains {
				require.Contains(t, out, expected)
			}
			for _, unexpected := range tt.absent {
				require.NotContains(t, out, unexpected)
			}
		})
	}
}

func TestApplyRemapDbDMLWithAndReturning(t *testing.T) {
	remap := map[string]string{"src": "dst"}
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "insert",
			sql:  "with ext_t as (select 1 as n) insert into src.sink select n from ext_t returning src.sink.id",
		},
		{
			name: "replace",
			sql:  "replace into src.sink values (1) returning src.sink.id",
		},
		{
			name: "update",
			sql:  "with ext_t as (select 1 as n) update src.sink set id = id where id in (select n from ext_t) returning src.sink.id",
		},
		{
			name: "delete",
			sql:  "with ext_t as (select 1 as n) delete from src.sink where id in (select n from ext_t) returning src.sink.id",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out := applyRemapDbToSQL(t, test.sql, remap)
			require.Contains(t, out, "dst.sink.id")
			require.NotContains(t, out, "src.sink.id")
			require.Contains(t, out, "dst.sink")
		})
	}

	qualifiedFunction := tree.NewUnresolvedName(
		tree.NewCStr("src", 1), tree.NewCStr("f_external", 1),
	)
	remapDbInStmt(&tree.Update{
		Returning: tree.SelectExprs{{Expr: &tree.FuncExpr{
			Func:     tree.FuncName2ResolvableFunctionReference(qualifiedFunction),
			FuncName: tree.NewCStr("f_external", 1),
		}}},
	}, remapDbContext{
		databases:           remap,
		lowerCaseTableNames: 1,
	})
	require.Equal(t, "dst", qualifiedFunction.TblNameOrigin())
}

func TestRemapDbInStmtRewritesExecutableWrapperReferences(t *testing.T) {
	remap := remapDbContext{
		databases:           map[string]string{"src": "dst"},
		lowerCaseTableNames: 1,
		remapUseDatabase:    true,
	}
	qualifiedTable := func(name string) *tree.TableName {
		return tree.NewTableName(tree.Identifier(name), tree.ObjectNamePrefix{
			SchemaName:     "src",
			ExplicitSchema: true,
		}, nil)
	}
	qualifiedObject := func(name string) *tree.UnresolvedObjectName {
		return tree.NewUnresolvedObjectName("src", name)
	}
	qualifiedColumn := func(table, name string) *tree.UnresolvedName {
		return tree.NewUnresolvedName(
			tree.NewCStr("src", 1),
			tree.NewCStr(table, 1),
			tree.NewCStr(name, 1),
		)
	}
	assertTable := func(t *testing.T, table *tree.TableName) {
		t.Helper()
		require.Equal(t, tree.Identifier("dst"), table.SchemaName)
	}
	assertObject := func(t *testing.T, object *tree.UnresolvedObjectName) {
		t.Helper()
		require.Equal(t, "dst", object.GetDBName())
	}
	assertColumn := func(t *testing.T, column *tree.UnresolvedName) {
		t.Helper()
		require.Equal(t, "dst", column.DbNameOrigin())
	}

	t.Run("do and merge", func(t *testing.T) {
		doColumn := qualifiedColumn("do_table", "value")
		remapDbInStmt(&tree.Do{Exprs: tree.Exprs{doColumn}}, remap)
		assertColumn(t, doColumn)

		target := qualifiedTable("merge_target")
		source := qualifiedTable("merge_source")
		onColumn := qualifiedColumn("merge_source", "id")
		conditionColumn := qualifiedColumn("merge_target", "matched")
		updateName := qualifiedColumn("merge_target", "value")
		updateValue := qualifiedColumn("merge_source", "value")
		insertValue := qualifiedColumn("merge_source", "new_value")
		returningColumn := qualifiedColumn("merge_target", "value")
		remapDbInStmt(&tree.Merge{
			Target: target,
			Source: source,
			On:     onColumn,
			Clauses: tree.MergeClauses{
				nil,
				{
					Condition:    conditionColumn,
					UpdateExprs:  tree.UpdateExprs{{Names: []*tree.UnresolvedName{updateName}, Expr: updateValue}},
					InsertValues: tree.Exprs{insertValue},
				},
			},
			Returning: tree.SelectExprs{{Expr: returningColumn}},
		}, remap)
		assertTable(t, target)
		assertTable(t, source)
		for _, column := range []*tree.UnresolvedName{
			onColumn, conditionColumn, updateName, updateValue, insertValue, returningColumn,
		} {
			assertColumn(t, column)
		}
	})

	t.Run("load dump and sequence statements", func(t *testing.T) {
		loadTable := qualifiedTable("load_data")
		dumpTable := qualifiedTable("dump_table")
		attachTable := qualifiedTable("attach_table")
		createSequence := qualifiedTable("new_sequence")
		dropFirst := qualifiedTable("drop_first")
		dropSecond := qualifiedTable("drop_second")
		alterSequence := qualifiedTable("alter_sequence")
		for _, statement := range []tree.Statement{
			&tree.Load{Table: loadTable},
			&tree.DumpTable{Table: dumpTable},
			&tree.LoadTable{Table: attachTable},
			&tree.CreateSequence{Name: createSequence},
			&tree.DropSequence{Names: tree.TableNames{dropFirst, dropSecond}},
			&tree.AlterSequence{Name: alterSequence},
		} {
			remapDbInStmt(statement, remap)
		}
		for _, table := range []*tree.TableName{
			loadTable, dumpTable, attachTable, createSequence, dropFirst, dropSecond, alterSequence,
		} {
			assertTable(t, table)
		}
	})

	t.Run("show statements", func(t *testing.T) {
		showCreateDatabase := &tree.ShowCreateDatabase{Name: "src"}
		showCreateTable := qualifiedObject("create_table")
		showCreateView := qualifiedObject("create_view")
		showColumnsTable := qualifiedObject("columns_table")
		showColumnsLike := qualifiedColumn("columns_table", "name")
		showColumnsWhere := qualifiedColumn("columns_table", "id")
		showIndexTable := qualifiedObject("index_table")
		showIndexWhere := qualifiedColumn("index_table", "id")
		showColumnNumber := qualifiedObject("column_number")
		showTableValues := qualifiedObject("table_values")
		showTableSize := qualifiedObject("table_size")
		showTarget := &tree.ShowTarget{DbName: "src"}
		showTableStatus := &tree.ShowTableStatus{DbName: "src"}
		showSequences := &tree.ShowSequences{DBName: "src"}
		showTables := &tree.ShowTables{DBName: "src"}
		showTableNumber := &tree.ShowTableNumber{DbName: "src"}
		useDatabase := tree.NewUse(tree.NewCStr("src", 1), false, tree.SecondaryRoleTypeAll, nil)
		for _, statement := range []tree.Statement{
			showCreateDatabase,
			&tree.ShowCreateTable{Name: showCreateTable},
			&tree.ShowCreateView{Name: showCreateView},
			&tree.ShowColumns{
				Table:  showColumnsTable,
				DBName: "src",
				Like:   &tree.ComparisonExpr{Left: showColumnsLike},
				Where:  &tree.Where{Expr: showColumnsWhere},
			},
			&tree.ShowIndex{
				TableName: showIndexTable,
				DbName:    "src",
				Where:     &tree.Where{Expr: showIndexWhere},
			},
			&tree.ShowColumnNumber{Table: showColumnNumber, DbName: "src"},
			&tree.ShowTableValues{Table: showTableValues, DbName: "src"},
			&tree.ShowTableSize{Table: showTableSize, DbName: "src"},
			showTarget,
			showTableStatus,
			showSequences,
			showTables,
			showTableNumber,
			useDatabase,
		} {
			remapDbInStmt(statement, remap)
		}
		require.Equal(t, "dst", showCreateDatabase.Name)
		for _, object := range []*tree.UnresolvedObjectName{
			showCreateTable, showCreateView, showColumnsTable, showIndexTable,
			showColumnNumber, showTableValues, showTableSize,
		} {
			assertObject(t, object)
		}
		for _, column := range []*tree.UnresolvedName{showColumnsLike, showColumnsWhere, showIndexWhere} {
			assertColumn(t, column)
		}
		require.Equal(t, "dst", showTarget.DbName)
		require.Equal(t, "dst", showTableStatus.DbName)
		require.Equal(t, "dst", showSequences.DBName)
		require.Equal(t, "dst", showTables.DBName)
		require.Equal(t, "dst", showTableNumber.DbName)
		require.Equal(t, "dst", useDatabase.Name.Compare())
	})
}

func TestRemapCloneRoutineStatementsClosesNestedDatabaseReferences(t *testing.T) {
	remapRoutine := func(t *testing.T, sql string) string {
		t.Helper()
		stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer freeStatements(stmts)
		require.NoError(t, remapCloneRoutineStatements(
			context.Background(), stmts, map[string]string{"source_db": "target_db"}, 1,
		))
		return tree.StringWithOpts(stmts[0], dialect.MYSQL, tree.WithSingleQuoteString())
	}

	t.Run("create table foreign key reference", func(t *testing.T) {
		out := remapRoutine(t, `begin
			create table source_db.child (
				id int,
				parent_id int,
				foreign key (parent_id) references source_db.parent(id)
			);
		end`)
		require.Contains(t, out, "target_db.child")
		require.Contains(t, out, "references target_db.parent")
		require.NotContains(t, out, "source_db")
	})

	t.Run("alter table add foreign key reference", func(t *testing.T) {
		out := remapRoutine(t, `begin
			alter table source_db.child
				add constraint fk_child_parent foreign key (parent_id)
				references source_db.parent(id);
		end`)
		require.Contains(t, out, "alter table target_db.child")
		require.Contains(t, out, "references target_db.parent")
		require.NotContains(t, out, "source_db")
	})

	t.Run("labeled repeat preserves control flow and remaps nested query", func(t *testing.T) {
		out := remapRoutine(t, `begin
			repeat_label: repeat
				select count(*) from source_db.t;
				if true then iterate repeat_label; end if;
				leave repeat_label;
			until true end repeat repeat_label;
		end`)
		require.Contains(t, out, "target_db.t")
		require.NotContains(t, out, "source_db")
		require.Contains(t, out, "end repeat repeat_label")

		formatted, err := parsers.Parse(context.Background(), dialect.MYSQL, out, 1)
		require.NoError(t, err)
		defer freeStatements(formatted)
	})

	t.Run("unrecognized alter option is rejected", func(t *testing.T) {
		stmts, err := parsers.Parse(context.Background(), dialect.MYSQL,
			`alter table source_db.child drop column parent_id`, 1)
		require.NoError(t, err)
		defer freeStatements(stmts)
		err = remapCloneRoutineStatements(
			context.Background(), stmts, map[string]string{"source_db": "target_db"}, 1,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot be safely remapped")
	})

	t.Run("table function arguments and embedded select", func(t *testing.T) {
		out := remapRoutine(t,
			`select * from unnest((select id from source_db.argument_source)) as f`)
		require.Contains(t, out, "target_db.argument_source")
		require.NotContains(t, out, "source_db")

		stmts, err := parsers.Parse(context.Background(), dialect.MYSQL,
			`select * from source_db.embedded_source`, 1)
		require.NoError(t, err)
		defer freeStatements(stmts)
		function := &tree.TableFunction{SelectStmt: stmts[0].(*tree.Select)}
		remapDbInTableExpr(function, remapDbContext{
			databases:           map[string]string{"source_db": "target_db"},
			lowerCaseTableNames: 1,
		})
		require.Contains(t,
			tree.StringWithOpts(function.SelectStmt, dialect.MYSQL, tree.WithSingleQuoteString()),
			"target_db.embedded_source",
		)
	})

	t.Run("every create table definition is audited", func(t *testing.T) {
		qualifiedColumn := func(table, name string) *tree.UnresolvedName {
			return tree.NewUnresolvedName(
				tree.NewCStr("source_db", 1),
				tree.NewCStr(table, 1),
				tree.NewCStr(name, 1),
			)
		}
		qualified := func(name string) *tree.TableName {
			return tree.NewTableName(tree.Identifier(name), tree.ObjectNamePrefix{
				SchemaName: "source_db", ExplicitSchema: true,
			}, nil)
		}
		column := qualifiedColumn("child", "value")
		keyPart := &tree.KeyPart{ColName: qualifiedColumn("child", "value")}
		defs := tree.TableDefs{
			&tree.ColumnTableDef{
				Name: column,
				Attributes: []tree.ColumnAttribute{
					&tree.AttributeNull{}, &tree.AttributeAutoIncrement{}, &tree.AttributeUniqueKey{},
					&tree.AttributeUnique{}, &tree.AttributeKey{}, &tree.AttributePrimaryKey{},
					&tree.AttributeCollate{}, &tree.AttributeCharset{}, &tree.AttributeColumnFormat{},
					&tree.AttributeStorage{}, &tree.AttributeLowCardinality{}, &tree.AttributeAutoRandom{},
					&tree.AttributeSRID{}, &tree.AttributeVisable{}, &tree.AttributeMongoDBPath{},
					&tree.AttributeMongoDBConvert{}, &tree.AttributeDefault{}, &tree.AttributeComment{},
					&tree.AttributeCheckConstraint{}, &tree.AttributeGeneratedAlways{}, &tree.AttributeOnUpdate{},
					&tree.AttributeReference{TableName: qualified("attribute_parent")}, keyPart,
				},
			},
			&tree.PrimaryKeyIndex{KeyParts: []*tree.KeyPart{keyPart}},
			&tree.Index{KeyParts: []*tree.KeyPart{keyPart}},
			&tree.UniqueIndex{KeyParts: []*tree.KeyPart{keyPart}},
			&tree.ForeignKey{
				KeyParts: []*tree.KeyPart{keyPart},
				Refer:    &tree.AttributeReference{TableName: qualified("foreign_parent")},
			},
			&tree.FullTextIndex{KeyParts: []*tree.KeyPart{keyPart}},
			&tree.CheckIndex{},
		}
		require.True(t, remapDbInTableDefs(defs, remapDbContext{
			databases:           map[string]string{"source_db": "target_db"},
			lowerCaseTableNames: 1,
		}))
		require.Equal(t, "target_db", column.DbNameOrigin())
		require.Equal(t, tree.Identifier("target_db"), defs[4].(*tree.ForeignKey).Refer.TableName.SchemaName)
	})
}

func TestApplyRemapDbByStatementKeepsPolicyBoundaries(t *testing.T) {
	ctx := context.Background()
	stmts, err := parsers.Parse(ctx, dialect.MYSQL,
		"select * from src.t; analyze table src.t(id)", 1)
	require.NoError(t, err)
	require.NoError(t, applyRemapDbByStatement(ctx, stmts, []map[string]string{
		{"src": "first_db"},
		{"src": "second_db"},
	}, 1))
	require.Equal(t, "select * from first_db.t", tree.String(stmts[0], dialect.MYSQL))
	require.Equal(t, "analyze table second_db.t(id)", tree.String(stmts[1], dialect.MYSQL))

	err = applyRemapDbByStatement(ctx, stmts, []map[string]string{{"src": "only_one"}}, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "remapdb policies")
}
