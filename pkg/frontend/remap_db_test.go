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
	applyRemapDb(stmts, remap)
	return tree.String(stmts[0], dialect.MYSQL)
}

func TestApplyRemapDb(t *testing.T) {
	remap := map[string]string{"dbxxx": "dbyyy"}

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
