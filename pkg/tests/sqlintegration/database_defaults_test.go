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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestDatabaseCharsetInheritanceContract(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "c03_database_defaults"
		const other = "c03_database_other"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema, "drop database if exists "+other)
		first, err := db.Conn(ctx)
		require.NoError(t, err)
		defer first.Close()
		second, err := db.Conn(ctx)
		require.NoError(t, err)
		defer second.Close()
		exec := func(t *testing.T, conn *sql.Conn, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		collation := func(t *testing.T, table, column string) string {
			t.Helper()
			var value string
			require.NoError(t, first.QueryRowContext(ctx,
				"select collation_name from information_schema.columns where table_schema=? and table_name=? and column_name=?", schema, table, column).Scan(&value))
			return value
		}
		count := func(t *testing.T, statement string) int {
			t.Helper()
			var value int
			require.NoError(t, first.QueryRowContext(ctx, statement).Scan(&value), statement)
			return value
		}
		exec(t, first, "create database "+schema+" collate utf8mb4_general_ci")
		exec(t, first, "create database "+other+" collate utf8mb4_general_ci")
		exec(t, first, "use "+schema)
		exec(t, second, "use "+other)

		t.Run("database default is durable target state", func(t *testing.T) {
			exec(t, first, "create table before_alter(v varchar(8))")
			exec(t, first, "insert into before_alter values ('a'),('B')")
			exec(t, second, "alter database "+schema+" character set utf8mb4 collate utf8mb4_bin")
			exec(t, second, "create table "+schema+".after_alter(v varchar(8))")
			exec(t, second, "insert into "+schema+".after_alter values ('a'),('B')")
			require.Equal(t, "utf8mb4_general_ci", collation(t, "before_alter", "v"))
			require.Equal(t, "utf8mb4_bin", collation(t, "after_alter", "v"))
			var min, max, charset, rule string
			require.NoError(t, first.QueryRowContext(ctx, "select min(v),max(v) from before_alter").Scan(&min, &max))
			require.Equal(t, "a", min)
			require.Equal(t, "B", max)
			require.NoError(t, first.QueryRowContext(ctx, "select min(v),max(v) from after_alter").Scan(&min, &max))
			require.Equal(t, "B", min)
			require.Equal(t, "a", max)
			require.NoError(t, first.QueryRowContext(ctx, "select @@character_set_database,@@collation_database").Scan(&charset, &rule))
			require.Equal(t, "utf8mb4", charset)
			require.Equal(t, "utf8mb4_bin", rule)
		})

		t.Run("prepared create observes current generation", func(t *testing.T) {
			exec(t, first, "prepare c03_prepared from 'create table prepared_target(v varchar(8))'")
			defer func() { _, err := first.ExecContext(ctx, "deallocate prepare c03_prepared"); require.NoError(t, err) }()
			exec(t, second, "alter database "+schema+" collate utf8mb4_unicode_ci")
			exec(t, first, "execute c03_prepared")
			require.Equal(t, "utf8mb4_unicode_ci", collation(t, "prepared_target", "v"))
			exec(t, first, "insert into prepared_target values ('ß'),('ss')")
			require.Equal(t, 2, count(t, "select count(*) from prepared_target where v='ss'"))
			exec(t, second, "alter database "+schema+" collate utf8mb4_bin")
		})

		t.Run("like and ctas preserve source domain", func(t *testing.T) {
			exec(t, first, "create table source_ci(v varchar(8)) collate utf8mb4_general_ci")
			exec(t, first, "create table like_ci like source_ci")
			exec(t, first, "create table ctas_ci as select v from source_ci")
			require.Equal(t, "utf8mb4_general_ci", collation(t, "like_ci", "v"))
			require.Equal(t, "utf8mb4_general_ci", collation(t, "ctas_ci", "v"))
		})

		t.Run("table default does not rewrite existing columns", func(t *testing.T) {
			exec(t, first, "create table table_default(v varchar(8)) collate utf8mb4_bin")
			exec(t, first, "alter table table_default default collate=utf8mb4_general_ci")
			exec(t, first, "alter table table_default add column added varchar(8)")
			require.Equal(t, "utf8mb4_bin", collation(t, "table_default", "v"))
			require.Equal(t, "utf8mb4_general_ci", collation(t, "table_default", "added"))
			exec(t, first, "alter table table_default modify v varchar(8)")
			require.Equal(t, "utf8mb4_general_ci", collation(t, "table_default", "v"))
			_, err := first.ExecContext(ctx, "alter table table_default add column rejected int, character set latin1")
			require.Error(t, err)
			require.Equal(t, 0, count(t, "select count(*) from information_schema.columns where table_schema='"+schema+"' and table_name='table_default' and column_name='rejected'"))
		})

		t.Run("temporary declarations use the session table default", func(t *testing.T) {
			exec(t, first, "create temporary table temporary_default(v varchar(8)) collate utf8mb4_unicode_ci")
			defer exec(t, first, "drop temporary table temporary_default")
			exec(t, first, "insert into temporary_default values ('ß'),('ss')")
			exec(t, first, "alter table temporary_default add column added varchar(8) default 'ß'")
			require.Equal(t, 2, count(t, "select count(*) from temporary_default where added='ss'"))
			exec(t, first, "alter table temporary_default default collate utf8mb4_bin")
			require.Equal(t, 2, count(t, "select count(*) from temporary_default where v='ss'"))
			exec(t, first, "alter table temporary_default add column future varchar(8) default 'ß'")
			require.Equal(t, 0, count(t, "select count(*) from temporary_default where future='ss'"))
			exec(t, first, "alter table temporary_default modify added varchar(8)")
			require.Equal(t, 0, count(t, "select count(*) from temporary_default where added='ss'"))
		})

		t.Run("default replacement owns final index definition", func(t *testing.T) {
			exec(t, first, "create table default_indexes(v varchar(8), index iv(v)) collate=utf8mb4_bin")
			exec(t, first, "insert into default_indexes values ('a'),('B')")
			exec(t, first, "alter table default_indexes default collate=utf8mb4_general_ci, drop index iv")
			require.Equal(t, 0, count(t, "select count(*) from information_schema.statistics where table_schema='"+schema+"' and table_name='default_indexes' and index_name='iv'"))
			exec(t, first, "alter table default_indexes add index iv(v), default collate=utf8mb4_bin")
			require.Equal(t, 1, count(t, "select count(*) from default_indexes force index(iv) where v='a'"))
		})

		t.Run("conversion rebuilds secondary indexes", func(t *testing.T) {
			exec(t, first, "create table index_conversion(id int primary key, v varchar(8), index iv(v)) collate utf8mb4_bin")
			exec(t, first, "insert into index_conversion values (1,'ß'),(2,'ss')")
			exec(t, first, "alter table index_conversion convert to character set utf8mb4 collate utf8mb4_unicode_ci")
			require.Equal(t, "utf8mb4_unicode_ci", collation(t, "index_conversion", "v"))
			require.Equal(t, 2, count(t, "select count(*) from index_conversion force index(iv) where v='ss'"))
			require.Equal(t, 2, count(t, "select count(*) from index_conversion ignore index(iv) where v='ss'"))
		})

		t.Run("all metadata consumers expose the persisted domain", func(t *testing.T) {
			var value string
			require.NoError(t, first.QueryRowContext(ctx, "select table_collation from information_schema.tables where table_schema=? and table_name='index_conversion'", schema).Scan(&value))
			require.Equal(t, "utf8mb4_unicode_ci", value)
			require.NoError(t, first.QueryRowContext(ctx, "show full columns from index_conversion where Field='v'").Scan(new(any), new(any), &value, new(any), new(any), new(any), new(any), new(any), new(any)))
			require.Equal(t, "utf8mb4_unicode_ci", value)
			rows, err := first.QueryContext(ctx, "show table status like 'index_conversion'")
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err)
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			require.True(t, rows.Next())
			require.NoError(t, rows.Scan(pointers...))
			found := false
			for i, name := range columns {
				if name == "Collation" {
					found = true
					require.Equal(t, "utf8mb4_unicode_ci", fmt.Sprintf("%s", values[i]))
				}
			}
			require.True(t, found)
			require.NoError(t, rows.Err())
		})

		t.Run("failed repertoire and unique conversion preserve source", func(t *testing.T) {
			exec(t, first, "create table repertoire_conversion(v varchar(8)) collate utf8mb4_bin")
			exec(t, first, "insert into repertoire_conversion values ('😀')")
			_, err := first.ExecContext(ctx, "alter table repertoire_conversion convert to character set utf8 collate utf8_unicode_ci")
			require.Error(t, err)
			require.Equal(t, "utf8mb4_bin", collation(t, "repertoire_conversion", "v"))
			require.Equal(t, 1, count(t, "select count(*) from repertoire_conversion"))
			exec(t, first, "create table unique_conversion(v varchar(8) unique) collate utf8mb4_bin")
			exec(t, first, "insert into unique_conversion values ('ß'),('ss')")
			_, err = first.ExecContext(ctx, "alter table unique_conversion convert to character set utf8mb4 collate utf8mb4_unicode_ci")
			require.Error(t, err)
			require.Equal(t, "utf8mb4_bin", collation(t, "unique_conversion", "v"))
			require.Equal(t, 2, count(t, "select count(*) from unique_conversion"))
		})

		t.Run("binary conversion and length failure", func(t *testing.T) {
			exec(t, first, "create table binary_conversion(v varchar(1)) collate utf8mb4_bin")
			exec(t, first, "insert into binary_conversion values ('😀')")
			_, err := first.ExecContext(ctx, "alter table binary_conversion convert to character set binary")
			require.Error(t, err)
			require.Equal(t, "utf8mb4_bin", collation(t, "binary_conversion", "v"))
			exec(t, first, "alter table binary_conversion modify v varchar(8)")
			exec(t, first, "alter table binary_conversion convert to character set binary")
			var value string
			require.NoError(t, first.QueryRowContext(ctx, "select hex(v) from binary_conversion").Scan(&value))
			require.Equal(t, "F09F9880", value)
		})

		t.Run("binary COPY preserves an explicit cast inside a generated expression", func(t *testing.T) {
			exec(t, first, "create table explicit_binary(v varchar(4),g binary(2) generated always as (cast(v as binary(2))) stored) collate=utf8mb4_bin")
			exec(t, first, "insert into explicit_binary(v) values ('🧪')")
			exec(t, first, "alter table explicit_binary convert to character set binary")
			require.Equal(t, 1, count(t, "select count(*) from explicit_binary where hex(v)='F09FA7AA' and hex(g)='F09F'"))
		})

		t.Run("binary conversion validates generated target values", func(t *testing.T) {
			exec(t, first, "create table binary_generated(v varchar(4), g varchar(3) generated always as (concat(v,'x')) stored) collate=utf8mb4_bin")
			exec(t, first, "insert into binary_generated(v) values ('🧪')")
			_, err := first.ExecContext(ctx, "alter table binary_generated convert to character set binary")
			require.Error(t, err)
			require.Equal(t, 1, count(t, "select count(*) from binary_generated where g='🧪x'"))
			require.Equal(t, "utf8mb4_bin", collation(t, "binary_generated", "g"))
			exec(t, first, "alter table binary_generated modify g varchar(8) generated always as (concat(v,'x')) stored")
			exec(t, first, "alter table binary_generated convert to character set binary")
			require.Equal(t, 1, count(t, "select count(*) from binary_generated where hex(g)='F09FA7AA78'"))
			require.Equal(t, "binary", collation(t, "binary_generated", "g"))
		})
	})
}
