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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestUnicodeCollationConsumerContract(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		schema := "unicode_collation_29601"
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)

		exec := func(statement string) {
			t.Helper()
			execSQLRequire(t, ctx, db, statement)
		}
		queryStrings := func(statement string) []string {
			t.Helper()
			rows, queryErr := db.QueryContext(ctx, statement)
			require.NoError(t, queryErr, statement)
			defer rows.Close()
			var values []string
			for rows.Next() {
				var value string
				require.NoError(t, rows.Scan(&value))
				values = append(values, value)
			}
			require.NoError(t, rows.Err())
			return values
		}
		queryCount := func(statement string) int {
			t.Helper()
			var value int
			require.NoError(t, db.QueryRowContext(ctx, statement).Scan(&value), statement)
			return value
		}
		queryColumnCollation := func(table, column string) (string, string) {
			t.Helper()
			var charset, collation sql.NullString
			require.NoError(t, db.QueryRowContext(ctx,
				"select character_set_name, collation_name from information_schema.columns "+
					"where table_schema=? and table_name=? and column_name=?",
				schema, table, column).Scan(&charset, &collation))
			require.True(t, charset.Valid, "character_set_name must be exposed for %s.%s", table, column)
			require.True(t, collation.Valid, "collation_name must be exposed for %s.%s", table, column)
			return charset.String, collation.String
		}
		queryShowColumnField := func(table, column, field string) sql.NullString {
			t.Helper()
			rows, err := db.QueryContext(ctx, "show full columns from `"+table+"`")
			require.NoError(t, err)
			defer rows.Close()

			columnNames, err := rows.Columns()
			require.NoError(t, err)
			fieldIndex, valueIndex := -1, -1
			for i, name := range columnNames {
				switch name {
				case "Field":
					fieldIndex = i
				case field:
					valueIndex = i
				}
			}
			require.NotEqual(t, -1, fieldIndex)
			require.NotEqual(t, -1, valueIndex)

			asString := func(value any) string {
				switch value := value.(type) {
				case string:
					return value
				case []byte:
					return string(value)
				default:
					return ""
				}
			}
			for rows.Next() {
				values := make([]any, len(columnNames))
				dest := make([]any, len(values))
				for i := range values {
					dest[i] = &values[i]
				}
				require.NoError(t, rows.Scan(dest...))
				if asString(values[fieldIndex]) == column {
					require.NoError(t, rows.Err())
					value := sql.NullString{String: asString(values[valueIndex])}
					if values[valueIndex] != nil {
						value.Valid = true
					}
					return value
				}
			}
			require.NoError(t, rows.Err())
			t.Fatalf("column %s.%s was not returned by SHOW FULL COLUMNS", table, column)
			return sql.NullString{}
		}
		queryShowColumnCollation := func(table, column string) sql.NullString {
			return queryShowColumnField(table, column, "Collation")
		}

		exec("drop database if exists " + schema)
		exec("create database " + schema)
		exec("use " + schema)
		// Native Unicode primary-key bytes are not yet canonicalized by the
		// storage dedup/lock paths. Reject both single- and composite-key
		// definitions instead of allowing collation-equivalent values to commit.
		_, err = db.ExecContext(ctx, "create table pk_rejected (s varchar(64) collate utf8mb4_unicode_ci primary key)")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "create table composite_pk_rejected (s varchar(64) collate utf8mb4_unicode_ci, n int, primary key (s, n))")
		require.Error(t, err)
		exec("create table unique_native_allowed (id int primary key, s varchar(64) collate utf8mb4_unicode_ci, v int, unique key uk_s (s))")
		exec("insert into unique_native_allowed values (1, 'Alpha', 10)")
		_, err = db.ExecContext(ctx, "insert into unique_native_allowed values (2, 'alpha', 20)")
		require.Error(t, err)
		require.Equal(t, 1, queryCount("select count(*) from unique_native_allowed"))
		// The duplicate probe and the hidden unique-index lock must use the same
		// UCA key for ODKU and INSERT IGNORE as the plain insert path.
		exec("insert into unique_native_allowed values (2, 'Alpha ', 30) on duplicate key update v=values(v)")
		require.Equal(t, 30, queryCount("select v from unique_native_allowed where id=1"))
		exec("insert ignore into unique_native_allowed values (3, 'ALPHA', 40)")
		require.Equal(t, 1, queryCount("select count(*) from unique_native_allowed"))
		// UPDATE and DELETE must maintain the same canonical hidden key as
		// INSERT.  After moving the existing row away from alpha, that key can
		// be inserted again; deleting it must then release the beta key.
		exec("update unique_native_allowed set s='Beta' where id=1")
		exec("insert into unique_native_allowed values (4, 'alpha', 50)")
		exec("delete from unique_native_allowed where id=4")
		_, err = db.ExecContext(ctx, "insert into unique_native_allowed values (5, 'BETA ', 60)")
		require.Error(t, err)
		exec("replace into unique_native_allowed values (6, 'beta', 70)")
		require.Equal(t, 1, queryCount("select count(*) from unique_native_allowed where id=6"))
		require.Equal(t, 0, queryCount("select count(*) from unique_native_allowed where id=1"))
		// A parent UPDATE with ON UPDATE CASCADE must lock the same canonical
		// V1 key as the child-side FK probe, rather than the raw Unicode value.
		exec("create table update_fk_parent (id int primary key, s varchar(64) collate utf8mb4_unicode_ci, unique key uk_s (s))")
		exec("create table update_fk_child (id int primary key, parent_s varchar(64) collate utf8mb4_unicode_ci, unique key uk_parent_s (parent_s), foreign key (parent_s) references update_fk_parent(s) on update cascade)")
		exec("insert into update_fk_parent values (1, 'Alpha')")
		exec("insert into update_fk_child values (1, 'alpha')")
		exec("update update_fk_parent set s='Beta' where id=1")
		require.Equal(t, 1, queryCount("select count(*) from update_fk_child where parent_s='beta'"))
		// REPLACE must preserve the original parent value used by a foreign-key
		// action.  The hidden V1 key is an opaque binary key and must not be
		// reused as the old user-visible collation value.
		exec("create table replace_fk_parent (id int primary key, s varchar(64) collate utf8mb4_unicode_ci, unique key uk_s (s))")
		exec("create table replace_fk_child (id int primary key, parent_s varchar(64) collate utf8mb4_unicode_ci, foreign key (parent_s) references replace_fk_parent(s))")
		exec("insert into replace_fk_parent values (1, 'Alpha')")
		exec("insert into replace_fk_child values (1, 'alpha')")
		_, err = db.ExecContext(ctx, "replace into replace_fk_parent values (2, 'ALPHA')")
		require.Error(t, err)
		require.Equal(t, 1, queryCount("select count(*) from replace_fk_parent where id=1"))
		require.Equal(t, 1, queryCount("select count(*) from replace_fk_child where parent_s='ALPHA'"))
		exec("create table unique_utf8_native_allowed (id int primary key, s varchar(64) character set utf8 collate utf8_unicode_ci, unique key uk_s (s))")
		exec("insert into unique_utf8_native_allowed values (1, 'Alpha')")
		_, err = db.ExecContext(ctx, "insert into unique_utf8_native_allowed values (2, 'alpha')")
		require.Error(t, err)
		// Existing-table index mutation remains fail-closed while the migration
		// and backfill contract for versioned keys is not implemented.
		_, err = db.ExecContext(ctx, "create index idx_v on unique_native_allowed (v)")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "create unique index uk_s2 on unique_native_allowed (s)")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "alter table unique_native_allowed add index idx_v2 (v)")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "alter table unique_native_allowed drop index uk_s")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "drop index uk_s on unique_native_allowed")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "alter table unique_native_allowed drop column s")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "alter table unique_native_allowed modify s varchar(64)")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "alter table unique_native_allowed change s s varchar(64)")
		require.Error(t, err)
		require.Equal(t, 1, queryCount("select count(*) from unique_native_allowed"))

		exec("create table unique_native_composite (id int primary key, s varchar(64) collate utf8mb4_unicode_ci, n int, unique key uk_sn (s,n))")
		exec("insert into unique_native_composite values (1, 'Alpha', 1), (2, 'alpha', 2), (3, null, 1), (4, null, 1)")
		_, err = db.ExecContext(ctx, "insert into unique_native_composite values (5, 'ALPHA ', 1)")
		require.Error(t, err)
		require.Equal(t, 4, queryCount("select count(*) from unique_native_composite"))
		exec("replace into unique_native_composite values (5, 'ALPHA ', 1)")
		require.Equal(t, 1, queryCount("select count(*) from unique_native_composite where id=5"))
		require.Equal(t, 0, queryCount("select count(*) from unique_native_composite where id=1"))
		exec("create table unique_index_rejected (id int primary key, s varchar(64) collate utf8mb4_unicode_ci)")
		_, err = db.ExecContext(ctx, "create unique index uk_s on unique_index_rejected (s)")
		require.Error(t, err)
		exec("create table unique_alter_rejected (id int primary key, s varchar(64) collate utf8mb4_unicode_ci)")
		_, err = db.ExecContext(ctx, "alter table unique_alter_rejected add unique index uk_s (s)")
		require.Error(t, err)
		exec("create table unique_change_rejected (id int primary key, s varchar(64), unique key uk_s (s))")
		_, err = db.ExecContext(ctx, "alter table unique_change_rejected modify s varchar(64) collate utf8mb4_unicode_ci")
		require.Error(t, err)
		exec("create table unique_legacy_allowed (id int primary key, s varchar(64), unique key uk_s (s))")

		exec("create table words (id int primary key, s varchar(64) collate utf8mb4_unicode_ci)")
		exec("insert into words values (1,'Z'),(2,'a'),(3,'A'),(4,'b')")
		charset, collation := queryColumnCollation("words", "s")
		require.Equal(t, "utf8mb4", charset)
		require.Equal(t, "utf8mb4_unicode_ci", collation)
		showCollation := queryShowColumnCollation("words", "s")
		require.True(t, showCollation.Valid)
		require.Equal(t, "utf8mb4_unicode_ci", showCollation.String)

		// Keep the complete SHOW FULL COLUMNS metadata contract aligned with
		// information_schema.COLUMNS. String columns expose their collation,
		// while numeric/ENUM columns remain NULL; ENUM literal case must survive
		// the metadata formatting path unchanged.
		exec("create table show_full_columns_meta_probe(id int primary key,name varchar(100),amount decimal(10,2),choice enum('MiXeD','ABC'))")
		_, metaCollation := queryColumnCollation("show_full_columns_meta_probe", "name")
		showCollation = queryShowColumnCollation("show_full_columns_meta_probe", "name")
		require.True(t, showCollation.Valid)
		require.Equal(t, metaCollation, showCollation.String)
		require.False(t, queryShowColumnCollation("show_full_columns_meta_probe", "id").Valid)
		require.False(t, queryShowColumnCollation("show_full_columns_meta_probe", "amount").Valid)
		require.False(t, queryShowColumnCollation("show_full_columns_meta_probe", "choice").Valid)
		require.Equal(t, "enum('MiXeD','ABC')", queryShowColumnField("show_full_columns_meta_probe", "choice", "Type").String)
		require.Equal(t, 2, queryCount("select count(*) from words where lower(s)='a'"))
		// Conditional and ordered string expressions must carry the native
		// Unicode revision through their rebuilt result type. Ordinary NULLs are
		// ignorable, while weak literals must not replace the native domain.
		require.Equal(t, 2, queryCount("select count(*) from words where coalesce(s,'x')='a'"))
		require.Equal(t, 0, queryCount("select count(*) from words where coalesce('x',s)='a'"))
		require.Equal(t, 2, queryCount("select count(*) from words where coalesce(s,null)='a'"))
		require.Equal(t, 3, queryCount("select count(*) from words where case when id=1 then 'a' else s end='a'"))
		require.Equal(t, 2, queryCount("select count(*) from words where case when id=1 then null else s end='a'"))
		require.Equal(t, 3, queryCount("select count(*) from words where if(id=1,s,'a')='a'"))
		require.Equal(t, 2, queryCount("select count(*) from words where if(id=1,null,s)='a'"))
		// Typed NULL retains its static collation source. A native COLLATE keeps
		// these mixed-width expressions valid, but a revision-zero SQL CAST and
		// the native column have equal implicit priority and must be rejected.
		for _, expression := range []string{
			"coalesce(s,%s)",
			"case when id=1 then %s else s end",
			"if(id=1,%s,s)",
		} {
			query := "select count(*) from words where " + expression + "='a'"
			native := fmt.Sprintf(query, "cast(null as varchar(1)) collate utf8mb4_unicode_ci")
			require.Equal(t, 2, queryCount(native), native)

			legacy := fmt.Sprintf(query, "cast(null as varchar(1))")
			var count int
			queryErr := db.QueryRowContext(ctx, legacy).Scan(&count)
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, queryErr, &sqlErr, legacy)
			require.Equal(t, uint16(1267), sqlErr.Number, legacy)
		}
		require.Equal(t, 2, queryCount("select count(*) from words where least(s,'z')='a'"))
		require.Equal(t, 2, queryCount("select count(*) from words where greatest(s,'0')='a'"))
		require.Equal(t, 2, queryCount("select count(*) from words where repeat(s,1)='a'"))
		require.Equal(t, []string{"2", "3"}, queryStrings("select id from words where s='a' order by id"))
		require.Equal(t, []string{"2", "3"}, queryStrings("select id from words where s in ('a','zz') order by id"))
		require.Equal(t, []string{"1", "4"}, queryStrings("select id from words where s not in ('a','zz') order by id"))
		require.Equal(t, []string{"3", "1", "1", "2"}, queryStrings("select dense_rank() over (order by s) from words order by id"))
		require.Equal(t, []string{"1", "2", "2", "1"}, queryStrings("select count(*) over (partition by s) from words order by id"))
		require.Equal(t, 3, queryCount("select count(*) from (select s from words group by s) grouped"))
		require.Equal(t, []string{"a", "A", "b", "Z"}, queryStrings("select s from words order by s,id"))
		require.Equal(t, []string{"a", "A"}, queryStrings("select s from words order by s,id limit 2"))

		// Legacy CHAR keeps its historical byte-order tie behavior. TopN must
		// agree with the full ORDER BY prefix even though scalar CHAR equality
		// ignores trailing spaces.
		exec("create table chars (id int primary key, s char(4) collate utf8mb4_bin)")
		exec("insert into chars values (1,'a '),(2,'a'),(3,'b')")
		require.Equal(t, []string{"2", "1", "3"}, queryStrings("select id from chars order by s,id"))
		require.Equal(t, []string{"2"}, queryStrings("select id from chars order by s,id limit 1"))

		var original, serialized string
		require.NoError(t, db.QueryRowContext(ctx,
			"select hex(s), hex(serial_extract(serial(s),0 as varchar(64))) from words where id=4",
		).Scan(&original, &serialized))
		require.Equal(t, "62", original)
		require.Equal(t, original, serialized)

		exec("create table show_full_columns_controls (b binary(4), v varbinary(4), bl blob, n int, txt varchar(20) collate utf8mb4_bin)")
		require.False(t, queryShowColumnCollation("show_full_columns_controls", "b").Valid)
		require.False(t, queryShowColumnCollation("show_full_columns_controls", "v").Valid)
		require.False(t, queryShowColumnCollation("show_full_columns_controls", "bl").Valid)
		require.False(t, queryShowColumnCollation("show_full_columns_controls", "n").Valid)
		showCollation = queryShowColumnCollation("show_full_columns_controls", "txt")
		require.True(t, showCollation.Valid)
		require.Equal(t, "utf8mb4_bin", showCollation.String)

		exec("create table utf8mb3_write (s varchar(64) character set utf8mb3 collate utf8_unicode_ci)")
		charset, collation = queryColumnCollation("utf8mb3_write", "s")
		require.Equal(t, "utf8", charset)
		require.Equal(t, "utf8_unicode_ci", collation)
		showCollation = queryShowColumnCollation("utf8mb3_write", "s")
		require.True(t, showCollation.Valid)
		require.Equal(t, "utf8_unicode_ci", showCollation.String)
		_, err = db.ExecContext(ctx, "insert into utf8mb3_write values ('😀')")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "insert into utf8mb3_write values (unhex('30fbc130ffff20'))")
		require.Error(t, err)
	})
}
