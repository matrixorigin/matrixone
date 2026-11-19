// Copyright 2026 Matrix Origin
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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

// TestIssue31161ColumnDefaultMetadata is the SQL oracle for issue #31161
// (carrying #31159): public column-default metadata must render the typed
// default value (DECIMAL declared scale, MySQL-compatible BIT literal form)
// and mark expression defaults DEFAULT_GENERATED, on both
// information_schema.COLUMNS and SHOW COLUMNS, while inserted default values
// stay byte-identical to the pre-change behavior. Literal defaults, explicit
// NULL defaults and default-less columns keep empty EXTRA. The CREATE,
// ALTER ... ADD COLUMN, CREATE TABLE ... LIKE clone and reconnect paths all
// publish the same metadata, and a rejected invalid insert leaves the
// existing rows untouched. Design:
// docs/design/20261105-column-default-metadata-publication.md (revision 6).
func TestIssue31161ColumnDefaultMetadata(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port)

		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		exec := func(statement string) {
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "exec failed: %s", statement)
		}
		execError := func(statement, wantSubstring string) {
			_, err := conn.ExecContext(ctx, statement)
			require.Errorf(t, err, "exec must fail: %s", statement)
			if wantSubstring != "" {
				require.Containsf(t, err.Error(), wantSubstring, "exec error mismatch: %s", statement)
			}
		}
		queryValue := func(statement string) sql.NullString {
			var v sql.NullString
			require.NoErrorf(t, conn.QueryRowContext(ctx, statement).Scan(&v), "query failed: %s", statement)
			return v
		}
		requireValue := func(statement, want string) {
			v := queryValue(statement)
			require.Truef(t, v.Valid, "query returned NULL: %s", statement)
			require.Equalf(t, want, v.String, "query mismatch: %s", statement)
		}

		// colMeta is the (Default, Extra) pair one metadata surface publishes
		// for one column; defaultValid=false pins a SQL NULL Default.
		type colMeta struct {
			def      string
			defValid bool
			extra    string
		}
		// informationSchemaMeta reads information_schema.COLUMNS for one table.
		informationSchemaMeta := func(schema, table string) map[string]colMeta {
			rows, err := conn.QueryContext(ctx,
				"select column_name, column_default, extra from information_schema.columns "+
					"where table_schema = ? and table_name = ? order by ordinal_position", schema, table)
			require.NoErrorf(t, err, "information_schema read failed for %s.%s", schema, table)
			defer rows.Close()
			got := map[string]colMeta{}
			for rows.Next() {
				var name string
				var def, extra sql.NullString
				require.NoError(t, rows.Scan(&name, &def, &extra))
				got[name] = colMeta{def: def.String, defValid: def.Valid, extra: extra.String}
			}
			require.NoError(t, rows.Err())
			return got
		}
		// showColumnsMeta reads SHOW COLUMNS for one table.
		showColumnsMeta := func(schema, table string) map[string]colMeta {
			rows, err := conn.QueryContext(ctx, fmt.Sprintf("show columns from %s.%s", schema, table))
			require.NoErrorf(t, err, "show columns failed for %s.%s", schema, table)
			defer rows.Close()
			got := map[string]colMeta{}
			for rows.Next() {
				var field, typ, null, key, extra, comment sql.NullString
				var def sql.NullString
				require.NoError(t, rows.Scan(&field, &typ, &null, &key, &def, &extra, &comment))
				got[field.String] = colMeta{def: def.String, defValid: def.Valid, extra: extra.String}
			}
			require.NoError(t, rows.Err())
			return got
		}
		// requireMeta pins one table's metadata on both surfaces at once, so
		// information_schema.COLUMNS and SHOW COLUMNS cannot drift (I3a).
		requireMeta := func(schema, table string, want map[string]colMeta) {
			for surface, got := range map[string]map[string]colMeta{
				"information_schema": informationSchemaMeta(schema, table),
				"show_columns":       showColumnsMeta(schema, table),
			} {
				require.Equalf(t, want, got, "%s metadata mismatch for %s.%s", surface, schema, table)
			}
		}

		const dbName = "issue_31161_defaults"
		exec("drop database if exists " + dbName)
		exec("create database " + dbName)

		// I1: the issue's typed-rendering repro plus literal/NULL controls.
		exec("create table " + dbName + ".a (" +
			"id int primary key, " +
			"d decimal(10,4) default 1.2, " +
			"b bit(8) default b'00110011', " +
			"v varchar(10) default 'x', " +
			"n int default null)")
		exec("insert into " + dbName + ".a(id) values (1)")

		// I2: the #31159 expression-default repro plus expression variants.
		exec("create table " + dbName + ".b (" +
			"id int primary key, " +
			"ts timestamp(6) null default current_timestamp(6), " +
			"ts0 timestamp null default current_timestamp(), " +
			"u char(36) null default (uuid()), " +
			"j json null default (json_array()))")
		exec("insert into " + dbName + ".b(id) values (1)")

		// I3: folded-expression counterexamples next to rounding literals.
		exec("create table " + dbName + ".c (" +
			"id int primary key, " +
			"i2 int default (1+2), " +
			"d3 decimal(10,0) default 1.6)")
		exec("insert into " + dbName + ".c(id) values (1)")

		// I1 attributed/decimal256 literal variants, the rounding
		// counterexample, scientific-notation, boolean and signed literals.
		exec("create table " + dbName + ".d (" +
			"id int primary key, " +
			"du decimal(10,4) unsigned default 1.2, " +
			"dz decimal(10,4) zerofill default 1.2, " +
			"d40 decimal(40,4) default 1.2, " +
			"dr decimal(10,0) unsigned default '1.4999999999999999999', " +
			"ds decimal(10,4) unsigned default 1.2e0, " +
			"dbt decimal(10,4) unsigned default true, " +
			"dbf decimal(10,4) unsigned default false, " +
			"dp decimal(10,4) unsigned default +1.2)")
		exec("insert into " + dbName + ".d(id) values (1)")

		// I3a: explicit-cast and folded-function DECIMAL defaults keep their
		// origin text, publish DEFAULT_GENERATED and store the executed value.
		exec("create table " + dbName + ".e (" +
			"id int primary key, " +
			"dc decimal(10,4) unsigned default cast(1.6 as decimal(10,0)), " +
			"dc2 decimal(10,4) default cast(1.6 as decimal(10,0)), " +
			"da decimal(10,4) default abs(1.2), " +
			"dau decimal(10,4) unsigned default abs(1.2))")
		exec("insert into " + dbName + ".e(id) values (1)")

		// I1: bit/hex literal defaults on DECIMAL columns render the typed
		// value (the IsBin payload is a binary coefficient, never text).
		exec("create table " + dbName + ".f (" +
			"id int primary key, " +
			"db1 decimal(10,4) unsigned default b'00110011', " +
			"db2 decimal(10,0) unsigned default b'00110011', " +
			"dh1 decimal(10,4) unsigned default 0x33, " +
			"dh2 decimal(10,4) unsigned default x'33', " +
			"db3 decimal(10,4) default b'00110011')")
		exec("insert into " + dbName + ".f(id) values (1)")

		// I4: column-reference defaults mark DEFAULT_GENERATED and store the
		// referenced value; the referenced literal column stays a literal.
		exec("create table " + dbName + ".g (" +
			"id int primary key, " +
			"a int default 7, " +
			"b int default a, " +
			"cc int default (a), " +
			"d decimal(10,4) unsigned default a)")
		exec("insert into " + dbName + ".g(id) values (1)")

		// I5: variable-reference defaults mark DEFAULT_GENERATED and store
		// the session value; a quoted variable spelling stays a literal.
		exec("set time_zone = '+08:00'")
		exec("set @probe_d = 1.2")
		exec("create table " + dbName + ".h (" +
			"id int primary key, " +
			"v varchar(64) default @@time_zone, " +
			"w varchar(64) default (@@time_zone), " +
			"d decimal(10,4) unsigned default @probe_d, " +
			"v2 varchar(64) default '@@time_zone')")
		exec("insert into " + dbName + ".h(id) values (1)")

		// I1 uint64-boundary decimal carriers: an integer literal above
		// uint64 binds as a decimal128 value the attributed admission
		// preserves raw; the typed renderer widens and rescales it exactly
		// like the executor's assignment cast. u64max (fits uint64), the
		// quoted spelling and the plain (folded) form are the controls.
		exec("create table " + dbName + ".i (" +
			"id int primary key, " +
			"u64max decimal(40,4) unsigned default 18446744073709551615, " +
			"u64p1 decimal(40,4) unsigned default 18446744073709551616, " +
			"u64p1q decimal(40,4) unsigned default '18446744073709551616', " +
			"p64p1 decimal(40,4) default 18446744073709551616, " +
			"bigu decimal(40,4) unsigned default 12345678901234567890.1, " +
			"bign decimal(40,4) default -12345678901234567890.1)")
		exec("insert into " + dbName + ".i(id) values (1)")

		// DDL-time rejection of a negative literal default on an attributed
		// DECIMAL stays intact (I1 unhappy path at CREATE).
		execError("create table "+dbName+".bad_neg (d decimal(10,4) unsigned default -1.2)",
			"Invalid default value")
		// An unparenthesized uuid() default on a non-UUID column stays
		// rejected at CREATE (existing boundary); the parenthesized
		// expression-default spelling is the supported form, proven on b.
		execError("create table "+dbName+".bad_uuid (u varchar(36) default uuid())",
			"invalid default value")

		// I1: metadata and inserted values together.
		requireMeta(dbName, "a", map[string]colMeta{
			"id": {defValid: false, extra: ""},
			"d":  {def: "1.2000", defValid: true, extra: ""},
			"b":  {def: "b'110011'", defValid: true, extra: ""},
			"v":  {def: "x", defValid: true, extra: ""},
			"n":  {defValid: false, extra: ""},
		})
		requireValue("select d from "+dbName+".a where id = 1", "1.2000")
		requireValue("select b+0 from "+dbName+".a where id = 1", "51")
		requireValue("select v from "+dbName+".a where id = 1", "x")
		require.False(t, queryValue("select n from "+dbName+".a where id = 1").Valid)

		// I2: expression defaults publish DEFAULT_GENERATED on both surfaces
		// with the origin text preserved; the timestamp default populates.
		requireMeta(dbName, "b", map[string]colMeta{
			"id":  {defValid: false, extra: ""},
			"ts":  {def: "current_timestamp(6)", defValid: true, extra: "DEFAULT_GENERATED"},
			"ts0": {def: "current_timestamp()", defValid: true, extra: "DEFAULT_GENERATED"},
			"u":   {def: "(uuid())", defValid: true, extra: "DEFAULT_GENERATED"},
			"j":   {def: "(json_array())", defValid: true, extra: "DEFAULT_GENERATED"},
		})
		requireValue("select ts is not null from "+dbName+".b where id = 1", "1")
		requireValue("select u is not null from "+dbName+".b where id = 1", "1")
		// The parenthesized function defaults execute: uuid() stores its
		// 36-character text and json_array() stores the empty array.
		requireValue("select length(u) from "+dbName+".b where id = 1", "36")
		requireValue("select j from "+dbName+".b where id = 1", "[]")

		// I3: the folded expression keeps its origin text and mark; the
		// rounded literal renders the executed value.
		requireMeta(dbName, "c", map[string]colMeta{
			"id": {defValid: false, extra: ""},
			"i2": {def: "(1 + 2)", defValid: true, extra: "DEFAULT_GENERATED"},
			"d3": {def: "2", defValid: true, extra: ""},
		})
		requireValue("select i2 from "+dbName+".c where id = 1", "3")
		requireValue("select d3 from "+dbName+".c where id = 1", "2")

		// I1 attributed variants: typed rendering with inserted-value
		// equality, empty EXTRA everywhere on this table.
		requireMeta(dbName, "d", map[string]colMeta{
			"id":  {defValid: false, extra: ""},
			"du":  {def: "1.2000", defValid: true, extra: ""},
			"dz":  {def: "1.2000", defValid: true, extra: ""},
			"d40": {def: "1.2000", defValid: true, extra: ""},
			"dr":  {def: "1", defValid: true, extra: ""},
			"ds":  {def: "1.2000", defValid: true, extra: ""},
			"dbt": {def: "1.0000", defValid: true, extra: ""},
			"dbf": {def: "0.0000", defValid: true, extra: ""},
			"dp":  {def: "1.2000", defValid: true, extra: ""},
		})
		requireValue("select du from "+dbName+".d where id = 1", "1.2000")
		requireValue("select d40 from "+dbName+".d where id = 1", "1.2000")
		requireValue("select dr from "+dbName+".d where id = 1", "1")
		requireValue("select ds from "+dbName+".d where id = 1", "1.2000")
		requireValue("select dbt from "+dbName+".d where id = 1", "1.0000")
		requireValue("select dbf from "+dbName+".d where id = 1", "0.0000")
		requireValue("select dp from "+dbName+".d where id = 1", "1.2000")
		// Zerofill pads the display width; assert value equality instead of
		// the padded text form.
		requireValue("select dz = 1.2 from "+dbName+".d where id = 1", "1")

		// I3a: cast/function origins are preserved verbatim, marked, and the
		// executed values prove the metadata is publication-only.
		requireMeta(dbName, "e", map[string]colMeta{
			"id":  {defValid: false, extra: ""},
			"dc":  {def: "cast(1.6 as decimal(10))", defValid: true, extra: "DEFAULT_GENERATED"},
			"dc2": {def: "cast(1.6 as decimal(10))", defValid: true, extra: "DEFAULT_GENERATED"},
			"da":  {def: "abs(1.2)", defValid: true, extra: "DEFAULT_GENERATED"},
			"dau": {def: "abs(1.2)", defValid: true, extra: "DEFAULT_GENERATED"},
		})
		requireValue("select dc from "+dbName+".e where id = 1", "2.0000")
		requireValue("select dc2 from "+dbName+".e where id = 1", "2.0000")
		requireValue("select da from "+dbName+".e where id = 1", "1.2000")
		requireValue("select dau from "+dbName+".e where id = 1", "1.2000")

		// I1: the binary-coefficient decode pins 0.0051/51 next to the
		// inserted values; a text parse would have published 3.0000.
		requireMeta(dbName, "f", map[string]colMeta{
			"id":  {defValid: false, extra: ""},
			"db1": {def: "0.0051", defValid: true, extra: ""},
			"db2": {def: "51", defValid: true, extra: ""},
			"dh1": {def: "0.0051", defValid: true, extra: ""},
			"dh2": {def: "0.0051", defValid: true, extra: ""},
			"db3": {def: "0.0051", defValid: true, extra: ""},
		})
		requireValue("select db1 from "+dbName+".f where id = 1", "0.0051")
		requireValue("select db2 from "+dbName+".f where id = 1", "51")
		requireValue("select dh1 from "+dbName+".f where id = 1", "0.0051")
		requireValue("select dh2 from "+dbName+".f where id = 1", "0.0051")
		requireValue("select db3 from "+dbName+".f where id = 1", "0.0051")

		// I4: reference columns keep their reference origin and mark; the
		// referenced literal column keeps its literal metadata.
		requireMeta(dbName, "g", map[string]colMeta{
			"id": {defValid: false, extra: ""},
			"a":  {def: "7", defValid: true, extra: ""},
			"b":  {def: "a", defValid: true, extra: "DEFAULT_GENERATED"},
			"cc": {def: "(a)", defValid: true, extra: "DEFAULT_GENERATED"},
			"d":  {def: "a", defValid: true, extra: "DEFAULT_GENERATED"},
		})
		requireValue("select a from "+dbName+".g where id = 1", "7")
		requireValue("select b from "+dbName+".g where id = 1", "7")
		requireValue("select cc from "+dbName+".g where id = 1", "7")
		requireValue("select d from "+dbName+".g where id = 1", "7.0000")

		// I5: variable origins and marks; the stored values equal the session
		// readings; the quoted spelling stays an unmarked literal.
		requireMeta(dbName, "h", map[string]colMeta{
			"id": {defValid: false, extra: ""},
			"v":  {def: "@@time_zone", defValid: true, extra: "DEFAULT_GENERATED"},
			"w":  {def: "(@@time_zone)", defValid: true, extra: "DEFAULT_GENERATED"},
			"d":  {def: "@probe_d", defValid: true, extra: "DEFAULT_GENERATED"},
			"v2": {def: "@@time_zone", defValid: true, extra: ""},
		})
		requireValue("select v from "+dbName+".h where id = 1", "+08:00")
		requireValue("select w from "+dbName+".h where id = 1", "+08:00")
		requireValue("select d from "+dbName+".h where id = 1", "1.2000")
		requireValue("select v2 from "+dbName+".h where id = 1", "@@time_zone")

		// I1 uint64-boundary carriers: the widened rendering matches the
		// inserted values on both surfaces with empty EXTRA; the controls
		// (uint64-fitting, quoted, plain-folded) publish the same text.
		requireMeta(dbName, "i", map[string]colMeta{
			"id":     {defValid: false, extra: ""},
			"u64max": {def: "18446744073709551615.0000", defValid: true, extra: ""},
			"u64p1":  {def: "18446744073709551616.0000", defValid: true, extra: ""},
			"u64p1q": {def: "18446744073709551616.0000", defValid: true, extra: ""},
			"p64p1":  {def: "18446744073709551616.0000", defValid: true, extra: ""},
			"bigu":   {def: "12345678901234567890.1000", defValid: true, extra: ""},
			"bign":   {def: "-12345678901234567890.1000", defValid: true, extra: ""},
		})
		requireValue("select u64max from "+dbName+".i where id = 1", "18446744073709551615.0000")
		requireValue("select u64p1 from "+dbName+".i where id = 1", "18446744073709551616.0000")
		requireValue("select u64p1q from "+dbName+".i where id = 1", "18446744073709551616.0000")
		requireValue("select p64p1 from "+dbName+".i where id = 1", "18446744073709551616.0000")
		requireValue("select bigu from "+dbName+".i where id = 1", "12345678901234567890.1000")
		requireValue("select bign from "+dbName+".i where id = 1", "-12345678901234567890.1000")

		// ALTER ... ADD COLUMN publishes the same metadata (I1/I2).
		exec("alter table " + dbName + ".a add column ad decimal(10,4) default 1.2")
		exec("alter table " + dbName + ".b add column ats timestamp(6) null default current_timestamp(6)")
		aMeta := informationSchemaMeta(dbName, "a")
		require.Equal(t, colMeta{def: "1.2000", defValid: true, extra: ""}, aMeta["ad"])
		bMeta := showColumnsMeta(dbName, "b")
		require.Equal(t, colMeta{def: "current_timestamp(6)", defValid: true, extra: "DEFAULT_GENERATED"}, bMeta["ats"])
		// The pre-existing rows gain the ALTER literal default value.
		requireValue("select ad from "+dbName+".a where id = 1", "1.2000")

		// CREATE TABLE ... LIKE clones the metadata exactly (I1/I2).
		exec("create table " + dbName + ".a_like like " + dbName + ".a")
		requireMeta(dbName, "a_like", map[string]colMeta{
			"id": {defValid: false, extra: ""},
			"d":  {def: "1.2000", defValid: true, extra: ""},
			"b":  {def: "b'110011'", defValid: true, extra: ""},
			"v":  {def: "x", defValid: true, extra: ""},
			"n":  {defValid: false, extra: ""},
			"ad": {def: "1.2000", defValid: true, extra: ""},
		})
		exec("create table " + dbName + ".b_like like " + dbName + ".b")
		requireMeta(dbName, "b_like", map[string]colMeta{
			"id":  {defValid: false, extra: ""},
			"ts":  {def: "current_timestamp(6)", defValid: true, extra: "DEFAULT_GENERATED"},
			"ts0": {def: "current_timestamp()", defValid: true, extra: "DEFAULT_GENERATED"},
			"u":   {def: "(uuid())", defValid: true, extra: "DEFAULT_GENERATED"},
			"j":   {def: "(json_array())", defValid: true, extra: "DEFAULT_GENERATED"},
			"ats": {def: "current_timestamp(6)", defValid: true, extra: "DEFAULT_GENERATED"},
		})
		exec("create table " + dbName + ".i_like like " + dbName + ".i")
		requireMeta(dbName, "i_like", map[string]colMeta{
			"id":     {defValid: false, extra: ""},
			"u64max": {def: "18446744073709551615.0000", defValid: true, extra: ""},
			"u64p1":  {def: "18446744073709551616.0000", defValid: true, extra: ""},
			"u64p1q": {def: "18446744073709551616.0000", defValid: true, extra: ""},
			"p64p1":  {def: "18446744073709551616.0000", defValid: true, extra: ""},
			"bigu":   {def: "12345678901234567890.1000", defValid: true, extra: ""},
			"bign":   {def: "-12345678901234567890.1000", defValid: true, extra: ""},
		})

		// Invalid INSERT rejection: the statement fails, the row count stays
		// one and the existing row keeps its original values (I1 unhappy).
		execError("insert into "+dbName+".a(id, d) values (2, 'not_a_decimal')", "")
		requireValue("select count(*) from "+dbName+".a", "1")
		requireValue("select d from "+dbName+".a where id = 1", "1.2000")
		requireValue("select b+0 from "+dbName+".a where id = 1", "51")

		// Reconnect: a fresh session publishes the same metadata (I1/I2).
		require.NoError(t, conn.Close())
		require.NoError(t, db.Close())
		db2, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db2.Close()
		conn2, err := db2.Conn(ctx)
		require.NoError(t, err)
		defer conn2.Close()
		conn = conn2
		requireMeta(dbName, "a", map[string]colMeta{
			"id": {defValid: false, extra: ""},
			"d":  {def: "1.2000", defValid: true, extra: ""},
			"b":  {def: "b'110011'", defValid: true, extra: ""},
			"v":  {def: "x", defValid: true, extra: ""},
			"n":  {defValid: false, extra: ""},
			"ad": {def: "1.2000", defValid: true, extra: ""},
		})
		requireMeta(dbName, "b", map[string]colMeta{
			"id":  {defValid: false, extra: ""},
			"ts":  {def: "current_timestamp(6)", defValid: true, extra: "DEFAULT_GENERATED"},
			"ts0": {def: "current_timestamp()", defValid: true, extra: "DEFAULT_GENERATED"},
			"u":   {def: "(uuid())", defValid: true, extra: "DEFAULT_GENERATED"},
			"j":   {def: "(json_array())", defValid: true, extra: "DEFAULT_GENERATED"},
			"ats": {def: "current_timestamp(6)", defValid: true, extra: "DEFAULT_GENERATED"},
		})
		requireValue("select d from "+dbName+".a where id = 1", "1.2000")
		requireValue("select b+0 from "+dbName+".a where id = 1", "51")
		requireMeta(dbName, "i", map[string]colMeta{
			"id":     {defValid: false, extra: ""},
			"u64max": {def: "18446744073709551615.0000", defValid: true, extra: ""},
			"u64p1":  {def: "18446744073709551616.0000", defValid: true, extra: ""},
			"u64p1q": {def: "18446744073709551616.0000", defValid: true, extra: ""},
			"p64p1":  {def: "18446744073709551616.0000", defValid: true, extra: ""},
			"bigu":   {def: "12345678901234567890.1000", defValid: true, extra: ""},
			"bign":   {def: "-12345678901234567890.1000", defValid: true, extra: ""},
		})

		exec("drop database " + dbName)
	})
}
