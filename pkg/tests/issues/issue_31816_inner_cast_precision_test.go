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

package issues

// issue #31816: the explicit inner DECIMAL CAST keeps its own declared
// precision, scale and typed formatting through DML consumers. A real
// INSERT/UPDATE ... SELECT-projected or assigned expression must evaluate the
// cast at its own (w,s) before any outer assignment even when the destination
// column is wider, and a non-numeric intervening consumer must receive the
// typed decimal value (65.0), never a bare literal payload. Statement-ignore
// writes (INSERT IGNORE, UPDATE IGNORE) and loose sessions keep the
// established single out-of-range warning plus the clip at the CAST's own
// bound; strict non-IGNORE writes reject atomically; standalone queries keep
// the typed overflow rejection.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue31816InnerCastPrecisionThroughDML(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		rootCtx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()
		defer func() {
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer dropCancel()
			if _, derr := db.ExecContext(dropCtx, "drop database if exists issue_31816_inner"); derr != nil {
				t.Errorf("issue31816 cleanup drop failed: %v", derr)
			}
		}()
		conn, err := db.Conn(rootCtx)
		require.NoError(t, err)
		defer conn.Close()

		// Subtest-local helpers take the subtest's t so fatal assertions mark
		// the running subtest instead of the parent.
		execOK := func(t *testing.T, stmt string) {
			t.Helper()
			_, err := conn.ExecContext(rootCtx, stmt)
			require.NoError(t, err)
		}
		execErr := func(t *testing.T, stmt string, name string) error {
			t.Helper()
			_, err := conn.ExecContext(rootCtx, stmt)
			t.Logf("issue31816 %s: err=%v", name, err)
			return err
		}
		warnCodes := func(t *testing.T, name string) []string {
			t.Helper()
			ws, werr := conn.QueryContext(rootCtx, "show warnings")
			require.NoError(t, werr, name)
			defer ws.Close()
			var codes []string
			var level, code, msg sql.NullString
			for ws.Next() {
				require.NoError(t, ws.Scan(&level, &code, &msg), name)
				codes = append(codes, code.String)
			}
			require.NoError(t, ws.Err(), name)
			return codes
		}
		valueOf := func(t *testing.T, name, query string) string {
			t.Helper()
			var v sql.NullString
			require.NoError(t, conn.QueryRowContext(rootCtx, query).Scan(&v), name)
			return v.String
		}
		requireTypedInnerOverflow := func(t *testing.T, name string, err error) {
			t.Helper()
			require.Error(t, err, name)
			var merr *mysql.MySQLError
			require.ErrorAs(t, err, &merr, name)
			require.EqualValues(t, 20301, merr.Number, name)
			require.Contains(t, merr.Message, "4096", name)
			require.Contains(t, merr.Message, "Decimal64(4,1)", name)
			require.Contains(t, merr.Message, "beyond the range", name)
		}
		freshWide := func(t *testing.T, seed string) {
			t.Helper()
			execOK(t, "drop table if exists issue_31816_inner.wide")
			execOK(t, "create table issue_31816_inner.wide (id int primary key, d decimal(10,2))")
			if seed != "" {
				execOK(t, "insert into issue_31816_inner.wide values (1, "+seed+")")
			}
		}

		execOK(t, "create database if not exists issue_31816_inner")

		t.Run("inner_bound_rejects_strict_insert_over_wider_column", func(t *testing.T) {
			freshWide(t, "")
			execOK(t, "set session sql_mode='STRICT_TRANS_TABLES'")
			requireTypedInnerOverflow(t, "ins_radix_overflow_strict", execErr(t,
				"insert into issue_31816_inner.wide select 1, v from (select cast(0x1000 as decimal(4,1)) as v) s",
				"ins_radix_overflow_strict"))
			var rows int
			require.NoError(t, conn.QueryRowContext(rootCtx,
				"select count(*) from issue_31816_inner.wide").Scan(&rows))
			require.Zero(t, rows, "the strict rejection must leave no inserted row")
		})

		t.Run("inner_bound_clips_insert_ignore_over_wider_column", func(t *testing.T) {
			freshWide(t, "")
			execOK(t, "set session sql_mode='STRICT_TRANS_TABLES'")
			execOK(t, "insert ignore into issue_31816_inner.wide select 1, v from (select cast(0x1000 as decimal(4,1)) as v) s")
			require.Equal(t, []string{"1264"}, warnCodes(t, "ins_radix_ignore"),
				"INSERT IGNORE keeps the established single out-of-range warning")
			require.Equal(t, "999.90", valueOf(t, "ins_radix_ignore",
				"select d from issue_31816_inner.wide where id = 1"),
				"the inner CAST's own decimal(4,1) bound clips before the wider column stores 999.90")
		})

		t.Run("inner_bound_clips_loose_insert_over_wider_column", func(t *testing.T) {
			freshWide(t, "")
			execOK(t, "set session sql_mode=''")
			execOK(t, "insert into issue_31816_inner.wide select 1, v from (select cast(0x1000 as decimal(4,1)) as v) s")
			require.Equal(t, []string{"1264"}, warnCodes(t, "ins_radix_loose"))
			require.Equal(t, "999.90", valueOf(t, "ins_radix_loose",
				"select d from issue_31816_inner.wide where id = 1"))
			execOK(t, "set session sql_mode='STRICT_TRANS_TABLES'")
		})

		t.Run("inner_bound_clips_update_ignore_over_wider_column", func(t *testing.T) {
			freshWide(t, "1.00")
			execOK(t, "set session sql_mode='STRICT_TRANS_TABLES'")
			_, uerr := conn.ExecContext(rootCtx,
				"update ignore issue_31816_inner.wide set d = cast(0x1000 as decimal(4,1)) where id = 1")
			require.NoError(t, uerr, "upd_radix_ignore")
			require.Equal(t, []string{"1264"}, warnCodes(t, "upd_radix_ignore"),
				"UPDATE IGNORE keeps the established single out-of-range warning")
			require.Equal(t, "999.90", valueOf(t, "upd_radix_ignore",
				"select d from issue_31816_inner.wide where id = 1"),
				"the inner CAST's own decimal(4,1) bound clips before the wider column stores 999.90")
		})

		t.Run("inner_bound_rejects_strict_update_preserving_original_value", func(t *testing.T) {
			freshWide(t, "12.34")
			execOK(t, "set session sql_mode='STRICT_TRANS_TABLES'")
			requireTypedInnerOverflow(t, "upd_radix_strict", execErr(t,
				"update issue_31816_inner.wide set d = cast(0x1000 as decimal(4,1)) where id = 1",
				"upd_radix_strict"))
			require.Equal(t, "12.34", valueOf(t, "upd_radix_strict",
				"select d from issue_31816_inner.wide where id = 1"),
				"the rejected strict UPDATE must preserve the original row value")
		})

		t.Run("typed_value_flows_to_nonnumeric_consumer", func(t *testing.T) {
			execOK(t, "drop table if exists issue_31816_inner.txt")
			execOK(t, "create table issue_31816_inner.txt (id int primary key, s varchar(32))")
			execOK(t, "insert into issue_31816_inner.txt select 2, v from (select concat(cast(x'41' as decimal(5,1))) as v) s")
			require.Equal(t, "65.0", valueOf(t, "concat_typed",
				"select s from issue_31816_inner.txt where id = 2"),
				"the typed decimal cast value must reach the varchar destination as 65.0, not a bare literal payload")
			execOK(t, "insert into issue_31816_inner.txt select 3, v from (select cast(x'41' as decimal(5,1)) as v) s")
			require.Equal(t, "65.0", valueOf(t, "plain_typed",
				"select s from issue_31816_inner.txt where id = 3"))
		})

		typedMysql := func(t *testing.T, name string, err error, want ...string) {
			t.Helper()
			require.Error(t, err, name)
			var merr *mysql.MySQLError
			require.ErrorAs(t, err, &merr, name)
			require.EqualValues(t, 20301, merr.Number, name)
			for _, w := range want {
				require.Contains(t, merr.Message, w, name)
			}
		}

		t.Run("prepared_writes_keep_ignore_and_strict_policy", func(t *testing.T) {
			freshWide(t, "")
			execOK(t, "set session sql_mode='STRICT_TRANS_TABLES'")
			stmtInsVals, perr := conn.PrepareContext(rootCtx,
				"insert ignore into issue_31816_inner.wide values (1, cast(0x1000 as decimal(4,1)))")
			require.NoError(t, perr, "prep_ins_vals_ignore")
			defer stmtInsVals.Close()
			_, xerr := stmtInsVals.ExecContext(rootCtx)
			require.NoError(t, xerr, "exec_ins_vals_ignore")
			require.Equal(t, []string{"1264"}, warnCodes(t, "prep_ins_vals_ignore"),
				"a prepared INSERT IGNORE keeps the single out-of-range warning")
			require.Equal(t, "999.90", valueOf(t, "prep_ins_vals_ignore",
				"select d from issue_31816_inner.wide where id = 1"))

			freshWide(t, "")
			stmtInsSel, perr := conn.PrepareContext(rootCtx,
				"insert ignore into issue_31816_inner.wide select 1, v from (select cast(0x1000 as decimal(4,1)) as v) s")
			require.NoError(t, perr, "prep_ins_sel_ignore")
			defer stmtInsSel.Close()
			_, xerr = stmtInsSel.ExecContext(rootCtx)
			require.NoError(t, xerr, "exec_ins_sel_ignore")
			require.Equal(t, []string{"1264"}, warnCodes(t, "prep_ins_sel_ignore"))
			require.Equal(t, "999.90", valueOf(t, "prep_ins_sel_ignore",
				"select d from issue_31816_inner.wide where id = 1"))

			freshWide(t, "12.30")
			stmtUpd, perr := conn.PrepareContext(rootCtx,
				"update ignore issue_31816_inner.wide set d = cast(0x1000 as decimal(4,1)) where id = 1")
			require.NoError(t, perr, "prep_upd_ignore")
			defer stmtUpd.Close()
			_, xerr = stmtUpd.ExecContext(rootCtx)
			require.NoError(t, xerr, "exec_upd_ignore")
			require.Equal(t, []string{"1264"}, warnCodes(t, "prep_upd_ignore"))
			require.Equal(t, "999.90", valueOf(t, "prep_upd_ignore",
				"select d from issue_31816_inner.wide where id = 1"))

			// The ordinary strict prepared write itself rejects at the inner
			// bound, whether the typed profile surfaces at PREPARE or at
			// EXECUTE, and the prior row stays intact.
			var strictErr error
			stmtStrict, perr := conn.PrepareContext(rootCtx,
				"insert into issue_31816_inner.wide select 2, v from (select cast(0x1000 as decimal(4,1)) as v) s")
			if perr != nil {
				strictErr = perr
			} else {
				defer stmtStrict.Close()
				_, strictErr = stmtStrict.ExecContext(rootCtx)
			}
			typedMysql(t, "prep_strict_write", strictErr, "4096", "Decimal64(4,1)", "beyond the range")
			require.Equal(t, "999.90", valueOf(t, "prep_strict_prior_row",
				"select d from issue_31816_inner.wide where id = 1"),
				"the rejected prepared strict write must preserve the prior row")
		})

		t.Run("stored_view_rebind_keeps_released_coefficient_value", func(t *testing.T) {
			execOK(t, "drop view if exists issue_31816_inner.vx")
			execOK(t, "create view issue_31816_inner.vx as select cast(X'41' as decimal(4,4)) as vcol")
			require.Equal(t, "0.0065", valueOf(t, "view_released_coefficient",
				"select vcol from issue_31816_inner.vx"),
				"a stored view definition rebinds with the released coefficient semantics")
			require.Equal(t, "65.0000", valueOf(t, "direct_magnitude_control",
				"select cast(X'41' as decimal(6,4))"),
				"the interactive explicit query keeps the magnitude semantics")
			var narrow sql.NullString
			serr := conn.QueryRowContext(rootCtx, "select cast(X'41' as decimal(4,4))").Scan(&narrow)
			typedMysql(t, "direct_narrow_error_control", serr, "65", "Decimal64(4,4)", "beyond the range")
			execOK(t, "drop view issue_31816_inner.vx")
		})

		t.Run("folded_constant_keeps_not_null_metadata", func(t *testing.T) {
			execOK(t, "drop table if exists issue_31816_inner.ctas_fold")
			execOK(t, "create table issue_31816_inner.ctas_fold as select cast(X'41' as decimal(5,1)) as c, null as n")
			var cNullable, nNullable string
			query := "select is_nullable from information_schema.columns where table_schema='issue_31816_inner' and table_name='ctas_fold' and column_name="
			require.NoError(t, conn.QueryRowContext(rootCtx, query+"'c'").Scan(&cNullable))
			require.Equal(t, "NO", cNullable, "a provably non-null folded constant must create a NOT NULL CTAS column")
			require.NoError(t, conn.QueryRowContext(rootCtx, query+"'n'").Scan(&nNullable))
			require.Equal(t, "YES", nNullable, "the nullable control column must stay nullable")
			rows, rerr := conn.QueryContext(rootCtx, "select cast(X'41' as decimal(5,1))")
			require.NoError(t, rerr, "fold_metadata_query")
			defer rows.Close()
			cols, cerr := rows.ColumnTypes()
			require.NoError(t, cerr, "fold_metadata_columns")
			if len(cols) > 0 {
				nullable, okAny := cols[0].Nullable()
				t.Logf("issue31816 fold_metadata driver nullable=%v supported=%v", nullable, okAny)
			}
			require.NoError(t, rows.Err(), "fold_metadata_rows")
		})

		t.Run("stored_view_bytes_preserve_source_pinned_legacy_shape", func(t *testing.T) {
			execOK(t, "drop view if exists issue_31816_inner.vx")
			execOK(t, "create view issue_31816_inner.vx as select cast(X'41' as decimal(4,4)) as vcol")
			// The released v4.2.8/hotfix writer marshalled ViewData{Stmt,
			// DefaultDatabase, SQLMode, SecurityType} (pkg/sql/plan/
			// build_ddl.go:215 at tag v4.2.8-hotfix-20261119); the current
			// writer adds later fields but the stored statement itself is
			// the preserved legacy payload. Read the catalog bytes back
			// through the normal current storage and pin the statement text.
			var createSQL sql.NullString
			serr := conn.QueryRowContext(rootCtx,
				"select rel_createsql from mo_catalog.mo_tables where relname = 'vx'").Scan(&createSQL)
			require.NoError(t, serr, "stored_view_bytes")
			require.True(t, createSQL.Valid, "stored_view_bytes_present")
			require.Contains(t, strings.ToLower(createSQL.String), "cast(x'41' as decimal(4,4))",
				"the catalog bytes must preserve the released source statement")
			t.Logf("issue31816 legacy_viewdata_stmt=%q", createSQL.String)
			require.Equal(t, "0.0065", valueOf(t, "legacy_rebind_coefficient",
				"select vcol from issue_31816_inner.vx"))
			execOK(t, "drop view issue_31816_inner.vx")
		})

		t.Run("delete_predicate_keeps_prior_write_conversion", func(t *testing.T) {
			freshWide(t, "")
			execOK(t, "insert into issue_31816_inner.wide values (1, 999.90), (2, 5.00)")
			execOK(t, "set session sql_mode=''")
			execOK(t, "delete from issue_31816_inner.wide where d = cast(0x1000 as decimal(4,1))")
			require.Equal(t, []string{"1264", "1264"}, warnCodes(t, "del_radix_loose"),
				"a loose DELETE predicate keeps the write conversion's established out-of-range warning for each scanned row (two rows here)")
			require.Equal(t, "5.00", valueOf(t, "del_radix_loose_kept",
				"select d from issue_31816_inner.wide where id = 2"),
				"the non-matching control row must survive the clipped predicate delete")
			var rows int
			require.NoError(t, conn.QueryRowContext(rootCtx,
				"select count(*) from issue_31816_inner.wide where id = 1").Scan(&rows))
			require.Zero(t, rows, "the 999.90 row matches the clipped 999.9 predicate and is deleted")
			execOK(t, "set session sql_mode='STRICT_TRANS_TABLES'")
			freshWide(t, "")
			execOK(t, "insert into issue_31816_inner.wide values (1, 999.90), (2, 5.00)")
			requireTypedInnerOverflow(t, "del_radix_strict", execErr(t,
				"delete from issue_31816_inner.wide where d = cast(0x1000 as decimal(4,1))",
				"del_radix_strict"))
			require.Equal(t, "999.90", valueOf(t, "del_radix_strict_kept",
				"select d from issue_31816_inner.wide where id = 1"),
				"the rejected strict DELETE must preserve every prior row")
		})

		t.Run("replace_writes_keep_prior_write_conversion", func(t *testing.T) {
			freshWide(t, "")
			execOK(t, "set session sql_mode='STRICT_TRANS_TABLES'")
			requireTypedInnerOverflow(t, "rep_radix_strict", execErr(t,
				"replace into issue_31816_inner.wide values (1, cast(0x1000 as decimal(4,1)))",
				"rep_radix_strict"))
			var rows int
			require.NoError(t, conn.QueryRowContext(rootCtx,
				"select count(*) from issue_31816_inner.wide").Scan(&rows))
			require.Zero(t, rows, "the rejected strict REPLACE must not write")
			execOK(t, "set session sql_mode=''")
			execOK(t, "replace into issue_31816_inner.wide values (1, cast(0x1000 as decimal(4,1)))")
			require.Equal(t, []string{"1264"}, warnCodes(t, "rep_radix_loose"))
			require.Equal(t, "999.90", valueOf(t, "rep_radix_loose",
				"select d from issue_31816_inner.wide where id = 1"))
			execOK(t, "set session sql_mode='STRICT_TRANS_TABLES'")
		})
	})
}

// The folded-constant wire/CTAS oracle runs in its own bounded test: the
// shared write-policy fixture keeps one 300-second root context for its own
// subtests, and the direct-protocol oracle pair (DECIMAL64 + DECIMAL128
// folds, CTAS metadata for both classes, ordinary-literal and NULL controls)
// deserves its own equally bounded lifecycle instead of overflowing the
// write fixture's budget.
func TestIssue31816FoldedMetadataWireOracle(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		rootCtx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()
		defer func() {
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer dropCancel()
			if _, derr := db.ExecContext(dropCtx, "drop database if exists issue_31816_inner"); derr != nil {
				t.Errorf("issue31816 fold cleanup drop failed: %v", derr)
			}
		}()
		conn, err := db.Conn(rootCtx)
		require.NoError(t, err)
		defer conn.Close()
		execOK := func(t *testing.T, stmt string) {
			t.Helper()
			_, err := conn.ExecContext(rootCtx, stmt)
			require.NoError(t, err)
		}
		valueOf := func(t *testing.T, name, query string) string {
			t.Helper()
			var v sql.NullString
			require.NoError(t, conn.QueryRowContext(rootCtx, query).Scan(&v), name)
			return v.String
		}
		execOK(t, "create database if not exists issue_31816_inner")
		t.Run("folded_metadata_wire_and_decimal128_oracles", func(t *testing.T) {
			rows, rerr := conn.QueryContext(rootCtx, "select cast(X'41' as decimal(5,1))")
			require.NoError(t, rerr, "wire_query")
			defer rows.Close()
			cols, cerr := rows.ColumnTypes()
			require.NoError(t, cerr, "wire_columns")
			require.Len(t, cols, 1, "wire_column_count")
			nullable, okAny := cols[0].Nullable()
			require.True(t, okAny, "wire_nullable_supported")
			require.False(t, nullable, "the folded radix constant wire metadata must report NOT NULL")
			require.Contains(t, cols[0].DatabaseTypeName(), "DECIMAL", "wire_type_name")
			require.True(t, rows.Next(), "wire_row")
			var direct sql.NullString
			require.NoError(t, rows.Scan(&direct))
			require.Equal(t, "65.0", direct.String, "wire_value")
			require.NoError(t, rows.Err(), "wire_rows")
			rows.Close()

			// The DECIMAL128 fold branch gets its own direct wire oracle so
			// both constant classes prove value, type and NOT NULL at the
			// real MySQL-protocol consumer.
			rows128, r128err := conn.QueryContext(rootCtx, "select cast(0xf4240 as decimal(20,4)) as w128")
			require.NoError(t, r128err, "wire128_query")
			defer rows128.Close()
			cols128, c128err := rows128.ColumnTypes()
			require.NoError(t, c128err, "wire128_columns")
			require.Len(t, cols128, 1, "wire128_column_count")
			nullable128, ok128 := cols128[0].Nullable()
			require.True(t, ok128, "wire128_nullable_supported")
			require.False(t, nullable128, "wire128_nullable")
			require.Contains(t, cols128[0].DatabaseTypeName(), "DECIMAL", "wire128_type_name")
			require.True(t, rows128.Next(), "wire128_row")
			var direct128 sql.NullString
			require.NoError(t, rows128.Scan(&direct128))
			require.Equal(t, "1000000.0000", direct128.String, "wire128_value")
			require.NoError(t, rows128.Err(), "wire128_rows")

			// The shared test connection serializes one open result set at a
			// time: the wire rows are closed above and the DECIMAL128 rows
			// close here before the CTAS statements below.
			rows128.Close()

			execOK(t, "drop table if exists issue_31816_inner.ctas_wire")
			execOK(t, "create table issue_31816_inner.ctas_wire as select cast(X'41' as decimal(5,1)) as c64, cast(0xf4240 as decimal(20,4)) as w128, cast(65 as decimal(5,1)) as oc, null as n")
			query := "select is_nullable, data_type from information_schema.columns where table_schema='issue_31816_inner' and table_name='ctas_wire' and column_name="
			var nullableFlag, typeStr string
			for _, tc := range []struct{ col, wantNullable string }{{"c64", "NO"}, {"w128", "NO"}, {"oc", "NO"}, {"n", "YES"}} {
				require.NoError(t, conn.QueryRowContext(rootCtx, query+"'"+tc.col+"'").Scan(&nullableFlag, &typeStr), tc.col)
				require.Equal(t, tc.wantNullable, nullableFlag, tc.col)
				t.Logf("issue31816 ctas_%s type=%v", tc.col, typeStr)
			}
			require.Equal(t, "65.0", valueOf(t, "ctas_c64_row", "select c64 from issue_31816_inner.ctas_wire"))
			require.Equal(t, "1000000.0000", valueOf(t, "ctas_w128_row", "select w128 from issue_31816_inner.ctas_wire"))
			require.Equal(t, "65.0", valueOf(t, "ctas_oc_row", "select oc from issue_31816_inner.ctas_wire"))
			rows2, r2err := conn.QueryContext(rootCtx, "select n from issue_31816_inner.ctas_wire")
			require.NoError(t, r2err, "ctas_null_query")
			defer rows2.Close()
			require.True(t, rows2.Next(), "ctas_null_row")
			var nullCell sql.NullString
			require.NoError(t, rows2.Scan(&nullCell))
			require.False(t, nullCell.Valid, "the NULL control cell must stay NULL through CTAS")
			require.NoError(t, rows2.Err(), "ctas_null_rows")
		})
	})
}

// A stored SQL function's saved body is persisted catalog text: it must rebind
// through the historical coefficient semantics like any pre-upgrade
// definition, while the call-site argument expressions and direct interactive
// casts keep the new magnitude route.
func TestIssue31816StoredUdfBodyCompatibility(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		rootCtx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()
		defer func() {
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer dropCancel()
			if _, derr := db.ExecContext(dropCtx, "drop database if exists issue_31816_udf"); derr != nil {
				t.Errorf("issue31816 udf cleanup drop failed: %v", derr)
			}
		}()
		conn, err := db.Conn(rootCtx)
		require.NoError(t, err)
		defer conn.Close()
		execOK := func(t *testing.T, stmt string) {
			t.Helper()
			_, err := conn.ExecContext(rootCtx, stmt)
			require.NoError(t, err)
		}
		valueOf := func(t *testing.T, name, query string) string {
			t.Helper()
			var v sql.NullString
			require.NoError(t, conn.QueryRowContext(rootCtx, query).Scan(&v), name)
			return v.String
		}
		execOK(t, "create database if not exists issue_31816_udf")
		execOK(t, "use issue_31816_udf")
		t.Run("stored_body_keeps_released_coefficient_value", func(t *testing.T) {
			execOK(t, "drop function if exists f41 ()")
			execOK(t, "create function f41() returns decimal(4,4) language sql as 'select cast(X''41'' as decimal(4,4))'")
			require.Equal(t, "0.0065", valueOf(t, "udf_narrow_body",
				"select f41()"),
				"a stored narrow body keeps the released coefficient result")
			execOK(t, "drop function if exists f41w ()")
			execOK(t, "create function f41w() returns decimal(6,4) language sql as 'select cast(X''41'' as decimal(6,4))'")
			require.Equal(t, "0.0065", valueOf(t, "udf_wider_body",
				"select f41w()"),
				"a stored wider body keeps the released coefficient result instead of flipping to the magnitude")
		})
		t.Run("scalar_stored_body_keeps_released_coefficient_value", func(t *testing.T) {
			// A body without a SELECT binds through the UDF binder's separate
			// scalar expression route, so it must keep the same historical
			// persisted semantics and restore the caller scope afterwards.
			execOK(t, "drop function if exists fs41 ()")
			execOK(t, "create function fs41 () returns decimal(4,4) language sql as 'cast(X''41'' as decimal(4,4))'")
			require.Equal(t, "0.0065", valueOf(t, "udf_scalar_narrow",
				"select fs41()"),
				"a scalar stored body keeps the released coefficient result")
			execOK(t, "drop function if exists fs41w ()")
			execOK(t, "create function fs41w () returns decimal(6,4) language sql as 'cast(X''41'' as decimal(6,4))'")
			require.Equal(t, "0.0065", valueOf(t, "udf_scalar_wide",
				"select fs41w()"),
				"a scalar wider body keeps the released coefficient result")
			// The caller scope is restored after successful body binds and
			// after a rejected body alike: the same session's direct route
			// keeps the new magnitude semantics.
			execOK(t, "drop function if exists fbad ()")
			execOK(t, "create function fbad () returns decimal(4,4) language sql as 'nosuchfn(X''41'')'")
			var badCall sql.NullString
			berr := conn.QueryRowContext(rootCtx, "select fbad()").Scan(&badCall)
			require.Error(t, berr, "the failing body surfaces its own error")
			require.Equal(t, "65.0000", valueOf(t, "caller_scope_direct_after_bodies",
				"select cast(X'41' as decimal(6,4))"),
				"the direct interactive route stays magnitude after every body bind")
		})
		t.Run("call_site_argument_keeps_magnitude_boundaries", func(t *testing.T) {
			execOK(t, "drop function if exists farg (d decimal(6,4))")
			execOK(t, "create function farg(d decimal(6,4)) returns decimal(6,4) language sql as 'select cast($1 as decimal(6,4))'")
			require.Equal(t, "65.0000", valueOf(t, "udf_arg_magnitude",
				"select farg(cast(X'41' as decimal(6,4)))"),
				"a call-site argument expression keeps the direct magnitude route")
			require.Equal(t, "65.0000", valueOf(t, "direct_wide_control",
				"select cast(X'41' as decimal(6,4))"))
			// The same QueryRow error-control idiom as the stored-view narrow
			// control keeps the driver's row-set accounting (rowserrcheck)
			// clean while asserting the typed call-site rejection.
			var callNarrow sql.NullString
			cerr := conn.QueryRowContext(rootCtx, "select farg(cast(X'41' as decimal(4,4)))").Scan(&callNarrow)
			require.Error(t, cerr, "the direct narrow cast at the call site keeps its typed rejection")
			require.Contains(t, cerr.Error(), "20301", "typed out-of-range rejection")
		})
	})
}
