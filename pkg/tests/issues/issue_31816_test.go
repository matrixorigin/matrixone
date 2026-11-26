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

// TestIssue31816HexBitLiteralDecimalValue pins the #31816 public contract in
// its upstream-scoped form: a syntax-owned HEX/BIT literal bound as the
// direct operand of an explicit DECIMAL CAST carries MySQL's unsigned integer
// magnitude, and the cast's own scale and precision policy applies to that
// numeric value. Range policy is route-specific and pinned exactly on this
// tree: an over-wide magnitude is rejected by the cast conversion's own
// invalid-input diagnostic (driver error 20301, computed at bind time before
// any session mode is consulted), never wrapped negative and never silently
// truncated, while ordinary non-literal numeric inputs keep the engine's
// existing clamp-plus-warning-1264 policy in each checked mode - that policy
// is upstream-owned and asserted here only as the unchanged control. The
// persisted-expression half of the contract (DEFAULT/ON UPDATE/generated/
// CHECK/table-dump rebinding keeps the historical coefficient binding, so
// genuine prepatch schemas still export and verify) lives in pkg/sql/plan's
// table-dump witness test.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue31816HexBitLiteralDecimalValue(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		rootCtx, cancelRoot := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancelRoot()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		// Ownership: the owned database is dropped by a deferred bounded
		// DROP registered in this scope right at acquisition, BEFORE the
		// deferred db.Close (LIFO), and its error path is visible so early
		// require exits can never leak a successful generation behind.
		defer db.Close()
		defer func() {
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer dropCancel()
			if _, derr := db.ExecContext(dropCtx, "drop database if exists issue_31816_radix"); derr != nil {
				t.Errorf("issue31816 cleanup drop failed: %v", derr)
			}
		}()

		conn, err := db.Conn(rootCtx)
		require.NoError(t, err)
		defer conn.Close()

		mustExec := func(stmt string) {
			t.Helper()
			_, err := conn.ExecContext(rootCtx, stmt)
			require.NoError(t, err)
		}
		requireValue := func(name, query, want string) string {
			t.Helper()
			var got sql.NullString
			err := conn.QueryRowContext(rootCtx, query).Scan(&got)
			require.NoError(t, err, name)
			require.Equal(t, want, got.String, name)
			t.Logf("issue31816 %s: %q -> %q", name, query, got.String)
			return got.String
		}
		selectErr := func(name, query string) error {
			t.Helper()
			rows, qerr := conn.QueryContext(rootCtx, query)
			if qerr != nil {
				t.Logf("issue31816 %s: err=%v", name, qerr)
				return qerr
			}
			defer rows.Close()
			for rows.Next() {
			}
			if rerr := rows.Err(); rerr != nil {
				t.Logf("issue31816 %s: rows err=%v", name, rerr)
				return rerr
			}
			t.Logf("issue31816 %s: err=<nil>", name)
			return nil
		}
		requireRadixOverflow := func(name, query string) {
			t.Helper()
			qerr := selectErr(name, query)
			require.Error(t, qerr, name)
			var merr *mysql.MySQLError
			require.ErrorAs(t, qerr, &merr, name)
			require.EqualValues(t, 20301, merr.Number, name)
			require.Contains(t, merr.Message, "4702394921427289928", name)
			require.Contains(t, merr.Message, "Decimal64(5,1)", name)
			require.Contains(t, merr.Message, "beyond the range", name)
			require.NotContains(t, merr.Message, "binary literal beyond the range", name)
		}
		requireClampControl := func(name, query string) {
			t.Helper()
			requireValue(name, query, "9999.9")
			ws, werr := conn.QueryContext(rootCtx, "show warnings")
			require.NoError(t, werr, name)
			defer ws.Close()
			var level, code, msg sql.NullString
			require.True(t, ws.Next(), name)
			require.NoError(t, ws.Scan(&level, &code, &msg), name)
			require.Equal(t, "1264", code.String, name)
			require.NoError(t, ws.Err(), name)
			t.Logf("issue31816 %s: clamp warning code=%s msg=%s", name, code.String, msg.String)
		}
		setMode := func(mode string) {
			t.Helper()
			_, merr := conn.ExecContext(rootCtx, "set session sql_mode='"+mode+"'")
			require.NoError(t, merr)
		}

		mustExec("create database issue_31816_radix")
		mustExec("create table issue_31816_radix.t (id int primary key, d decimal(10,2))")

		// The exact red witnesses from the issue body: the explicit CAST
		// applies its scale to the unsigned integer magnitude.
		requireValue("hex_scale1", "select cast(x'41' as decimal(5,1))", "65.0")
		requireValue("hex_scale2", "select cast(x'41' as decimal(10,2))", "65.00")
		requireValue("hex_scale3", "select cast(0x0100 as decimal(10,3))", "256.000")
		requireValue("bit_scale2", "select cast(b'1000001' as decimal(6,2))", "65.00")
		requireValue("paren_transparent", "select cast((x'41') as decimal(5,1))", "65.0")

		// The exact previously failing SELECT overflow predicate, plus its
		// ordinary decimal-text control, under both the session's default
		// mode and STRICT_TRANS_TABLES: the radix route rejects the
		// over-wide typed magnitude in every mode, the ordinary input
		// preserves the engine's existing clamp policy in every mode.
		for _, mode := range []string{"", "STRICT_TRANS_TABLES"} {
			label := mode
			if label == "" {
				label = "default"
			}
			setMode(mode)
			requireRadixOverflow("sel_radix_overflow_"+label,
				"select cast(0x4142434445464748 as decimal(5,1))")
			requireClampControl("sel_text_control_"+label,
				"select cast('4702394921427289928' as decimal(5,1))")
			if mode == "" {
				// A loose write stays on the write owner's conversion policy:
				// the magnitude clips at the CAST's own decimal(5,1) with the
				// established single out-of-range warning, and the wider
				// column stores the clipped value.
				mustExec("insert into issue_31816_radix.t values (1, cast(0x4142434445464748 as decimal(5,1)))")
				// Read the statement's own warnings before any subsequent
				// statement replaces the warning slot.
				warnCode := func() string {
					ws, werr := conn.QueryContext(rootCtx, "show warnings")
					require.NoError(t, werr)
					defer ws.Close()
					var level, code, msg sql.NullString
					require.True(t, ws.Next(), "ins_radix_overflow_default")
					require.NoError(t, ws.Scan(&level, &code, &msg))
					require.NoError(t, ws.Err())
					return code.String
				}
				require.Equal(t, "1264", warnCode(), "ins_radix_overflow_default")
				requireValue("ins_radix_overflow_default",
					"select d from issue_31816_radix.t where id = 1", "9999.90")
				mustExec("delete from issue_31816_radix.t where id = 1")
			} else {
				requireRadixOverflow("ins_radix_overflow_"+label,
					"insert into issue_31816_radix.t values (1, cast(0x4142434445464748 as decimal(5,1)))")
			}
			mustExec("insert into issue_31816_radix.t values (1, cast('4702394921427289928' as decimal(5,1)))")
			requireValue("ins_text_control_"+label,
				"select d from issue_31816_radix.t where id = 1", "9999.90")
			mustExec("delete from issue_31816_radix.t where id = 1")
		}
		setMode("")
		var modeVal sql.NullString
		require.NoError(t, conn.QueryRowContext(rootCtx, "select @@sql_mode").Scan(&modeVal))
		require.NotContains(t, modeVal.String, "STRICT_TRANS_TABLES",
			"the session must leave the strict pair cleanly restored")
		t.Logf("issue31816 mode_restored: @@sql_mode=%q", modeVal.String)
		var rowCount int
		require.NoError(t, conn.QueryRowContext(rootCtx,
			"select count(*) from issue_31816_radix.t").Scan(&rowCount))
		require.Zero(t, rowCount, "rejected radix inserts must never leave a row")

		// The connection still serves the next statement after rejections.
		requireValue("after_error_alive", "select 1", "1")

		// The changed route reaches write expressions through the wire.
		mustExec("insert into issue_31816_radix.t values (3, cast(x'96' as decimal(10,2)))")
		requireValue("write_expression", "select d from issue_31816_radix.t where id = 3", "150.00")

		// Upstream's direct-VALUES radix domain stays unmoved.
		mustExec("insert into issue_31816_radix.t values (4, x'96')")
		requireValue("direct_values_route", "select d from issue_31816_radix.t where id = 4", "150.00")

		// HEX/BIT are never negative: a wide carrier decodes as one positive
		// magnitude inside the target's typed range.
		requireValue("wide_highbit", "select cast(0xffffffffffffffff as decimal(20,0))", "18446744073709551615")

		// Realistic opaque binary boundary: a varbinary column value going
		// to DECIMAL keeps the engine's established text-conversion route
		// (unparseable bytes truncate with warning 1292), untouched by this
		// change because the operand is bound data, not a literal.
		mustExec("create table issue_31816_radix.opq (id int primary key, b varbinary(8))")
		mustExec("insert into issue_31816_radix.opq values (1, x'41424344')")
		requireValue("opaque_binary_column", "select cast(b as decimal(10,2)) from issue_31816_radix.opq where id = 1", "0.00")

		// Ordinary numeric text on the same cast target is untouched.
		requireValue("ordinary_numeric", "select cast(65 as decimal(5,1))", "65.0")

		// Upstream's stored radix contracts keep their authoring and replay
		// values (#31161's rows): a bare literal default and a generated
		// column authored on this table, then applied by a NEW connection's
		// insert - the reconnect proves the persisted definition itself
		// serves a fresh insert, not a reread of a materialized cell.
		mustExec("create table issue_31816_radix.store (id int primary key, d decimal(4,4) default b'00110011')")
		mustExec("create table issue_31816_radix.gen (id int primary key, g decimal(4,4) generated always as (b'00110011') stored)")
		mustExec("insert into issue_31816_radix.store (id) values (1)")
		mustExec("insert into issue_31816_radix.gen (id) values (1)")

		reconn, err := db.Conn(rootCtx)
		require.NoError(t, err)
		defer reconn.Close()
		mustExecOn(t, rootCtx, reconn, "insert into issue_31816_radix.store (id) values (2)")
		requireValueOn(t, rootCtx, reconn, "reconnect_default_apply",
			"select d from issue_31816_radix.store where id = 2", "0.0051")
		mustExecOn(t, rootCtx, reconn, "insert into issue_31816_radix.gen (id) values (2)")
		requireValueOn(t, rootCtx, reconn, "reconnect_generated_apply",
			"select g from issue_31816_radix.gen where id = 2", "0.0051")
		requireValue("stored_default_first_conn", "select d from issue_31816_radix.store where id = 1", "0.0051")
		requireValue("generated_first_conn", "select g from issue_31816_radix.gen where id = 1", "0.0051")
	})
}
func mustExecOn(t *testing.T, ctx context.Context, conn *sql.Conn, stmt string) {
	t.Helper()
	_, err := conn.ExecContext(ctx, stmt)
	require.NoError(t, err)
}
func requireValueOn(t *testing.T, ctx context.Context, conn *sql.Conn, name, query, want string) {
	t.Helper()
	var got sql.NullString
	err := conn.QueryRowContext(ctx, query).Scan(&got)
	require.NoError(t, err, name)
	require.Equal(t, want, got.String, name)
	t.Logf("issue31816 %s: -> %q", name, got.String)
}
