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
	})
}
