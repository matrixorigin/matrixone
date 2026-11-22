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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Queue DROP behind a row lock, then let another CN compute ACCOUNT ADD B
// while A still exists. A stale ALTER must not re-grant A after DROP commits.
func TestDropAccountConcurrentPublicationGrant(t *testing.T) {
	RunBaseClusterTests(t, func(c Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		open := func(n int, user string) *sql.DB {
			t.Helper()
			cn, err := c.GetCNService(n)
			require.NoError(t, err)
			db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			return db
		}
		left, right := open(0, "dump"), open(1, "dump")
		exec := func(db *sql.DB, q string) {
			t.Helper()
			_, err := db.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		number := func(db *sql.DB, q string) int {
			t.Helper()
			var n int
			require.NoError(t, db.QueryRowContext(ctx, q).Scan(&n), q)
			return n
		}
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			for _, q := range []string{"drop publication if exists race32900p", "drop account if exists race32900a", "drop account if exists race32900b", "drop account if exists race32900keep", "drop database if exists race32900src"} {
				_, err := left.ExecContext(cleanup, q)
				if err != nil {
					t.Errorf("cleanup %s: %v", q, err)
				}
			}
		}()
		for _, a := range []string{"race32900a", "race32900b", "race32900keep"} {
			exec(left, "create account "+a+" admin_name 'admin' identified by '111'")
		}
		exec(left, "create database race32900src")
		exec(left, "create table race32900src.t (id int primary key)")
		exec(left, "insert into race32900src.t values (7)")
		exec(left, "create publication race32900p database race32900src account race32900a,race32900keep")
		keep := open(1, "race32900keep#admin#accountadmin")
		exec(keep, "create database race32900sub from sys publication race32900p")
		exec(open(0, "race32900a#admin#accountadmin"), "create database race32900sub from sys publication race32900p")
		const findA = "select account_id from mo_catalog.mo_account where account_name='race32900a'"
		id := number(left, findA)
		gate, err := left.BeginTx(ctx, nil)
		require.NoError(t, err)
		var ddl sync.WaitGroup
		defer ddl.Wait()
		defer gate.Rollback()
		var locked int
		require.NoError(t, gate.QueryRowContext(ctx, fmt.Sprintf("select account_id from mo_catalog.mo_account where account_id=%d for update", id)).Scan(&locked))
		require.Equal(t, id, locked)
		async := func(db *sql.DB, q string) <-chan error {
			done := make(chan error, 1)
			ddl.Go(func() {
				_, err := db.ExecContext(ctx, q)
				done <- err
			})
			return done
		}
		waitForDDL := func(n int) {
			t.Helper()
			cn, err := c.GetCNService(n)
			require.NoError(t, err)
			require.EventuallyWithT(t, func(tick *assert.CollectT) {
				var waiting int
				err := left.QueryRowContext(ctx, `select count(*) from mo_transactions() t
					where cn_id=? and table_id in (select cast(rel_id as char) from mo_catalog.mo_tables
					where account_id=0 and reldatabase='mo_catalog' and relname in ('mo_account','mo_feature_registry','mo_pubs'))`, cn.ServiceID()).Scan(&waiting)
				if assert.NoError(tick, err) {
					assert.Positive(tick, waiting)
				}
			}, 10*time.Second, 100*time.Millisecond)
		}
		drop := async(left, "drop account race32900a")
		waitForDDL(0)
		const addB = "alter publication race32900p account add race32900b"
		alter := async(right, addB)
		waitForDDL(1)
		require.NoError(t, gate.Rollback())
		require.NoError(t, <-drop)
		if err := <-alter; err != nil {
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &sqlErr)
			require.EqualValues(t, moerr.ErrTxnNeedRetry, sqlErr.Number)
			require.Zero(t, number(left, "select count(*) from mo_catalog.mo_subs where pub_name='race32900p' and sub_account_name='race32900b'"))
			exec(right, addB)
		}
		where := fmt.Sprintf(" from mo_catalog.mo_subs where pub_name='race32900p' and sub_account_id=%d", id)
		require.Zero(t, number(left, "select count(*)"+where))
		require.Zero(t, number(left, "select count(*) from mo_catalog.mo_account where account_name='race32900a'"))
		require.Equal(t, 1, number(left, "select count(*) from mo_catalog.mo_subs where pub_name='race32900p' and sub_account_name='race32900b' and status=0"))
		require.Equal(t, 7, number(keep, "select id from race32900sub.t"))
		exec(left, "create account race32900a admin_name 'admin' identified by '111'")
		newID := number(left, findA)
		require.NotEqual(t, id, newID)
		require.Zero(t, number(left, fmt.Sprintf("select count(*) from mo_catalog.mo_subs where pub_name='race32900p' and sub_account_id in (%d,%d)", id, newID)))
		var auth string
		require.NoError(t, left.QueryRowContext(ctx, "select account_list from mo_catalog.mo_pubs where account_id=0 and pub_name='race32900p'").Scan(&auth))
		require.Equal(t, "race32900b,race32900keep", auth)
	})
}
