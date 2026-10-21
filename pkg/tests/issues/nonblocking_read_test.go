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

// Reuse the shared two-CN fixture; each direction has fresh frontend sessions.
func runNonblockingReadAcrossCNs(t *testing.T, check func(*testing.T, context.Context, [3]*sql.Conn)) {
	t.Helper()
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		var dbs [2]*sql.DB
		for i := range dbs {
			cn, err := cluster.GetCNService(i)
			require.NoError(t, err)
			dbs[i], err = sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?timeout=5s&readTimeout=15s&writeTimeout=15s", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer dbs[i].Close()
		}
		admin, err := dbs[0].Conn(ctx)
		require.NoError(t, err)
		defer admin.Close()
		lockReadExec(t, ctx, admin, "CREATE DATABASE nonblocking_read_cross_cn")
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
			defer done()
			lockReadExec(t, cleanup, admin, "DROP DATABASE nonblocking_read_cross_cn")
		}()
		lockReadExec(t, ctx, admin, "CREATE TABLE nonblocking_read_cross_cn.t (id INT PRIMARY KEY, v INT)")
		lockReadExec(t, ctx, admin, "INSERT INTO nonblocking_read_cross_cn.t VALUES (1,10),(2,20),(3,30)")
		for holderCN := range dbs {
			t.Run(fmt.Sprintf("holder_CN%d_reader_CN%d", holderCN, 1-holderCN), func(t *testing.T) {
				var sessions [3]*sql.Conn // holder, reader, independent grant probe
				for i, db := range []*sql.DB{dbs[holderCN], dbs[1-holderCN], dbs[holderCN]} {
					conn, err := db.Conn(ctx)
					require.NoError(t, err)
					defer conn.Close()
					defer func() {
						cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
						defer done()
						_, _ = conn.ExecContext(cleanup, "ROLLBACK")
					}()
					lockReadExec(t, ctx, conn, "USE nonblocking_read_cross_cn")
					sessions[i] = conn
				}
				check(t, ctx, sessions)
				require.Equal(t, []int{60}, lockReadInts(t, ctx, sessions[1], "SELECT SUM(v) FROM t"))
			})
		}
	})
}

func lockReadExec(t *testing.T, ctx context.Context, conn *sql.Conn, query string) {
	t.Helper()
	_, err := conn.ExecContext(ctx, query)
	require.NoError(t, err, query)
}

func lockReadInts(t *testing.T, ctx context.Context, conn *sql.Conn, query string) []int {
	t.Helper()
	rows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err, query)
	defer rows.Close()
	var values []int
	for rows.Next() {
		var value int
		require.NoError(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.NoError(t, rows.Err())
	return values
}

func lockReadConflict(t *testing.T, err error) {
	t.Helper()
	var wire *mysql.MySQLError
	require.ErrorAs(t, err, &wire)
	require.EqualValues(t, 3572, wire.Number)
	require.Equal(t, "HY000", string(wire.SQLState[:]))
}

func lockReadNoWait(t *testing.T, ctx context.Context, conn *sql.Conn, query string) {
	t.Helper()
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := conn.ExecContext(bounded, query)
	lockReadConflict(t, err)
}

func TestNoWaitAcrossCNs(t *testing.T) {
	runNonblockingReadAcrossCNs(t, func(t *testing.T, ctx context.Context, s [3]*sql.Conn) {
		holder, reader, probe := s[0], s[1], s[2]
		lockReadExec(t, ctx, holder, "BEGIN")
		require.Equal(t, []int{1}, lockReadInts(t, ctx, holder, "SELECT id FROM t WHERE id=1 FOR UPDATE"))
		lockReadExec(t, ctx, reader, "BEGIN")
		lockReadExec(t, ctx, reader, "UPDATE t SET v=21 WHERE id=2")
		for _, mode := range []string{"UPDATE", "SHARE"} {
			lockReadNoWait(t, ctx, reader, "SELECT id FROM t WHERE id=1 FOR "+mode+" NOWAIT")
			lockReadNoWait(t, ctx, reader, "SELECT id FROM t WHERE id=1 AND id=(SELECT (SELECT t.id)) FOR "+mode+" NOWAIT")
			func() {
				stmt, err := reader.PrepareContext(ctx, "SELECT id FROM t WHERE id=? FOR "+mode+" NOWAIT")
				require.NoError(t, err)
				defer stmt.Close()
				bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				_, err = stmt.ExecContext(bounded, 1)
				lockReadConflict(t, err)
			}()
		}
		require.Equal(t, []int{61}, lockReadInts(t, ctx, reader, "SELECT SUM(v) FROM t"))
		lockReadExec(t, ctx, reader, "ROLLBACK")
		require.Equal(t, []int{60}, lockReadInts(t, ctx, reader, "SELECT SUM(v) FROM t"))
		lockReadExec(t, ctx, reader, "BEGIN")
		require.Equal(t, []int{2, 3}, lockReadInts(t, ctx, reader, "SELECT CAST(id AS CHAR CHARACTER SET binary) FROM t WHERE id>1 ORDER BY id FOR SHARE NOWAIT"))
		lockReadExec(t, ctx, probe, "BEGIN")
		for _, id := range []int{2, 3} {
			lockReadNoWait(t, ctx, probe, fmt.Sprintf("SELECT id FROM t WHERE id=%d FOR UPDATE NOWAIT", id))
		}
		require.Equal(t, []int{2, 3}, lockReadInts(t, ctx, probe, "SELECT id FROM t WHERE id>1 ORDER BY id FOR SHARE NOWAIT"))
		lockReadExec(t, ctx, probe, "ROLLBACK")
		lockReadExec(t, ctx, reader, "ROLLBACK")
		require.Equal(t, []int{2, 3}, lockReadInts(t, ctx, holder, "SELECT id FROM t WHERE id>1 ORDER BY id FOR UPDATE NOWAIT"))
		lockReadExec(t, ctx, holder, "ROLLBACK")
		lockReadExec(t, ctx, reader, "BEGIN")
		require.Equal(t, []int{1, 2, 3}, lockReadInts(t, ctx, reader, "SELECT id FROM t ORDER BY id FOR UPDATE NOWAIT"))
		lockReadExec(t, ctx, reader, "ROLLBACK")
	})
}

func TestShareSkipLockedAcrossCNs(t *testing.T) {
	runNonblockingReadAcrossCNs(t, func(t *testing.T, ctx context.Context, s [3]*sql.Conn) {
		holder, reader, probe := s[0], s[1], s[2]
		lockReadExec(t, ctx, holder, "BEGIN")
		require.Equal(t, []int{1}, lockReadInts(t, ctx, holder, "SELECT id FROM t WHERE id=1 FOR UPDATE"))
		lockReadExec(t, ctx, reader, "BEGIN")
		require.Equal(t, []int{2, 3}, lockReadInts(t, ctx, reader, "SELECT CAST(id AS CHAR CHARACTER SET binary) FROM t ORDER BY id LIMIT 2 FOR SHARE SKIP LOCKED"))
		require.Equal(t, []int{3}, lockReadInts(t, ctx, reader, "SELECT id FROM t ORDER BY id LIMIT 1 OFFSET 1 FOR SHARE SKIP LOCKED"))
		require.Equal(t, []int{3}, lockReadInts(t, ctx, reader, "SELECT CAST(id AS CHAR CHARACTER SET binary) FROM t WHERE id=(SELECT (SELECT t.id)) ORDER BY id LIMIT 1 OFFSET 1 FOR SHARE SKIP LOCKED"))
		lockReadExec(t, ctx, probe, "BEGIN")
		for _, id := range []int{2, 3} {
			lockReadNoWait(t, ctx, probe, fmt.Sprintf("SELECT id FROM t WHERE id=%d FOR UPDATE NOWAIT", id))
		}
		require.Equal(t, []int{2, 3}, lockReadInts(t, ctx, probe, "SELECT id FROM t WHERE id>1 ORDER BY id FOR SHARE NOWAIT"))
		lockReadExec(t, ctx, probe, "ROLLBACK")
		lockReadExec(t, ctx, reader, "ROLLBACK")
		require.Equal(t, []int{2, 3}, lockReadInts(t, ctx, holder, "SELECT id FROM t WHERE id>1 ORDER BY id FOR UPDATE NOWAIT"))
		lockReadExec(t, ctx, reader, "BEGIN")
		require.Empty(t, lockReadInts(t, ctx, reader, "SELECT id FROM t ORDER BY id LIMIT 2 FOR SHARE SKIP LOCKED"))
		lockReadExec(t, ctx, reader, "ROLLBACK")
		lockReadExec(t, ctx, holder, "ROLLBACK")
		lockReadExec(t, ctx, reader, "BEGIN")
		require.Equal(t, []int{1, 2, 3}, lockReadInts(t, ctx, reader, "SELECT id FROM t ORDER BY id FOR UPDATE NOWAIT"))
		lockReadExec(t, ctx, reader, "ROLLBACK")
	})
}
