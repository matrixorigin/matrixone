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
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

// TestIssue31422UpdateReturningRows locks the wire behavior behind issue
// #31422 on the real frontend: a no-op or partially no-op UPDATE RETURNING
// returns the final image of every matched row instead of tripping the
// internal row-count guard, while the client-facing affected-row report keeps
// each connection capability's semantics -- matched rows with
// CLIENT_FOUND_ROWS, physically changed rows without it. The fail-closed
// boundary is covered too: a NOT NULL rejection surfaces the write error (not
// the RETURNING guard) and leaves the same connection usable, and
// transactional no-op RETURNING commits while a rolled-back change leaves the
// table untouched.
func TestIssue31422UpdateReturningRows(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port

		const dbName = "issue_31422_returning"
		runQuadrant := func(foundRowsCapability bool) {
			flag := "clientFoundRows=false"
			quadrant := "default"
			if foundRowsCapability {
				flag = "clientFoundRows=true"
				quadrant = "clientFoundRows"
			}
			db, err := sql.Open("mysql",
				fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?%s&interpolateParams=false", port, flag))
			require.NoError(t, err)
			defer db.Close()
			// One pinned connection so select row_count() observes this
			// session's protocol counts.
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			exec := func(statement string) {
				_, err := conn.ExecContext(ctx, statement)
				require.NoErrorf(t, err, "%s quadrant: exec failed: %s", quadrant, statement)
			}
			returning := func(statement string, wantRows [][2]int, wantRowCount int) {
				rows, rerr := conn.QueryContext(ctx, statement)
				require.NoErrorf(t, rerr,
					"%s quadrant: unexpected error for %s: %v", quadrant, statement, rerr)
				defer rows.Close()
				var got [][2]int
				for rows.Next() {
					var id, v int
					require.NoError(t, rows.Scan(&id, &v))
					got = append(got, [2]int{id, v})
				}
				require.NoError(t, rows.Err())
				require.ElementsMatchf(t, wantRows, got,
					"%s quadrant: rows from %s: %v", quadrant, statement, got)
				var rc sql.NullInt64
				require.NoError(t, conn.QueryRowContext(ctx, "select row_count()").Scan(&rc))
				require.Truef(t, rc.Valid, "%s quadrant: row_count null after %s", quadrant, statement)
				require.Equalf(t, int64(wantRowCount), rc.Int64,
					"%s quadrant: row_count after %s", quadrant, statement)
			}

			exec("drop database if exists " + dbName)
			exec("create database " + dbName)
			exec("create table " + dbName + ".noop_t(id int primary key, v int not null)")
			exec("insert into " + dbName + ".noop_t values (1,7),(2,70),(3,70)")

			// A real change: both capabilities report one row.
			returning("update "+dbName+".noop_t set v = 8 where id = 1 returning id, v",
				[][2]int{{1, 8}}, 1)
			// The issue's core scenario: the no-op UPDATE still returns the
			// final row image; the affected-row count follows the capability.
			want := 0
			if foundRowsCapability {
				want = 1
			}
			returning("update "+dbName+".noop_t set v = 8 where id = 1 returning id, v",
				[][2]int{{1, 8}}, want)
			// Mixed matched/changed set: every matched image is returned.
			want = 1
			if foundRowsCapability {
				want = 2
			}
			returning("update "+dbName+".noop_t set v = case when id = 2 then v + 1 else v end "+
				"where id in (2, 3) returning id, v",
				[][2]int{{2, 71}, {3, 70}}, want)

			// Committed no-op RETURNING inside a transaction.
			exec("begin")
			returning("update "+dbName+".noop_t set v = 88 where id = 1 returning id, v",
				[][2]int{{1, 88}}, 1)
			want = 0
			if foundRowsCapability {
				want = 1
			}
			returning("update "+dbName+".noop_t set v = 88 where id = 1 returning id, v",
				[][2]int{{1, 88}}, want)
			exec("commit")
			var v int
			require.NoError(t, conn.QueryRowContext(ctx,
				"select v from "+dbName+".noop_t where id = 1").Scan(&v))
			require.Equal(t, 88, v)

			// Rolled-back change leaves the table untouched.
			exec("begin")
			returning("update "+dbName+".noop_t set v = 99 where id = 1 returning id, v",
				[][2]int{{1, 99}}, 1)
			exec("rollback")
			require.NoError(t, conn.QueryRowContext(ctx,
				"select v from "+dbName+".noop_t where id = 1").Scan(&v))
			require.Equal(t, 88, v)

			// NOT NULL rejection surfaces the write error, never the
			// RETURNING row-count guard, and the connection stays usable.
			rows, werr := conn.QueryContext(ctx,
				"update "+dbName+".noop_t set v = null where id = 1 returning id, v")
			if rows != nil {
				// A write rejection must not stream a result set. If the
				// driver still opened rows, checking Err keeps the rows
				// discipline honest and folds any rows-borne error into the
				// rejection assertions below.
				defer rows.Close()
				if rerr := rows.Err(); rerr != nil {
					werr = rerr
				}
			}
			require.Errorf(t, werr, "%s quadrant: expected the NOT NULL rejection", quadrant)
			require.Falsef(t, strings.Contains(werr.Error(), "does not match affected rows"),
				"%s quadrant: row-count guard fired instead of the write rejection: %v", quadrant, werr)
			require.ErrorContains(t, werr, "cannot be null")
			require.NoError(t, conn.QueryRowContext(ctx,
				"select v from "+dbName+".noop_t where id = 1").Scan(&v))
			require.Equal(t, 88, v)
		}
		runQuadrant(false)
		runQuadrant(true)
	})
}
