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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestRecursiveCTEMultiMemberCancellationAndPreparedReuse(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	port := cn.GetServiceConfig().CN.Frontend.Port

	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	observer, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	observer.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, observer.Close()) })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	_, err = conn.ExecContext(ctx, "SET cte_max_recursion_depth = 4294967295")
	require.NoError(t, err)
	var connectionID uint64
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT connection_id()").Scan(&connectionID))

	faultEnabledHere := fault.Enable()
	if faultEnabledHere {
		t.Cleanup(func() { fault.Disable() })
	}
	const barrier = "recursive_cte_32799_cancel"
	const waiters = "recursive_cte_32799_waiters"
	faultActive := false
	t.Cleanup(func() {
		if !faultActive {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, removeErr := fault.RemoveFaultPoint(cleanupCtx, waiters); removeErr != nil {
			t.Errorf("remove recursive CTE waiter observer: %v", removeErr)
		}
		if _, removeErr := fault.RemoveFaultPoint(cleanupCtx, barrier); removeErr != nil {
			t.Errorf("remove recursive CTE cancellation barrier: %v", removeErr)
		}
	})
	require.NoError(t, fault.AddFaultPoint(ctx, barrier, ":::", "wait", 0, "", false))
	faultActive = true
	require.NoError(t, fault.AddFaultPoint(ctx, waiters, ":::", "getwaiters", 0, barrier, false))

	// The second member is empty for the anchor, then emits (1,1); this pins
	// the next frontier after the first member has already advanced to n=2.
	stmt, err := conn.PrepareContext(ctx, `WITH RECURSIVE r(n, member_id) AS (
  SELECT 0, 0
  UNION DISTINCT
  SELECT n + 1 + COALESCE(CASE WHEN n = 2 THEN trigger_fault_point(?) ELSE 0 END, 0), 0
  FROM r WHERE n < ?
  UNION DISTINCT
  SELECT 1, 1 FROM r WHERE n = 1
)
SELECT COUNT(*) FROM r`)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stmt.Close()) })

	queryCtx, queryCancel := context.WithCancel(context.Background())
	queryDone := make(chan error, 1)
	queryActive := true
	t.Cleanup(func() {
		if !queryActive {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, killErr := observer.ExecContext(cleanupCtx, fmt.Sprintf("KILL QUERY %d", connectionID))
		if killErr != nil {
			t.Errorf("cleanup KILL QUERY %d: %v", connectionID, killErr)
		}
		if faultActive {
			_, waiterErr := fault.RemoveFaultPoint(cleanupCtx, waiters)
			if waiterErr != nil {
				t.Errorf("remove recursive CTE waiter observer: %v", waiterErr)
			}
			_, barrierErr := fault.RemoveFaultPoint(cleanupCtx, barrier)
			if barrierErr != nil {
				t.Errorf("release recursive CTE cancellation barrier: %v", barrierErr)
			}
			faultActive = waiterErr != nil || barrierErr != nil
		}
		select {
		case <-queryDone:
		case <-cleanupCtx.Done():
			t.Errorf("recursive query did not stop after cleanup KILL QUERY: %v", cleanupCtx.Err())
		}
		queryActive = false
		queryCancel()
	})
	go func() {
		queryErr := func() (err error) {
			rows, err := stmt.QueryContext(queryCtx, barrier, int64(4_294_967_295))
			if err != nil {
				return err
			}
			defer func() {
				if closeErr := rows.Close(); err == nil {
					err = closeErr
				}
			}()
			for rows.Next() {
				var ignored int64
				if scanErr := rows.Scan(&ignored); scanErr != nil {
					return scanErr
				}
			}
			return rows.Err()
		}()
		queryDone <- queryErr
	}()

	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		count, _, ok := fault.TriggerFault(waiters)
		if ok && count > 0 {
			break
		}
		select {
		case queryErr := <-queryDone:
			queryActive = false
			t.Fatalf("recursive statement ended before reaching the later-member barrier: %v", queryErr)
		case <-deadline.C:
			t.Fatal("recursive member did not reach the post-frontier cancellation barrier")
		case <-ticker.C:
		}
	}

	_, err = observer.ExecContext(ctx, fmt.Sprintf("KILL QUERY %d", connectionID))
	require.NoError(t, err)
	_, err = fault.RemoveFaultPoint(ctx, waiters)
	require.NoError(t, err)
	_, err = fault.RemoveFaultPoint(ctx, barrier)
	require.NoError(t, err)
	faultActive = false
	var queryErr error
	select {
	case queryErr = <-queryDone:
		queryActive = false
	case <-ctx.Done():
		t.Fatalf("KILL QUERY did not stop the recursive statement: %v", ctx.Err())
	}
	queryCancel()
	var sqlErr *mysql.MySQLError
	require.ErrorAs(t, queryErr, &sqlErr)
	require.Equal(t, uint16(moerr.ER_QUERY_INTERRUPTED), sqlErr.Number)
	require.Equal(t, "70100", string(sqlErr.SQLState[:]))

	reuseCtx, reuseCancel := context.WithTimeout(ctx, 8*time.Second)
	defer reuseCancel()
	var count int64
	require.NoError(t, stmt.QueryRowContext(reuseCtx, barrier, int64(3)).Scan(&count),
		"the same prepared statement must be reusable after server-side cancellation")
	require.Equal(t, int64(5), count)
	var reusedID uint64
	require.NoError(t, conn.QueryRowContext(reuseCtx, "SELECT connection_id()").Scan(&reusedID))
	require.Equal(t, connectionID, reusedID, "the canceled query must leave its session usable")
	var one int
	require.NoError(t, conn.QueryRowContext(reuseCtx, "SELECT 1").Scan(&one))
	require.Equal(t, 1, one)
}
