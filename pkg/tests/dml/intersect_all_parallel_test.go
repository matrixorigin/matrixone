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

package dml

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntersectAllParallelMultiplicity(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		peer, err := c.GetCNService(1)
		require.NoError(t, err)
		clusterInventory := clusterservice.GetMOCluster(cn.ServiceID())
		inventory, ok := clusterInventory.(cnWorkStateInventory)
		require.True(t, ok)
		refresher, ok := clusterInventory.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
		readinessCtx, cancelReadiness := context.WithTimeout(ctx, 30*time.Second)
		readiness, readinessErr := waitForCNReadiness(
			readinessCtx, cnWorkStatePollInterval, inventory, refresher, cn.ServiceID(), peer.ServiceID())
		cancelReadiness()
		require.NoError(t, readinessErr,
			"last refresh error=%v, admission-ready CNs=%v, normally discoverable CNs=%v",
			readiness.lastRefreshErr, readiness.admissionReady, readiness.normallyDiscoverable)
		peerAddr := readiness.peerAddr
		require.NotEmpty(t, peerAddr)

		db := openRetestSQLDB(t, c)
		defer db.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		defer cleanupTestDatabases(t, db, name)
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		var singleCount int64
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from (select 1 v intersect all select 1 v) q").Scan(&singleCount))
		require.Equal(t, int64(1), singleCount)
		for _, table := range []string{"left_bag", "right_bag"} {
			execSQLDB(t, ctx, db, "create table "+table+"(v int)")
		}

		// A flush does not pin block layout: auto-merge can collapse the tiny
		// objects before planning. Pause only these empty test tables, and wait
		// for scheduler acknowledgement before constructing parallel scan work.
		inspect := func(ctx context.Context, command string) (string, error) {
			var raw string
			err := db.QueryRowContext(ctx, "select mo_ctl('dn','inspect','"+command+"')").Scan(&raw)
			if err == nil && (strings.Contains(raw, "run err:") || strings.Contains(raw, "parse err:")) {
				err = fmt.Errorf("%s: %s", command, raw)
			}
			return raw, err
		}
		for _, table := range []string{"left_bag", "right_bag"} {
			target := name + "." + table
			var last string
			err = waitForIntersectFixture(ctx, func(queryCtx context.Context) (bool, error) {
				var queryErr error
				last, queryErr = inspect(queryCtx, "merge show -t "+target)
				return strings.Contains(last, "\n\tauto merge: true"), queryErr
			})
			require.NoError(t, err, "merge registration: %s", last)
			// Register restoration before sending the pause: an unsuccessful
			// observation must not leave a successfully queued pause behind.
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				response, err := inspect(cleanup, "merge switch on -t "+target)
				assert.NoError(t, err)
				assert.Contains(t, response, "merge enabled for table")
				response, err = inspect(cleanup, "merge show -t "+target)
				assert.NoError(t, err)
				assert.Contains(t, response, "\n\tauto merge: true")
			}()
			last, err = inspect(ctx, "merge switch off -t "+target)
			require.NoError(t, err)
			require.Contains(t, last, "merge disabled for table")
			err = waitForIntersectFixture(ctx, func(queryCtx context.Context) (bool, error) {
				var queryErr error
				last, queryErr = inspect(queryCtx, "merge show -t "+target)
				return strings.Contains(last, "\n\tauto merge: false") && strings.Contains(last, "\n\tmerge tasks in queue: 0"), queryErr
			})
			require.NoError(t, err, "merge pause: %s", last)
		}

		// Sixteen physical blocks on each side cross calcDOP's block threshold.
		// Unmatched filler keys do not change the issue's 3-row intersection.
		for _, input := range []struct {
			table       string
			values      []string
			fillerStart int
			fillerCount int
		}{
			{"left_bag", []string{"1", "1", "2", "null", "null", "3"}, 100, 10},
			{"right_bag", []string{"1", "1", "1", "null", "4"}, 200, 11},
		} {
			for i := range input.fillerCount {
				input.values = append(input.values, strconv.Itoa(input.fillerStart+i))
			}
			for _, value := range input.values {
				execSQLDB(t, ctx, db, "insert into "+input.table+" values("+value+")")
				execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+"."+input.table+"')")
			}
		}

		for _, table := range []string{"left_bag", "right_bag"} {
			var rows, blocks, objects int64
			err = waitForIntersectFixture(ctx, func(queryCtx context.Context) (bool, error) {
				queryErr := db.QueryRowContext(queryCtx, "select table_cnt,block_number,accurate_object_number from table_stats('"+name+"."+table+"','refresh','full') g").Scan(&rows, &blocks, &objects)
				return rows == 16 && blocks == 16 && objects == 16, queryErr
			})
			require.NoError(t, err, "parallel scan fixture: %s rows=%d blocks=%d objects=%d", table, rows, blocks, objects)
		}
		oldForce := plan.GetForceScanOnMultiCN()
		plan.SetForceScanOnMultiCN(true)
		defer plan.SetForceScanOnMultiCN(oldForce)
		var oldMaxDop int64
		require.NoError(t, db.QueryRowContext(ctx, "select @@max_dop").Scan(&oldMaxDop))
		execSQLDB(t, ctx, db, "set max_dop=4")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			_, err := db.ExecContext(cleanup, "set max_dop="+strconv.FormatInt(oldMaxDop, 10))
			assert.NoError(t, err)
		}()
		for _, input := range []struct {
			table string
			want  int
		}{{"left_bag", 16}, {"right_bag", 16}} {
			func() {
				rows, err := db.QueryContext(ctx, "select v from "+input.table)
				require.NoError(t, err)
				defer rows.Close()
				count := 0
				for rows.Next() {
					var v sql.NullInt64
					require.NoError(t, rows.Scan(&v))
					count++
				}
				require.NoError(t, rows.Err())
				require.Equal(t, input.want, count, input.table)
			}()
		}
		for _, input := range []struct {
			table string
			want  int64
		}{{"left_bag", 16}, {"right_bag", 16}} {
			var count int64
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+input.table).Scan(&count))
			require.Equal(t, input.want, count, input.table)
		}
		const set = "select v from left_bag intersect all select v from right_bag"
		physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+set)
		require.NoError(t, err)
		require.Contains(t, strings.ToUpper(physical.ColumnName), "MULTICN(")
		require.Contains(t, physical.Text, peerAddr)
		require.Contains(t, strings.ToLower(physical.Text), "intersect all")
		for _, table := range []string{"left_bag", "right_bag"} {
			require.Regexp(t, regexp.MustCompile(`(?m)Scope [^\n]*Magic: Remote, addr:`+regexp.QuoteMeta(peerAddr)+`[^\n]*\n\s*DataSource: [^\n]*`+table), physical.Text)
			require.Regexp(t, regexp.MustCompile(`(?m)Scope [^\n]*mcpu: [2-9][0-9]*[^\n]*\n\s*DataSource: [^\n]*`+table), physical.Text)
		}
		t.Logf("INTERSECT ALL physical plan:\n%s", physical.Text)

		rows, err := db.QueryContext(ctx, "select v,count(*) from ("+set+") q group by v order by v is null,v")
		require.NoError(t, err)
		func() {
			defer rows.Close()
			for _, want := range []struct {
				v     sql.NullInt64
				count int64
			}{
				{sql.NullInt64{Int64: 1, Valid: true}, 2},
				{sql.NullInt64{}, 1},
			} {
				require.True(t, rows.Next())
				var v sql.NullInt64
				var count int64
				require.NoError(t, rows.Scan(&v, &count))
				require.Equal(t, want.v, v)
				require.Equal(t, want.count, count)
			}
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}()
		for _, query := range []string{
			set,
			"select v from right_bag intersect all select v from left_bag",
		} {
			var total, nonnull, sum int64
			err := db.QueryRowContext(ctx, "select count(*),count(v),coalesce(sum(v),0) from ("+query+") q").
				Scan(&total, &nonnull, &sum)
			require.NoError(t, err)
			require.Equal(t, []int64{3, 2, 2}, []int64{total, nonnull, sum}, query)
		}
		var empty int64
		err = db.QueryRowContext(ctx, "select count(*) from (select v from left_bag where v=99 intersect all select v from right_bag) q").Scan(&empty)
		require.NoError(t, err)
		require.Zero(t, empty)
	})
}

// Observe synchronously so a phase timeout cancels the SQL query and the
// observation finishes before callers inspect results or restore settings.
func waitForIntersectFixture(ctx context.Context, observe func(context.Context) (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ready, err := observe(ctx)
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func TestIntersectFixtureWaitFinishesBlockedObservationBeforeCleanup(t *testing.T) {
	for _, lateSuccess := range []bool{false, true} {
		t.Run(fmt.Sprintf("late_success=%v", lateSuccess), func(t *testing.T) {
			// The timeout is the behavior under test; no sleep or elapsed-time
			// assertion acts as synchronization. The callback blocks on the
			// waiter's context and records termination before cleanup can start.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			finished := make(chan struct{})
			err := waitForIntersectFixture(ctx, func(queryCtx context.Context) (bool, error) {
				<-queryCtx.Done()
				defer close(finished)
				if lateSuccess {
					return true, nil
				}
				return false, queryCtx.Err()
			})
			require.ErrorIs(t, err, context.DeadlineExceeded)
			select {
			case <-finished:
			default:
				t.Fatal("observation still running when cleanup would start")
			}
		})
	}
}
