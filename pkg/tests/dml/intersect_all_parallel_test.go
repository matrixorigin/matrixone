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
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
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
		for _, table := range []string{"left_bag", "right_bag"} {
			execSQLDB(t, ctx, db, "create table "+table+"(v int)")
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

		oldForce := plan.GetForceScanOnMultiCN()
		plan.SetForceScanOnMultiCN(true)
		defer plan.SetForceScanOnMultiCN(oldForce)
		var oldMaxDop int64
		require.NoError(t, db.QueryRowContext(ctx, "select @@max_dop").Scan(&oldMaxDop))
		execSQLDB(t, ctx, db, "set max_dop=4")
		defer execSQLDB(t, ctx, db, "set max_dop="+strconv.FormatInt(oldMaxDop, 10))
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
		}
		require.Regexp(t, regexp.MustCompile(`mcpu: [2-9][0-9]*`), physical.Text)
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
