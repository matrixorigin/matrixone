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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestPreparedPointUpdateReusesPlanAfterWorkspaceWrites(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists prepared_stats_reuse")
		inTxn := false
		defer func() {
			if inTxn {
				cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				_, err := conn.ExecContext(cleanup, "rollback")
				if err != nil {
					t.Errorf("rollback: %v", err)
				}
			}
		}()
		exec := func(q string) { _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("create database prepared_stats_reuse")
		exec("use prepared_stats_reuse")
		exec("create table t(id int primary key, v int)")
		values := make([]string, 128)
		for i := range values {
			values[i] = fmt.Sprintf("(%d,0)", i+1)
		}
		exec("insert into t values " + strings.Join(values, ","))
		read, err := conn.PrepareContext(ctx, "select v from t where id=?")
		require.NoError(t, err)
		defer read.Close()
		update, err := conn.PrepareContext(ctx, "update t set v=v+1 where id=?")
		require.NoError(t, err)
		defer update.Close()
		count := func() uint64 {
			m := &dto.Metric{}
			err := v2.TxnStatementBuildPlanHistogram.(interface{ Write(*dto.Metric) error }).Write(m)
			require.NoError(t, err)
			return m.GetHistogram().GetSampleCount()
		}
		reads := func(want int) {
			for range 20 {
				var got int
				require.NoError(t, read.QueryRowContext(ctx, 1).Scan(&got))
				require.Equal(t, want, got)
			}
		}
		exec("begin")
		inTxn = true
		before := count()
		reads(0)
		cleanReads := count() - before
		exec("commit")
		inTxn = false
		exec("begin")
		inTxn = true
		before = count()
		for i := 1; i <= 20; i++ {
			result, err := update.ExecContext(ctx, i)
			require.NoError(t, err)
			n, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
		}
		updates := count() - before
		before = count()
		reads(1)
		dirtyReads := count() - before
		exec("rollback")
		inTxn = false
		var restored int
		require.NoError(t, read.QueryRowContext(ctx, 1).Scan(&restored))
		require.Zero(t, restored)
		require.Equal(t, uint64(1), updates, "only the first parameter specialization should plan the point UPDATE")
		require.Equal(t, cleanReads, dirtyReads, "workspace row versions must not add point SELECT planning")
		var countRows int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from t").Scan(&countRows))
		require.Equal(t, 128, countRows)
		// Changing parameter domains still passes through specialization.
		for _, value := range []any{"1", float64(1), int64(1)} {
			require.NoError(t, read.QueryRowContext(ctx, value).Scan(&restored))
			require.Zero(t, restored)
		}
		require.ErrorIs(t, read.QueryRowContext(ctx, nil).Scan(&restored), sql.ErrNoRows)

		// This UPDATE has an installed specialization (unlike the INT SELECT,
		// whose wider parameter triggers value-dependent rebinding).
		exec("begin")
		inTxn = true
		exec("insert into t values(129,7)")
		before = count()
		result, err := update.ExecContext(ctx, 129)
		require.NoError(t, err)
		affected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), affected)
		require.Equal(t, uint64(0), count()-before, "new-key visibility must use the cached specialization")
		require.NoError(t, conn.QueryRowContext(ctx, "select v from t where id=129").Scan(&restored))
		require.Equal(t, 8, restored)
		exec("rollback")
		inTxn = false
		before = count()
		result, err = update.ExecContext(ctx, 129)
		require.NoError(t, err)
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.Zero(t, affected)
		require.Equal(t, uint64(0), count()-before, "rollback must refresh the cached reader too")

		exec("create table composite_t(a int, b int, v int, primary key(a,b))")
		exec("insert into composite_t select id,1,v from t")
		composite, err := conn.PrepareContext(ctx, "update composite_t set v=v+1 where a=? and b=?")
		require.NoError(t, err)
		defer composite.Close()
		exec("begin")
		inTxn = true
		before = count()
		for i := 1; i <= 20; i++ {
			result, err := composite.ExecContext(ctx, i, 1)
			require.NoError(t, err)
			n, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(1), n)
		}
		require.Equal(t, uint64(1), count()-before, "complete composite PK has the same bounded update contract")
		var sum int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*),sum(v) from composite_t").Scan(&countRows, &sum))
		require.Equal(t, 128, countRows)
		require.Equal(t, 20, sum)
		exec("rollback")
		inTxn = false
		require.NoError(t, conn.QueryRowContext(ctx, "select sum(v) from composite_t").Scan(&sum))
		require.Zero(t, sum)
		// Real COM_QUERY dispatch: the same cached SQL sees growing tables.
		exec("begin")
		inTxn = true
		var pointPlans, sensitivePlans uint64
		for i := 129; i < 132; i++ {
			exec(fmt.Sprintf("insert into t values(%d,0)", i))
			before = count()
			require.NoError(t, conn.QueryRowContext(ctx, "select v from t where id=1").Scan(&restored))
			pointPlans += count() - before
			require.Zero(t, restored)
			before = count()
			require.NoError(t, conn.QueryRowContext(ctx, "select sum(v) from t where id>=1").Scan(&sum))
			sensitivePlans += count() - before
			require.Zero(t, sum)
		}
		require.Equal(t, uint64(1), pointPlans, "ordinary point cache must survive growth")
		require.Equal(t, uint64(3), sensitivePlans, "ordinary range plan must be rebuilt after each growth")
		exec("rollback")
		inTxn = false
		exec("create table string_t(id varchar(8) primary key,v int)")
		exec("insert into string_t values('1',0),('01',0)")
		stringUpdate, err := conn.PrepareContext(ctx, "update string_t set v=v+1 where id=?")
		require.NoError(t, err)
		defer stringUpdate.Close()
		for _, tc := range []struct {
			value    any
			affected int64
		}{{"1", 1}, {int64(1), 2}, {"1", 1}} {
			result, err := stringUpdate.ExecContext(ctx, tc.value)
			require.NoError(t, err)
			affected, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, tc.affected, affected)
		}
		require.NoError(t, conn.QueryRowContext(ctx, "select sum(v) from string_t").Scan(&sum))
		require.Equal(t, 4, sum)
		require.NoError(t, conn.QueryRowContext(ctx, "select v from string_t where id='01'").Scan(&restored))
		require.Equal(t, 1, restored)
		require.NoError(t, conn.QueryRowContext(ctx, "select v from string_t where id='1'").Scan(&restored))
		require.Equal(t, 3, restored)

	})
}
