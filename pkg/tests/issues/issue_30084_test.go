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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue30084ODKUPartitionSnapshotValidation(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		execSQLRequire(t, ctx, db, "create database `"+name+"`")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			execSQLMaybe(t, cleanup, db, "drop database `"+name+"`")
		}()

		for _, tc := range []struct {
			name      string
			partition string
		}{
			{name: "plain"},
			{name: "key", partition: "partition by key(id) partitions 8"},
			{name: "range", partition: "partition by range(id) (partition p0 values less than (2), partition p1 values less than maxvalue)"},
			{name: "non_pk_partition", partition: "partition by hash(uk) partitions 2"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				table := fmt.Sprintf("`%s`.`%s`", name, tc.name)
				execSQLRequire(t, ctx, db, "create table "+table+"(id int primary key, uk int unique, v int) "+tc.partition)
				execSQLRequire(t, ctx, db, "insert into "+table+" values(1,10,1)")
				commitTS := cn.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
				testutils.WaitLogtailApplied(t, commitTS, cn)
				var tableID uint64
				require.NoError(t, db.QueryRowContext(ctx,
					"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?",
					name, tc.name).Scan(&tableID))
				writer, err := db.Conn(ctx)
				require.NoError(t, err)
				defer writer.Close()
				statement := "insert into " + table + " values(100099,10,999) on duplicate key update v=v+1"
				issue30084CommitBetweenScanAndLock(t, ctx, cn, writer, db, tableID, statement)
				commitTS = cn.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
				testutils.WaitLogtailApplied(t, commitTS, cn)
				for _, predicate := range []string{"", " where id=1", " where uk=10"} {
					rows, err := db.QueryContext(ctx, "select id,uk,v from "+table+predicate)
					require.NoError(t, err)
					var actual [][3]int
					for rows.Next() {
						var row [3]int
						require.NoError(t, rows.Scan(&row[0], &row[1], &row[2]))
						actual = append(actual, row)
					}
					require.NoError(t, rows.Err())
					require.NoError(t, rows.Close())
					require.Equal(t, [][3]int{{1, 10, 3}}, actual, predicate)
				}
			})
		}
	})
}

func issue30084CommitBetweenScanAndLock(t *testing.T, ctx context.Context, cn embed.ServiceOperator,
	writer *sql.Conn, db *sql.DB, tableID uint64, statement string,
) {
	t.Helper()
	paused, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var once sync.Once
	tc := moruntime.MustGetTestingContext(cn.ServiceID())
	tc.SetBeforeLockFunc(func(_ []byte, lockedTable uint64) {
		if lockedTable == tableID && first.CompareAndSwap(false, true) {
			close(paused)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})
	// The existing before-lock hook requires this hook, but no lock result is
	// changed. Only scheduling is controlled; both writes commit through TN.
	tc.SetAdjustLockResultFunc(func([]byte, uint64, *pblock.Result) {})
	defer tc.SetBeforeLockFunc(nil)
	defer tc.SetAdjustLockResultFunc(nil)
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	joined := false
	defer func() {
		stop()
		once.Do(func() { close(release) })
		if !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("ODKU did not terminate during cleanup")
			}
		}
	}()
	go func() { _, err := writer.ExecContext(workCtx, statement); done <- err }()
	select {
	case <-paused:
	case err := <-done:
		joined = true
		t.Fatalf("ODKU did not reach its base row lock: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The first writer already has its statement snapshot. The competing
	// writer commits before it takes the row lock, so there is no active lock
	// conflict to trigger an unconditional retry.
	execSQLRequire(t, ctx, db, statement)
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		joined = true
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
