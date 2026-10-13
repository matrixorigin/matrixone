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
	"errors"
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
	t.Cleanup(func() {
		if t.Failed() {
			require.NoError(t, embed.CloseBaseClusterTests())
		}
	})
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
			composite bool
			incoming  string
		}{
			{name: "plain"},
			{name: "key", partition: "partition by key(id) partitions 8"},
			{name: "range", partition: "partition by range(id) (partition p0 values less than (2), partition p1 values less than maxvalue)"},
			{name: "non_pk_partition", partition: "partition by hash(uk) partitions 2"},
			{name: "hash_repeated", partition: "partition by hash(id+id) partitions 4", incoming: "(-1,10,999)"},
			{name: "range_repeated", partition: "partition by range(id+id) (partition p0 values less than (1), partition p1 values less than maxvalue)", incoming: "(-1,10,999)"},
			{name: "range_product", partition: "partition by range(id*id) (partition p0 values less than (0), partition p1 values less than maxvalue)", incoming: "(-1,10,999)"},
			{name: "nested_repeated", partition: "partition by range((id+id)*id) (partition p0 values less than (1), partition p1 values less than maxvalue)", incoming: "(-1,10,999)"},
			{name: "composite", partition: "partition by key(id) partitions 8", composite: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				table := fmt.Sprintf("`%s`.`%s`", name, tc.name)
				columns, seed, incoming := "(id int primary key, uk int unique, v int)", "(1,10,1)", "(100099,10,999)"
				if tc.incoming != "" {
					incoming = tc.incoming
				}
				if tc.composite {
					columns, seed, incoming = "(id int, shard int, uk int unique, v int, primary key(id,shard))", "(1,7,10,1)", "(100099,7,10,999)"
				}
				execSQLRequire(t, ctx, db, "create table "+table+columns+" "+tc.partition)
				execSQLRequire(t, ctx, db, "insert into "+table+" values"+seed)
				commitTS := cn.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
				testutils.WaitLogtailApplied(t, commitTS, cn)
				var tableID uint64
				require.NoError(t, db.QueryRowContext(ctx,
					"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?",
					name, tc.name).Scan(&tableID))
				writer, err := db.Conn(ctx)
				require.NoError(t, err)
				defer writer.Close()
				statement := "insert into " + table + " values" + incoming + " on duplicate key update v=v+1"
				require.NoError(t, issue30084CommitBetweenScanAndLock(ctx, cn, writer, tableID, statement, func() error {
					_, err := db.ExecContext(ctx, statement)
					return err
				}))
				commitTS = cn.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
				testutils.WaitLogtailApplied(t, commitTS, cn)
				projection, pkPredicate := "id,0 as shard,uk,v", " where id=1"
				expected := [][4]int{{1, 0, 10, 3}}
				if tc.composite {
					projection, pkPredicate = "id,shard,uk,v", " where id=1 and shard=7"
					expected[0][1] = 7
				}
				for _, predicate := range []string{"", pkPredicate, " where uk=10"} {
					func() {
						rows, err := db.QueryContext(ctx, "select "+projection+" from "+table+predicate)
						require.NoError(t, err)
						defer rows.Close()
						var actual [][4]int
						for rows.Next() {
							var row [4]int
							require.NoError(t, rows.Scan(&row[0], &row[1], &row[2], &row[3]))
							actual = append(actual, row)
						}
						require.NoError(t, rows.Err())
						require.Equal(t, expected, actual, predicate)
					}()
				}
			})
			if t.Failed() {
				return
			}
		}
		// Exercise terminal paths on the same fixture, then make a normal
		// request to prove hooks do not leak into the next operation.
		table := fmt.Sprintf("`%s`.`barrier_lifecycle`", name)
		execSQLRequire(t, ctx, db, "create table "+table+"(id int primary key, v int)")
		execSQLRequire(t, ctx, db, "insert into "+table+" values(1,1)")
		var tableID uint64
		require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='barrier_lifecycle'", name).Scan(&tableID))
		writer, err := db.Conn(ctx)
		require.NoError(t, err)
		defer writer.Close()
		err = issue30084CommitBetweenScanAndLock(ctx, cn, writer, tableID, "invalid SQL", func() error { t.Error("invalid writer reached barrier"); return nil })
		require.ErrorContains(t, err, "did not reach")
		canceled, stop := context.WithCancel(ctx)
		err = issue30084CommitBetweenScanAndLock(canceled, cn, writer, tableID, "update "+table+" set v=v+1 where id=1", func() error { stop(); return nil })
		stop()
		require.ErrorIs(t, err, context.Canceled)
		execSQLRequire(t, ctx, db, "update "+table+" set v=v+1 where id=1")
	})
}

func issue30084CommitBetweenScanAndLock(ctx context.Context, cn embed.ServiceOperator,
	writer *sql.Conn, tableID uint64, statement string, compete func() error,
) (err error) {
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	paused, release, callbackExited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var once sync.Once
	tc := moruntime.MustGetTestingContext(cn.ServiceID())
	tc.SetBeforeLockFunc(func(_ []byte, lockedTable uint64) {
		if lockedTable == tableID && first.CompareAndSwap(false, true) {
			defer close(callbackExited)
			close(paused)
			select {
			case <-release:
			case <-workCtx.Done():
			}
		}
	})
	// The before-lock hook is gated by this hook; results stay unchanged.
	tc.SetAdjustLockResultFunc(func([]byte, uint64, *pblock.Result) {})
	defer tc.SetBeforeLockFunc(nil)
	defer tc.SetAdjustLockResultFunc(nil)
	done := make(chan error, 1)
	joined := false
	defer func() {
		stop()
		once.Do(func() { close(release) })
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		if !joined {
			select {
			case <-done:
			case <-timer.C:
				err = errors.Join(err, errors.New("ODKU did not terminate during cleanup"))
				return
			}
		}
		if first.Load() {
			select {
			case <-callbackExited:
			case <-timer.C:
				err = errors.Join(err, errors.New("before-lock callback did not terminate during cleanup"))
			}
		}
	}()
	go func() { _, err := writer.ExecContext(workCtx, statement); done <- err }()
	select {
	case <-paused:
	case err := <-done:
		joined = true
		return fmt.Errorf("ODKU did not reach its base row lock: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}
	// The competitor commits after the first statement's snapshot, before
	// its row lock. No active lock conflict can mask a missed partition check.
	if err := compete(); err != nil {
		return err
	}
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		joined = true
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
