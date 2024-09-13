// Copyright 2021 - 2024 Matrix Origin
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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestWWConflict(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			cn2, err := c.GetCNService(1)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			table := "t"

			testutils.CreateTableAndWaitCNApplied(
				t,
				db,
				table,
				"create table "+table+" (id int primary key, v int)",
				cn1,
				cn2,
			)

			committedAt := testutils.ExecSQL(
				t,
				db,
				cn1,
				"insert into "+table+" values (1, 1)",
			)

			// workflow:
			// cn1: txn1 update t
			// cn1: txn1 lock mo_tables, and found changed in lock op
			// cn1: retry lock mo_tables
			// cn1: lock t
			// cn2: start txn2 update t
			// cn2: commit
			// cn1: lock t completed
			// cn1: txn1 commit
			// no ww conflict error

			var wg sync.WaitGroup
			wg.Add(2)

			txn2StartedC := make(chan struct{})
			txn2CommittedC := make(chan struct{})

			// txn1 workflow
			go func() {
				defer wg.Done()

				var retried atomic.Bool
				var txn2Triggered atomic.Bool
				exec1 := testutils.GetSQLExecutor(cn1)
				err := exec1.ExecTxn(
					ctx,
					func(txn executor.TxnExecutor) error {
						defer func() {
							runtime.MustGetTestingContext(cn1.ServiceID()).SetAdjustLockResultFunc(nil)
							runtime.MustGetTestingContext(cn1.ServiceID()).SetBeforeLockFunc(nil)
						}()

						tx1 := txn.Txn().Txn().ID

						runtime.MustGetTestingContext(cn1.ServiceID()).SetBeforeLockFunc(
							func(txnID []byte, tableID uint64) {
								if !bytes.Equal(txnID, tx1) {
									return
								}

								if tableID == catalog.MO_TABLES_ID || txn2Triggered.Load() {
									return
								}

								txn2Triggered.Store(true)

								// start txn2 update
								close(txn2StartedC)

								// wait txn2 update committed
								<-txn2CommittedC
							},
						)

						runtime.MustGetTestingContext(cn1.ServiceID()).SetAdjustLockResultFunc(
							func(
								txnID []byte,
								tableID uint64,
								result *lock.Result,
							) {
								if !bytes.Equal(txnID, tx1) {
									return
								}

								if tableID != catalog.MO_TABLES_ID {
									return
								}

								if !retried.Load() {
									retried.Store(true)
									if !result.HasConflict && !result.HasPrevCommit {
										result.HasConflict = true
										result.HasPrevCommit = true
										result.Timestamp = txn.Txn().SnapshotTS().Next()
									}
								}
							},
						)

						// update t set v = 2 where id = 1
						res, err := txn.Exec(
							"update "+table+" set v = 1 where id = 1",
							executor.StatementOption{},
						)
						if err != nil {
							return err
						}
						res.Close()

						return nil
					},
					executor.Options{}.
						WithDatabase(db).
						WithMinCommittedTS(committedAt),
				)
				require.NoError(t, err)
			}()

			// txn2 workflow
			go func() {
				defer func() {
					close(txn2CommittedC)
					wg.Done()
				}()

				<-txn2StartedC
				exec := testutils.GetSQLExecutor(cn2)

				res, err := exec.Exec(
					ctx,
					"update "+table+" set v = 2 where id = 1",
					executor.Options{}.
						WithDatabase(db).
						WithMinCommittedTS(committedAt),
				)
				require.NoError(t, err)
				res.Close()
			}()

			wg.Wait()
		},
	)
}

// #18754
func TestBinarySearchBlkDataOnUnSortedFakePKCol(t *testing.T) {
	c, err := embed.NewCluster(embed.WithCNCount(1))
	require.NoError(t, err)
	require.NoError(t, c.Start())
	defer func() {
		require.NoError(t, c.Close())
	}()

	cn, err := c.GetCNService(0)
	require.NoError(t, err)

	sqlExecutor := testutils.GetSQLExecutor(cn)
	require.NotNil(t, sqlExecutor)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))

	_, err = sqlExecutor.Exec(ctx, "create database testdb;", executor.Options{})
	require.NoError(t, err)

	_, err = sqlExecutor.Exec(ctx,
		"create table hhh(a int) cluster by(`a`);",
		executor.Options{}.WithDatabase("testdb"))
	require.NoError(t, err)

	willInsertRows := 10000
	for i := 0; i < 100; i++ {
		_, err = sqlExecutor.Exec(ctx,
			fmt.Sprintf(
				"insert into hhh "+
					"select FLOOR(RAND()*1000*1000)"+
					"from generate_series(1, %d);", willInsertRows/100),
			executor.Options{}.WithDatabase("testdb"))
		require.NoError(t, err)

		_, err = sqlExecutor.Exec(ctx,
			"select mo_ctl('dn', 'flush', 'testdb.hhh');",
			executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err)
	}

	res, err := sqlExecutor.Exec(ctx,
		"select count(*) from hhh",
		executor.Options{}.WithDatabase("testdb"))
	require.NoError(t, err)

	n := int64(0)
	res.ReadRows(
		func(rows int, cols []*vector.Vector) bool {
			n = executor.GetFixedRows[int64](cols[0])[0]
			return true
		},
	)
	require.Equal(t, int64(willInsertRows), n)

	eng := cn.RawService().(cnservice.Service).GetEngine()
	require.NotNil(t, eng)

	sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		op := txn.Txn()
		db, err := eng.Database(ctx, "testdb", op)
		require.NoError(t, err)
		proc := op.GetWorkspace().(*disttae.Transaction).GetProc()
		rel, err := db.Relation(ctx, "hhh", proc)
		require.NoError(t, err)

		for r := 0; r < 100; r++ {
			var keys []int64
			for i := 0; i < willInsertRows; i++ {
				keys = append(keys, rand.Int63()%int64(willInsertRows))
			}

			vec := vector.NewVec(types.T_int64.ToType())
			for i := 0; i < len(keys); i++ {
				vector.AppendFixed[int64](vec, keys[i], false, proc.GetMPool())
			}

			rel.PrimaryKeysMayBeModified(ctx, types.TS{}, types.MaxTs(), vec)

			vec.Free(proc.GetMPool())
		}

		return nil
	}, executor.Options{})
}
