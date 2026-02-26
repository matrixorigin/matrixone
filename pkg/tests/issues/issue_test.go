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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
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

func cleanDatabase(
	t *testing.T,
	ctx context.Context,
	cn cnservice.Service, dbName string,
) {
	exec := cn.GetSQLExecutor()
	require.NotNil(t, exec)

	exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		eng := cn.GetEngine()
		require.NoError(t, eng.Delete(ctx, dbName, txn.Txn()))
		return nil
	}, executor.Options{})
}

// #18754
func TestBinarySearchBlkDataOnUnSortedFakePKCol(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			sqlExecutor := testutils.GetSQLExecutor(cn)
			require.NotNil(t, sqlExecutor)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))

			_, err = sqlExecutor.Exec(ctx, "create database testdb;", executor.Options{})
			require.NoError(t, err)
			defer func() {
				cleanDatabase(t, ctx, cn.RawService().(cnservice.Service), "testdb")
			}()

			_, err = sqlExecutor.Exec(ctx,
				"create table hhh(a int) cluster by(`a`);",
				executor.Options{}.WithDatabase("testdb"))
			require.NoError(t, err)

			willInsertRows := 50
			for i := 0; i < 5; i++ {
				_, err = sqlExecutor.Exec(ctx,
					fmt.Sprintf(
						"insert into hhh "+
							"select FLOOR(RAND()*1000*1000)"+
							"from generate_series(1, %d);", willInsertRows/5),
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

				var keys []int64
				for r := 0; r < 5; r++ {
					keys = keys[:0]
					for i := 0; i < willInsertRows; i++ {
						keys = append(keys, rand.Int63()%int64(willInsertRows))
					}

					vec := vector.NewVec(types.T_int64.ToType())
					for i := 0; i < len(keys); i++ {
						vector.AppendFixed[int64](vec, keys[i], false, proc.GetMPool())
					}

					bat := batch.NewWithSize(1)
					bat.SetVector(0, vec)
					rel.PrimaryKeysMayBeModified(ctx, types.TS{}, types.MaxTs(), bat, 0, -1)

					vec.Free(proc.GetMPool())
				}

				return nil
			}, executor.Options{})
		})
}

func TestCNFlushS3Deletes(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()

			ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))

			exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
			require.NotNil(t, exec)

			{
				_, err := exec.Exec(ctx, "create database a;", executor.Options{})
				require.NoError(t, err)

				defer func() {
					cleanDatabase(t, ctx, cn.RawService().(cnservice.Service), "a")
				}()

				_, err = exec.Exec(ctx, "create table t1 (a int primary key, b varchar(256));",
					executor.Options{}.WithDatabase("a"))
				require.NoError(t, err)

				_, err = exec.Exec(ctx, "insert into t1 select *,'yep' from generate_series(1,512*1024)g;",
					executor.Options{}.WithDatabase("a"))
				require.NoError(t, err)

				resp, err := exec.Exec(ctx, "select count(1) from t1;",
					executor.Options{}.WithDatabase("a"))
				require.NoError(t, err)

				resp.ReadRows(func(rows int, cols []*vector.Vector) bool {
					cnt := vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
					require.Equal(t, int64(512*1024), cnt)
					return true
				})
			}

			deletion.SetCNFlushDeletesThreshold(1)
			defer deletion.SetCNFlushDeletesThreshold(32)

			{
				_, err := exec.Exec(ctx, "delete from t1 where a > 1;", executor.Options{}.WithDatabase("a"))
				require.NoError(t, err)

				resp, err := exec.Exec(ctx, "select count(1) from t1;", executor.Options{}.WithDatabase("a"))
				require.NoError(t, err)

				resp.ReadRows(func(rows int, cols []*vector.Vector) bool {
					cnt := vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
					require.Equal(t, int64(1), cnt)
					return true
				})

				resp, err = exec.Exec(ctx, "select * from t1 where a = 1;", executor.Options{}.WithDatabase("a"))
				require.NoError(t, err)

				require.Equal(t, int(1), len(resp.Batches))
				require.Equal(t, int(1), resp.Batches[0].RowCount())
				resp.ReadRows(func(rows int, cols []*vector.Vector) bool {
					aVals := executor.GetFixedRows[int32](cols[0])
					bdata, barea := vector.MustVarlenaRawData(cols[1])
					bVal := bdata[0].GetString(barea)

					require.Equal(t, int(1), len(aVals))
					require.Equal(t, int32(1), aVals[0])
					require.Equal(t, "yep", bVal)

					return true
				})
			}
		})
}

//#16493
/*
// create table t (id int auto_increment primary key, id2 int);
// 				CN1 										CN2
// insert into t(id2) values (1), (2)
// 											insert into t(id) values (3)
// insert into t(id2) values (1), (2)

There is no lock competition, but there is data modification
*/
func TestDedupForAutoPk(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
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
				"create table "+table+" (id int auto_increment primary key, id2 int)",
				cn1,
				cn2,
			)

			committedAt := testutils.ExecSQL(
				t,
				db,
				cn1,
				"insert into "+table+"(id2) values (1), (2)",
			)

			exec2 := testutils.GetSQLExecutor(cn2)
			err = exec2.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					res, err := txn.Exec(
						"insert into "+table+"(id) values (3)",
						executor.StatementOption{},
					)
					require.NoError(t, err)
					res.Close()
					return nil
				},
				executor.Options{}.
					WithDatabase(db).
					WithMinCommittedTS(committedAt),
			)
			require.NoError(t, err)

			exec1 := testutils.GetSQLExecutor(cn1)
			err = exec1.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					res, err := txn.Exec(
						"insert into "+table+"(id2) values (1), (2)",
						executor.StatementOption{},
					)
					require.NoError(t, err)
					res.Close()
					return nil
				},
				executor.Options{}.
					WithDatabase(db).
					WithWaitCommittedLogApplied(),
			)
			require.NoError(t, err)

			// check the result
			exec := testutils.GetSQLExecutor(cn1)
			res, err := exec.Exec(
				ctx,
				"select id from t;",
				executor.Options{}.
					WithDatabase(db).
					WithWaitCommittedLogApplied(),
			)
			require.NoError(t, err)

			res.ReadRows(
				func(rows int, cols []*vector.Vector) bool {
					for i := 0; i < rows; i++ {
						n := executor.GetFixedRows[int32](cols[0])[i]
						t.Logf("the value of rows %d is %d", i, n)
					}
					return true
				},
			)
			res.Close()
		})
}

func TestLockNeedUpgrade(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
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
				"create table "+table+" (id int primary key, id2 int)",
				cn1,
				cn2,
			)

			_ = testutils.ExecSQL(
				t,
				db,
				cn1,
				"truncate table "+table+";",
				"insert into "+table+" select result, result from generate_series(1,5000) g;",
			)

			_ = testutils.ExecSQL(
				t,
				db,
				cn1,
				"insert into "+table+" select result, result from generate_series(5001,10000) g;",
			)

			_ = testutils.ExecSQL(
				t,
				db,
				cn1,
				"insert into "+table+" select result, result from generate_series(10001,15000) g;",
			)

			committedAt := testutils.ExecSQL(
				t,
				db,
				cn1,
				"insert into "+table+" select result, result from generate_series(15001,20000) g;",
			)

			// case 1
			// test for local LocalTable that need upgrade row level lock to table level lock
			exec1 := testutils.GetSQLExecutor(cn1)
			err = exec1.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					res, err := txn.Exec(
						"delete from "+table+" where id > 1",
						executor.StatementOption{},
					)
					require.NoError(t, err)
					res.Close()
					return nil
				},
				executor.Options{}.
					WithDatabase(db).
					WithMinCommittedTS(committedAt),
			)
			require.NoError(t, err)

			_ = testutils.ExecSQL(
				t,
				db,
				cn1,
				"truncate table "+table+";",
				"insert into "+table+" select result, result from generate_series(1,5000) g;",
			)

			_ = testutils.ExecSQL(
				t,
				db,
				cn1,
				"insert into "+table+" select result, result from generate_series(5001,10000) g;",
			)

			_ = testutils.ExecSQL(
				t,
				db,
				cn1,
				"insert into "+table+" select result, result from generate_series(10001,15000) g;",
			)

			committedAt = testutils.ExecSQL(
				t,
				db,
				cn1,
				"insert into "+table+" select result, result from generate_series(15001,20000) g;",
			)

			// case 2
			// test for remote LockTable that need upgrade row level lock to table level lock
			exec2 := testutils.GetSQLExecutor(cn2)
			err = exec2.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					res, err := txn.Exec(
						"delete from "+table+" where id > 1",
						executor.StatementOption{},
					)
					require.NoError(t, err)
					res.Close()
					return nil
				},
				executor.Options{}.
					WithDatabase(db).
					WithMinCommittedTS(committedAt),
			)
			require.NoError(t, err)
		},
	)
}

func TestIssue19551(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000)
			defer cancel()

			var allocator lockservice.LockTableAllocator
			c.ForeachServices(
				func(s embed.ServiceOperator) bool {
					if s.ServiceType() != metadata.ServiceType_TN {
						return true
					}

					tn := s.RawService().(tnservice.Service)
					allocator = tn.GetLockTableAllocator()
					return false
				},
			)

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)
			lockSID := lockservice.GetLockServiceByServiceID(cn1.ServiceID()).GetServiceID()

			db := testutils.GetDatabaseName(t)
			table := "t"

			testutils.CreateTableAndWaitCNApplied(
				t,
				db,
				table,
				"create table "+table+" (id int primary key, v int)",
				cn1,
			)

			// workflow:
			// start txn1, txn2 on cn1
			// txn1, txn2 both do insert before CN is marked invalid
			// mark cn1 invalid
			// txn1 commits first, gets ErrCannotCommitOnInvalidCN, triggers markAllActiveTxnAborted
			// wait abort active txn completed (service resume)
			// txn2 commits, gets ErrTxnClosed (already marked aborted)
			// start txn3 and commit will success
			//
			// Note: txn1 may also get ErrTxnClosed if a background task (e.g. mo_table_stats)
			// commits while CN is invalid and triggers markAllActiveTxnAborted before txn1.

			var wg sync.WaitGroup
			wg.Add(2)

			txn1StartedC := make(chan struct{})
			txn2StartedC := make(chan struct{})
			txn1InsertedC := make(chan struct{})
			txn2InsertedC := make(chan struct{})
			invalidMarkedC := make(chan struct{})
			txn1CommittedC := make(chan struct{})

			// txn1
			exec := testutils.GetSQLExecutor(cn1)
			go func() {
				defer wg.Done()
				defer close(txn1CommittedC)

				err := exec.ExecTxn(
					ctx,
					func(txn executor.TxnExecutor) error {
						close(txn1StartedC)

						res, err := txn.Exec(
							"insert into "+table+" values (1, 1)",
							executor.StatementOption{},
						)
						if err != nil {
							return err
						}
						res.Close()

						close(txn1InsertedC)
						// wait until CN is marked invalid, then return to trigger commit
						<-invalidMarkedC
						return nil
					},
					executor.Options{}.WithDatabase(db),
				)
				require.Error(t, err)
				// txn1 gets ErrCannotCommitOnInvalidCN when its commit reaches TN validation.
				// However, if a background task (e.g. mo_table_stats) commits while CN is invalid
				// before txn1, it triggers markAllActiveTxnAborted first, marking txn1 as aborted,
				// in which case txn1 gets ErrTxnClosed instead.
				require.Truef(
					t,
					moerr.IsMoErrCode(err, moerr.ErrCannotCommitOnInvalidCN) ||
						moerr.IsMoErrCode(err, moerr.ErrTxnClosed),
					fmt.Sprintf("got: %v", err))
			}()

			go func() {
				defer wg.Done()

				err := exec.ExecTxn(
					ctx,
					func(txn executor.TxnExecutor) error {
						close(txn2StartedC)

						res, err := txn.Exec(
							"insert into "+table+" values (2, 2)",
							executor.StatementOption{},
						)
						if err != nil {
							return err
						}
						res.Close()

						close(txn2InsertedC)
						// wait txn1 committed first, which triggers markAllActiveTxnAborted
						<-txn1CommittedC

						// wait service resume, which means markAllActiveTxnAborted + Resume completed
						for {
							if !allocator.HasInvalidService(lockSID) {
								break
							}
							time.Sleep(time.Millisecond * 100)
						}
						return nil
					},
					executor.Options{}.WithDatabase(db),
				)
				require.Error(t, err)
				require.Truef(
					t,
					moerr.IsMoErrCode(err, moerr.ErrTxnClosed),
					fmt.Sprintf("got: %v", err))

			}()

			<-txn1StartedC
			<-txn2StartedC
			// wait both txns to finish insert before marking CN invalid,
			// so that commit happens immediately after invalidMarkedC is closed,
			// minimizing the window for background tasks to interfere.
			<-txn1InsertedC
			<-txn2InsertedC

			// mark cn1 invalid
			allocator.AddInvalidService(lockSID)
			require.True(t, allocator.HasInvalidService(lockSID))
			close(invalidMarkedC)
			wg.Wait()

			// service is resume, txn3 can commit ok
			err = exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					res, err := txn.Exec(
						"insert into "+table+" values (3, 3)",
						executor.StatementOption{},
					)
					if err != nil {
						return err
					}
					res.Close()

					return nil
				},
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
		},
	)
}

func TestSpeedupAbortAllTxn(t *testing.T) {
	c, err := embed.NewCluster(
		embed.WithPreStart(
			func(so embed.ServiceOperator) {
				if so.ServiceType() == metadata.ServiceType_CN {
					so.Adjust(
						func(sc *embed.ServiceConfig) {
							sc.CN.Txn.MaxActive = 1
						},
					)
				}
			},
		),
	)
	require.NoError(t, err)
	require.NoError(t, c.Start())
	defer func() {
		require.NoError(t, c.Close())
	}()

	op, err := c.GetCNService(0)
	require.NoError(t, err)

	waitC := make(chan struct{})
	cn := op.RawService().(cnservice.Service)
	eng := cn.GetEngine().(*disttae.Engine)
	logtailClient := eng.PushClient()
	logtailClient.SetReconnectHandler(func() {
		waitC <- struct{}{}
	})

	c1 := make(chan struct{})
	c2 := make(chan struct{})
	actionC := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	taskservice.DebugCtlTaskFramework(true)

	// active will commit failed
	go func() {
		defer wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
		defer cancel()
		exec := cn.GetSQLExecutor()
		err := exec.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				res, err := txn.Exec(
					"create database TestSpeedupAbortAllTxn",
					executor.StatementOption{},
				)
				require.NoError(t, err)
				res.Close()
				close(c1)

				// wait txn in active
				<-c2
				close(actionC)

				<-waitC

				// Wait for push client to be fully ready before returning.
				// reconnectHandler is called before push client is fully recovered,
				// so we need to wait here to avoid committing when push client is not ready.
				for !eng.PushClient().IsSubscriberReady() {
					time.Sleep(time.Millisecond * 10)
				}

				return nil
			},
			executor.Options{}.WithDatabase("mo_catalog").WithUserTxn(),
		)
		require.NoError(t, err)
	}()

	// wait active txn will canceled
	go func() {
		defer wg.Done()

		<-c1

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
		defer cancel()

		tc := cn.GetTxnClient()
		_, err := tc.New(
			ctx,
			timestamp.Timestamp{},
			client.WithUserTxn(),
			client.WithWaitActiveHandle(
				func() {
					close(c2)
				},
			),
		)
		require.NoError(t, err)
	}()

	<-actionC
	require.NoError(t, logtailClient.Disconnect())
	waitLogtailResume(cn)

	wg.Wait()
}

func waitLogtailResume(cn cnservice.Service) {
	exec := cn.GetSQLExecutor()
	fn := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		res, err := exec.Exec(
			ctx,
			"select * from mo_tables",
			executor.Options{}.WithDatabase("mo_catalog"),
		)
		if err != nil {
			return err
		}
		res.Close()
		return nil
	}

	for {
		if err := fn(); err == nil {
			return
		}
		time.Sleep(time.Second)
	}
}

// #15087
func TestLikePatternPlus(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			exec := testutils.GetSQLExecutor(cn)
			require.NotNil(t, exec)

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))

			cases := []struct {
				sql      string
				expected bool
			}{
				{sql: "select '__++' like '__++%';", expected: true},
				{sql: "select '__++__' like '__\\\\+';", expected: false},
				{sql: "select '__++__' like '__+';", expected: false},
			}

			for _, c := range cases {
				res, err := exec.Exec(ctx, c.sql, executor.Options{})
				require.NoError(t, err, c.sql)

				var got bool
				res.ReadRows(
					func(rows int, cols []*vector.Vector) bool {
						got = executor.GetFixedRows[bool](cols[0])[0]
						return true
					},
				)
				res.Close()

				require.Equal(t, c.expected, got, c.sql)
			}
		},
	)
}

func TestFaultInjection(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()

			ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))

			exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
			require.NotNil(t, exec)

			{
				_, err := exec.Exec(
					ctx,
					"select fault_inject('all.','enable_fault_injection','');",
					executor.Options{})
				require.NoError(t, err)
			}
		})
}
