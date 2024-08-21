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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
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

			db := getDatabaseName(t)
			table := "t"

			createTableAndWaitCNApplied(
				t,
				db,
				table,
				"create table "+table+" (id int primary key, v int)",
				cn1,
				cn2,
			)

			committedAt := execSQL(
				t,
				db,
				"insert into "+table+" values (1, 1)",
				cn1,
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
				exec1 := getSQLExecutor(cn1)
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
				exec := getSQLExecutor(cn2)

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
