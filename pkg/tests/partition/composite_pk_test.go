// Copyright 2021 - 2025 Matrix Origin
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

package partition

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCompositePKWithHashPartition(t *testing.T) {

	var txns sync.Map
	txn1 := "txn1"
	txn1C := make(chan struct{})
	txn2CompletedC := make(chan struct{})
	var targetTable atomic.Uint64
	var once sync.Once
	fn := func(txnID []byte, tableID uint64, rows [][]byte) {
		v, ok := txns.Load(txn1)
		if ok && bytes.Equal(txnID, v.([]byte)) && tableID == targetTable.Load() {
			once.Do(func() {
				close(txn1C)
			})
			<-txn2CompletedC
		}
	}

	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			sql := "create table %s (a varchar(10), b varchar(10), c int, primary key(a, b)) partition by hash(a) partitions 10"
			sql = fmt.Sprintf(sql, t.Name())

			testutils.ExecSQL(
				t,
				db,
				cn,
				sql,
			)

			testutils.ExecSQL(
				t,
				db,
				cn,
				fmt.Sprintf("insert into %s values('a', 'b', 1)", t.Name()),
			)

			wg := sync.WaitGroup{}
			wg.Add(2)

			exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*60000)
			defer cancel()

			query := fmt.Sprintf("select c from %s where a = 'a' and b = 'b' for update", t.Name())
			update := fmt.Sprintf("update %s set c = c + 1 where a = 'a' and b = 'b'", t.Name())

			// txn1
			go func() {
				defer wg.Done()

				err := exec.ExecTxn(
					ctx,
					func(txn executor.TxnExecutor) error {
						txnOp := txn.Txn()
						txns.Store(txn1, txnOp.Txn().ID)

						targetTable.Store(testutils.GetTableID(t, db, t.Name(), txn))
						require.NotEqual(t, uint64(0), targetTable.Load())

						txn.Use(db)
						res, err := txn.Exec(query, executor.StatementOption{})
						require.NoError(t, err)

						var v int32
						res.ReadRows(
							func(rows int, cols []*vector.Vector) bool {
								v = executor.GetFixedRows[int32](cols[0])[0]
								return false
							},
						)
						res.Close()
						require.Equal(t, int32(2), v)
						return nil
					},
					executor.Options{}.WithDatabase(db),
				)
				require.NoError(t, err)
			}()

			// txn2
			go func() {
				defer wg.Done()

				err := exec.ExecTxn(
					ctx,
					func(txn executor.TxnExecutor) error {
						<-txn1C

						res, err := txn.Exec(query, executor.StatementOption{})
						require.NoError(t, err)

						var v int32
						res.ReadRows(
							func(rows int, cols []*vector.Vector) bool {
								v = executor.GetFixedRows[int32](cols[0])[0]
								return false
							},
						)
						res.Close()
						require.Equal(t, int32(1), v)

						_, err = txn.Exec(update, executor.StatementOption{})
						require.NoError(t, err)
						return nil
					},
					executor.Options{}.WithDatabase(db),
				)
				require.NoError(t, err)
				close(txn2CompletedC)
			}()

			wg.Wait()
		},
		embed.WithCNCount(1),
		embed.WithPreStart(
			func(op embed.ServiceOperator) {
				op.Adjust(
					func(sc *embed.ServiceConfig) {
						if op.ServiceType() == metadata.ServiceType_CN {
							sc.CN.LockService.BeforeLock = fn
						}
					},
				)
			},
		),
	)
}
