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

package disttae

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCatalogReadResultsReleaseBatchOwnership(t *testing.T) {
	t.Run("delete table releases zero one and two acquired results", func(t *testing.T) {
		readErr := errors.New("catalog read failed")
		successTableRowID := types.RandomRowid()
		successColumnRowID := types.RandomRowid()
		failedWriteTableRowID := types.RandomRowid()
		failedWriteColumnRowID := types.RandomRowid()

		tests := []struct {
			name        string
			exec        func(*testing.T, int, *mpool.MPool, *mpool.MPool) (executor.Result, error)
			cancelWrite bool
			wantCalls   int
			wantErr     error
			wantPanic   string
			verify      func(*testing.T, *Transaction)
		}{
			{
				name: "first read error acquires no result",
				exec: func(_ *testing.T, _ int, _, _ *mpool.MPool) (executor.Result, error) {
					return executor.Result{}, readErr
				},
				wantCalls: 1,
				wantErr:   readErr,
			},
			{
				name: "second read error releases first result",
				exec: func(t *testing.T, call int, firstMP, _ *mpool.MPool) (executor.Result, error) {
					if call == 1 {
						return newRowIDResult(t, firstMP, types.RandomRowid()), nil
					}
					return executor.Result{}, readErr
				},
				wantCalls: 2,
				wantErr:   readErr,
			},
			{
				name: "panic after second read releases both distinct results",
				exec: func(t *testing.T, call int, firstMP, secondMP *mpool.MPool) (executor.Result, error) {
					if call == 1 {
						return newRowIDResult(t, firstMP, types.RandomRowid()), nil
					}
					return newRowIDResult(t, secondMP, types.RandomRowid(), types.RandomRowid()), nil
				},
				wantCalls: 2,
				wantPanic: "delete table 42-tbl failed 2, 1",
			},
			{
				name: "successful delete retains copied rowids in catalog writes",
				exec: func(t *testing.T, call int, firstMP, secondMP *mpool.MPool) (executor.Result, error) {
					if call == 1 {
						return newRowIDResult(t, firstMP, successTableRowID), nil
					}
					return newRowIDResult(t, secondMP, successColumnRowID), nil
				},
				wantCalls: 2,
				verify: func(t *testing.T, txn *Transaction) {
					requireCatalogDeleteRowIDs(t, txn, successTableRowID, successColumnRowID)
				},
			},
			{
				name: "write error after second read releases both distinct results",
				exec: func(t *testing.T, call int, firstMP, secondMP *mpool.MPool) (executor.Result, error) {
					if call == 1 {
						return newRowIDResult(t, firstMP, failedWriteTableRowID), nil
					}
					return newRowIDResult(t, secondMP, failedWriteColumnRowID), nil
				},
				cancelWrite: true,
				wantCalls:   2,
				wantErr:     context.Canceled,
				verify: func(t *testing.T, txn *Transaction) {
					require.Empty(t, txn.writes, "failed catalog write must not enter the workspace")
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				firstMP := newResultOwnershipMPool(t)
				secondMP := newResultOwnershipMPool(t)
				firstBaseline := resultOwnershipBytes(firstMP)
				secondBaseline := resultOwnershipBytes(secondMP)
				calls := 0
				exec := executor.NewMemExecutor(func(string) (executor.Result, error) {
					calls++
					return tt.exec(t, calls, firstMP, secondMP)
				})
				_, db, txn, ctx := newResultOwnershipFixture(t, exec, true)
				if tt.cancelWrite {
					writeCtx, cancel := context.WithCancel(txn.proc.Ctx)
					cancel()
					txn.proc.Ctx = writeCtx
				}

				invoke := func() error {
					_, err := db.deleteTable(ctx, "tbl", true, false)
					return err
				}
				if tt.wantPanic != "" {
					require.PanicsWithValue(t, tt.wantPanic, func() { _ = invoke() })
				} else if tt.wantErr != nil {
					require.ErrorIs(t, invoke(), tt.wantErr)
				} else {
					require.NoError(t, invoke())
				}

				require.Equal(t, tt.wantCalls, calls)
				require.Equal(t, firstBaseline, resultOwnershipBytes(firstMP), "first result batch must be released")
				require.Equal(t, secondBaseline, resultOwnershipBytes(secondMP), "second result batch must be released")
				if tt.verify != nil {
					tt.verify(t, txn)
				}
			})
		}
	})

	t.Run("database delete releases malformed row result during panic", func(t *testing.T) {
		resultMP := newResultOwnershipMPool(t)
		baseline := resultOwnershipBytes(resultMP)
		calls := 0
		exec := executor.NewMemExecutor(func(string) (executor.Result, error) {
			calls++
			if calls == 1 {
				return executor.Result{Mp: resultMP}, nil
			}
			return newRowIDResult(t, resultMP, types.RandomRowid(), types.RandomRowid()), nil
		})
		eng, db, txn, ctx := newResultOwnershipFixture(t, exec, false)
		txn.databaseOps.addCreateDatabase(genDatabaseKey(1, "db"), 0, db)

		require.PanicsWithValue(t, "delete table failed: query failed", func() {
			_ = eng.Delete(ctx, "db", txn.op)
		})
		require.Equal(t, 2, calls)
		require.Equal(t, baseline, resultOwnershipBytes(resultMP), "database row result must be released")
	})

	t.Run("name lookup returns copied strings after releasing result", func(t *testing.T) {
		resultMP := newResultOwnershipMPool(t)
		baseline := resultOwnershipBytes(resultMP)
		exec := executor.NewMemExecutor(func(string) (executor.Result, error) {
			result := executor.NewMemResult(
				[]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
				resultMP,
			)
			result.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(result, 0, []string{"tbl"}))
			require.NoError(t, executor.AppendStringRows(result, 1, []string{"db"}))
			res := result.GetResult()
			t.Cleanup(res.Close)
			return res, nil
		})
		_, _, txn, ctx := newResultOwnershipFixture(t, exec, false)

		dbName, tableName, err := loadNameByIdFromStorage(ctx, txn.op, 1, 42)
		require.NoError(t, err)
		require.Equal(t, "db", dbName)
		require.Equal(t, "tbl", tableName)
		require.Equal(t, baseline, resultOwnershipBytes(resultMP), "name lookup result must be released")
	})
}

func newResultOwnershipFixture(
	t *testing.T,
	sqlExecutor executor.SQLExecutor,
	withTable bool,
) (*Engine, *txnDatabase, *Transaction, context.Context) {
	t.Helper()

	procMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(procMP) })
	proc := testutil.NewProcessWithMPool(t, "", procMP)
	t.Cleanup(proc.Free)

	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sqlExecutor)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, sqlExecutor)
		}
	})

	eng := &Engine{
		packerPool: fileservice.NewPool(
			1,
			func() *types.Packer { return types.NewPacker() },
			func(packer *types.Packer) { packer.Reset() },
			func(packer *types.Packer) { packer.Close() },
		),
	}
	previousServer, hadPreviousServer := rt.GetGlobalVariables(moruntime.ColexecServer)
	server := colexec.NewServer(eng.service)
	t.Cleanup(func() {
		if hadPreviousServer {
			rt.SetGlobalVariables(moruntime.ColexecServer, previousServer)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.ColexecServer, server)
		}
	})
	txn := &Transaction{
		proc:        proc,
		engine:      eng,
		tableCache:  new(sync.Map),
		tableOps:    newTableOps(),
		databaseOps: newDbOps(),
		tnStores:    []DNStore{{}},
	}
	t.Cleanup(func() {
		txn.Lock()
		defer txn.Unlock()
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	})
	op := newTxnOperatorForTestWithWorkspace(t, txn)
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	txn.op = op
	db := &txnDatabase{
		op:           op,
		accountId:    1,
		databaseId:   7,
		databaseName: "db",
	}
	if withTable {
		txn.tableOps.addCreateTable(
			genTableKey(1, "tbl", 7, "db"),
			0,
			&txnTable{
				accountId: 1,
				tableId:   42,
				tableName: "tbl",
				db:        db,
				tableDef: &planpb.TableDef{Cols: []*planpb.ColDef{
					{Name: "c", OriginName: "c"},
				}},
			},
		)
	}
	return eng, db, txn, defines.AttachAccountId(context.Background(), 1)
}

func newRowIDResult(t *testing.T, mp *mpool.MPool, rowIDs ...types.Rowid) executor.Result {
	t.Helper()
	result := executor.NewMemResult([]types.Type{types.T_Rowid.ToType()}, mp)
	result.NewBatchWithRowCount(len(rowIDs))
	require.NoError(t, executor.AppendFixedRows(result, 0, rowIDs))
	res := result.GetResult()
	t.Cleanup(res.Close)
	return res
}

func newResultOwnershipMPool(t *testing.T) *mpool.MPool {
	t.Helper()
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	return mp
}

func resultOwnershipBytes(mp *mpool.MPool) int64 {
	return mp.CurrNB() + mp.OnHeapCurrNB()
}

func requireCatalogDeleteRowIDs(
	t *testing.T,
	txn *Transaction,
	tableRowID types.Rowid,
	columnRowID types.Rowid,
) {
	t.Helper()
	require.Len(t, txn.writes, 2)
	require.Equal(t, uint64(catalog.MO_TABLES_ID), txn.writes[0].tableId)
	require.Equal(t, tableRowID, vector.GetFixedAtNoTypeCheck[types.Rowid](txn.writes[0].bat.Vecs[0], 0))
	require.Equal(t, uint64(catalog.MO_COLUMNS_ID), txn.writes[1].tableId)
	require.Equal(t, columnRowID, vector.GetFixedAtNoTypeCheck[types.Rowid](txn.writes[1].bat.Vecs[0], 0))
}
