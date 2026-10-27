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

package txnimpl

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/require"
)

func TestOversizeWALAfterApplyAppendRollsBack(t *testing.T) {
	ctx := context.Background()
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	t.Cleanup(func() {
		mgr.Stop()
		c.Close()
		require.NoError(t, driver.Close())
	})

	schema := catalog.MockSchemaAll(3, 1)
	schema.Extra.BlockMaxRows = 10000
	setup, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	db, err := setup.CreateDatabase("wal_size_rollback", "", "")
	require.NoError(t, err)
	rel, err := db.CreateRelation(schema)
	require.NoError(t, err)
	tableEntry := rel.GetMeta().(*catalog.TableEntry)
	require.NoError(t, setup.Commit(ctx))

	bat := catalog.MockBatch(schema, 1)
	t.Cleanup(bat.Close)
	appendRow := func(limit int) error {
		txn, err := mgr.StartTxn(nil)
		if err != nil {
			return err
		}
		defer func() {
			if txn.GetTxnState(false) == txnif.TxnStateActive {
				_ = txn.Rollback(ctx)
			}
		}()
		txnDB, err := txn.GetDatabase("wal_size_rollback")
		if err != nil {
			return err
		}
		txnRel, err := txnDB.GetRelationByName(schema.Name)
		if err != nil {
			return err
		}
		if err = txnRel.Append(ctx, bat); err != nil {
			return err
		}
		if limit > 0 {
			txn.GetStore().(*txnStore).cmdMgr.maxWalSize = limit
		}
		return txn.Commit(ctx)
	}

	// The workspace passes PrePrepare's size check. A one-byte final budget
	// forces rejection only after PreApplyCommit has applied the append.
	err = appendRow(1)
	require.ErrorContains(t, err, "WAL record is too large")
	require.Zero(t, tableEntry.GetRows())
	require.NoError(t, appendRow(0))
	require.Equal(t, uint64(1), tableEntry.GetRows())

	readTxn, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	readDB, err := readTxn.GetDatabase("wal_size_rollback")
	require.NoError(t, err)
	readRel, err := readDB.GetRelationByName(schema.Name)
	require.NoError(t, err)
	pkIdx := schema.GetSingleSortKeyIdx()
	pk := bat.Vecs[pkIdx].Get(0)
	value, isNull, err := readRel.GetValueByFilter(ctx, handle.NewEQFilter(pk), pkIdx)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, pk, value)
	require.NoError(t, readTxn.Commit(ctx))
}
