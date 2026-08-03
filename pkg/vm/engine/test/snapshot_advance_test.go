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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type snapshotAdvanceTombstoneMode int

const (
	snapshotAdvanceInMemoryTombstone snapshotAdvanceTombstoneMode = iota
	snapshotAdvancePersistedTombstone
)

type snapshotAdvanceHarness struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	pack   *testutil.EnginePack
	schema *catalog2.Schema
	rel    engine.Relation
	txn    client.TxnOperator
}

func newSnapshotAdvanceHarness(t *testing.T, mode snapshotAdvanceTombstoneMode) *snapshotAdvanceHarness {
	t.Helper()

	const (
		databaseName = "db1"
		tableName    = "test1"
		rowCount     = 20
	)

	ctx, cancel := context.WithTimeout(
		context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(0)),
		time.Minute,
	)
	pack := testutil.InitEnginePack(testutil.TestOptions{}, t)
	h := &snapshotAdvanceHarness{
		t:      t,
		ctx:    ctx,
		cancel: cancel,
		pack:   pack,
		schema: catalog2.MockSchemaEnhanced(1, 0, 2),
	}
	h.schema.Name = tableName

	createTxn := pack.StartCNTxn()
	_, h.rel = pack.CreateDBAndTable(createTxn, databaseName, h.schema)
	require.NoError(t, createTxn.Commit(ctx))

	var err error
	var insertTxn client.TxnOperator
	_, h.rel, insertTxn, err = pack.D.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	insertBat := containers.ToCNBatch(catalog2.MockBatch(h.schema, rowCount))
	require.NoError(t, testutil.WriteToRelation(ctx, insertTxn, h.rel, insertBat, false, true))
	require.NoError(t, insertTxn.Commit(ctx))

	// Give FlushTable both an appendable object and committed tombstones so it
	// deterministically rewrites the surviving rows into a new object.
	var committedDeleteTxn client.TxnOperator
	_, h.rel, committedDeleteTxn, err = pack.D.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	committedDeletes := h.collectDeletes(committedDeleteTxn, h.rel, rowCount/2)
	require.NoError(t, testutil.WriteToRelation(
		ctx, committedDeleteTxn, h.rel, committedDeletes, true, true,
	))
	require.NoError(t, committedDeleteTxn.Commit(ctx))

	h.txn, err = pack.D.NewTxnOperator(
		ctx,
		pack.D.Now(),
		client.WithTxnMode(pbtxn.TxnMode_Pessimistic),
		client.WithTxnIsolation(pbtxn.TxnIsolation_RC),
	)
	require.NoError(t, err)
	db, err := pack.D.Engine.Database(ctx, databaseName, h.txn)
	require.NoError(t, err)
	h.rel, err = db.Relation(ctx, tableName, nil)
	require.NoError(t, err)

	uncommittedDeletes := h.collectDeletes(h.txn, h.rel, rowCount/2)
	switch mode {
	case snapshotAdvanceInMemoryTombstone:
		require.NoError(t, testutil.WriteToRelation(
			ctx, h.txn, h.rel, uncommittedDeletes, true, true,
		))
	case snapshotAdvancePersistedTombstone:
		h.writePersistedDeletes(uncommittedDeletes)
	default:
		t.Fatalf("unknown tombstone mode %d", mode)
	}
	require.Zero(t, h.countRows(h.txn, h.rel))
	return h
}

func (h *snapshotAdvanceHarness) close() {
	h.t.Helper()
	if h.txn != nil && h.txn.Status() == pbtxn.TxnStatus_Active {
		require.NoError(h.t, h.txn.Rollback(h.ctx))
	}
	h.pack.Close()
	h.cancel()
}

func (h *snapshotAdvanceHarness) collectDeletes(
	txn client.TxnOperator,
	relation engine.Relation,
	limit int,
) *batch.Batch {
	h.t.Helper()

	reader, err := testutil.GetRelationReader(h.ctx, h.pack.D, txn, relation, nil, h.pack.Mp, h.t)
	require.NoError(h.t, err)
	defer reader.Close()

	deletes := batch.NewWithSize(2)
	deletes.Attrs = []string{catalog.Row_ID, h.schema.GetPrimaryKey().Name}
	deletes.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	deletes.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	ret := testutil.EmptyBatchFromSchema(h.schema)
	for deletes.RowCount() < limit {
		done, err := reader.Read(
			h.ctx,
			[]string{h.schema.GetPrimaryKey().Name, catalog.Row_ID},
			nil,
			h.pack.Mp,
			ret,
		)
		require.NoError(h.t, err)
		if done {
			break
		}

		rows := min(ret.RowCount(), limit-deletes.RowCount())
		for i := 0; i < rows; i++ {
			require.NoError(h.t, vector.AppendFixed(
				deletes.Vecs[0],
				vector.GetFixedAtNoTypeCheck[types.Rowid](ret.Vecs[1], i),
				false,
				h.pack.Mp,
			))
			require.NoError(h.t, vector.AppendFixed(
				deletes.Vecs[1],
				vector.GetFixedAtNoTypeCheck[int32](ret.Vecs[0], i),
				false,
				h.pack.Mp,
			))
		}
		deletes.SetRowCount(deletes.Vecs[0].Length())
	}
	require.Equal(h.t, limit, deletes.RowCount())
	return deletes
}

func (h *snapshotAdvanceHarness) writePersistedDeletes(deletes *batch.Batch) {
	h.t.Helper()

	ws := h.txn.GetWorkspace()
	ws.StartStatement()

	proc := h.rel.GetProcess().(*process.Process)
	w := colexec.NewCNS3TombstoneWriter(
		proc.Mp(), proc.GetFileService(), types.T_int32.ToType(), -1,
	)
	defer w.Close()
	require.NoError(h.t, w.Write(h.ctx, deletes))
	stats, err := w.Sync(h.ctx)
	require.NoError(h.t, err)
	require.Len(h.t, stats, 1)

	statsBat := batch.NewWithSize(1)
	statsBat.Attrs = []string{catalog.ObjectMeta_ObjectStats}
	statsBat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(h.t, vector.AppendBytes(
		statsBat.Vecs[0], stats[0].Marshal(), false, h.pack.Mp,
	))
	statsBat.SetRowCount(1)

	transaction := ws.(*disttae.Transaction)
	require.NoError(h.t, transaction.WriteFile(
		disttae.DELETE,
		catalog.System_Account,
		h.rel.GetDBID(h.ctx),
		h.rel.GetTableID(h.ctx),
		"db1",
		"test1",
		stats[0].ObjectLocation().String(),
		statsBat,
		h.pack.D.Engine.GetTNServices()[0],
	))
	require.NoError(h.t, ws.IncrStatementID(h.ctx, false))
	ws.EndStatement()
	ws.UpdateSnapshotWriteOffset()
}

func (h *snapshotAdvanceHarness) countRows(
	txn client.TxnOperator,
	relation engine.Relation,
) int {
	h.t.Helper()

	reader, err := testutil.GetRelationReader(h.ctx, h.pack.D, txn, relation, nil, h.pack.Mp, h.t)
	require.NoError(h.t, err)
	defer reader.Close()

	ret := testutil.EmptyBatchFromSchema(h.schema)
	rows := 0
	for {
		done, err := reader.Read(h.ctx, ret.Attrs, nil, h.pack.Mp, ret)
		require.NoError(h.t, err)
		if done {
			return rows
		}
		rows += ret.RowCount()
	}
}

func (h *snapshotAdvanceHarness) flushAndAdvance() {
	h.t.Helper()

	require.NoError(h.t, h.pack.T.GetDB().FlushTable(
		h.ctx,
		catalog.System_Account,
		h.rel.GetDBID(h.ctx),
		h.rel.GetTableID(h.ctx),
		types.TimestampToTS(h.pack.D.Now()),
	))
	require.NoError(h.t, h.txn.GetWorkspace().AdvanceSnapshot(h.ctx, h.pack.D.Now()))
}

func (h *snapshotAdvanceHarness) transferAtStatementBoundary() {
	h.t.Helper()

	ws := h.txn.GetWorkspace()
	ws.StartStatement()
	forceTransferCtx := context.WithValue(h.ctx, disttae.UT_ForceTransCheck{}, "yes")
	require.NoError(h.t, ws.IncrStatementID(forceTransferCtx, false))
	ws.EndStatement()
	ws.UpdateSnapshotWriteOffset()
}

func Test_RCSnapshotAdvancePreservesUncommittedDeletes(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode snapshotAdvanceTombstoneMode
	}{
		{name: "in-memory tombstone", mode: snapshotAdvanceInMemoryTombstone},
		{name: "persisted tombstone", mode: snapshotAdvancePersistedTombstone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newSnapshotAdvanceHarness(t, tc.mode)
			defer h.close()

			originalSnapshot := h.txn.SnapshotTS()
			h.flushAndAdvance()
			require.True(t, originalSnapshot.Less(h.txn.SnapshotTS()))
			require.Zero(t, h.countRows(h.txn, h.rel))
		})
	}
}

func Test_RCSnapshotAdvanceHarnessRollback(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode snapshotAdvanceTombstoneMode
	}{
		{name: "in-memory tombstone", mode: snapshotAdvanceInMemoryTombstone},
		{name: "persisted tombstone", mode: snapshotAdvancePersistedTombstone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newSnapshotAdvanceHarness(t, tc.mode)
			defer h.close()

			h.flushAndAdvance()
			require.Zero(t, h.countRows(h.txn, h.rel))
			require.NoError(t, h.txn.Rollback(h.ctx))

			_, relation, txn, err := h.pack.D.GetTable(h.ctx, "db1", "test1")
			require.NoError(t, err)
			require.Equal(t, 10, h.countRows(txn, relation))
			require.NoError(t, txn.Commit(h.ctx))
			h.txn = nil
		})
	}
}

func Test_RCSnapshotAdvanceHarnessCommit(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode snapshotAdvanceTombstoneMode
	}{
		{name: "in-memory tombstone", mode: snapshotAdvanceInMemoryTombstone},
		{name: "persisted tombstone", mode: snapshotAdvancePersistedTombstone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newSnapshotAdvanceHarness(t, tc.mode)
			defer h.close()

			h.flushAndAdvance()
			require.Zero(t, h.countRows(h.txn, h.rel))
			require.NoError(t, h.txn.Commit(h.ctx))
			h.txn = nil

			_, relation, txn, err := h.pack.D.GetTable(h.ctx, "db1", "test1")
			require.NoError(t, err)
			require.Zero(t, h.countRows(txn, relation))
			require.NoError(t, txn.Commit(h.ctx))
		})
	}
}

func Test_RCSnapshotAdvanceHarnessSubsequentStatement(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode snapshotAdvanceTombstoneMode
	}{
		{name: "in-memory tombstone", mode: snapshotAdvanceInMemoryTombstone},
		{name: "persisted tombstone", mode: snapshotAdvancePersistedTombstone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newSnapshotAdvanceHarness(t, tc.mode)
			defer h.close()

			h.flushAndAdvance()
			require.Zero(t, h.countRows(h.txn, h.rel))

			for range 3 {
				advancedSnapshot := h.txn.Txn().SnapshotTS
				h.transferAtStatementBoundary()
				require.True(t, advancedSnapshot.LessEq(h.txn.Txn().SnapshotTS))
				require.Zero(t, h.countRows(h.txn, h.rel))
			}
		})
	}
}

func Test_RCSnapshotAdvanceHarnessStatementRollback(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mode     snapshotAdvanceTombstoneMode
		commit   bool
		wantRows int
	}{
		{name: "in-memory tombstone/commit", mode: snapshotAdvanceInMemoryTombstone, commit: true},
		{name: "in-memory tombstone/rollback", mode: snapshotAdvanceInMemoryTombstone, wantRows: 10},
		{name: "persisted tombstone/commit", mode: snapshotAdvancePersistedTombstone, commit: true},
		{name: "persisted tombstone/rollback", mode: snapshotAdvancePersistedTombstone, wantRows: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newSnapshotAdvanceHarness(t, tc.mode)
			defer h.close()

			// Model a statement that advances its snapshot and then fails. Persisted
			// tombstone transfer appends replacement writes after this statement's
			// offset, so RollbackLastStatement removes them.
			ws := h.txn.GetWorkspace()
			ws.StartStatement()
			require.NoError(t, ws.IncrStatementID(h.ctx, false))
			h.flushAndAdvance()
			require.Zero(t, h.countRows(h.txn, h.rel))
			require.NoError(t, ws.RollbackLastStatement(h.ctx))
			ws.EndStatement()

			// The next statement must transfer the original tombstones again before
			// the transaction is allowed to commit or continue reading.
			h.transferAtStatementBoundary()
			require.Zero(t, h.countRows(h.txn, h.rel))

			if tc.commit {
				require.NoError(t, h.txn.Commit(h.ctx))
			} else {
				require.NoError(t, h.txn.Rollback(h.ctx))
			}
			h.txn = nil

			_, relation, txn, err := h.pack.D.GetTable(h.ctx, "db1", "test1")
			require.NoError(t, err)
			require.Equal(t, tc.wantRows, h.countRows(txn, relation))
			require.NoError(t, txn.Commit(h.ctx))
		})
	}
}
