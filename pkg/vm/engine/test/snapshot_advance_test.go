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
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type snapshotAdvanceTombstoneMode int

const (
	snapshotAdvanceInMemoryTombstone snapshotAdvanceTombstoneMode = iota
	snapshotAdvancePersistedTombstone
	snapshotAdvanceMixedTombstones
)

type snapshotAdvanceHarness struct {
	t          *testing.T
	ctx        context.Context
	cancel     context.CancelFunc
	pack       *testutil.EnginePack
	schema     *catalog2.Schema
	rel        engine.Relation
	txn        client.TxnOperator
	deletedPKs []int32
}

func newSnapshotAdvanceHarness(t *testing.T, mode snapshotAdvanceTombstoneMode, stringKeys ...string) *snapshotAdvanceHarness {
	t.Helper()

	const (
		databaseName = "db1"
		tableName    = "test1"
	)

	rowCount := 20
	if len(stringKeys) > 0 {
		rowCount = len(stringKeys)
	}
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
	if len(stringKeys) > 0 {
		h.schema.ColDefs[0].Type = types.T_varchar.ToType()
	}

	createTxn := pack.StartCNTxn()
	_, h.rel = pack.CreateDBAndTable(createTxn, databaseName, h.schema)
	require.NoError(t, createTxn.Commit(ctx))

	var err error
	var insertTxn client.TxnOperator
	_, h.rel, insertTxn, err = pack.D.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)
	insertBat := containers.ToCNBatch(catalog2.MockBatch(h.schema, rowCount))
	if len(stringKeys) > 0 {
		insertBat.Vecs[0].Reset(h.schema.ColDefs[0].Type)
		for _, key := range stringKeys {
			require.NoError(t, vector.AppendBytes(insertBat.Vecs[0], []byte(key), false, pack.Mp))
		}
	}
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
	if len(stringKeys) == 0 {
		h.deletedPKs = slices.Clone(vector.MustFixedColNoTypeCheck[int32](uncommittedDeletes.Vecs[1]))
	}
	switch mode {
	case snapshotAdvanceInMemoryTombstone:
		require.NoError(t, testutil.WriteToRelation(
			ctx, h.txn, h.rel, uncommittedDeletes, true, true,
		))
	case snapshotAdvancePersistedTombstone:
		h.writePersistedDeletes(uncommittedDeletes)
	case snapshotAdvanceMixedTombstones:
		inMemory, err := uncommittedDeletes.Window(0, rowCount/4)
		require.NoError(t, err)
		defer inMemory.Clean(nil)
		persisted, err := uncommittedDeletes.Window(rowCount/4, rowCount/2)
		require.NoError(t, err)
		defer persisted.Clean(nil)
		require.NoError(t, testutil.WriteToRelation(ctx, h.txn, h.rel, inMemory, true, true))
		h.writePersistedDeletes(persisted)
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
	deletes.Vecs[1] = vector.NewVec(h.schema.GetPrimaryKey().Type)

	ret := testutil.EmptyBatchFromSchema(h.schema)
	defer ret.Clean(h.pack.Mp)
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
			require.NoError(h.t, deletes.Vecs[1].UnionOne(ret.Vecs[0], int64(i), h.pack.Mp))
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
	// Exercise the actual shared CLONE/SNAPSHOT/DDL advance protocol, including
	// its physical-only history timestamp, over real relocated tombstones.
	cloneTS, err := databranchutils.AdvanceLineageSnapshot(h.ctx, h.txn)
	require.NoError(h.t, err)
	require.Equal(h.t, h.txn.SnapshotTS().PhysicalTime-1, cloneTS)
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
		{name: "mixed tombstones", mode: snapshotAdvanceMixedTombstones},
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
		{name: "mixed tombstones", mode: snapshotAdvanceMixedTombstones},
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
		{name: "mixed tombstones", mode: snapshotAdvanceMixedTombstones},
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
		{name: "mixed tombstones", mode: snapshotAdvanceMixedTombstones},
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
		name              string
		mode              snapshotAdvanceTombstoneMode
		commit            bool
		wantRows          int
		commitImmediately bool
	}{
		{name: "in-memory tombstone/commit", mode: snapshotAdvanceInMemoryTombstone, commit: true},
		{name: "in-memory tombstone/rollback", mode: snapshotAdvanceInMemoryTombstone, wantRows: 10},
		{name: "persisted tombstone/commit", mode: snapshotAdvancePersistedTombstone, commit: true},
		{name: "persisted tombstone/rollback", mode: snapshotAdvancePersistedTombstone, wantRows: 10},
		{name: "in-memory tombstone/immediate commit", mode: snapshotAdvanceInMemoryTombstone, commit: true, commitImmediately: true},
		{name: "persisted tombstone/immediate commit", mode: snapshotAdvancePersistedTombstone, commit: true, commitImmediately: true},
		{name: "mixed tombstones/immediate commit", mode: snapshotAdvanceMixedTombstones, commit: true, commitImmediately: true},
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

			// Reading again and committing immediately are separate recovery paths.
			// Commit must also restore transfers removed by statement rollback.
			if !tc.commitImmediately {
				h.transferAtStatementBoundary()
				require.Zero(t, h.countRows(h.txn, h.rel))
			}

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

// A no-op UPDATE still deletes the old Rowid and inserts a replacement. Check
// exact keys, not just successful execution, across relocation and repeated refresh.
func TestRCLineageSnapshotPreservesUniqueReplacements(t *testing.T) {
	for _, mode := range []snapshotAdvanceTombstoneMode{snapshotAdvanceInMemoryTombstone, snapshotAdvancePersistedTombstone, snapshotAdvanceMixedTombstones} {
		t.Run(map[snapshotAdvanceTombstoneMode]string{snapshotAdvanceInMemoryTombstone: "in-memory", snapshotAdvancePersistedTombstone: "persisted", snapshotAdvanceMixedTombstones: "mixed"}[mode], func(t *testing.T) {
			h := newSnapshotAdvanceHarness(t, mode)
			defer h.close()
			replacements := batch.NewWithSize(1)
			defer replacements.Clean(h.pack.Mp)
			replacements.Attrs = []string{h.schema.GetPrimaryKey().Name}
			replacements.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			for _, pk := range h.deletedPKs {
				require.NoError(t, vector.AppendFixed(replacements.Vecs[0], pk, false, h.pack.Mp))
			}
			replacements.SetRowCount(len(h.deletedPKs))
			require.NoError(t, testutil.WriteToRelation(h.ctx, h.txn, h.rel, replacements, false, true))
			require.Equal(t, len(h.deletedPKs), h.countRows(h.txn, h.rel))

			// Fail a second relation lookup during the same mixed transfer. This
			// proves successful resolution is reused, rather than just prefilled.
			if mode == snapshotAdvanceMixedTombstones {
				fault.Enable()
				defer fault.Disable()
				require.NoError(t, fault.AddFaultPoint(h.ctx,
					objectio.FJ_CNReenterSnapshotOffsetOnGetTable, "2:::", "echo", 0, "", false))
				defer fault.RemoveFaultPoint(h.ctx, objectio.FJ_CNReenterSnapshotOffsetOnGetTable)
			}
			h.flushAndAdvance()
			if mode == snapshotAdvanceMixedTombstones {
				_, err := fault.RemoveFaultPoint(h.ctx, objectio.FJ_CNReenterSnapshotOffsetOnGetTable)
				require.NoError(t, err)
				fault.Disable()
			}
			assertKeys := func() {
				require.Equal(t, len(h.deletedPKs), h.countRows(h.txn, h.rel))
				rows := h.collectDeletes(h.txn, h.rel, len(h.deletedPKs))
				defer rows.Clean(h.pack.Mp)
				require.ElementsMatch(t, h.deletedPKs, vector.MustFixedColNoTypeCheck[int32](rows.Vecs[1]))
			}
			assertKeys()
			// A second lineage refresh must retain the same replacement set.
			_, err := databranchutils.AdvanceLineageSnapshot(h.ctx, h.txn)
			require.NoError(t, err)
			assertKeys()
			require.NoError(t, h.txn.Rollback(h.ctx))
			h.txn = nil
			_, rel, txn, err := h.pack.D.GetTable(h.ctx, "db1", "test1")
			require.NoError(t, err)
			h.txn, h.rel = txn, rel
			assertKeys()
		})
	}
}

type failTransferReadFS struct {
	fileservice.FileService
	armed       atomic.Bool
	fired       atomic.Bool
	failure     error
	readStarted chan struct{}
}

func (f *failTransferReadFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	// The tombstone source requests Rowid and PK together after flow construction;
	// coarse filtering reads a single metadata extent.
	if len(v.Entries) > 1 && f.armed.Swap(false) {
		f.fired.Store(true)
		if f.readStarted != nil {
			close(f.readStarted)
			<-ctx.Done()
			return ctx.Err()
		}
		return f.failure
	}
	return f.FileService.Read(ctx, v)
}
func TestTransferPartialIORecovery(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "read failure"
		if canceled {
			name = "canceled read"
		}
		t.Run(name, func(t *testing.T) {
			h := newSnapshotAdvanceHarness(t, snapshotAdvanceMixedTombstones)
			defer h.close()
			ws := h.txn.GetWorkspace()
			ws.StartStatement()
			require.NoError(t, ws.IncrStatementID(h.ctx, false))
			require.NoError(t, h.pack.T.GetDB().FlushTable(h.ctx, catalog.System_Account, h.rel.GetDBID(h.ctx), h.rel.GetTableID(h.ctx), types.TimestampToTS(h.pack.D.Now())))
			proc := h.rel.GetProcess().(*process.Process)
			original := proc.GetFileService()
			defer proc.SetFileService(original)
			fs, err := fileservice.Get[fileservice.FileService](original, defines.SharedFileServiceName)
			require.NoError(t, err)
			var failure error = errors.New("injected persisted transfer read failure")
			ctx, cancel := context.WithCancel(h.ctx)
			defer cancel()
			if canceled {
				failure = context.Canceled
			}
			wrapped := &failTransferReadFS{FileService: fs, failure: failure}
			if canceled {
				wrapped.readStarted = make(chan struct{})
				done := make(chan struct{})
				go func() {
					defer close(done)
					select {
					case <-wrapped.readStarted:
						cancel()
					case <-ctx.Done():
					}
				}()
				defer func() { cancel(); <-done }()
			}
			wrapped.armed.Store(true)
			proc.SetFileService(wrapped)
			snapshot := h.txn.SnapshotTS()
			before := proc.Mp().CurrNB()
			_, err = databranchutils.AdvanceLineageSnapshot(ctx, h.txn)
			require.True(t, snapshot.Less(h.txn.SnapshotTS()), "failure occurs after snapshot advancement")
			require.True(t, wrapped.fired.Load(), "must reach constructed flow source IO")
			require.Equal(t, before, proc.Mp().CurrNB(), "failed flow must release transient batch memory")
			require.ErrorIs(t, err, failure)
			require.Equal(t, 5, h.countRows(h.txn, h.rel), "memory deletes have moved, persisted deletes have not")
			require.NoError(t, ws.RollbackLastStatement(h.ctx))
			ws.EndStatement()
			require.NoError(t, h.txn.Commit(h.ctx))
			h.txn = nil
			_, rel, txn, err := h.pack.D.GetTable(h.ctx, "db1", "test1")
			require.NoError(t, err)
			h.txn, h.rel = txn, rel
			require.Zero(t, h.countRows(txn, rel), "both halves must persist after immediate COMMIT")

		})
	}
}

// 129 maximum-width PKs cross the byte boundary and leave a one-row tail;
// cardinality is the minimum boundary input, not a performance workload.
func TestRCMemoryTransferWidePKBoundary(t *testing.T) {
	keys := make([]string, 258)
	suffix := strings.Repeat("x", types.MaxVarcharLen-8)
	for i := range keys {
		keys[i] = fmt.Sprintf("%08d", i) + suffix
	}
	h := newSnapshotAdvanceHarness(t, snapshotAdvanceInMemoryTombstone, keys...)
	defer h.close()
	// Read the exact target identities through a separate transaction, then
	// inspect deletion entries: zero visible rows alone could mask bad pairing.
	require.NoError(t, h.pack.T.GetDB().FlushTable(h.ctx, catalog.System_Account, h.rel.GetDBID(h.ctx), h.rel.GetTableID(h.ctx), types.TimestampToTS(h.pack.D.Now())))
	testutil2.MergeBlocks(t, 0, h.pack.T.GetDB(), "db1", h.schema, false)
	_, oracleRel, oracleTxn, err := h.pack.D.GetTable(h.ctx, "db1", "test1")
	require.NoError(t, err)
	defer oracleTxn.Rollback(h.ctx)
	oracle := h.collectDeletes(oracleTxn, oracleRel, len(keys)/2)
	defer oracle.Clean(h.pack.Mp)
	expected := make(map[string]types.Rowid, oracle.RowCount())
	for i, rid := range vector.MustFixedColNoTypeCheck[types.Rowid](oracle.Vecs[0]) {
		expected[string(oracle.Vecs[1].GetBytesAt(i))] = rid
	}
	var before, after dto.Metric
	histogram := v2.BatchTransferTombstonesDurationHistogram.(prometheus.Metric)
	require.NoError(t, histogram.Write(&before))
	_, err = databranchutils.AdvanceLineageSnapshot(h.ctx, h.txn)
	require.NoError(t, err)
	require.NoError(t, histogram.Write(&after))
	require.Equal(t, before.GetHistogram().GetSampleCount()+2, after.GetHistogram().GetSampleCount(), "byte-triggered batch plus one-row tail")

	checked := 0
	ws := h.txn.GetWorkspace().(*disttae.Transaction)
	ws.ForEachTableWrites(h.rel.GetDBID(h.ctx), h.rel.GetTableID(h.ctx), int(ws.WriteOffset()), func(e disttae.Entry) {
		if e.Type() != disttae.DELETE || e.FileName() != "" {
			return
		}
		for i, rid := range vector.MustFixedColNoTypeCheck[types.Rowid](e.Bat().Vecs[0]) {
			key := string(e.Bat().Vecs[1].GetBytesAt(i))
			want, ok := expected[key]
			require.True(t, ok)
			require.Equal(t, want, rid)
			checked++
		}
	})
	require.Equal(t, len(keys)/2, checked)
	require.Zero(t, h.countRows(h.txn, h.rel))
	require.NoError(t, h.txn.Commit(h.ctx))
	h.txn = nil
	_, rel, txn, err := h.pack.D.GetTable(h.ctx, "db1", "test1")
	require.NoError(t, err)
	h.txn, h.rel = txn, rel
	require.Zero(t, h.countRows(txn, rel))
}
