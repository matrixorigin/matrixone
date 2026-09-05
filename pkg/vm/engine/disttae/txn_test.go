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

package disttae

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestValidateAutoIncrEpochAdvance(t *testing.T) {
	require.NoError(t, validateAutoIncrEpochAdvance(0, 0))
	require.NoError(t, validateAutoIncrEpochAdvance(math.MaxUint32-1, 1))
	require.Error(t, validateAutoIncrEpochAdvance(math.MaxUint32, 1))
	require.Error(t, validateAutoIncrEpochAdvance(math.MaxUint32-1, 2))
}

func TestTransactionAutoIncrEpochFenceCapabilityUsesTargetSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stores   []DNStore
		expected bool
	}{
		{name: "no target", expected: false},
		{name: "legacy target", stores: []DNStore{{ServiceID: "old"}}, expected: false},
		{name: "new target", stores: []DNStore{{ServiceID: "new", AutoIncrEpochFenceSupported: true}}, expected: true},
		{
			name: "mixed targets fail closed",
			stores: []DNStore{
				{ServiceID: "new", AutoIncrEpochFenceSupported: true},
				{ServiceID: "old"},
			},
			expected: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := &Transaction{tnStores: tc.stores}
			require.Equal(t, tc.expected, txn.SupportsAutoIncrEpochFence())
		})
	}
}

func TestPrecommitEntryCarriesAutoIncrEpoch(t *testing.T) {
	proc := testutil.NewProc(t)
	bat := newDeleteBatchForTest(t, proc, []int64{1})
	defer bat.Clean(proc.Mp())

	for _, tc := range []struct {
		name    string
		version uint32
		known   bool
	}{
		{name: "known", version: 7, known: true},
		{name: "known zero", version: 0, known: true},
		{name: "old cn compatibility", version: 0, known: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := toPBEntry(Entry{
				typ:                DELETE,
				tableId:            42,
				databaseId:         7,
				bat:                bat,
				autoIncrEpoch:      tc.version,
				autoIncrEpochKnown: tc.known,
			})
			require.NoError(t, err)

			data, err := encoded.Marshal()
			require.NoError(t, err)
			decoded := new(api.Entry)
			require.NoError(t, decoded.Unmarshal(data))
			require.Equal(t, tc.version, decoded.AutoIncrEpoch)
			require.Equal(t, tc.known, decoded.AutoIncrEpochKnown)
		})
	}
}

func TestRequiresAutoIncrEpochFenceCommit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		entries  []*api.Entry
		expected bool
	}{
		{name: "ordinary legacy write"},
		{name: "known zero remains rolling-upgrade compatible", entries: []*api.Entry{{AutoIncrEpochKnown: true}}},
		{name: "fenced DML requires guarded commit", entries: []*api.Entry{{AutoIncrEpochKnown: true, AutoIncrEpoch: 1}}, expected: true},
		{name: "unknown nonzero is legacy and rejected by new TN epoch validation", entries: []*api.Entry{{AutoIncrEpoch: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, requiresAutoIncrEpochFenceCommit(tc.entries))
		})
	}
}

type recordingAutoIncrEpochFenceCommitter struct {
	client.TxnOperator
	required bool
}

func (op *recordingAutoIncrEpochFenceCommitter) RequireAutoIncrEpochFenceCommit() {
	op.required = true
}

func TestTransactionMarksAutoIncrEpochFenceBeforeWorkspaceFlush(t *testing.T) {
	op := &recordingAutoIncrEpochFenceCommitter{}
	txn := &Transaction{op: op}

	require.NoError(t, txn.requireAutoIncrEpochFenceCommit(0, true))
	require.False(t, op.required)
	require.NoError(t, txn.requireAutoIncrEpochFenceCommit(1, true))
	require.True(t, op.required)
}

type unsupportedAutoIncrEpochTxnOperator struct {
	client.TxnOperator
}

func TestAutoIncrEpochWritePathsRejectBeforeWorkspaceMutation(t *testing.T) {
	newTxn := func(t *testing.T) *Transaction {
		return &Transaction{
			op:        &unsupportedAutoIncrEpochTxnOperator{},
			proc:      testutil.NewProc(t),
			workspace: newTxnWorkspace(),
		}
	}

	t.Run("row", func(t *testing.T) {
		txn := newTxn(t)
		_, err := txn.writeBatchWithAutoIncrEpochKnown(
			context.Background(),
			INSERT, "", 1, 2, 3, "db", "tbl", nil, DNStore{}, 1, true,
		)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), err)
		require.Zero(t, txn.workspace.activeMutationCount())
		require.Zero(t, txn.workspace.usageSnapshot().totalBytes)
	})

	t.Run("file", func(t *testing.T) {
		txn := newTxn(t)
		err := txn.writeFileLockedWithAutoIncrEpochKnown(
			INSERT, 1, 2, 3, "db", "tbl", "file", nil, DNStore{}, 1, true,
		)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), err)
		require.Zero(t, txn.workspace.activeMutationCount())
		require.False(t, txn.hasS3Op.Load())
		require.Zero(t, txn.workspace.usageSnapshot().totalBytes)
	})

	t.Run("skip transfer file", func(t *testing.T) {
		txn := newTxn(t)
		err := txn.writeFileLockedSkipTransferWithAutoIncrEpochKnown(
			INSERT, 1, 2, 3, "db", "tbl", "file", nil, DNStore{}, 1, true,
		)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), err)
		require.Zero(t, txn.workspace.activeMutationCount())
		require.False(t, txn.hasS3Op.Load())
		require.Zero(t, txn.workspace.usageSnapshot().totalBytes)
	})
}

func TestWorkspaceFlushKeySeparatesAutoIncrEpochs(t *testing.T) {
	base := tableKey{accountId: 1, databaseId: 7, dbName: "db", name: "tbl"}
	batches := map[workspaceTableKey]int{
		{tableKey: base, autoIncrEpoch: 0, autoIncrEpochKnown: false}: 1,
		{tableKey: base, autoIncrEpoch: 0, autoIncrEpochKnown: true}:  1,
	}
	require.Len(t, batches, 2)
}

func Test_BatchAllocNewRowIds(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("A", func(t *testing.T) {
		txn := Transaction{
			proc: proc,
		}

		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

		for i := 0; i < 10; i++ {
			ll := rand.Intn(100) + 1
			vec, err := txn.batchAllocNewRowIds(ll)
			require.NoError(t, err)
			require.Equal(t, ll, vec.Length())

			rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](vec)
			require.Equal(t, int(0), int(rowIds[0].GetRowOffset()))
			require.Equal(t, int(ll-1), int(rowIds[len(rowIds)-1].GetRowOffset()))

			vec.Free(common.DefaultAllocator)
		}
	})

	t.Run("B", func(t *testing.T) {
		txn := Transaction{
			proc: proc,
		}

		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

		ll := options.DefaultBlockMaxRows*11 + 1
		mm1 := make(map[types.Blockid]struct{})
		mm2 := make(map[types.Objectid]struct{})

		vec, err := txn.batchAllocNewRowIds(ll)
		require.NoError(t, err)
		require.Equal(t, ll, vec.Length())

		rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](vec)
		for i := range rowIds {
			if i%options.DefaultBlockMaxRows == 0 {
				require.Equal(t, 0, int(rowIds[i].GetRowOffset()))
				if i > 0 {
					require.Equal(t, int(rowIds[i-1].GetBlockOffset()+1), int(rowIds[i].GetBlockOffset()))
					require.Equal(t, int(options.DefaultBlockMaxRows-1), int(rowIds[i-1].GetRowOffset()))
				}
			}

			mm1[*rowIds[i].BorrowBlockID()] = struct{}{}
			mm2[*rowIds[i].BorrowObjectID()] = struct{}{}
		}

		require.Equal(t, 12, len(mm1))
		require.Equal(t, 1, len(mm2))

		vec.Free(common.DefaultAllocator)
	})

	t.Run("C", func(t *testing.T) {
		txn := Transaction{
			proc: proc,
		}

		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

		ll := math.MaxUint16
		for i := 0; i < ll; i++ {
			err := txn.currentRowId.IncrObj()
			require.NoError(t, err)
		}

		for i := 0; i < ll; i++ {
			err := txn.currentRowId.IncrBlk()
			require.NoError(t, err)
		}

		_, err := txn.batchAllocNewRowIds(1)
		require.Error(t, err)
	})

}

func TestWriteBatchPublishesClosedPKCheckDescriptor(t *testing.T) {
	proc := testutil.NewProc(t)
	op := newTxnOperatorForTest(t)

	t.Run("insert without engine is rejected before publication", func(t *testing.T) {
		txn := &Transaction{proc: proc, op: op, workspace: newTxnWorkspace()}
		bat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1, 2})

		_, err := txn.WriteBatch(INSERT, "", 0, 1, 42, "db", "tbl", bat, DNStore{})
		require.ErrorContains(t, err, "without transaction engine")
		require.Zero(t, txn.workspace.activeMutationCount())
		bat.Clean(proc.Mp())
		closeWorkspaceForTest(t, txn)
	})

	t.Run("delete without engine is rejected before publication", func(t *testing.T) {
		txn := &Transaction{proc: proc, op: op, workspace: newTxnWorkspace()}
		bat := newDeleteBatchForTest(t, proc, []int64{1})

		_, err := txn.WriteBatch(DELETE, "", 0, 1, 42, "db", "tbl", bat, DNStore{})
		require.ErrorContains(t, err, "without transaction engine")
		require.Zero(t, txn.workspace.activeMutationCount())
		bat.Clean(proc.Mp())
		closeWorkspaceForTest(t, txn)
	})

	t.Run("insert with active pk table resolves position", func(t *testing.T) {
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		bat := newInt64BatchForTest(t, txn.proc, []string{"pk"}, []int64{1, 2})

		_, err := txn.WriteBatch(INSERT, "", 1, 7, 42, "db", "tbl", bat, DNStore{})
		require.NoError(t, err)
		entries := workspaceEntriesForTest(t, txn)
		require.Len(t, entries, 1)
		require.True(t, entries[0].pkCheck.enabled)
		require.Equal(t, 1, entries[0].pkCheck.vectorPos)
		closeWorkspaceForTest(t, txn)
	})

	t.Run("user write records planned AUTO_INCREMENT epoch", func(t *testing.T) {
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		bat := newInt64BatchForTest(t, txn.proc, []string{"pk"}, []int64{1})

		_, err := txn.writeBatchWithAutoIncrEpoch(context.Background(), INSERT, "", 1, 7, 42, "db", "tbl", bat, DNStore{}, 7)
		require.NoError(t, err)
		entries := workspaceEntriesForTest(t, txn)
		require.Len(t, entries, 1)
		require.Equal(t, uint32(7), entries[0].autoIncrEpoch)
		require.True(t, entries[0].autoIncrEpochKnown)
		closeWorkspaceForTest(t, txn)
	})

	t.Run("missing pk attr is rejected before publication", func(t *testing.T) {
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		bat := newInt64BatchForTest(t, txn.proc, []string{"other"}, []int64{1, 2})

		_, err := txn.WriteBatch(INSERT, "", 1, 7, 42, "db", "tbl", bat, DNStore{})
		require.ErrorContains(t, err, "primary-key column pk not found")
		require.Zero(t, txn.workspace.activeMutationCount())
		bat.Clean(txn.proc.Mp())
		closeWorkspaceForTest(t, txn)
	})
}

func TestTransactionCheckDupUsesWriteEntryPKMetadata(t *testing.T) {
	newTxn := func(t *testing.T, proc *process.Process, entries ...Entry) *Transaction {
		t.Helper()
		txn := &Transaction{
			op:        newTxnOperatorForTest(t),
			proc:      proc,
			workspace: newTxnWorkspace(),
		}
		for _, entry := range entries {
			appendWorkspaceEntryForTest(txn, entry)
		}
		t.Cleanup(func() { closeWorkspaceForTest(t, txn) })
		return txn
	}

	t.Run("insert duplicate", func(t *testing.T) {
		proc := testutil.NewProc(t)
		txn := newTxn(t, proc,
			Entry{
				typ:          INSERT,
				tableId:      42,
				databaseId:   7,
				tableName:    "tbl",
				databaseName: "db",
				bat:          newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1, 1}),
				pkCheck:      workspacePKCheck{vectorPos: 0, enabled: true},
			})

		err := txn.checkDup(context.Background())
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("delete duplicate", func(t *testing.T) {
		proc := testutil.NewProc(t)
		txn := newTxn(t, proc,
			Entry{
				typ:          DELETE,
				tableId:      42,
				databaseId:   7,
				tableName:    "tbl",
				databaseName: "db",
				bat:          newDeleteBatchForTest(t, proc, []int64{3, 3}),
				pkCheck:      workspacePKCheck{vectorPos: 1, enabled: true},
			})

		err := txn.checkDup(context.Background())
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("no pk check and unique", func(t *testing.T) {
		proc := testutil.NewProc(t)
		txn := newTxn(t, proc,
			Entry{
				typ:          INSERT,
				tableId:      42,
				databaseId:   7,
				tableName:    "tbl",
				databaseName: "db",
				bat:          newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1, 2}),
			},
			Entry{
				typ:          DELETE,
				tableId:      42,
				databaseId:   7,
				tableName:    "tbl",
				databaseName: "db",
				bat:          newDeleteBatchForTest(t, proc, []int64{4, 5}),
				pkCheck:      workspacePKCheck{vectorPos: 1, enabled: true},
			})

		require.NoError(t, txn.checkDup(context.Background()))
	})

	t.Run("out of range descriptor fails invariant", func(t *testing.T) {
		proc := testutil.NewProc(t)
		txn := newTxn(t, proc)
		appendWorkspaceEntryForTest(txn, Entry{
			typ:          INSERT,
			accountId:    1,
			tableId:      42,
			databaseId:   7,
			tableName:    "tbl",
			databaseName: "db",
			bat:          newInt64BatchForTest(t, proc, []string{"pk"}, []int64{9, 9}),
			pkCheck:      workspacePKCheck{vectorPos: 3, enabled: true},
		})

		err := txn.checkDup(context.Background())
		require.ErrorContains(t, err, "descriptor out of range")
	})

	t.Run("entry without pk check is excluded", func(t *testing.T) {
		proc := testutil.NewProc(t)
		txn := newTxn(t, proc,
			Entry{
				typ:          INSERT,
				accountId:    1,
				tableId:      42,
				databaseId:   7,
				tableName:    "tbl",
				databaseName: "db",
				bat:          newInt64BatchForTest(t, proc, []string{"value"}, []int64{6, 6}),
			})
		candidates, err := txn.workspace.pkCandidateEntries(txn.workspace.currentReadView())
		require.NoError(t, err)
		require.Empty(t, candidates.entries)
		candidates.Close()
		require.NoError(t, txn.checkDup(context.Background()))
	})

	t.Run("delete without exact descriptor is excluded", func(t *testing.T) {
		proc := testutil.NewProc(t)
		txn := newTxn(t, proc, Entry{
			typ:          DELETE,
			accountId:    1,
			tableId:      42,
			databaseId:   7,
			tableName:    "tbl",
			databaseName: "db",
			bat:          newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1}),
		})

		candidates, err := txn.workspace.pkCandidateEntries(txn.workspace.currentReadView())
		require.NoError(t, err)
		require.Empty(t, candidates.entries)
		candidates.Close()
		require.NoError(t, txn.checkDup(context.Background()))
	})
}

func TestTransactionGetTableNilGuards(t *testing.T) {
	txn := &Transaction{}

	_, err := txn.getTable(context.Background(), 0, "db", "tbl")
	require.Error(t, err)
	require.Contains(t, err.Error(), "disttae txn engine is nil")

	txn.engine = &Engine{}
	_, err = txn.getTable(nil, 0, "db", "tbl")
	require.Error(t, err)
	require.Contains(t, err.Error(), "disttae table lookup context is nil")

	_, err = txn.getTable(context.Background(), 0, "db", "tbl")
	require.Error(t, err)
	require.Contains(t, err.Error(), "disttae txn operator is nil")
}

func TestTxnTableWriteTnPartitionHonorsCanceledContext(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	txn.tnStores = []DNStore{{}}
	tbl := txn.workspace.activeTable(genTableKey(1, "tbl", 7, "db"))
	require.NotNil(t, tbl)
	tbl.extraInfo = &api.SchemaExtra{}
	bat := newDeleteBatchForTest(t, txn.proc, []int64{1})
	defer bat.Clean(txn.proc.Mp())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tbl.writeTnPartition(ctx, bat)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, txn.workspace.activeMutationCount())
}

func TestTxnTableDeleteHonorsCanceledContext(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	txn.op.(*mock_frontend.MockTxnOperator).EXPECT().IsSnapOp().Return(false)
	txn.tnStores = []DNStore{{}}
	tbl := txn.workspace.activeTable(genTableKey(1, "tbl", 7, "db"))
	require.NotNil(t, tbl)
	tbl.extraInfo = &api.SchemaExtra{}
	bat := newDeleteBatchForTest(t, txn.proc, []int64{1})
	defer bat.Clean(txn.proc.Mp())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tbl.Delete(ctx, bat, "")
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, txn.workspace.activeMutationCount())
}

func TestResolveWorkspacePKMetadataForWriteEarlyExit(t *testing.T) {
	txn := &Transaction{}

	check, index, err := txn.resolveWorkspacePKMetadataForWrite(
		context.Background(), INSERT, 0, "db", "tbl", 1, nil)
	require.NoError(t, err)
	require.False(t, check.enabled)
	require.False(t, index.enabled)

	proc := testutil.NewProc(t)
	bat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1})

	check, index, err = txn.resolveWorkspacePKMetadataForWrite(
		context.Background(), ALTER, 0, "db", "tbl", 1, bat)
	require.NoError(t, err)
	require.False(t, check.enabled)
	require.False(t, index.enabled)

	check, index, err = txn.resolveWorkspacePKMetadataForWrite(
		context.Background(), INSERT, 0, "db", "tbl", catalog.MO_TABLES_ID, bat)
	require.NoError(t, err)
	require.False(t, check.enabled)
	require.False(t, index.enabled)

	_, _, err = txn.resolveWorkspacePKMetadataForWrite(
		context.Background(), INSERT, 0, "db", "tbl", 42, bat)
	require.ErrorContains(t, err, "without transaction engine")
}

func TestMergeTxnWorkspaceKeepsCatalogBeforeDependentData(t *testing.T) {
	proc := testutil.NewProc(t)
	txn := &Transaction{
		op:        newTxnOperatorForTest(t),
		proc:      proc,
		workspace: newTxnWorkspace(),
	}

	const (
		dbID            = uint64(7)
		criticalTableID = uint64(5768603)
	)

	defer closeWorkspaceForTest(t, txn)

	// Two entries on the same table are enough to create a nil hole during merge.
	// The remaining distinct-table inserts push the merge count over the threshold
	// used by mergeTxnWorkspaceLocked.
	for i := 0; i < 30; i++ {
		tableID := uint64(1000 + i)
		if i == 1 {
			tableID = 1000
		}
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			tableId:      tableID,
			databaseId:   dbID,
			tableName:    "t",
			databaseName: "db",
			bat:          newInsertBatchWithRowIDForTest(t, proc, []int64{int64(i)}),
		})
	}

	txn.appendWorkspaceEntryLocked(Entry{
		typ:          INSERT,
		tableId:      catalog.MO_TABLES_ID,
		databaseId:   catalog.MO_CATALOG_ID,
		tableName:    catalog.MO_TABLES,
		databaseName: catalog.MO_CATALOG,
		note:         noteForCreate(criticalTableID, "critical"),
		bat:          newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1}),
	})
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          INSERT,
		tableId:      criticalTableID,
		databaseId:   dbID,
		tableName:    "critical",
		databaseName: "db",
		bat:          newInsertBatchWithRowIDForTest(t, proc, []int64{100}),
	})

	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))

	createIdx, dataIdx := -1, -1
	entries := workspaceEntriesForTest(t, txn)
	for i, e := range entries {
		if e.tableId == catalog.MO_TABLES_ID && e.note == noteForCreate(criticalTableID, "critical") {
			createIdx = i
		}
		if e.tableId == criticalTableID {
			dataIdx = i
		}
	}
	require.NotEqual(t, -1, createIdx)
	require.NotEqual(t, -1, dataIdx)
	require.Less(t, createIdx, dataIdx)
}

func TestMergeTxnWorkspaceDeduplicatesDeleteSelections(t *testing.T) {
	proc := testutil.NewProc(t)
	bat := newInsertBatchWithRowIDForTest(t, proc, []int64{10, 20, 30, 40})
	txn := &Transaction{
		proc:      proc,
		workspace: newTxnWorkspace(),
	}
	txn.appendWorkspaceEntryLocked(Entry{
		typ:        INSERT,
		tableId:    42,
		databaseId: 7,
		bat:        bat,
	})
	defer closeWorkspaceForTest(t, txn)

	// Two internal delete passes select the same rows. Their raw event count
	// equals the batch row count, but only half of the rows are deleted.
	entry := workspaceEntriesForTest(t, txn)[0]
	txn.addWorkspaceEntrySelectionsLocked(&entry, []int64{1, 3})
	txn.addWorkspaceEntrySelectionsLocked(&entry, []int64{1, 3})
	selections, err := txn.workspace.mutationSelections(entry.workspaceMutationID)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3}, selections)

	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
	entry = workspaceEntriesForTest(t, txn)[0]
	require.Equal(t, 4, entry.bat.RowCount())
	require.Equal(t, []int64{10, 20, 30, 40},
		vector.MustFixedColWithTypeCheck[int64](entry.bat.Vecs[1]))
	require.Equal(t, []int64{1, 3},
		mustMutationSelections(t, txn.workspace, entry.workspaceMutationID))
	require.Equal(t, 4, txn.workspace.usageSnapshot().inMemoryInsertRows)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

func TestIssue25589RollbackLastStatementRestoresWorkspaceAccounting(t *testing.T) {
	proc := testutil.NewProc(t)
	op := newTxnOperatorForTest(t)
	op.EXPECT().EnterRollbackStmt()
	op.EXPECT().ExitRollbackStmt()
	txn := &Transaction{
		op:         op,
		proc:       proc,
		workspace:  newTxnWorkspace(),
		tableCache: new(sync.Map),
		isCCPRTxn:  true,
	}

	committed := newInsertBatchWithRowIDForTest(t, proc, []int64{1, 2})
	rolledBack := newInsertBatchWithRowIDForTest(t, proc, []int64{3, 4, 5})
	rolledBackDelete := newDeleteBatchForTest(t, proc, []int64{1, 2})
	txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: committed})
	txn.workspace.advanceStatement()
	txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: rolledBack})
	txn.appendWorkspaceEntryLocked(Entry{typ: DELETE, databaseId: 7, tableId: 42, bat: rolledBackDelete})
	require.NoError(t, txn.RollbackLastStatement(context.Background()))
	entries := workspaceEntriesForTest(t, txn)
	require.Len(t, entries, 1)
	require.Same(t, committed, entries[0].bat)
	usage := txn.workspace.usageSnapshot()
	require.Equal(t, uint64(committed.Size()), usage.totalBytes)
	require.Equal(t, uint64(committed.Size()), usage.inMemoryInsertBytes)
	require.Equal(t, committed.RowCount(), usage.inMemoryInsertRows)
	require.Zero(t, usage.inMemoryDeleteRows)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	closeWorkspaceForTest(t, txn)
}

func TestIssue25589MergeTxnWorkspaceRestoresAccountingForTablesInVain(t *testing.T) {
	proc := testutil.NewProc(t)
	bat := newInsertBatchWithRowIDForTest(t, proc, []int64{1, 2, 3})
	txn := &Transaction{
		proc:      proc,
		workspace: newTxnWorkspace(),
	}
	txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: bat})
	require.NoError(t, txn.workspace.markTableDropped(0, 7, 42))

	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
	require.Zero(t, txn.workspace.activeMutationCount())
	usage := txn.workspace.usageSnapshot()
	require.Zero(t, usage.totalBytes)
	require.Zero(t, usage.inMemoryInsertBytes)
	require.Zero(t, usage.inMemoryInsertRows)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	closeWorkspaceForTest(t, txn)
}

func TestIssue25589MergeTxnWorkspaceRestoresAccountingAfterShrink(t *testing.T) {
	proc := testutil.NewProc(t)
	bat := newInsertBatchWithRowIDForTest(t, proc, []int64{1, 2, 3, 4})
	txn := &Transaction{
		proc:      proc,
		workspace: newTxnWorkspace(),
	}
	txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: bat})
	entry := workspaceEntriesForTest(t, txn)[0]
	txn.addWorkspaceEntrySelectionsLocked(&entry, []int64{1, 3})

	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
	entry = workspaceEntriesForTest(t, txn)[0]
	usage := txn.workspace.usageSnapshot()
	require.Equal(t, 4, entry.bat.RowCount())
	require.Equal(t, []int64{1, 3},
		mustMutationSelections(t, txn.workspace, entry.workspaceMutationID))
	require.Equal(t, uint64(entry.bat.Size()), usage.totalBytes)
	require.Equal(t, uint64(entry.bat.Size()), usage.inMemoryInsertBytes)
	require.Equal(t, entry.bat.RowCount(), usage.inMemoryInsertRows)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	closeWorkspaceForTest(t, txn)
}

func TestMergeTxnWorkspaceDoesNotCompactAcrossStatementAttempts(t *testing.T) {
	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc:      proc,
		workspace: newTxnWorkspace(),
	}
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
	defer closeWorkspaceForTest(t, txn)

	for i := 0; i < workspaceCompactionSealRows; i++ {
		txn.appendWorkspaceEntryLocked(Entry{
			typ: INSERT, databaseId: 7, tableId: 42,
			bat: newInsertBatchWithRowIDForTest(t, proc, []int64{int64(i)}),
		})
	}
	txn.workspace.advanceStatement()
	currentValues := make([]int64, workspaceCompactionSealRows)
	for i := range currentValues {
		currentValues[i] = int64(workspaceCompactionSealRows + i)
	}
	txn.appendWorkspaceEntryLocked(Entry{
		typ: INSERT, databaseId: 7, tableId: 42,
		bat: newInsertBatchWithRowIDForTest(t, proc, currentValues),
	})

	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
	entries, err := txn.workspace.commitEntries()
	require.NoError(t, err)
	defer entries.Close()
	require.Len(t, entries.entries, 2)
	require.Equal(t, uint64(0), entries.entries[0].statementID)
	require.Equal(t, uint64(1), entries.entries[0].attemptID)
	require.Equal(t, uint64(1), entries.entries[1].statementID)
	require.Equal(t, uint64(1), entries.entries[1].attemptID)
	require.Equal(t, workspaceCompactionSealRows, entries.entries[0].bat.RowCount())
	require.Equal(t, workspaceCompactionSealRows, entries.entries[1].bat.RowCount())
}

func TestIncrStatementIDCompactsCompletedAttemptDuringRetryPreparation(t *testing.T) {
	proc := testutil.NewProc(t)
	op := newTxnOperatorForTest(t)
	op.EXPECT().EnterRollbackStmt()
	op.EXPECT().ExitRollbackStmt()
	op.EXPECT().EnterIncrStmt()
	op.EXPECT().ExitIncrStmt()
	txn := &Transaction{
		op:                      op,
		proc:                    proc,
		workspace:               newTxnWorkspace(),
		tableCache:              new(sync.Map),
		isCCPRTxn:               true,
		writeWorkspaceThreshold: math.MaxUint64,
	}
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
	defer closeWorkspaceForTest(t, txn)

	// Leave enough small batches in a completed statement to trigger the
	// transaction-level compaction performed by IncrStatementID. The current
	// statement then fails without adding its own writes.
	for i := 0; i < workspaceCompactionSealRows; i++ {
		txn.appendWorkspaceEntryLocked(Entry{
			typ: INSERT, databaseId: 7, tableId: 42,
			bat: newInsertBatchWithRowIDForTest(t, proc, []int64{int64(i)}),
		})
	}
	_, err := txn.workspace.advanceStatement()
	require.NoError(t, err)
	txn.StartStatement()
	require.NoError(t, txn.RollbackLastStatement(context.Background()))
	require.Equal(t, statementAttemptRolledBack, txn.workspace.journal.current.state)
	require.True(t, txn.workspace.journal.retryPending)

	// Retry preparation compacts the completed statement before opening the
	// next attempt. This is the exact ordering used by compile retry handling.
	require.NoError(t, txn.IncrStatementID(context.Background(), false))
	require.Equal(t, uint64(1), txn.workspace.journal.current.statementID)
	require.Equal(t, uint64(2), txn.workspace.journal.current.attemptID)
	require.Equal(t, statementAttemptOpen, txn.workspace.journal.current.state)

	entries, err := txn.workspace.commitEntries()
	require.NoError(t, err)
	require.Len(t, entries.entries, 1)
	require.Equal(t, workspaceCompactionSealRows, entries.entries[0].bat.RowCount())
	require.Equal(t, uint64(0), entries.entries[0].statementID)
	require.Equal(t, uint64(1), entries.entries[0].attemptID)
	entries.Close()
	txn.EndStatement()
}

func TestMergeTxnWorkspaceKeepsOlderMutationRollbackable(t *testing.T) {
	proc := testutil.NewProc(t)
	bat := newInsertBatchWithRowIDForTest(t, proc, []int64{10, 20, 30, 40})
	txn := &Transaction{
		proc:      proc,
		workspace: newTxnWorkspace(),
	}
	defer closeWorkspaceForTest(t, txn)

	txn.appendWorkspaceEntryLocked(Entry{
		typ: INSERT, databaseId: 7, tableId: 42, bat: bat,
	})
	txn.workspace.advanceStatement()
	entry := workspaceEntriesForTest(t, txn)[0]
	txn.addWorkspaceEntrySelectionsLocked(&entry, []int64{1, 3})

	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
	entry = workspaceEntriesForTest(t, txn)[0]
	require.Equal(t, 4, entry.bat.RowCount())
	require.Equal(t, []int64{1, 3},
		mustMutationSelections(t, txn.workspace, entry.workspaceMutationID))

	rolledBack, err := txn.workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Empty(t, rolledBack.mutationIDs)
	rolledBack.Close()

	entry = workspaceEntriesForTest(t, txn)[0]
	require.Equal(t, 4, entry.bat.RowCount())
	require.Empty(t,
		mustMutationSelections(t, txn.workspace, entry.workspaceMutationID))
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

func TestIssue25589MergeTxnWorkspacePreservesAccountingWhenCombiningBatches(t *testing.T) {
	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc:      proc,
		workspace: newTxnWorkspace(),
	}
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

	for i := 0; i < workspaceCompactionSealRows; i++ {
		txn.appendWorkspaceEntryLocked(Entry{
			typ:        INSERT,
			databaseId: 7,
			tableId:    42,
			bat:        newInsertBatchWithRowIDForTest(t, proc, []int64{int64(i)}),
		})
	}
	_, err := txn.workspace.advanceStatement()
	require.NoError(t, err)

	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
	entries := workspaceEntriesForTest(t, txn)
	require.Len(t, entries, 1)
	usage := txn.workspace.usageSnapshot()
	require.Equal(t, workspaceCompactionSealRows, entries[0].bat.RowCount())
	require.Equal(t, uint64(entries[0].bat.Size()), usage.totalBytes)
	require.Equal(t, uint64(entries[0].bat.Size()), usage.inMemoryInsertBytes)
	require.Equal(t, workspaceCompactionSealRows, usage.inMemoryInsertRows)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	closeWorkspaceForTest(t, txn)
}

func TestMergeTxnWorkspaceSealsImmutablePayloadOnlyOnce(t *testing.T) {
	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc:      proc,
		workspace: newTxnWorkspace(),
	}
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
	defer closeWorkspaceForTest(t, txn)

	appendRows := func(start, count int) {
		for i := 0; i < count; i++ {
			txn.appendWorkspaceEntryLocked(Entry{
				typ: INSERT, databaseId: 7, tableId: 42,
				bat: newInsertBatchWithRowIDForTest(
					t, proc, []int64{int64(start + i)}),
			})
		}
		_, err := txn.workspace.advanceStatement()
		require.NoError(t, err)
	}

	appendRows(0, workspaceCompactionSealRows)
	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
	entries := workspaceEntriesForTest(t, txn)
	require.Len(t, entries, 1)
	require.Equal(t, workspaceCompactionSealRows, entries[0].bat.RowCount())
	sealedBatch := entries[0].bat
	sealedMutationID := entries[0].workspaceMutationID
	require.False(t, txn.workspace.pendingCompactions.contains(sealedMutationID))

	// A sub-threshold tail must remain pending without reopening or copying the
	// immutable payload that was already sealed by the previous plan.
	appendRows(workspaceCompactionSealRows, workspaceCompactionSealRows-1)
	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
	entries = workspaceEntriesForTest(t, txn)
	require.Len(t, entries, workspaceCompactionSealRows)
	require.Same(t, sealedBatch, entries[0].bat)
	require.False(t, txn.workspace.pendingCompactions.contains(sealedMutationID))

	// The next row seals only the pending tail. The first payload stays at the
	// same generation, which bounds compaction copying to once per logical row.
	appendRows(2*workspaceCompactionSealRows-1, 1)
	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
	entries = workspaceEntriesForTest(t, txn)
	require.Len(t, entries, 2)
	require.Same(t, sealedBatch, entries[0].bat)
	require.Equal(t, workspaceCompactionSealRows, entries[0].bat.RowCount())
	require.Equal(t, workspaceCompactionSealRows, entries[1].bat.RowCount())
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}

func TestIssue25589DumpInsertRestoresWorkspaceAccounting(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	tbl := txn.workspace.activeTable(genTableKey(1, "tbl", 7, "db"))
	require.NotNil(t, tbl)
	tbl.tableDef.Cols[0].Typ = pbplan.Type{Id: int32(types.T_int64)}
	txn.tnStores = []DNStore{{}}
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   7,
		tableId:      42,
		databaseName: "db",
		tableName:    "tbl",
		bat:          newInsertBatchWithRowIDForTest(t, txn.proc, []int64{1, 2, 3}),
	})

	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)
	var pkCount int
	txn.Lock()
	err = txn.dumpInsertBatchLocked(
		context.Background(), fs, workspaceDumpAll(false), &pkCount)
	txn.Unlock()
	require.NoError(t, err)
	require.Equal(t, 3, pkCount)
	usage := txn.workspace.usageSnapshot()
	require.Zero(t, usage.inMemoryInsertBytes)
	require.Zero(t, usage.inMemoryInsertRows)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	closeWorkspaceForTest(t, txn)
}

func TestIssue25589SoftDeleteObjectUsesWorkspaceAccounting(t *testing.T) {
	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc:      proc,
		workspace: newTxnWorkspace(),
		tnStores:  []DNStore{{}},
	}
	op := newTxnOperatorForTestWithWorkspace(t, txn)
	op.EXPECT().IsSnapOp().Return(false)
	txn.op = op
	tbl := &txnTable{
		accountId: 1,
		tableId:   42,
		tableName: "tbl",
		extraInfo: &api.SchemaExtra{},
		db: &txnDatabase{
			op:           op,
			databaseId:   7,
			databaseName: "db",
		},
	}
	rowID := types.RandomRowid()

	require.NoError(t, tbl.SoftDeleteObject(
		context.Background(),
		rowID.BorrowObjectID(),
		false,
	))
	entries := workspaceEntriesForTest(t, txn)
	require.Len(t, entries, 1)
	usage := txn.workspace.usageSnapshot()
	require.Equal(t, SOFT_DELETE_OBJECT, entries[0].typ)
	require.Equal(t, uint64(entries[0].bat.Size()), usage.totalBytes)
	require.Zero(t, usage.inMemoryInsertBytes)
	require.Zero(t, usage.inMemoryInsertRows)
	require.Zero(t, usage.inMemoryDeleteRows)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())

	closeWorkspaceForTest(t, txn)
}

func TestResolveWorkspacePKMetadataForWriteWithActiveTxnTable(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "pk")

	check, index, err := txn.resolveWorkspacePKMetadataForWrite(
		context.Background(),
		INSERT,
		1,
		"db",
		"tbl",
		42,
		newInt64BatchForTest(t, txn.proc, []string{"pk"}, []int64{1}),
	)
	require.NoError(t, err)
	require.True(t, check.enabled)
	require.Equal(t, 0, check.vectorPos)
	require.True(t, index.enabled)
	require.Equal(t, 0, index.vectorPos)

	check, index, err = txn.resolveWorkspacePKMetadataForWrite(
		context.Background(),
		INSERT,
		1,
		"db",
		"tbl",
		42,
		newInt64BatchForTest(t, txn.proc, []string{"PK"}, []int64{1}),
	)
	require.NoError(t, err)
	require.True(t, check.enabled)
	require.Equal(t, 0, check.vectorPos)
	require.True(t, index.enabled)
	require.Equal(t, 0, index.vectorPos)

	check, index, err = txn.resolveWorkspacePKMetadataForWrite(
		context.Background(),
		DELETE,
		1,
		"db",
		"tbl",
		42,
		newDeleteBatchForTest(t, txn.proc, []int64{1}),
	)
	require.NoError(t, err)
	require.True(t, check.enabled)
	require.Equal(t, 1, check.vectorPos)
	require.False(t, index.enabled)

	_, _, err = txn.resolveWorkspacePKMetadataForWrite(
		context.Background(),
		INSERT,
		1,
		"db",
		"tbl",
		42,
		newInt64BatchForTest(t, txn.proc, []string{"other"}, []int64{1}),
	)
	require.ErrorContains(t, err, "primary-key column pk not found")

	_, _, err = txn.resolveWorkspacePKMetadataForWrite(
		context.Background(),
		DELETE,
		1,
		"db",
		"tbl",
		42,
		newInt64BatchForTest(t, txn.proc, []string{"pk"}, []int64{1}),
	)
	require.ErrorContains(t, err, "has no primary-key vector")
}

func TestResolveWorkspacePKMetadataIndexesCompositePrimaryKey(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, catalog.CPrimaryKeyColName)
	bat := newInt64BatchForTest(
		t, txn.proc, []string{catalog.CPrimaryKeyColName}, []int64{1})

	check, index, err := txn.resolveWorkspacePKMetadataForWrite(
		context.Background(), INSERT, 1, "db", "tbl", 42, bat)
	require.NoError(t, err)
	// Composite keys are checked by their constituent-key path, not by the
	// generic transaction-local duplicate checker.
	require.False(t, check.enabled)
	// The hidden composite-key vector is nevertheless the canonical key for
	// current-state point reads and must be indexed.
	require.True(t, index.enabled)
	require.Equal(t, 0, index.vectorPos)
}

func TestWriteFileLockedDoesNotRequestPKCheck(t *testing.T) {
	proc := testutil.NewProc(t)
	txn := &Transaction{proc: proc, workspace: newTxnWorkspace()}
	bat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1})

	err := txn.WriteFileLocked(ALTER, 0, 1, 2, "db", "tbl", "file", bat, DNStore{})
	require.NoError(t, err)
	entries := workspaceEntriesForTest(t, txn)
	require.Len(t, entries, 1)
	require.False(t, entries[0].pkCheck.enabled)

	bat.Clean(proc.Mp())
	closeWorkspaceForTest(t, txn)
}

func newTxnOperatorForTest(t *testing.T) *mock_frontend.MockTxnOperator {
	return newTxnOperatorForTestWithWorkspace(t, nil)
}

func newTxnOperatorForTestWithWorkspace(
	t *testing.T,
	workspace client.Workspace,
) *mock_frontend.MockTxnOperator {
	t.Helper()
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("txn-test")}).AnyTimes()
	op.EXPECT().NextSequence().Return(uint64(1)).AnyTimes()
	op.EXPECT().Status().Return(txnpb.TxnStatus_Active).AnyTimes()
	op.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	return op
}

func newTransactionWithActivePKTableForTest(
	t *testing.T,
	pkName string,
) *Transaction {
	t.Helper()
	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc:      proc,
		engine:    &Engine{},
		workspace: newTxnWorkspace(),
	}
	op := newTxnOperatorForTestWithWorkspace(t, txn)
	txn.op = op

	db := &txnDatabase{
		op:           op,
		databaseId:   7,
		databaseName: "db",
	}
	require.NoError(t, txn.workspace.addDatabaseOp(genDatabaseKey(1, "db"), INSERT, db.databaseId, db))
	tbl := &txnTable{
		accountId: 1,
		tableId:   42,
		tableName: "tbl",
		db:        db,
		tableDef: &pbplan.TableDef{
			Cols: []*pbplan.ColDef{
				{Name: pkName},
			},
			Pkey: &pbplan.PrimaryKeyDef{
				PkeyColName: pkName,
			},
		},
	}
	require.NoError(t, txn.workspace.addTableOp(
		genTableKey(1, "tbl", 7, "db"),
		INSERT,
		tbl.tableId,
		tbl,
	))
	return txn
}

// TestCheckPKDupSkipsNulls verifies that checkPKDup correctly skips NULL
// values per SQL standard (NULL != NULL), preventing false duplicate errors.
func TestCheckPKDupSkipsNulls(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	t.Run("int64_all_nulls_no_dup", func(t *testing.T) {
		// All NULLs should never produce a duplicate
		pk := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup, "all-NULL rows must not report duplicate")
		require.Empty(t, m, "NULL rows must not be added to the map")
		pk.Free(mp)
	})

	t.Run("int64_mixed_nulls_and_values", func(t *testing.T) {
		// Two NULLs + two distinct values: no duplicate
		pk := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(pk, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, int64(2), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp)) // NULL

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 4)
		require.False(t, dup, "NULLs should be skipped, 1 and 2 are distinct")
		require.Len(t, m, 2, "only non-NULL values should be in the map")
		pk.Free(mp)
	})

	t.Run("int64_real_dup_among_nulls", func(t *testing.T) {
		// Real duplicate among NULLs should still be caught
		pk := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(pk, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp))  // NULL
		require.NoError(t, vector.AppendFixed(pk, int64(1), false, mp)) // dup!

		m := make(map[any]bool)
		dup, entry := checkPKDup(m, pk, 0, 3)
		require.True(t, dup, "real duplicate 1 must be caught")
		require.Contains(t, entry, "1")
		pk.Free(mp)
	})

	t.Run("varchar_nulls_no_dup", func(t *testing.T) {
		// String type NULLs should be skipped
		pk := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(pk, []byte("hello"), false, mp))
		require.NoError(t, vector.AppendBytes(pk, nil, true, mp)) // NULL
		require.NoError(t, vector.AppendBytes(pk, nil, true, mp)) // NULL
		require.NoError(t, vector.AppendBytes(pk, []byte("world"), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 4)
		require.False(t, dup, "NULLs should be skipped for varchar")
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("varchar_null_empty_string_no_collision", func(t *testing.T) {
		// NULL and empty string "" are different: NULL is skipped, "" is a value
		pk := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(pk, nil, true, mp))         // NULL
		require.NoError(t, vector.AppendBytes(pk, []byte(""), false, mp)) // empty string
		require.NoError(t, vector.AppendBytes(pk, nil, true, mp))         // NULL

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup, "NULL and empty string must not collide")
		require.Len(t, m, 1, "only the empty string should be in the map")
		pk.Free(mp)
	})

	t.Run("partial_range_with_nulls", func(t *testing.T) {
		// Test start/count range with NULLs
		pk := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(pk, int64(10), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp)) // NULL at pos 1
		require.NoError(t, vector.AppendFixed(pk, int64(20), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp)) // NULL at pos 3

		// Check only range [1,3) — NULL at 1, value 20 at 2
		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 1, 2)
		require.False(t, dup)
		require.Len(t, m, 1, "only pos 2 (value 20) should be in map")
		pk.Free(mp)
	})

	t.Run("array_float32_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_array_float32.ToType())
		require.NoError(t, vector.AppendArray(pk, []float32{1.0, 2.0}, false, mp))
		require.NoError(t, vector.AppendArray(pk, []float32{0}, true, mp))         // NULL
		require.NoError(t, vector.AppendArray(pk, []float32{1.0, 2.0}, false, mp)) // dup!

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.True(t, dup, "real duplicate array should be caught")
		pk.Free(mp)
	})

	t.Run("array_float32_all_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_array_float32.ToType())
		require.NoError(t, vector.AppendArray(pk, []float32{0}, true, mp))
		require.NoError(t, vector.AppendArray(pk, []float32{0}, true, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 2)
		require.False(t, dup, "all-NULL arrays must not report duplicate")
		require.Empty(t, m)
		pk.Free(mp)
	})

	t.Run("array_float64_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_array_float64.ToType())
		require.NoError(t, vector.AppendArray(pk, []float64{1.0}, false, mp))
		require.NoError(t, vector.AppendArray(pk, []float64{0}, true, mp)) // NULL
		require.NoError(t, vector.AppendArray(pk, []float64{2.0}, false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup, "NULLs should be skipped for float64 arrays")
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	// Narrow vector element types must be handled (not hit the default panic).
	// These columns are rejected as primary keys at DDL admission, but checkPKDup
	// still handles them defensively so it degrades to normal dedup, never panics.
	t.Run("array_int8_dup_and_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_array_int8.ToType())
		require.NoError(t, vector.AppendArray(pk, []int8{1, 2, 3}, false, mp))
		require.NoError(t, vector.AppendArray(pk, []int8{0}, true, mp)) // NULL
		require.NoError(t, vector.AppendArray(pk, []int8{1, 2, 3}, false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.True(t, dup, "duplicate int8 array must be caught, NULL skipped")
		pk.Free(mp)
	})

	t.Run("array_uint8_distinct", func(t *testing.T) {
		pk := vector.NewVec(types.T_array_uint8.ToType())
		require.NoError(t, vector.AppendArray(pk, []uint8{0, 1, 2, 3}, false, mp))
		require.NoError(t, vector.AppendArray(pk, []uint8{255, 254, 0, 128}, false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 2)
		require.False(t, dup, "distinct uint8 arrays must not report duplicate")
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("array_bf16_dup", func(t *testing.T) {
		pk := vector.NewVec(types.T_array_bf16.ToType())
		require.NoError(t, vector.AppendArray(pk, []types.BF16{types.BF16FromFloat32(1.5), types.BF16FromFloat32(2.5)}, false, mp))
		require.NoError(t, vector.AppendArray(pk, []types.BF16{types.BF16FromFloat32(1.5), types.BF16FromFloat32(2.5)}, false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 2)
		require.True(t, dup, "duplicate bf16 array must be caught")
		pk.Free(mp)
	})

	t.Run("array_float16_distinct", func(t *testing.T) {
		pk := vector.NewVec(types.T_array_float16.ToType())
		require.NoError(t, vector.AppendArray(pk, []types.Float16{types.Float16FromFloat32(1), types.Float16FromFloat32(2)}, false, mp))
		require.NoError(t, vector.AppendArray(pk, []types.Float16{types.Float16FromFloat32(3), types.Float16FromFloat32(4)}, false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 2)
		require.False(t, dup, "distinct f16 arrays must not report duplicate")
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("bool_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_bool.ToType())
		require.NoError(t, vector.AppendFixed(pk, true, false, mp))
		require.NoError(t, vector.AppendFixed(pk, false, true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, false, false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("int8_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_int8.ToType())
		require.NoError(t, vector.AppendFixed(pk, int8(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int8(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, int8(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("int16_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_int16.ToType())
		require.NoError(t, vector.AppendFixed(pk, int16(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int16(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, int16(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("int32_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(pk, int32(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int32(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, int32(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("uint8_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_uint8.ToType())
		require.NoError(t, vector.AppendFixed(pk, uint8(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, uint8(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, uint8(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("uint16_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_uint16.ToType())
		require.NoError(t, vector.AppendFixed(pk, uint16(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, uint16(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, uint16(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("uint32_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_uint32.ToType())
		require.NoError(t, vector.AppendFixed(pk, uint32(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, uint32(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, uint32(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("uint64_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed(pk, uint64(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, uint64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, uint64(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("float32_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_float32.ToType())
		require.NoError(t, vector.AppendFixed(pk, float32(1.0), false, mp))
		require.NoError(t, vector.AppendFixed(pk, float32(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, float32(2.0), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("float64_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_float64.ToType())
		require.NoError(t, vector.AppendFixed(pk, float64(1.0), false, mp))
		require.NoError(t, vector.AppendFixed(pk, float64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, float64(2.0), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("date_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_date.ToType())
		require.NoError(t, vector.AppendFixed(pk, types.Date(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Date(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, types.Date(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("datetime_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_datetime.ToType())
		require.NoError(t, vector.AppendFixed(pk, types.Datetime(100), false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Datetime(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, types.Datetime(200), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("uuid_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_uuid.ToType())
		u1 := types.Uuid{1}
		u2 := types.Uuid{2}
		require.NoError(t, vector.AppendFixed(pk, u1, false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Uuid{}, true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, u2, false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("decimal64_nulls", func(t *testing.T) {
		tp := types.T_decimal64.ToType()
		tp.Scale = 2
		pk := vector.NewVec(tp)
		require.NoError(t, vector.AppendFixed(pk, types.Decimal64(100), false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Decimal64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, types.Decimal64(200), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("decimal128_nulls", func(t *testing.T) {
		tp := types.T_decimal128.ToType()
		tp.Scale = 2
		pk := vector.NewVec(tp)
		d1 := types.Decimal128{B0_63: 100, B64_127: 0}
		d2 := types.Decimal128{B0_63: 200, B64_127: 0}
		require.NoError(t, vector.AppendFixed(pk, d1, false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Decimal128{}, true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, d2, false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("timestamp_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_timestamp.ToType())
		require.NoError(t, vector.AppendFixed(pk, types.Timestamp(100), false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Timestamp(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, types.Timestamp(200), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("time_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_time.ToType())
		require.NoError(t, vector.AppendFixed(pk, types.Time(100), false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Time(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, types.Time(200), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("enum_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_enum.ToType())
		require.NoError(t, vector.AppendFixed(pk, types.Enum(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Enum(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, types.Enum(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("bit_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_bit.ToType())
		require.NoError(t, vector.AppendFixed(pk, uint64(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, uint64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, uint64(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})
}

// TestDupVectorWithoutNulls tests the extracted helper that filters NULLs
// and duplicates the vector for safe InplaceSort.
func TestDupVectorWithoutNulls(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	t.Run("no_nulls", func(t *testing.T) {
		v := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(v, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(v, int64(2), false, mp))

		out, err := dupVectorWithoutNulls(v, mp)
		require.NoError(t, err)
		require.Equal(t, 2, out.Length())
		require.False(t, out.HasNull())
		out.Free(mp)
		v.Free(mp)
	})

	t.Run("some_nulls", func(t *testing.T) {
		v := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(v, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(v, int64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(v, int64(2), false, mp))
		require.NoError(t, vector.AppendFixed(v, int64(0), true, mp)) // NULL

		out, err := dupVectorWithoutNulls(v, mp)
		require.NoError(t, err)
		require.Equal(t, 2, out.Length())
		require.False(t, out.HasNull())
		out.Free(mp)
		v.Free(mp)
	})

	t.Run("all_nulls", func(t *testing.T) {
		v := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(v, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(v, int64(0), true, mp))

		out, err := dupVectorWithoutNulls(v, mp)
		require.NoError(t, err)
		require.Equal(t, 0, out.Length())
		out.Free(mp)
		v.Free(mp)
	})

	t.Run("varchar_with_nulls", func(t *testing.T) {
		v := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(v, []byte("hello"), false, mp))
		require.NoError(t, vector.AppendBytes(v, nil, true, mp)) // NULL
		require.NoError(t, vector.AppendBytes(v, []byte("world"), false, mp))

		out, err := dupVectorWithoutNulls(v, mp)
		require.NoError(t, err)
		require.Equal(t, 2, out.Length())
		require.False(t, out.HasNull())
		out.Free(mp)
		v.Free(mp)
	})
}

func TestDupVectorWithoutNullsLeavesSealedStatementOwner(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	for _, tc := range []struct {
		name      string
		withNulls bool
	}{
		{name: "no nulls"},
		{name: "with nulls", withNulls: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry, err := mpool.NewAllocationAccountRegistry(1, 8)
			require.NoError(t, err)
			account, err := registry.Open(1 << 20)
			require.NoError(t, err)
			selection, err := vector.NewAllocationAccountSelection(
				account,
				mpool.AllocationOwner(1),
				mpool.AllocationSite(1),
				mpool.AllocationSite(2),
				mpool.AllocationSite(3),
				mpool.AllocationSite(4),
			)
			require.NoError(t, err)
			source, err := vector.NewOffHeapVecWithTypeAndAllocation(
				types.T_int64.ToType(),
				selection,
			)
			require.NoError(t, err)
			require.NoError(t, vector.AppendFixed(source, int64(1), false, mp))
			if tc.withNulls {
				require.NoError(t, vector.AppendFixed(source, int64(0), true, mp))
			}
			require.NoError(t, vector.AppendFixed(source, int64(2), false, mp))

			used := account.Seal().Used
			require.NotZero(t, used)
			out, err := dupVectorWithoutNulls(source, mp)
			require.NoError(t, err)
			require.Nil(t, out.AllocationAccountSelection())
			require.Equal(t, used, account.Snapshot().Used)
			require.Equal(t, []int64{1, 2}, vector.MustFixedColWithTypeCheck[int64](out))

			out.Free(mp)
			source.Free(mp)
			require.Zero(t, account.Snapshot().Used)
			_, err = registry.Finalize(account)
			require.NoError(t, err)
		})
	}
}

func newInt64BatchForTest(
	t *testing.T,
	proc *process.Process,
	attrs []string,
	cols ...[]int64,
) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(len(cols))
	bat.SetAttributes(attrs)
	for i, vals := range cols {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(vec, vals, nil, proc.Mp()))
		bat.Vecs[i] = vec
	}
	bat.SetRowCount(len(cols[0]))
	return bat
}

func newDeleteBatchForTest(
	t *testing.T,
	proc *process.Process,
	pks []int64,
) *batch.Batch {
	t.Helper()
	rowids := make([]types.Rowid, len(pks))
	for i := range rowids {
		rowids[i] = types.RandomRowid()
	}

	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})

	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(rowidVec, rowids, nil, proc.Mp()))
	bat.Vecs[0] = rowidVec

	pkVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(pkVec, pks, nil, proc.Mp()))
	bat.Vecs[1] = pkVec

	bat.SetRowCount(len(pks))
	return bat
}

func newInsertBatchWithRowIDForTest(
	t *testing.T,
	proc *process.Process,
	pks []int64,
) *batch.Batch {
	t.Helper()
	rowids := make([]types.Rowid, len(pks))
	for i := range rowids {
		rowids[i] = types.RandomRowid()
	}

	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})

	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(rowidVec, rowids, nil, proc.Mp()))
	bat.Vecs[0] = rowidVec

	pkVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(pkVec, pks, nil, proc.Mp()))
	bat.Vecs[1] = pkVec

	bat.SetRowCount(len(pks))
	return bat
}

// TestConcurrentCheckPKDup verifies that checkPKDup works correctly when
// called concurrently with different vectors, each containing NULLs.
// This simulates the real production path where multiple INSERT txns
// perform PK duplicate checking concurrently.
func TestConcurrentCheckPKDup(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	const numGoroutines = 8
	const rowsPerGoroutine = 50

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)
	results := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			pk := vector.NewVec(types.T_int64.ToType())
			for i := 0; i < rowsPerGoroutine; i++ {
				isNull := (i % 5) == 0 // 20% NULLs
				if isNull {
					vector.AppendFixed(pk, int64(0), true, mp)
				} else {
					// Unique values per goroutine
					vector.AppendFixed(pk, int64(goroutineID*1000+i), false, mp)
				}
			}
			defer pk.Free(mp)

			m := make(map[any]bool)
			dup, _ := checkPKDup(m, pk, 0, rowsPerGoroutine)
			results <- dup
			if dup {
				errors <- moerr.NewInternalErrorNoCtxf("unexpected duplicate in goroutine %d", goroutineID)
			}
		}(g)
	}

	wg.Wait()
	close(errors)
	close(results)

	for err := range errors {
		t.Errorf("concurrent checkPKDup error: %v", err)
	}

	// All goroutines should report no duplicates
	for dup := range results {
		require.False(t, dup, "no duplicates expected with unique values per goroutine")
	}
}

// TestConcurrentCheckPKDup_RealDupWithNulls ensures that concurrent
// checkPKDup calls correctly detect real duplicates even when NULLs
// are present, but never flag NULLs as duplicates of each other.
func TestConcurrentCheckPKDup_RealDupWithNulls(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	const numGoroutines = 4
	var wg sync.WaitGroup
	dupDetected := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			pk := vector.NewVec(types.T_int64.ToType())
			// [1, NULL, 2, NULL, 1] — contains real dup (1 appears twice)
			vector.AppendFixed(pk, int64(1), false, mp)
			vector.AppendFixed(pk, int64(0), true, mp)
			vector.AppendFixed(pk, int64(2), false, mp)
			vector.AppendFixed(pk, int64(0), true, mp)
			vector.AppendFixed(pk, int64(1), false, mp) // dup!
			defer pk.Free(mp)

			m := make(map[any]bool)
			dup, _ := checkPKDup(m, pk, 0, 5)
			dupDetected <- dup
		}()
	}

	wg.Wait()
	close(dupDetected)

	for dup := range dupDetected {
		require.True(t, dup, "real duplicate (value=1) must be detected even with NULLs present")
	}
}

// TestDupVectorWithoutNulls_ConcurrentSafety verifies that dupVectorWithoutNulls
// produces independent copies safe for concurrent InplaceSort.
func TestDupVectorWithoutNulls_ConcurrentSafety(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	// Original vector with NULLs
	orig := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(orig, int64(30), false, mp))
	require.NoError(t, vector.AppendFixed(orig, int64(0), true, mp))
	require.NoError(t, vector.AppendFixed(orig, int64(10), false, mp))
	require.NoError(t, vector.AppendFixed(orig, int64(0), true, mp))
	require.NoError(t, vector.AppendFixed(orig, int64(20), false, mp))
	defer orig.Free(mp)

	const numGoroutines = 4
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			dup, err := dupVectorWithoutNulls(orig, mp)
			require.NoError(t, err)
			defer dup.Free(mp)

			require.Equal(t, 3, dup.Length(), "should have 3 non-NULL values")
			require.False(t, dup.HasNull(), "filtered vector should have no NULLs")

			// InplaceSort on the copy should not corrupt original
			dup.InplaceSort()
		}()
	}

	wg.Wait()

	// Original should be unchanged
	require.Equal(t, 5, orig.Length())
	require.True(t, orig.HasNull())
}

// enableReenterWorkspaceReadViewFault turns on the fault that makes getTable
// simulate the internal-SQL leg of the issue #25557 deadlock chain (locking
// the transaction and capturing the workspace read view) and then fail
// with an injected error.
func enableReenterWorkspaceReadViewFault(t *testing.T) {
	fault.Enable()
	t.Cleanup(func() {
		// Fault injection is a process-level global switch. Removing the
		// fault point alone does not turn it off; subsequent tests would
		// run with injection still active, causing order-dependent
		// failures.
		fault.Disable()
	})
	rmFault, err := objectio.SimpleInject(objectio.FJ_CNReenterWorkspaceReadViewOnGetTable)
	require.NoError(t, err)
	t.Cleanup(rmFault)
}

// enableRogueUpdateOnGetTableFault turns on the iarg=2 variant of the fault:
// besides the internal-SQL simulation, getTable performs a rogue
// PublishReadView — a statement-boundary advance the fixed
// internal SQL never does — and then fails with the injected error.
func enableRogueUpdateOnGetTableFault(t *testing.T) {
	fault.Enable()
	t.Cleanup(func() {
		fault.Disable()
	})
	require.NoError(t, fault.AddFaultPoint(
		context.Background(), objectio.FJ_CNReenterWorkspaceReadViewOnGetTable,
		":::", "echo", 2, "", false))
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(
			context.Background(), objectio.FJ_CNReenterWorkspaceReadViewOnGetTable)
	})
}

// newDumpableTxnForTest builds a minimal Transaction whose workspace holds
// one user-table INSERT entry that dumpBatchLocked will try to flush,
// reaching getTable. The zero-valued engine config (all thresholds 0)
// guarantees the entry is not skipped.
func newDumpableTxnForTest(t *testing.T) *Transaction {
	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc:      proc,
		engine:    &Engine{},
		workspace: newTxnWorkspace(),
	}
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          INSERT,
		accountId:    0,
		tableId:      42, // must not be a catalog table id
		databaseId:   7,
		tableName:    "tbl",
		databaseName: "db",
		bat:          newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1, 2}),
	})
	t.Cleanup(func() {
		closeWorkspaceForTest(t, txn)
	})
	return txn
}

// requireBoundaryUntouched asserts that the internal SQL simulated inside
// the dump did not advance the statement boundary: boundary advances are
// statement-boundary actions, and an advance during a dump can cover
// workspace entries the compaction is about to rewrite.
func requireBoundaryUntouched(t *testing.T, txn *Transaction) {
	t.Helper()
	require.True(t, txn.workspace.publishedReadView().IsZero(),
		"internal SQL must not publish a new workspace read view")
}

// TestIssue25557_DumpBatchReentrantGetTableDoesNotDeadlock covers the
// original issue #25557 entry point:
//
//	txnTable.Write -> dumpBatch [acquires txn.Lock()]
//	  -> dumpBatchLocked -> dumpInsertBatchLocked -> getTable
//	    -> internal SQL -> NewCompile [locks the workspace]
//
// The fault point makes the internal-SQL leg deterministic. Before the fix
// the internal SQL re-entered txn.Lock() on the same goroutine and
// self-deadlocked.
func TestIssue25557_DumpBatchReentrantGetTableDoesNotDeadlock(t *testing.T) {
	enableReenterWorkspaceReadViewFault(t)
	txn := newDumpableTxnForTest(t)
	preDumpLen := txn.workspace.activeMutationCount()

	errCh := make(chan error, 1)
	go func() {
		errCh <- txn.dumpBatch(context.Background(), workspaceDumpAll(false))
	}()

	select {
	case err := <-errCh:
		// The injected error proves getTable was reached and the simulated
		// internal SQL locked the workspace instead of deadlocking.
		require.ErrorContains(t, err, "reenter workspace read view")
		requireBoundaryUntouched(t, txn)
		require.Equal(t, preDumpLen, txn.workspace.activeMutationCount(),
			"an aborted dump must leave the workspace untouched")
	case <-time.After(10 * time.Second):
		t.Fatal("dumpBatch deadlocked when getTable ran the internal-SQL " +
			"leg with txn.Lock held")
	}
}

// TestIssue25557_LockedDumpInsertBatchReentrantGetTableDoesNotDeadlock covers
// the locked entry points that call dumpBatchLocked directly without going
// through the dumpBatch wrapper (IncrStatementID, commit):
//
//	IncrStatementID [acquires txn.Lock()]
//	  -> dumpBatchLocked -> dumpInsertBatchLocked -> getTable
//	    -> internal SQL -> NewCompile [locks the workspace]
//
// This is the reproducer shape from the PR #25560 review: a fix scoped to
// the dumpBatch wrapper does not cover this path.
func TestIssue25557_LockedDumpInsertBatchReentrantGetTableDoesNotDeadlock(t *testing.T) {
	enableReenterWorkspaceReadViewFault(t)
	txn := newDumpableTxnForTest(t)
	preDumpLen := txn.workspace.activeMutationCount()

	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		txn.Lock()
		defer txn.Unlock()
		var pkCount int
		errCh <- txn.dumpInsertBatchLocked(
			context.Background(), fs, workspaceDumpAll(false), &pkCount)
	}()

	select {
	case err := <-errCh:
		require.ErrorContains(t, err, "reenter workspace read view")
		requireBoundaryUntouched(t, txn)
		require.Equal(t, preDumpLen, txn.workspace.activeMutationCount(),
			"an aborted dump must leave the workspace untouched")
	case <-time.After(10 * time.Second):
		t.Fatal("dumpInsertBatchLocked deadlocked when getTable ran the " +
			"internal-SQL leg with txn.Lock held")
	}
}

// TestIssue25557_DumpDeleteBatchReentrantGetTableDoesNotDeadlock covers the
// delete flush path, which has the same getTable -> internal SQL shape as
// the insert path.
func TestIssue25557_DumpDeleteBatchReentrantGetTableDoesNotDeadlock(t *testing.T) {
	enableReenterWorkspaceReadViewFault(t)

	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc:      proc,
		engine:    &Engine{},
		workspace: newTxnWorkspace(),
	}
	txn.appendWorkspaceEntryLocked(Entry{
		typ:          DELETE,
		accountId:    0,
		tableId:      42, // must not be a catalog table id
		databaseId:   7,
		tableName:    "tbl",
		databaseName: "db",
		bat:          newDeleteBatchForTest(t, proc, []int64{1, 2}),
	})
	t.Cleanup(func() {
		closeWorkspaceForTest(t, txn)
	})
	preDumpLen := txn.workspace.activeMutationCount()

	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		txn.Lock()
		defer txn.Unlock()
		errCh <- txn.dumpDeleteBatchLocked(
			context.Background(), fs, workspaceDumpAll(false))
	}()

	select {
	case err := <-errCh:
		require.ErrorContains(t, err, "reenter workspace read view")
		requireBoundaryUntouched(t, txn)
		require.Equal(t, preDumpLen, txn.workspace.activeMutationCount(),
			"an aborted dump must leave the workspace untouched")
	case <-time.After(10 * time.Second):
		t.Fatal("dumpDeleteBatchLocked deadlocked when getTable ran the " +
			"internal-SQL leg with txn.Lock held")
	}
}

// TestIssue25557_RogueBoundaryAdvanceCapturesConsistentState pins down the
// defense-in-depth property of the resolution window: even if some code did
// advance the statement boundary inside the window (which the fixed internal
// SQL never does — simulated here by the iarg=2 fault variant), it can only
// capture a consistent pre-compaction workspace, because the dump does not
// mutate the logical workspace before the window closes.
func TestIssue25557_RogueBoundaryAdvanceCapturesConsistentState(t *testing.T) {
	enableRogueUpdateOnGetTableFault(t)
	txn := newDumpableTxnForTest(t)
	preDumpLen := txn.workspace.activeMutationCount()

	errCh := make(chan error, 1)
	go func() {
		errCh <- txn.dumpBatch(context.Background(), workspaceDumpAll(false))
	}()

	select {
	case err := <-errCh:
		require.ErrorContains(t, err, "reenter workspace read view")
		published := txn.workspace.publishedReadView()
		require.Equal(t, uint64(preDumpLen), published.MaxMutationID(),
			"a view published inside the window must cover the complete "+
				"pre-dump workspace")
		require.Equal(t, preDumpLen, txn.workspace.activeMutationCount())
		// The captured logical read view must be safe for every reader.
		seen := 0
		require.NoError(t, txn.ForEachTableMutation(txn.CurrentReadView(), 0, 7, 42, func(workspaceEntryView) {
			seen++
		}))
		require.Equal(t, preDumpLen, seen)
	case <-time.After(10 * time.Second):
		t.Fatal("dumpBatch deadlocked on the rogue-update fault variant")
	}
}

// TestForEachTableMutationStableReadViewSurvivesRetirement verifies that a
// reader no longer depends on a mutable workspace slice position.
func TestForEachTableMutationStableReadViewSurvivesRetirement(t *testing.T) {
	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc:      proc,
		workspace: newTxnWorkspace(),
	}
	mutationID := txn.appendWorkspaceEntryLocked(Entry{
		databaseId: 7,
		tableId:    42,
		bat:        newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1}),
	})
	view := txn.CurrentReadView()
	require.NoError(t, txn.workspace.retireMutation(mutationID))

	seen := 0
	require.NoError(t, txn.ForEachTableMutation(view, 0, 7, 42, func(workspaceEntryView) {
		seen++
	}))
	require.Equal(t, 1, seen)

	seen = 0
	require.NoError(t, txn.ForEachTableMutation(txn.CurrentReadView(), 0, 7, 42, func(workspaceEntryView) {
		seen++
	}))
	require.Zero(t, seen)
	require.NoError(t, txn.workspace.close(proc.Mp()))
}
