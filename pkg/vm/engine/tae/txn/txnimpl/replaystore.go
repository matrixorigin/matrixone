// Copyright 2021 Matrix Origin
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

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

var ErrDebugReplay = moerr.NewInternalErrorNoCtx("debug")

type replayAObjectCreateObserver interface {
	RecordReplayAObjectCreate(id *common.ID, isTombstone bool, ts types.TS)
}

type replayTxnStore struct {
	txnbase.NoopTxnStore
	Cmd            *txnbase.TxnCmd
	Observer       wal.ReplayObserver
	catalog        *catalog.Catalog
	ctx            context.Context
	preparedTables map[uint64]*catalog.TableEntry
	preparedTxnID  string
	checkpointEnd  types.TS
}

func MakeReplayTxn(
	ctx context.Context,
	mgr *txnbase.TxnManager,
	txnCtx *txnbase.TxnCtx,
	lsn uint64,
	cmd *txnbase.TxnCmd,
	observer wal.ReplayObserver,
	catalog *catalog.Catalog,
	checkpointEnd types.TS,
) *txnbase.Txn {
	store := &replayTxnStore{
		Cmd:           cmd,
		Observer:      observer,
		catalog:       catalog,
		ctx:           ctx,
		checkpointEnd: checkpointEnd,
	}
	txn := txnbase.NewPersistedTxn(
		mgr,
		txnCtx,
		store,
		lsn,
		store.prepareCommit,
		store.prepareRollback,
		store.applyCommit,
		store.applyRollback)
	return txn
}
func (store *replayTxnStore) GetContext() context.Context {
	return store.ctx
}
func (store *replayTxnStore) IsOffline() bool  { return false }
func (store *replayTxnStore) IsReadonly() bool { return false }

func (store *replayTxnStore) prepareCommit(txn txnif.AsyncTxn) (err error) {
	// PrepareCommit all commands
	// Check idempotent of each command
	// Record all idempotent error commands and skip apply|rollback later
	store.Observer.OnTimeStamp(txn.GetPrepareTS())
	for _, command := range store.Cmd.Cmds {
		command.SetReplayTxn(txn)
		store.prepareCmd(command, txn)
	}
	store.registerPreparedDMLTables(txn)
	return
}

func (store *replayTxnStore) applyCommit(txn txnif.AsyncTxn) (err error) {
	store.Cmd.ApplyCommit()
	visibilityTS := txn.GetCommitTS()
	store.resolvePreparedDMLTables(&visibilityTS)
	return
}

func (store *replayTxnStore) applyRollback(txn txnif.AsyncTxn) (err error) {
	store.Cmd.ApplyRollback()
	store.resolvePreparedDMLTables(nil)
	return
}

func (store *replayTxnStore) registerPreparedDMLTables(txn txnif.AsyncTxn) {
	dirty := txn.GetMemo().GetDirty()
	if dirty == nil || dirty.IsEmpty() {
		return
	}
	store.preparedTxnID = txn.GetID()
	store.preparedTables = make(map[uint64]*catalog.TableEntry, dirty.TableCount())
	for _, record := range dirty.Tables {
		db, err := store.catalog.GetDatabaseByID(record.DbID)
		if err != nil {
			// The checkpoint-restored catalog may have GCed a database that
			// an idempotently replayed dirty-table memo still references.
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				continue
			}
			panic(err)
		}
		table, err := db.GetTableEntryByID(record.ID)
		if err != nil {
			// Keep replay idempotent for tables removed by the checkpoint.
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				continue
			}
			panic(err)
		}
		table.RegisterReplayedPreparedDML(store.preparedTxnID)
		store.preparedTables[record.ID] = table
	}
}

func (store *replayTxnStore) resolvePreparedDMLTables(visibilityTS *types.TS) {
	for _, table := range store.preparedTables {
		table.ResolveReplayedPreparedDML(store.preparedTxnID, visibilityTS)
	}
	store.preparedTables = nil
}

func (store *replayTxnStore) prepareRollback(txn txnif.AsyncTxn) (err error) {
	panic(moerr.NewInternalErrorNoCtxf("cannot prepareRollback rollback replay txn: %s",
		txn.String()))
}

func (store *replayTxnStore) prepareCmd(txncmd txnif.TxnCmd, txn txnif.TxnReader) {
	if txncmd.GetType() != txnbase.IOET_WALTxnEntry {
		common.DoIfDebugEnabled(func() {
			logutil.Debug(
				"",
				common.OperationField("replay-cmd"),
				common.OperandField(txncmd.Desc()),
			)
		})
	}
	switch cmd := txncmd.(type) {
	case *catalog.EntryCommand[*catalog.EmptyMVCCNode, *catalog.DBNode],
		*catalog.EntryCommand[*catalog.TableMVCCNode, *catalog.TableNode],
		*catalog.EntryCommand[*catalog.MetadataMVCCNode, *catalog.ObjectNode],
		*catalog.EntryCommand[*catalog.ObjectMVCCNode, *catalog.ObjectNode],
		*catalog.EntryCommand[*catalog.MetadataMVCCNode, *catalog.BlockNode]:
		store.catalog.ReplayCmd(txncmd, store.Observer)
	case *AppendCmd:
		store.replayAppendData(cmd, txn, store.Observer)
	case *updates.UpdateCmd:
		store.replayDataCmds(
			cmd, store.Observer)
	}
}

func (store *replayTxnStore) replayAppendData(cmd *AppendCmd, txn txnif.TxnReader, observer wal.ReplayObserver) {
	hasActive := false
	_, sarg, _ := fault.TriggerFault("replay debug log")
	for _, info := range cmd.Infos {
		id := info.GetDest()
		blk, _, err := store.ensureReplayAObject(id, cmd.IsTombstone, cmd.Ts, observer)
		if sarg != "" {
			err = ErrDebugReplay
		}
		if err != nil {
			logutil.Infof("cmd %v\ncatalog: %v", cmd.String(), store.catalog.SimplePPString(3))
			if err != ErrDebugReplay {
				panic(err)
			}
		}
		if !blk.IsActive() {
			continue
		}
		if blk.ObjectPersisted() {
			continue
		}
		hasActive = true
	}

	if !hasActive {
		return
	}

	data := cmd.Data
	if data != nil {
		defer data.Close()
	}

	for _, info := range cmd.Infos {
		id := info.GetDest()
		blk, _, err := store.ensureReplayAObject(id, cmd.IsTombstone, cmd.Ts, observer)
		if sarg != "" {
			err = ErrDebugReplay
		}
		if err != nil {
			logutil.Infof("cmd %v\ncatalog: %v", cmd.String(), store.catalog.SimplePPString(3))
			if err != ErrDebugReplay {
				panic(err)
			}
		}
		if !blk.IsActive() {
			continue
		}
		if blk.ObjectPersisted() {
			continue
		}
		table := blk.GetTable()
		source, previous, fromCheckpoint := table.GetReplayAppendSchemas(txn, cmd.IsTombstone)
		if source == nil {
			panic(moerr.NewInternalErrorNoCtxf("no visible WAL append schema for table %d at %s",
				id.TableID, txn.GetStartTS().ToString()))
		}
		if !cmd.IsTombstone {
			// New appends across a rename use PrepareTS names in physical
			// sequence order. With a checkpoint the StartTS version may
			// have collapsed to this very schema.
			prepared := table.GetAppendSchemaAtPrepare(txn)
			if prepared != nil && prepared != source && sameAppendColumnLayout(source, prepared) &&
				walColumnsInSchemaOrder(data.Attrs, prepared) {
				source = prepared
				fromCheckpoint = false
			}
		}
		if previous != nil && !walColumnsInSchemaOrder(data.Attrs, source) &&
			isLegacyRenameAppend(data.Attrs, previous, source) {
			source = previous
			fromCheckpoint = false
		}
		start := info.GetSrcOff()
		bat := data.CloneWindow(int(start), int(info.GetSrcLen()))
		bat.Compact()
		defer bat.Close()
		startTS := txn.GetStartTS()
		if fromCheckpoint && !pkgcatalog.IsSystemTable(table.ID) && startTS.LT(&store.checkpointEnd) &&
			!walColumnsInSchemaOrder(bat.Attrs, source) {
			if err = recoverCheckpointAppendNames(bat, source); err != nil {
				panic(moerr.NewInternalErrorNoCtxf("cannot resolve WAL append columns for table %d (checkpoint schema version %d): %v",
					id.TableID, source.Version, err))
			}
		}
		if err = blk.GetObjectData().OnReplayAppendPayload(bat, info.GetDestOff(), source); err != nil || sarg != "" {
			logutil.Infof("cmd %v\ncatalog: %v", cmd.String(), store.catalog.SimplePPString(3))
			if sarg == "" {
				panic(err)
			}
		}
	}
}

// Legacy WAL writes every column in schema order. If recovery has only the
// checkpoint's renamed schema, a complete ordinal layout proves that no
// sequence could have been retired or inserted since the writer's snapshot:
// an ADD would change the width, and a DROP would leave a sequence gap or a
// larger allocation watermark. Sparse and evolved layouts require the source
// schema. Reject an unproven mapping even if all names and types match,
// because a rename can permute names without changing the physical layout.
func recoverCheckpointAppendNames(bat *containers.Batch, schema *catalog.Schema) error {
	n := len(schema.ColDefs)
	if schema.Version == 0 || schema.ColumnReadSpec() != nil ||
		schema.PhyAddrKey == nil || schema.PhyAddrKey.Idx != n-1 ||
		int(schema.Extra.NextColSeqnum) != n-1 || len(bat.Attrs) != n || len(bat.Vecs) != n ||
		len(bat.Nameidx) != n || bat.Attrs[n-1] != schema.PhyAddrKey.Name {
		return moerr.NewInternalErrorNoCtx("cannot prove a complete dense WAL column layout")
	}
	for i, col := range schema.ColDefs {
		pos, ok := bat.Nameidx[bat.Attrs[i]]
		if !ok || pos != i || bat.Attrs[i] == "" || bat.Vecs[i] == nil ||
			!col.Type.Eq(*bat.Vecs[i].GetType()) || (!col.PhyAddr && int(col.SeqNum) != i) {
			return moerr.NewInternalErrorNoCtxf("invalid WAL column layout at position %d", i)
		}
	}
	clear(bat.Nameidx)
	for i, col := range schema.ColDefs {
		bat.Attrs[i] = col.Name
		bat.Nameidx[col.Name] = i
	}
	return nil
}

func walColumnsInSchemaOrder(attrs []string, schema *catalog.Schema) bool {
	if len(attrs) != len(schema.ColDefs) {
		return false
	}
	for i, attr := range attrs {
		if attr != schema.ColDefs[i].Name {
			return false
		}
	}
	return true
}

// Older writers could leave buffered rows under their original names after
// their own rename. Require the complete ordered layout, including the row ID.
func isLegacyRenameAppend(attrs []string, previous, current *catalog.Schema) bool {
	return sameAppendColumnLayout(previous, current) && walColumnsInSchemaOrder(attrs, previous)
}

func (store *replayTxnStore) replayDataCmds(cmd *updates.UpdateCmd, observer wal.ReplayObserver) {
	switch cmd.GetType() {
	case updates.IOET_WALTxnCommand_AppendNode:
		store.replayAppend(cmd, observer)
		// case updates.IOET_WALTxnCommand_DeleteNode, updates.IOET_WALTxnCommand_PersistedDeleteNode:
		// 	store.replayDelete(cmd, observer)
	}
}

func (store *replayTxnStore) replayAppend(cmd *updates.UpdateCmd, observer wal.ReplayObserver) {
	appendNode := cmd.GetAppendNode()
	id := appendNode.GetID()
	_, sarg, _ := fault.TriggerFault("replay debug log")
	obj, _, err := store.ensureReplayAObject(id, appendNode.IsTombstone(), replayAppendNodeCreateTS(appendNode), observer)
	if sarg != "" {
		err = ErrDebugReplay
	}
	if err != nil {
		logutil.Infof("cmd %v\ncatalog: %v", cmd.String(), store.catalog.SimplePPString(3))
		if err != ErrDebugReplay {
			panic(err)
		}
	}
	if !obj.IsActive() {
		return
	}
	if obj.ObjectPersisted() {
		return
	}
	if err = obj.GetObjectData().OnReplayAppend(appendNode); err != nil || sarg != "" {
		logutil.Infof("cmd %v\ncatalog: %v", cmd.String(), store.catalog.SimplePPString(3))
		if sarg == "" {
			panic(err)
		}
	}
}

func replayAppendNodeCreateTS(node *updates.AppendNode) types.TS {
	if txn := node.GetTxn(); txn != nil {
		return txn.GetCommitTS()
	}
	if ts := node.GetCommitTS(); !ts.IsEmpty() {
		return ts
	}
	return node.GetPrepare()
}

func (store *replayTxnStore) ensureReplayAObject(
	id *common.ID,
	isTombstone bool,
	createHint types.TS,
	observer wal.ReplayObserver,
) (obj *catalog.ObjectEntry, created bool, err error) {
	database, err := store.catalog.GetDatabaseByID(id.DbID)
	if err != nil {
		return
	}
	obj, err = database.GetObjectEntryByID(id, isTombstone)
	if err == nil {
		return
	}
	if !moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
		return
	}
	table, err := database.GetTableEntryByID(id.TableID)
	if err != nil {
		return
	}
	stats := objectio.NewObjectStatsWithObjectID(id.ObjectID(), true, isTombstone, false)
	var factory catalog.ObjectDataFactory
	if store.catalog.DataFactory != nil {
		factory = store.catalog.DataFactory.MakeObjectFactory()
	}
	obj, err = table.CreateCommittedObject(
		createHint,
		&objectio.CreateObjOpt{Stats: stats, IsTombstone: isTombstone},
		factory,
	)
	if err != nil {
		return
	}
	created = true
	if recorder, ok := observer.(replayAObjectCreateObserver); ok {
		recorder.RecordReplayAObjectCreate(id, isTombstone, createHint)
	}
	return
}
