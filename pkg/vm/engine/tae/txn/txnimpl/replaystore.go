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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type replayTxnStore struct {
	txnbase.NoopTxnStore
	Cmd         *txnbase.TxnCmd
	Observer    wal.ReplayObserver
	catalog     *catalog.Catalog
	dataFactory *tables.DataFactory
	wal         wal.Driver
	ctx         context.Context
}

func MakeReplayTxn(
	ctx context.Context,
	mgr *txnbase.TxnManager,
	txnCtx *txnbase.TxnCtx,
	lsn uint64,
	cmd *txnbase.TxnCmd,
	observer wal.ReplayObserver,
	catalog *catalog.Catalog,
	dataFactory *tables.DataFactory,
	wal wal.Driver) *txnbase.Txn {
	store := &replayTxnStore{
		Cmd:         cmd,
		Observer:    observer,
		catalog:     catalog,
		dataFactory: dataFactory,
		wal:         wal,
		ctx:         ctx,
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
func (store *replayTxnStore) IsReadonly() bool { return false }

func (store *replayTxnStore) prepareCommit(txn txnif.AsyncTxn) (err error) {
	// PrepareCommit all commands
	// Check idempotent of each command
	// Record all idempotent error commands and skip apply|rollback later
	store.Observer.OnTimeStamp(txn.GetPrepareTS())
	for _, command := range store.Cmd.Cmds {
		command.SetReplayTxn(txn)
		store.prepareCmd(command)
	}
	return
}

func (store *replayTxnStore) applyCommit(txn txnif.AsyncTxn) (err error) {
	store.Cmd.ApplyCommit()
	return
}

func (store *replayTxnStore) applyRollback(txn txnif.AsyncTxn) (err error) {
	store.Cmd.ApplyRollback()
	return
}

func (store *replayTxnStore) prepareRollback(txn txnif.AsyncTxn) (err error) {
	panic(moerr.NewInternalErrorNoCtx("cannot prepareRollback rollback replay txn: %s",
		txn.String()))
}

func (store *replayTxnStore) prepareCmd(txncmd txnif.TxnCmd) {
	if txncmd.GetType() != txnbase.IOET_WALTxnEntry {
		logutil.Debug("", common.OperationField("replay-cmd"),
			common.OperandField(txncmd.Desc()))
	}
	var err error
	switch cmd := txncmd.(type) {
	case *catalog.EntryCommand[*catalog.EmptyMVCCNode, *catalog.DBNode],
		*catalog.EntryCommand[*catalog.TableMVCCNode, *catalog.TableNode],
		*catalog.EntryCommand[*catalog.MetadataMVCCNode, *catalog.ObjectNode],
		*catalog.EntryCommand[*catalog.ObjectMVCCNode, *catalog.ObjectNode],
		*catalog.EntryCommand[*catalog.MetadataMVCCNode, *catalog.BlockNode]:
		store.catalog.ReplayCmd(txncmd, store.dataFactory, store.Observer)
	case *AppendCmd:
		store.replayAppendData(cmd, store.Observer)
	case *updates.UpdateCmd:
		store.replayDataCmds(cmd, store.Observer)
	}
	if err != nil {
		panic(err)
	}
}

func (store *replayTxnStore) replayAppendData(cmd *AppendCmd, observer wal.ReplayObserver) {
	hasActive := false
	for _, info := range cmd.Infos {
		id := info.GetDest()
		database, err := store.catalog.GetDatabaseByID(id.DbID)
		if err != nil {
			panic(err)
		}
		blk, err := database.GetBlockEntryByID(id)
		if err != nil {
			panic(err)
		}
		if !blk.IsActive() {
			continue
		}
		if !blk.GetMetaLoc().IsEmpty() {
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
		database, err := store.catalog.GetDatabaseByID(id.DbID)
		if err != nil {
			panic(err)
		}
		blk, err := database.GetBlockEntryByID(id)
		if err != nil {
			panic(err)
		}
		if !blk.IsActive() {
			continue
		}
		if !blk.GetMetaLoc().IsEmpty() {
			continue
		}
		start := info.GetSrcOff()
		bat := data.CloneWindow(int(start), int(info.GetSrcLen()))
		bat.Compact()
		defer bat.Close()
		if err = blk.GetBlockData().OnReplayAppendPayload(bat); err != nil {
			panic(err)
		}
	}
}

func (store *replayTxnStore) replayDataCmds(cmd *updates.UpdateCmd, observer wal.ReplayObserver) {
	switch cmd.GetType() {
	case updates.IOET_WALTxnCommand_AppendNode:
		store.replayAppend(cmd, observer)
	case updates.IOET_WALTxnCommand_DeleteNode, updates.IOET_WALTxnCommand_PersistedDeleteNode:
		store.replayDelete(cmd, observer)
	}
}

func (store *replayTxnStore) replayDelete(cmd *updates.UpdateCmd, observer wal.ReplayObserver) {
	deleteNode := cmd.GetDeleteNode()
	if deleteNode.Is1PC() {
		if _, err := deleteNode.TxnMVCCNode.ApplyCommit(); err != nil {
			panic(err)
		}
	}
	id := deleteNode.GetID()
	database, err := store.catalog.GetDatabaseByID(id.DbID)
	if err != nil {
		panic(err)
	}
	blk, err := database.GetBlockEntryByID(id)
	if err != nil {
		panic(err)
	}
	if !blk.IsActive() {
		return
	}
	blkData := blk.GetBlockData()
	err = blkData.OnReplayDelete(deleteNode)
	if err != nil {
		panic(err)
	}

}

func (store *replayTxnStore) replayAppend(cmd *updates.UpdateCmd, observer wal.ReplayObserver) {
	appendNode := cmd.GetAppendNode()
	if appendNode.Is1PC() {
		if _, err := appendNode.TxnMVCCNode.ApplyCommit(); err != nil {
			panic(err)
		}
	}
	id := appendNode.GetID()
	database, err := store.catalog.GetDatabaseByID(id.DbID)
	if err != nil {
		panic(err)
	}
	blk, err := database.GetBlockEntryByID(id)
	if err != nil {
		panic(err)
	}
	if !blk.IsActive() {
		return
	}
	if !blk.GetMetaLoc().IsEmpty() {
		return
	}
	if err = blk.GetBlockData().OnReplayAppend(appendNode); err != nil {
		panic(err)
	}
}
