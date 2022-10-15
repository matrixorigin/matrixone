// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"bytes"

	//"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

const DefaultReplayCacheSize = 2 * common.M

type Replayer struct {
	DataFactory  *tables.DataFactory
	db           *DB
	maxTs        types.TS
	cache        *bytes.Buffer
	staleIndexes []*wal.Index
	once         sync.Once
}

func newReplayer(dataFactory *tables.DataFactory, db *DB) *Replayer {
	return &Replayer{
		DataFactory:  dataFactory,
		db:           db,
		cache:        bytes.NewBuffer(make([]byte, DefaultReplayCacheSize)),
		staleIndexes: make([]*wal.Index, 0),
	}
}

func (replayer *Replayer) PreReplayWal() {
	processor := new(catalog.LoopProcessor)
	processor.BlockFn = func(entry *catalog.BlockEntry) (err error) {
		entry.InitData(replayer.DataFactory)
		blkData := entry.GetBlockData()
		replayer.OnTimeStamp(blkData.GetMaxCheckpointTS())
		return
	}
	processor.SegmentFn = func(entry *catalog.SegmentEntry) (err error) {
		if entry.GetTable().IsVirtual() {
			return moerr.GetOkStopCurrRecur()
		}
		dropCommit := entry.TreeMaxDropCommitEntry()
		if dropCommit != nil && dropCommit.GetLogIndex().LSN <= replayer.db.Wal.GetCheckpointed() {
			return moerr.GetOkStopCurrRecur()
		}
		entry.InitData(replayer.DataFactory)
		return
	}
	if err := replayer.db.Catalog.RecurLoop(processor); err != nil {
		if !moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			panic(err)
		}
	}
}

func (replayer *Replayer) Replay() {
	if err := replayer.db.Wal.Replay(replayer.OnReplayEntry); err != nil {
		panic(err)
	}
	if _, err := replayer.db.Wal.Checkpoint(replayer.staleIndexes); err != nil {
		panic(err)
	}
}

func (replayer *Replayer) OnStaleIndex(idx *wal.Index) {
	replayer.staleIndexes = append(replayer.staleIndexes, idx)
}

func (replayer *Replayer) OnReplayEntry(group uint32, lsn uint64, payload []byte, typ uint16, info any) {
	replayer.once.Do(replayer.PreReplayWal)
	if group != wal.GroupPrepare && group != wal.GroupC {
		return
	}
	idxCtx := store.NewIndex(lsn, 0, 0)
	r := bytes.NewBuffer(payload)
	txnCmd, _, err := txnbase.BuildCommandFrom(r)
	if err != nil {
		panic(err)
	}
	defer txnCmd.Close()
	replayer.OnReplayCmd(txnCmd, idxCtx, txnif.CmdInvalid, types.TS{})
	if err != nil {
		panic(err)
	}
}

func (replayer *Replayer) GetMaxTS() types.TS {
	return replayer.maxTs
}

func (replayer *Replayer) OnTimeStamp(ts types.TS) {
	if ts.Greater(replayer.maxTs) {
		replayer.maxTs = ts
	}
}

func (replayer *Replayer) OnReplayCmd(txncmd txnif.TxnCmd, idxCtx *wal.Index, cmdType txnif.CmdType, commitTS types.TS) {
	if idxCtx != nil && idxCtx.Size > 0 {
		logutil.Debug("", common.OperationField("replay-cmd"),
			common.OperandField(txncmd.Desc()),
			common.AnyField("index", idxCtx.String()))
	}
	var err error
	switch cmd := txncmd.(type) {
	case *txnbase.TxnCmd:
		idxCtx.Size = cmd.CmdSize
		internalCnt := uint32(0)
		cmdType := txnif.CmdPrepare
		if !cmd.Is2PC() {
			cmdType = txnif.Cmd1PC
		}
		for i, command := range cmd.Cmds {
			_, ok := command.(*txnimpl.AppendCmd)
			if ok {
				internalCnt++
				replayer.OnReplayCmd(command, nil, cmdType, commitTS)
			} else {
				idx := idxCtx.Clone()
				idx.CSN = uint32(i) - internalCnt
				replayer.OnReplayCmd(command, idx, cmdType, commitTS)
			}
		}
	case *catalog.EntryCommand:
		replayer.db.Catalog.ReplayCmd(txncmd, replayer.DataFactory, idxCtx, replayer, replayer.cache, cmdType, commitTS)
	case *txnimpl.AppendCmd:
		replayer.db.onReplayAppendCmd(cmd, replayer, cmdType, commitTS)
	case *updates.UpdateCmd:
		err = replayer.db.onReplayUpdateCmd(cmd, idxCtx, replayer, cmdType, commitTS)
	}
	if err != nil {
		panic(err)
	}
}

func (db *DB) onReplayAppendCmd(cmd *txnimpl.AppendCmd, observer wal.ReplayObserver, cmdType txnif.CmdType, commitTS types.TS) {
	if cmdType == txnif.CmdCommit {
		return
	}
	hasActive := false
	for _, info := range cmd.Infos {
		database, err := db.Catalog.GetDatabaseByID(info.GetDBID())
		if err != nil {
			panic(err)
		}
		id := info.GetDest()
		blk, err := database.GetBlockEntryByID(id)
		if err != nil {
			panic(err)
		}
		if !blk.IsActive() {
			continue
		}
		if observer != nil {
			observer.OnTimeStamp(blk.GetBlockData().GetMaxCheckpointTS())
		}
		if !blk.GetBlockData().GetMaxCheckpointTS().IsEmpty() {
			continue
		}
		hasActive = true
	}

	if !hasActive {
		return
	}

	var data *containers.Batch

	for _, subTxnCmd := range cmd.Cmds {
		switch subCmd := subTxnCmd.(type) {
		case *txnbase.BatchCmd:
			data = subCmd.Bat
		case *txnbase.PointerCmd:
			batEntry, err := db.Wal.LoadEntry(subCmd.Group, subCmd.Lsn)
			if err != nil {
				panic(err)
			}
			r := bytes.NewBuffer(batEntry.GetPayload())
			txnCmd, _, err := txnbase.BuildCommandFrom(r)
			if err != nil {
				panic(err)
			}
			data = txnCmd.(*txnbase.BatchCmd).Bat
			batEntry.Free()
		}
	}
	if data != nil {
		defer data.Close()
	}

	for _, info := range cmd.Infos {
		database, err := db.Catalog.GetDatabaseByID(info.GetDBID())
		if err != nil {
			panic(err)
		}
		id := info.GetDest()
		blk, err := database.GetBlockEntryByID(id)
		if err != nil {
			panic(err)
		}
		if !blk.IsActive() {
			continue
		}
		if observer != nil {
			observer.OnTimeStamp(blk.GetBlockData().GetMaxCheckpointTS())
		}
		if cmd.Ts.LessEq(blk.GetBlockData().GetMaxCheckpointTS()) {
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

func (db *DB) onReplayUpdateCmd(cmd *updates.UpdateCmd, idxCtx *wal.Index, observer wal.ReplayObserver, cmdType txnif.CmdType, commitTS types.TS) (err error) {
	switch cmd.GetType() {
	case txnbase.CmdAppend:
		db.onReplayAppend(cmd, idxCtx, observer, cmdType, commitTS)
	case txnbase.CmdDelete:
		db.onReplayDelete(cmd, idxCtx, observer, cmdType, commitTS)
	}
	return
}

func (db *DB) onReplayDelete(cmd *updates.UpdateCmd, idxCtx *wal.Index, observer wal.ReplayObserver, cmdType txnif.CmdType, commitTS types.TS) {
	switch cmdType {
	case txnif.CmdInvalid:
		panic("invalid type")
	case txnif.CmdPrepare:
		return
	}
	database, err := db.Catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		panic(err)
	}
	deleteNode := cmd.GetDeleteNode()
	deleteNode.SetLogIndex(idxCtx)
	if cmdType == txnif.Cmd1PC || deleteNode.Is1PC() {
		commitTS = deleteNode.GetPrepareTS()
	}
	deleteNode.OnReplayCommit(commitTS)
	id := deleteNode.GetID()
	blk, err := database.GetBlockEntryByID(id)
	if err != nil {
		panic(err)
	}
	if !blk.IsActive() {
		observer.OnStaleIndex(idxCtx)
		return
	}
	if commitTS.LessEq(blk.GetBlockData().GetMaxCheckpointTS()) {
		observer.OnStaleIndex(idxCtx)
		return
	}
	blkData := blk.GetBlockData()
	err = blkData.OnReplayDelete(deleteNode)
	if err != nil {
		panic(err)
	}
	if observer != nil {
		observer.OnTimeStamp(commitTS)
	}
}

func (db *DB) onReplayAppend(cmd *updates.UpdateCmd, idxCtx *wal.Index, observer wal.ReplayObserver, cmdType txnif.CmdType, commitTS types.TS) {
	switch cmdType {
	case txnif.CmdInvalid:
		panic("invalid type")
	case txnif.CmdPrepare:
		return
	}
	database, err := db.Catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		panic(err)
	}
	appendNode := cmd.GetAppendNode()
	appendNode.SetLogIndex(idxCtx)
	if cmdType == txnif.Cmd1PC || appendNode.Is1PC() {
		commitTS = appendNode.GetPrepare()
	}
	appendNode.OnReplayCommit(commitTS)
	id := appendNode.GetID()
	blk, err := database.GetBlockEntryByID(id)
	if err != nil {
		panic(err)
	}
	if !blk.IsActive() {
		observer.OnStaleIndex(idxCtx)
		return
	}
	if appendNode.GetCommitTS().LessEq(blk.GetBlockData().GetMaxCheckpointTS()) {
		observer.OnStaleIndex(idxCtx)
		return
	}
	if err = blk.GetBlockData().OnReplayAppend(appendNode); err != nil {
		panic(err)
	}
	if observer != nil {
		observer.OnTimeStamp(appendNode.GetCommitTS())
	}
}
