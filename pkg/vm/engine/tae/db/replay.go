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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type Replayer struct {
	DataFactory   *tables.DataFactory
	db            *DB
	maxTs         types.TS
	staleIndexes  []*wal.Index
	once          sync.Once
	ckpedTS       types.TS
	wg            sync.WaitGroup
	applyDuration time.Duration
	txnCmdChan    chan *txnbase.TxnCmd
}

func newReplayer(dataFactory *tables.DataFactory, db *DB, ckpedTS types.TS) *Replayer {
	return &Replayer{
		DataFactory:  dataFactory,
		db:           db,
		staleIndexes: make([]*wal.Index, 0),
		ckpedTS:      ckpedTS,
		wg:           sync.WaitGroup{},
		txnCmdChan:   make(chan *txnbase.TxnCmd, 100),
	}
}

func (replayer *Replayer) PreReplayWal() {
	processor := new(catalog.LoopProcessor)
	processor.BlockFn = func(entry *catalog.BlockEntry) (err error) {
		entry.InitData(replayer.DataFactory)
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
	replayer.wg.Add(1)
	go replayer.applyTxnCmds()
	if err := replayer.db.Wal.Replay(replayer.OnReplayEntry); err != nil {
		panic(err)
	}
	replayer.txnCmdChan <- txnbase.NewLastTxnCmd()
	close(replayer.txnCmdChan)
	replayer.wg.Wait()
	logutil.Info("open-tae", common.OperationField("replay"),
		common.OperandField("wal"),
		common.AnyField("apply logentries cost", replayer.applyDuration))
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
	head := objectio.DecodeIOEntryHeader(payload)
	codec := objectio.GetIOEntryCodec(*head)
	entry, err := codec.Decode(payload[4:])
	txnCmd := entry.(*txnbase.TxnCmd)
	txnCmd.Idx = idxCtx
	if err != nil {
		panic(err)
	}
	replayer.txnCmdChan <- txnCmd
}
func (replayer *Replayer) applyTxnCmds() {
	defer replayer.wg.Done()
	for {
		txnCmd := <-replayer.txnCmdChan
		if txnCmd.IsLastCmd() {
			break
		}
		t0 := time.Now()
		replayer.OnReplayTxn(txnCmd, txnCmd.Idx, txnCmd.Idx.LSN)
		txnCmd.Close()
		replayer.applyDuration += time.Since(t0)

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

func (replayer *Replayer) OnReplayTxn(cmd txnif.TxnCmd, walIdx *wal.Index, lsn uint64) {
	var err error
	txnCmd := cmd.(*txnbase.TxnCmd)
	if txnCmd.PrepareTS.LessEq(replayer.maxTs) {
		return
	}
	txn := txnimpl.MakeReplayTxn(replayer.db.TxnMgr, txnCmd.TxnCtx, lsn,
		txnCmd, replayer, replayer.db.Catalog, replayer.DataFactory, replayer.db.Wal)
	if err = replayer.db.TxnMgr.OnReplayTxn(txn); err != nil {
		panic(err)
	}
	if txn.Is2PC() {
		if _, err = txn.Prepare(); err != nil {
			panic(err)
		}
	} else {
		if err = txn.Commit(); err != nil {
			panic(err)
		}
	}
}
