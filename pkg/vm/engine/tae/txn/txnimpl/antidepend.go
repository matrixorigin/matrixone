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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type warChecker struct {
	txn      txnif.AsyncTxn
	catalog  *catalog.Catalog
	symTable map[string]bool
}

func newWarChecker(txn txnif.AsyncTxn, c *catalog.Catalog) *warChecker {
	return &warChecker{
		symTable: make(map[string]bool),
		txn:      txn,
		catalog:  c,
	}
}

func (checker *warChecker) readSymbol(symbol string) {
	if _, ok := checker.symTable[symbol]; !ok {
		checker.symTable[symbol] = false
	}
}

func (checker *warChecker) Read(dbId uint64, id *common.ID) {
	buf := txnbase.KeyEncoder.EncodeDB(dbId)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeTable(dbId, id.TableID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeSegment(dbId, id.TableID, id.SegmentID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeBlock(dbId, id.TableID, id.SegmentID, id.BlockID)
	checker.readSymbol(string(buf))
}

func (checker *warChecker) ReadDB(id uint64) {
	buf := txnbase.KeyEncoder.EncodeDB(id)
	checker.readSymbol(string(buf))
}

func (checker *warChecker) ReadTable(dbId uint64, id *common.ID) {
	buf := txnbase.KeyEncoder.EncodeDB(dbId)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeTable(dbId, id.TableID)
	checker.readSymbol(string(buf))
}

func (checker *warChecker) ReadSegment(dbId uint64, id *common.ID) {
	buf := txnbase.KeyEncoder.EncodeDB(dbId)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeTable(dbId, id.TableID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeSegment(dbId, id.TableID, id.SegmentID)
	checker.readSymbol(string(buf))
}

func (checker *warChecker) ReadBlock(dbId uint64, id *common.ID) {
	buf := txnbase.KeyEncoder.EncodeDB(dbId)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeTable(dbId, id.TableID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeSegment(dbId, id.TableID, id.SegmentID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeBlock(dbId, id.TableID, id.SegmentID, id.BlockID)
	checker.readSymbol(string(buf))
}

func (checker *warChecker) check() (err error) {
	var entry *catalog.BaseEntry
	for key := range checker.symTable {
		keyt, did, tid, sid, bid := txnbase.KeyEncoder.Decode([]byte(key))
		db, err := checker.catalog.GetDatabaseByID(did)
		if err != nil {
			panic(err)
		}
		switch keyt {
		case txnbase.KeyT_DBEntry:
			entry = db.BaseEntry
		case txnbase.KeyT_TableEntry:
			tb, err := db.GetTableEntryByID(tid)
			if err != nil {
				panic(err)
			}
			entry = tb.BaseEntry
		case txnbase.KeyT_SegmentEntry:
			tb, err := db.GetTableEntryByID(tid)
			if err != nil {
				panic(err)
			}
			seg, err := tb.GetSegmentByID(sid)
			if err != nil {
				panic(err)
			}
			entry = seg.BaseEntry
		case txnbase.KeyT_BlockEntry:
			tb, err := db.GetTableEntryByID(tid)
			if err != nil {
				panic(err)
			}
			seg, err := tb.GetSegmentByID(sid)
			if err != nil {
				panic(err)
			}
			blk, err := seg.GetBlockEntryByID(bid)
			if err != nil {
				panic(err)
			}
			entry = blk.BaseEntry
		}
		if entry != nil {
			commitTs := checker.txn.GetCommitTS()
			entry.RLock()
			if entry.DeleteBefore(commitTs) {
				if !entry.IsCommitting() {
					entry.RUnlock()
					return txnif.ErrTxnRWConflict
				}
				eTxn := entry.GetTxn()
				entry.RUnlock()
				state := eTxn.GetTxnState(true)
				if state == txnif.TxnStateCommitted {
					logutil.Infof("ErrTxnRWConflict Found:[%s]<===RW===[%s]", eTxn.String(), checker.txn.String())
					return txnif.ErrTxnRWConflict
				} else if state == txnif.TxnStateUnknown {
					return txnif.ErrTxnInternal
				}
				entry.RLock()
			}
			entry.RUnlock()
		}
	}
	return
}
