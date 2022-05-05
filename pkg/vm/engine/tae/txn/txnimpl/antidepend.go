package txnimpl

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type warChecker struct {
	txn      txnif.AsyncTxn
	db       *catalog.DBEntry
	symTable map[string]bool
}

func newWarChecker(txn txnif.AsyncTxn, db *catalog.DBEntry) *warChecker {
	return &warChecker{
		symTable: make(map[string]bool),
		db:       db,
		txn:      txn,
	}
}

func (checker *warChecker) readSymbol(symbol string) {
	if _, ok := checker.symTable[symbol]; !ok {
		checker.symTable[symbol] = false
	}
}

func (checker *warChecker) Read(id *common.ID) {
	buf := txnbase.KeyEncoder.EncodeDB(checker.db.ID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeTable(checker.db.ID, id.TableID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeSegment(checker.db.ID, id.TableID, id.SegmentID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeBlock(checker.db.ID, id.TableID, id.SegmentID, id.BlockID)
}

func (checker *warChecker) ReadDB(id uint64) {
	buf := txnbase.KeyEncoder.EncodeDB(id)
	checker.readSymbol(string(buf))
}

func (checker *warChecker) ReadTable(id *common.ID) {
	buf := txnbase.KeyEncoder.EncodeDB(checker.db.ID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeTable(checker.db.ID, id.TableID)
	checker.readSymbol(string(buf))
}

func (checker *warChecker) ReadSegment(id *common.ID) {
	buf := txnbase.KeyEncoder.EncodeDB(checker.db.ID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeTable(checker.db.ID, id.TableID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeSegment(checker.db.ID, id.TableID, id.SegmentID)
	checker.readSymbol(string(buf))
}

func (checker *warChecker) ReadBlock(id *common.ID) {
	buf := txnbase.KeyEncoder.EncodeDB(checker.db.ID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeTable(checker.db.ID, id.TableID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeSegment(checker.db.ID, id.TableID, id.SegmentID)
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeBlock(checker.db.ID, id.TableID, id.SegmentID, id.BlockID)
	checker.readSymbol(string(buf))
}

func (checker *warChecker) check() (err error) {
	var entry *catalog.BaseEntry
	for key, _ := range checker.symTable {
		keyt, did, tid, sid, bid := txnbase.KeyEncoder.Decode([]byte(key))
		if checker.db != nil && checker.db.GetID() != did {
			panic(fmt.Sprintf("not expected: %d, %d", checker.db.GetID(), did))
		}
		switch keyt {
		case txnbase.KeyT_DBEntry:
			if checker.db != nil {
				entry = checker.db.BaseEntry
			}
		case txnbase.KeyT_TableEntry:
			tb, err := checker.db.GetTableEntryByID(tid)
			if err != nil {
				panic(err)
			}
			entry = tb.BaseEntry
		case txnbase.KeyT_SegmentEntry:
			tb, err := checker.db.GetTableEntryByID(tid)
			if err != nil {
				panic(err)
			}
			seg, err := tb.GetSegmentByID(sid)
			if err != nil {
				panic(err)
			}
			entry = seg.BaseEntry
		case txnbase.KeyT_BlockEntry:
			tb, err := checker.db.GetTableEntryByID(tid)
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
					return txnif.TxnRWConflictErr
				}
				eTxn := entry.GetTxn()
				entry.RUnlock()
				state := eTxn.GetTxnState(true)
				if state != txnif.TxnStateRollbacked {
					logutil.Infof("TxnRWConflictErr Found:[%s]<===RW===[%s]", eTxn.String(), checker.txn.String())
					return txnif.TxnRWConflictErr
				}
				entry.RLock()
			}
			entry.RUnlock()
		}
	}
	return
}
