package txnimpl

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/sirupsen/logrus"
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

func (checker *warChecker) readDBVar(db *catalog.DBEntry) {
	buf := txnbase.KeyEncoder.EncodeDB(db.GetID())
	checker.readSymbol(string(buf))
}

func (checker *warChecker) readTableVar(tb *catalog.TableEntry) {
	buf := txnbase.KeyEncoder.EncodeDB(tb.GetDB().GetID())
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeTable(tb.GetDB().GetID(), tb.GetID())
	checker.readSymbol(string(buf))
}

func (checker *warChecker) readSegmentVar(seg *catalog.SegmentEntry) {
	buf := txnbase.KeyEncoder.EncodeDB(seg.GetTable().GetDB().GetID())
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeTable(seg.GetTable().GetDB().GetID(), seg.GetTable().GetID())
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeSegment(seg.GetTable().GetDB().GetID(), seg.GetTable().GetID(), seg.GetID())
	checker.readSymbol(string(buf))
}

func (checker *warChecker) readBlockVar(blk *catalog.BlockEntry) {
	buf := txnbase.KeyEncoder.EncodeDB(blk.GetSegment().GetTable().GetDB().GetID())
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeTable(blk.GetSegment().GetTable().GetDB().GetID(), blk.GetSegment().GetTable().GetID())
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeSegment(blk.GetSegment().GetTable().GetDB().GetID(), blk.GetSegment().GetTable().GetID(), blk.GetSegment().GetID())
	checker.readSymbol(string(buf))
	buf = txnbase.KeyEncoder.EncodeBlock(blk.GetSegment().GetTable().GetDB().GetID(), blk.GetSegment().GetTable().GetID(), blk.GetSegment().GetID(), blk.GetID())
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
					logrus.Infof("TxnRWConflictErr Found:[%s]<===RW===[%s]", eTxn.String(), checker.txn.String())
					return txnif.TxnRWConflictErr
				}
				entry.RLock()
			}
			entry.RUnlock()
		}
	}
	return
}
