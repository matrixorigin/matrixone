package txnimpl

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnSegment struct {
	*txnbase.TxnSegment
	entry *catalog.SegmentEntry
	store txnif.TxnStore
}

type segmentIt struct {
	sync.RWMutex
	txn    txnif.AsyncTxn
	linkIt *common.LinkIt
	curr   *catalog.SegmentEntry
}

func newSegmentIt(txn txnif.AsyncTxn, meta *catalog.TableEntry) *segmentIt {
	it := &segmentIt{
		txn:    txn,
		linkIt: meta.MakeSegmentIt(true),
	}
	if it.linkIt.Valid() {
		it.curr = it.linkIt.Get().GetPayload().(*catalog.SegmentEntry)
	}
	return it
}

func (it *segmentIt) Close() error { return nil }

func (it *segmentIt) Valid() bool { return it.linkIt.Valid() }

func (it *segmentIt) Next() {
	valid := true
	for {
		it.linkIt.Next()
		node := it.linkIt.Get()
		if node == nil {
			it.curr = nil
			break
		}
		entry := node.GetPayload().(*catalog.SegmentEntry)
		entry.RLock()
		valid = entry.TxnCanRead(it.txn, entry.RWMutex)
		entry.RUnlock()
		if valid {
			it.curr = entry
			break
		}
	}
}

func (it *segmentIt) GetSegment() handle.Segment {
	return newSegment(it.txn, it.curr)
}

func newSegment(txn txnif.AsyncTxn, meta *catalog.SegmentEntry) *txnSegment {
	seg := &txnSegment{
		TxnSegment: &txnbase.TxnSegment{
			Txn: txn,
		},
		entry: meta,
	}
	return seg
}

func (seg *txnSegment) GetMeta() interface{} { return seg.entry }
func (seg *txnSegment) String() string       { return seg.entry.String() }
func (seg *txnSegment) GetID() uint64        { return seg.entry.GetID() }
func (seg *txnSegment) MakeBlockIt() (it handle.BlockIt) {
	return newBlockIt(seg.Txn, seg.entry)
}

func (seg *txnSegment) CreateBlock() (blk handle.Block, err error) {
	return seg.Txn.GetStore().CreateBlock(seg.entry.GetTable().GetID(), seg.entry.GetID())
}
