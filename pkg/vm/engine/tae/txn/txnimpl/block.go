package txnimpl

import (
	"bytes"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnBlock struct {
	*txnbase.TxnBlock
	entry *catalog.BlockEntry
	store txnif.TxnStore
}

type blockIt struct {
	sync.RWMutex
	txn    txnif.AsyncTxn
	linkIt *common.LinkIt
	curr   *catalog.BlockEntry
}

type relBlockIt struct {
	sync.RWMutex
	rel       handle.Relation
	segmentIt handle.SegmentIt
	blockIt   handle.BlockIt
}

func newBlockIt(txn txnif.AsyncTxn, meta *catalog.SegmentEntry) *blockIt {
	it := &blockIt{
		txn:    txn,
		linkIt: meta.MakeBlockIt(true),
	}
	if it.linkIt.Valid() {
		it.curr = it.linkIt.Get().GetPayload().(*catalog.BlockEntry)
	}
	return it
}

func (it *blockIt) Close() error { return nil }

func (it *blockIt) Valid() bool { return it.linkIt.Valid() }

func (it *blockIt) Next() {
	valid := true
	for {
		it.linkIt.Next()
		node := it.linkIt.Get()
		if node == nil {
			it.curr = nil
			break
		}
		entry := node.GetPayload().(*catalog.BlockEntry)
		entry.RLock()
		valid = entry.TxnCanRead(it.txn, entry.RWMutex)
		entry.RUnlock()
		if valid {
			it.curr = entry
			break
		}
	}
}

func (it *blockIt) GetBlock() handle.Block {
	return newBlock(it.txn, it.curr)
}

func newBlock(txn txnif.AsyncTxn, meta *catalog.BlockEntry) *txnBlock {
	blk := &txnBlock{
		TxnBlock: &txnbase.TxnBlock{
			Txn: txn,
		},
		entry: meta,
	}
	return blk
}

func (blk *txnBlock) GetMeta() interface{}    { return blk.entry }
func (blk *txnBlock) String() string          { return blk.entry.String() }
func (blk *txnBlock) ID() uint64              { return blk.entry.GetID() }
func (blk *txnBlock) Fingerprint() *common.ID { return blk.entry.AsCommonID() }

// TODO: temp use coarse rows
func (blk *txnBlock) Rows() int { return blk.entry.GetBlockData().Rows(blk.Txn, true) }

func (blk *txnBlock) GetVectorCopy(attr string, compressed, decompressed *bytes.Buffer) (vec *vector.Vector, err error) {
	return blk.entry.GetBlockData().GetVectorCopy(blk.Txn, attr, compressed, decompressed)
}

func newRelationBlockIt(rel handle.Relation) *relBlockIt {
	segmentIt := rel.MakeSegmentIt()
	if !segmentIt.Valid() {
		return new(relBlockIt)
	}
	seg := segmentIt.GetSegment()
	blockIt := seg.MakeBlockIt()
	return &relBlockIt{
		blockIt:   blockIt,
		segmentIt: segmentIt,
		rel:       rel,
	}
}

func (it *relBlockIt) Close() error { return nil }
func (it *relBlockIt) Valid() bool {
	if it.segmentIt == nil || !it.segmentIt.Valid() {
		return false
	}
	if !it.blockIt.Valid() {
		it.segmentIt.Next()
		if !it.segmentIt.Valid() {
			return false
		}
		seg := it.segmentIt.GetSegment()
		it.blockIt = seg.MakeBlockIt()
		return it.blockIt.Valid()
	}
	return true
}

func (it *relBlockIt) GetBlock() handle.Block {
	return it.blockIt.GetBlock()
}

func (it *relBlockIt) Next() {
	it.blockIt.Next()
	if it.blockIt.Valid() {
		return
	}
	it.segmentIt.Next()
	if !it.segmentIt.Valid() {
		return
	}
	seg := it.segmentIt.GetSegment()
	it.blockIt = seg.MakeBlockIt()
}
