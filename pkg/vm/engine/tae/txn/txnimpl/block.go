package txnimpl

import (
	"bytes"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
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
	for it.linkIt.Valid() {
		curr := it.linkIt.Get().GetPayload().(*catalog.BlockEntry)
		curr.RLock()
		if curr.TxnCanRead(it.txn, curr.RWMutex) {
			curr.RUnlock()
			it.curr = curr
			break
		}
		curr.RUnlock()
		it.linkIt.Next()
	}
	// if it.linkIt.Valid() {
	// 	it.curr = it.linkIt.Get().GetPayload().(*catalog.BlockEntry)
	// }
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

func (blk *txnBlock) GetMeta() interface{} { return blk.entry }
func (blk *txnBlock) String() string {
	blkData := blk.entry.GetBlockData()
	return blkData.PPString(common.PPL1, 0, "")
	// return blk.entry.String()
}

// func (blk *txnBlock) IsAppendable() bool {
// 	if !blk.entry.IsAppendable() {
// 		return false
// 	}
// 	return
// }

func (blk *txnBlock) GetTotalChanges() int {
	return blk.entry.GetBlockData().GetTotalChanges()
}
func (blk *txnBlock) IsAppendableBlock() bool { return blk.entry.IsAppendable() }
func (blk *txnBlock) ID() uint64              { return blk.entry.GetID() }
func (blk *txnBlock) Fingerprint() *common.ID { return blk.entry.AsCommonID() }
func (blk *txnBlock) BatchDedup(pks *gvec.Vector) (err error) {
	blkData := blk.entry.GetBlockData()
	blk.Txn.GetStore().LogBlockID(blk.entry.GetSegment().GetTable().GetID(), blk.entry.GetID())
	return blkData.BatchDedup(blk.Txn, pks)
}

func (blk *txnBlock) RangeDelete(start, end uint32) (err error) {
	return blk.Txn.GetStore().RangeDelete(blk.entry.AsCommonID(), start, end)
}

// func (blk *txnBlock) GetByFilter(filter handle.Filter) (uint32, error) {
// 	return blk.Txn.GetStore().GetByFilter(blk.entry.AsCommonID(), filter)
// }

func (blk *txnBlock) Update(row uint32, col uint16, v interface{}) (err error) {
	return blk.Txn.GetStore().Update(blk.entry.AsCommonID(), row, col, v)
}

// TODO: temp use coarse rows
func (blk *txnBlock) Rows() int { return blk.entry.GetBlockData().Rows(blk.Txn, true) }

func (blk *txnBlock) GetColumnDataById(colIdx int, compressed, decompressed *bytes.Buffer) (vec *vector.Vector, deletes *roaring.Bitmap, err error) {
	return blk.entry.GetBlockData().GetColumnDataById(blk.Txn, colIdx, compressed, decompressed)
}
func (blk *txnBlock) GetColumnDataByName(attr string, compressed, decompressed *bytes.Buffer) (vec *vector.Vector, deletes *roaring.Bitmap, err error) {
	return blk.entry.GetBlockData().GetColumnDataByName(blk.Txn, attr, compressed, decompressed)
}

func (blk *txnBlock) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return blk.Txn.GetStore().LogTxnEntry(blk.entry.GetSegment().GetTable().GetID(), entry, readed)
}

func (blk *txnBlock) GetSegment() (seg handle.Segment) {
	seg = newSegment(blk.Txn, blk.entry.GetSegment())
	return
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
