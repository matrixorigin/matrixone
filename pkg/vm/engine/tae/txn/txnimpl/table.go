package txnimpl

import (
	"errors"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var (
	ErrDuplicateNode = errors.New("tae: duplicate node")
)

type Table interface {
	io.Closer
	GetSchema() *catalog.Schema
	GetID() uint64
	RangeDeleteLocalRows(start, end uint32) error
	Append(data *batch.Batch) error
	LocalDeletesToString() string
	IsLocalDeleted(row uint32) bool
	GetLocalPhysicalAxis(row uint32) (int, uint32)
	UpdateLocalValue(row uint32, col uint16, value interface{}) error
	Update(inode uint32, segmentId, blockId uint64, row uint32, col uint16, v interface{}) error
	RangeDelete(inode uint32, segmentId, blockId uint64, start, end uint32) error
	Rows() uint32
	BatchDedupLocal(data *gbat.Batch) error
	BatchDedupLocalByCol(col *gvec.Vector) error
	BatchDedup(col *gvec.Vector) error
	AddUpdateNode(txnif.UpdateNode) error
	IsDeleted() bool
	PreCommit() error
	PreCommitDededup() error
	ApplyAppend()
	PrepareCommit() error
	PrepareRollback() error
	ApplyCommit() error
	ApplyRollback() error

	LogSegmentID(sid uint64)
	LogBlockID(bid uint64)

	WaitSynced()

	SetCreateEntry(txnif.TxnEntry)
	SetDropEntry(txnif.TxnEntry) error
	GetMeta() *catalog.TableEntry

	GetValue(id *common.ID, row uint32, col uint16) (interface{}, error)
	GetByFilter(*handle.Filter) (id *common.ID, offset uint32, err error)
	GetSegment(id uint64) (handle.Segment, error)
	CreateSegment() (handle.Segment, error)
	CreateNonAppendableSegment() (handle.Segment, error)
	CreateBlock(sid uint64) (handle.Block, error)
	GetBlock(id *common.ID) (handle.Block, error)
	SoftDeleteBlock(id *common.ID) error
	CreateNonAppendableBlock(sid uint64) (handle.Block, error)
	CollectCmd(*commandManager) error

	LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error)
}

type txnTable struct {
	store       *txnStore
	createEntry txnif.TxnEntry
	dropEntry   txnif.TxnEntry
	inodes      []InsertNode
	appendable  base.INodeHandle
	updateNodes map[common.ID]txnif.UpdateNode
	deleteNodes map[common.ID]txnif.DeleteNode
	appends     []*appendCtx
	tableHandle data.TableHandle
	entry       *catalog.TableEntry
	handle      handle.Relation
	index       TableIndex
	rows        uint32
	logs        []wal.LogEntry
	maxSegId    uint64
	maxBlkId    uint64

	txnEntries []txnif.TxnEntry
	csnStart   uint32
}

func newTxnTable(store *txnStore, handle handle.Relation) *txnTable {
	tbl := &txnTable{
		store:       store,
		inodes:      make([]InsertNode, 0),
		handle:      handle,
		entry:       handle.GetMeta().(*catalog.TableEntry),
		index:       NewSimpleTableIndex(),
		updateNodes: make(map[common.ID]txnif.UpdateNode),
		deleteNodes: make(map[common.ID]txnif.DeleteNode),
		appends:     make([]*appendCtx, 0),
		logs:        make([]wal.LogEntry, 0),
		txnEntries:  make([]txnif.TxnEntry, 0),
	}
	return tbl
}

func (tbl *txnTable) LogSegmentID(sid uint64) {
	if tbl.maxSegId < sid {
		tbl.maxSegId = sid
	}
}

func (tbl *txnTable) LogBlockID(bid uint64) {
	if tbl.maxBlkId < bid {
		tbl.maxBlkId = bid
	}
}

func (tbl *txnTable) WaitSynced() {
	for _, e := range tbl.logs {
		e.WaitDone()
		e.Free()
	}
}

func (tbl *txnTable) CollectCmd(cmdMgr *commandManager) error {
	for i, node := range tbl.inodes {
		h := tbl.store.nodesMgr.Pin(node)
		if h == nil {
			panic("not expected")
		}
		forceFlush := i < len(tbl.inodes)-1
		// csn := cmdMgr.GetCSN()
		csn := uint32(0xffff) // Special cmd
		cmd, entry, err := node.MakeCommand(csn, forceFlush)
		if err != nil {
			panic(err)
		}
		if entry != nil {
			tbl.logs = append(tbl.logs, entry)
		}
		node.ToTransient()
		h.Close()
		if cmd != nil {
			cmdMgr.AddInternalCmd(cmd)
		}
	}
	tbl.csnStart = uint32(cmdMgr.GetCSN())
	for _, txnEntry := range tbl.txnEntries {
		csn := cmdMgr.GetCSN()
		cmd, err := txnEntry.MakeCommand(csn)
		if err != nil {
			return err
		}
		if cmd == nil {
			panic(txnEntry)
		}
		cmdMgr.AddCmd(cmd)
	}
	return nil
}

func (tbl *txnTable) GetSegment(id uint64) (seg handle.Segment, err error) {
	var meta *catalog.SegmentEntry
	if meta, err = tbl.entry.GetSegmentByID(id); err != nil {
		return
	}
	if !meta.TxnCanRead(tbl.store.txn, nil) {
		err = txnbase.ErrNotFound
	}
	seg = newSegment(tbl.store.txn, meta)
	return
}

func (tbl *txnTable) CreateNonAppendableSegment() (seg handle.Segment, err error) {
	var meta *catalog.SegmentEntry
	var factory catalog.SegmentDataFactory
	if tbl.store.dataFactory != nil {
		factory = tbl.store.dataFactory.MakeSegmentFactory()
	}
	if meta, err = tbl.entry.CreateSegment(tbl.store.txn, catalog.ES_NotAppendable, factory); err != nil {
		return
	}
	seg = newSegment(tbl.store.txn, meta)
	tbl.txnEntries = append(tbl.txnEntries, meta)
	tbl.store.warChecker.ReadTable(meta.GetTable().AsCommonID())
	return
}

func (tbl *txnTable) CreateSegment() (seg handle.Segment, err error) {
	var meta *catalog.SegmentEntry
	var factory catalog.SegmentDataFactory
	if tbl.store.dataFactory != nil {
		factory = tbl.store.dataFactory.MakeSegmentFactory()
	}
	if meta, err = tbl.entry.CreateSegment(tbl.store.txn, catalog.ES_Appendable, factory); err != nil {
		return
	}
	seg = newSegment(tbl.store.txn, meta)
	tbl.txnEntries = append(tbl.txnEntries, meta)
	tbl.store.warChecker.ReadTable(meta.GetTable().AsCommonID())
	return
}

func (tbl *txnTable) SoftDeleteBlock(id *common.ID) (err error) {
	var seg *catalog.SegmentEntry
	if seg, err = tbl.entry.GetSegmentByID(id.SegmentID); err != nil {
		return
	}
	meta, err := seg.DropBlockEntry(id.BlockID, tbl.store.txn)
	if err != nil {
		return
	}
	tbl.txnEntries = append(tbl.txnEntries, meta)
	tbl.store.warChecker.ReadSegment(seg.AsCommonID())
	return
}

func (tbl *txnTable) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	tbl.txnEntries = append(tbl.txnEntries, entry)
	for _, id := range readed {
		tbl.store.warChecker.Read(id)
	}
	return
}

func (tbl *txnTable) GetBlock(id *common.ID) (blk handle.Block, err error) {
	var seg *catalog.SegmentEntry
	if seg, err = tbl.entry.GetSegmentByID(id.SegmentID); err != nil {
		return
	}
	meta, err := seg.GetBlockEntryByID(id.BlockID)
	if err != nil {
		return
	}
	blk = newBlock(tbl.store.txn, meta)
	return
}

func (tbl *txnTable) CreateNonAppendableBlock(sid uint64) (blk handle.Block, err error) {
	return tbl.createBlock(sid, catalog.ES_NotAppendable)
}

func (tbl *txnTable) CreateBlock(sid uint64) (blk handle.Block, err error) {
	return tbl.createBlock(sid, catalog.ES_Appendable)
}

func (tbl *txnTable) createBlock(sid uint64, state catalog.EntryState) (blk handle.Block, err error) {
	var seg *catalog.SegmentEntry
	if seg, err = tbl.entry.GetSegmentByID(sid); err != nil {
		return
	}
	if !seg.IsAppendable() && state == catalog.ES_Appendable {
		err = data.ErrNotAppendable
		return
	}
	var factory catalog.BlockDataFactory
	if tbl.store.dataFactory != nil {
		segData := seg.GetSegmentData()
		factory = tbl.store.dataFactory.MakeBlockFactory(segData.GetSegmentFile())
	}
	meta, err := seg.CreateBlock(tbl.store.txn, state, factory)
	if err != nil {
		return
	}
	tbl.txnEntries = append(tbl.txnEntries, meta)
	tbl.store.warChecker.ReadSegment(seg.AsCommonID())
	return newBlock(tbl.store.txn, meta), err
}

func (tbl *txnTable) SetCreateEntry(e txnif.TxnEntry) {
	if tbl.createEntry != nil {
		panic("logic error")
	}
	tbl.createEntry = e
	tbl.txnEntries = append(tbl.txnEntries, e)
	tbl.store.warChecker.ReadDB(tbl.entry.GetDB().GetID())
}

func (tbl *txnTable) SetDropEntry(e txnif.TxnEntry) error {
	if tbl.dropEntry != nil {
		panic("logic error")
	}
	if tbl.createEntry != nil {
		return txnbase.ErrDDLDropCreated
	}
	tbl.dropEntry = e
	tbl.txnEntries = append(tbl.txnEntries, e)
	tbl.store.warChecker.ReadDB(tbl.entry.GetDB().GetID())
	return nil
}

func (tbl *txnTable) IsDeleted() bool {
	return tbl.dropEntry != nil
}

func (tbl *txnTable) GetSchema() *catalog.Schema {
	return tbl.entry.GetSchema()
}

func (tbl *txnTable) GetMeta() *catalog.TableEntry {
	return tbl.entry
}

func (tbl *txnTable) GetID() uint64 {
	return tbl.entry.GetID()
}

func (tbl *txnTable) Close() error {
	var err error
	if tbl.appendable != nil {
		if tbl.appendable.Close(); err != nil {
			return err
		}
	}
	for _, node := range tbl.inodes {
		if err = node.Close(); err != nil {
			return err
		}
	}
	tbl.index.Close()
	tbl.index = nil
	tbl.appendable = nil
	tbl.inodes = nil
	tbl.updateNodes = nil
	tbl.deleteNodes = nil
	tbl.tableHandle = nil
	tbl.logs = nil
	return nil
}

func (tbl *txnTable) registerInsertNode() error {
	if tbl.appendable != nil {
		tbl.appendable.Close()
	}
	id := common.ID{
		TableID:   tbl.entry.GetID(),
		SegmentID: uint64(len(tbl.inodes)),
		BlockID:   tbl.store.txn.GetID(),
	}
	n := NewInsertNode(tbl, tbl.store.nodesMgr, id, tbl.store.driver)
	tbl.appendable = tbl.store.nodesMgr.Pin(n)
	tbl.inodes = append(tbl.inodes, n)
	return nil
}

func (tbl *txnTable) AddDeleteNode(id *common.ID, node txnif.DeleteNode) error {
	nid := *id
	u := tbl.deleteNodes[nid]
	if u != nil {
		return ErrDuplicateNode
	}
	tbl.deleteNodes[nid] = node
	tbl.txnEntries = append(tbl.txnEntries, node)
	return nil
}

func (tbl *txnTable) AddUpdateNode(node txnif.UpdateNode) error {
	id := *node.GetID()
	u := tbl.updateNodes[id]
	if u != nil {
		return ErrDuplicateNode
	}
	tbl.updateNodes[id] = node
	tbl.txnEntries = append(tbl.txnEntries, node)
	return nil
}

func (tbl *txnTable) Append(data *batch.Batch) error {
	err := tbl.BatchDedup(data.Vecs[tbl.entry.GetSchema().PrimaryKey])
	if err != nil {
		return err
	}
	if tbl.appendable == nil {
		if err = tbl.registerInsertNode(); err != nil {
			return err
		}
	}
	appended := uint32(0)
	offset := uint32(0)
	length := uint32(vector.Length(data.Vecs[0]))
	for {
		h := tbl.appendable
		n := h.GetNode().(*insertNode)
		toAppend := n.PrepareAppend(data, offset)
		size := compute.EstimateSize(data, offset, toAppend)
		logutil.Debugf("Offset=%d, ToAppend=%d, EstimateSize=%d", offset, toAppend, size)
		err = n.Expand(size, func() error {
			appended, err = n.Append(data, offset)
			return err
		})
		if err != nil {
			logutil.Info(tbl.store.nodesMgr.String())
			panic(err)
		}
		space := n.GetSpace()
		logutil.Debugf("Appended: %d, Space:%d", appended, space)
		start := tbl.rows
		if err = tbl.index.BatchInsert(data.Vecs[tbl.GetSchema().PrimaryKey], int(offset), int(appended), start, false); err != nil {
			break
		}
		offset += appended
		tbl.rows += appended
		if space == 0 {
			if err = tbl.registerInsertNode(); err != nil {
				break
			}
		}
		if offset >= length {
			break
		}
	}
	return err
}

// 1. Split the interval into multiple intervals, with each interval belongs to only one insert node
// 2. For each new interval, call insert node RangeDelete
// 3. Update the table index
func (tbl *txnTable) RangeDeleteLocalRows(start, end uint32) error {
	first, firstOffset := tbl.GetLocalPhysicalAxis(start)
	last, lastOffset := tbl.GetLocalPhysicalAxis(end)
	var err error
	if last == first {
		node := tbl.inodes[first]
		err = node.RangeDelete(firstOffset, lastOffset)
		if err == nil {
			for i := firstOffset; i <= lastOffset; i++ {
				v, _ := node.GetValue(int(tbl.entry.GetSchema().PrimaryKey), i)
				if err = tbl.index.Delete(v); err != nil {
					break
				}
			}
		}
	} else {
		node := tbl.inodes[first]
		err = node.RangeDelete(firstOffset, txnbase.MaxNodeRows-1)
		node = tbl.inodes[last]
		err = node.RangeDelete(0, lastOffset)
		for i := uint32(0); i <= lastOffset; i++ {
			v, _ := node.GetValue(int(tbl.entry.GetSchema().PrimaryKey), i)
			if err = tbl.index.Delete(v); err != nil {
				break
			}
		}
		if last > first+1 && err == nil {
			for i := first + 1; i < last; i++ {
				node = tbl.inodes[i]
				if err = node.RangeDelete(0, txnbase.MaxNodeRows); err != nil {
					break
				}
				for i := uint32(0); i <= txnbase.MaxNodeRows; i++ {
					v, _ := node.GetValue(int(tbl.entry.GetSchema().PrimaryKey), i)
					if err = tbl.index.Delete(v); err != nil {
						break
					}
				}
			}
		}
	}
	return err
}

func (tbl *txnTable) LocalDeletesToString() string {
	s := fmt.Sprintf("<txnTable-%d>[LocalDeletes]:\n", tbl.GetID())
	for i, n := range tbl.inodes {
		s = fmt.Sprintf("%s\t<INode-%d>: %s\n", s, i, n.PrintDeletes())
	}
	return s
}

func (tbl *txnTable) IsLocalDeleted(row uint32) bool {
	npos, noffset := tbl.GetLocalPhysicalAxis(row)
	n := tbl.inodes[npos]
	return n.IsRowDeleted(noffset)
}

func (tbl *txnTable) GetLocalPhysicalAxis(row uint32) (int, uint32) {
	npos := int(row) / int(txnbase.MaxNodeRows)
	noffset := row % uint32(txnbase.MaxNodeRows)
	return npos, noffset
}

func (tbl *txnTable) RangeDelete(inode uint32, segmentId, blockId uint64, start, end uint32) (err error) {
	if inode != 0 {
		return tbl.RangeDeleteLocalRows(start, end)
	}
	id := tbl.entry.AsCommonID()
	id.SegmentID = segmentId
	id.BlockID = blockId
	node := tbl.deleteNodes[*id]
	if node != nil {
		chain := node.GetChain().(*updates.DeleteChain)
		controller := chain.GetController()
		writeLock := controller.GetExclusiveLock()
		err = controller.CheckNotDeleted(start, end, tbl.store.txn.GetStartTS())
		if err == nil {
			if err = controller.CheckNotUpdated(start, end, tbl.store.txn.GetStartTS()); err == nil {
				node.RangeDeleteLocked(start, end)
			}
		}
		writeLock.Unlock()
		if err != nil {
			seg, _ := tbl.entry.GetSegmentByID(segmentId)
			blk, _ := seg.GetBlockEntryByID(blockId)
			tbl.store.warChecker.ReadBlock(blk.AsCommonID())
		}
		return
	}
	seg, err := tbl.entry.GetSegmentByID(segmentId)
	if err != nil {
		return
	}
	blk, err := seg.GetBlockEntryByID(blockId)
	if err != nil {
		return
	}
	blkData := blk.GetBlockData()
	node2, err := blkData.RangeDelete(tbl.store.txn, start, end)
	if err == nil {
		id := blk.AsCommonID()
		tbl.AddDeleteNode(id, node2)
		tbl.store.warChecker.ReadBlock(id)
	}
	return
}

func (tbl *txnTable) GetByFilter(filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	offset, err = tbl.index.Find(filter.Val)
	if err == nil {
		id = &common.ID{}
		id.PartID = 1
		id.TableID = tbl.entry.ID
		err = nil
		return
	}
	blockIt := tbl.handle.MakeBlockIt()
	for blockIt.Valid() {
		h := blockIt.GetBlock()
		block := h.GetMeta().(*catalog.BlockEntry).GetBlockData()
		offset, err = block.GetByFilter(tbl.store.txn, filter)
		if err == nil {
			id = h.Fingerprint()
			break
		}
		blockIt.Next()
	}
	return
}

func (tbl *txnTable) GetValue(id *common.ID, row uint32, col uint16) (v interface{}, err error) {
	if id.PartID != 0 {
		return tbl.GetLocalValue(row, col)
	}
	segMeta, err := tbl.entry.GetSegmentByID(id.SegmentID)
	if err != nil {
		panic(err)
	}
	meta, err := segMeta.GetBlockEntryByID(id.BlockID)
	if err != nil {
		panic(err)
	}
	block := meta.GetBlockData()
	return block.GetValue(tbl.store.txn, row, col)
}

func (tbl *txnTable) updateWithFineLock(node txnif.UpdateNode, txn txnif.AsyncTxn, row uint32, v interface{}) (err error) {
	chain := node.GetChain().(*updates.ColumnChain)
	controller := chain.GetController()
	sharedLock := controller.GetSharedLock()
	if err = controller.CheckNotDeleted(row, row, txn.GetStartTS()); err == nil {
		chain.Lock()
		err = chain.TryUpdateNodeLocked(row, v, node)
		chain.Unlock()
	}
	sharedLock.Unlock()
	return
}

func (tbl *txnTable) Update(inode uint32, segmentId, blockId uint64, row uint32, col uint16, v interface{}) (err error) {
	if inode != 0 {
		return tbl.UpdateLocalValue(row, col, v)
	}
	node := tbl.updateNodes[common.ID{
		TableID:   tbl.GetID(),
		SegmentID: segmentId,
		BlockID:   blockId,
		Idx:       col,
	}]
	if node != nil {
		err = tbl.updateWithFineLock(node, tbl.store.txn, row, v)
		if err != nil {
			seg, _ := tbl.entry.GetSegmentByID(segmentId)
			blk, _ := seg.GetBlockEntryByID(blockId)
			tbl.store.warChecker.ReadBlock(blk.AsCommonID())
		}
		return
	}
	seg, err := tbl.entry.GetSegmentByID(segmentId)
	if err != nil {
		return
	}
	blk, err := seg.GetBlockEntryByID(blockId)
	if err != nil {
		return
	}
	blkData := blk.GetBlockData()
	node2, err := blkData.Update(tbl.store.txn, row, col, v)
	if err == nil {
		tbl.AddUpdateNode(node2)
		tbl.store.warChecker.ReadBlock(blk.AsCommonID())
	}
	return
}

// 1. Get insert node and offset in node
// 2. Get row
// 3. Build a new row
// 4. Delete the row in the node
// 5. Append the new row
func (tbl *txnTable) UpdateLocalValue(row uint32, col uint16, value interface{}) error {
	npos, noffset := tbl.GetLocalPhysicalAxis(row)
	n := tbl.inodes[npos]
	window, err := n.Window(uint32(noffset), uint32(noffset))
	if err != nil {
		return err
	}
	if err = n.RangeDelete(uint32(noffset), uint32(noffset)); err != nil {
		return err
	}
	v, _ := n.GetValue(int(tbl.entry.GetSchema().PrimaryKey), row)
	if err = tbl.index.Delete(v); err != nil {
		panic(err)
	}
	err = tbl.Append(window)
	return err
}

func (tbl *txnTable) Rows() uint32 {
	cnt := len(tbl.inodes)
	if cnt == 0 {
		return 0
	}
	return (uint32(cnt)-1)*txnbase.MaxNodeRows + tbl.inodes[cnt-1].Rows()
}

func (tbl *txnTable) PreCommitDededup() (err error) {
	if tbl.index == nil || tbl.index.Count() == 0 {
		return
	}
	schema := tbl.entry.GetSchema()
	pks := tbl.index.KeyToVector(schema.ColDefs[schema.PrimaryKey].Type)
	segIt := tbl.entry.MakeSegmentIt(false)
	for segIt.Valid() {
		seg := segIt.Get().GetPayload().(*catalog.SegmentEntry)
		if seg.GetID() < tbl.maxSegId {
			return
		}
		{
			seg.RLock()
			uncreated := seg.IsCreatedUncommitted()
			dropped := seg.IsDroppedCommitted()
			seg.RUnlock()
			if uncreated || dropped {
				segIt.Next()
				continue
			}
		}
		segData := seg.GetSegmentData()
		// TODO: Add a new batch dedup method later
		if err = segData.BatchDedup(tbl.store.txn, pks); err == data.ErrDuplicate {
			return
		}
		if err == nil {
			segIt.Next()
			continue
		}
		err = nil
		blkIt := seg.MakeBlockIt(false)
		for blkIt.Valid() {
			blk := blkIt.Get().GetPayload().(*catalog.BlockEntry)
			if blk.GetID() < tbl.maxBlkId {
				return
			}
			{
				blk.RLock()
				uncreated := blk.IsCreatedUncommitted()
				dropped := blk.IsDroppedCommitted()
				blk.RUnlock()
				if uncreated || dropped {
					blkIt.Next()
					continue
				}
			}
			// logutil.Infof("%s: %d-%d, %d-%d: %s", tbl.txn.String(), tbl.maxSegId, tbl.maxBlkId, seg.GetID(), blk.GetID(), pks.String())
			blkData := blk.GetBlockData()
			// TODO: Add a new batch dedup method later
			if err = blkData.BatchDedup(tbl.store.txn, pks); err != nil {
				return
			}
			blkIt.Next()
		}
		segIt.Next()
	}
	return
}

func (tbl *txnTable) BatchDedup(pks *gvec.Vector) (err error) {
	if err = tbl.BatchDedupLocalByCol(pks); err != nil {
		return err
	}
	segIt := tbl.handle.MakeSegmentIt()
	for segIt.Valid() {
		seg := segIt.GetSegment()
		if err = seg.BatchDedup(pks); err == data.ErrDuplicate {
			break
		}
		if err == data.ErrPossibleDuplicate {
			err = nil
			blkIt := seg.MakeBlockIt()
			for blkIt.Valid() {
				block := blkIt.GetBlock()
				if err = block.BatchDedup(pks); err != nil {
					break
				}
				blkIt.Next()
			}
		}
		if err != nil {
			break
		}
		segIt.Next()
	}
	segIt.Close()
	return
}

func (tbl *txnTable) BatchDedupLocal(bat *gbat.Batch) error {
	return tbl.BatchDedupLocalByCol(bat.Vecs[tbl.GetSchema().PrimaryKey])
}

func (tbl *txnTable) BatchDedupLocalByCol(col *gvec.Vector) error {
	index := NewSimpleTableIndex()
	err := index.BatchInsert(col, 0, gvec.Length(col), 0, true)
	if err != nil {
		return err
	}
	return tbl.index.BatchDedup(col)
}

func (tbl *txnTable) GetLocalValue(row uint32, col uint16) (interface{}, error) {
	npos, noffset := tbl.GetLocalPhysicalAxis(row)
	n := tbl.inodes[npos]
	h := tbl.store.nodesMgr.Pin(n)
	defer h.Close()
	return n.GetValue(int(col), noffset)
}

func (tbl *txnTable) PrepareRollback() (err error) {
	for _, txnEntry := range tbl.txnEntries {
		if err = txnEntry.PrepareRollback(); err != nil {
			break
		}
	}
	return
}

func (tbl *txnTable) ApplyAppend() {
	var err error
	for _, ctx := range tbl.appends {
		var (
			destOff    uint32
			appendNode txnif.AppendNode
		)
		bat, _ := ctx.node.Window(ctx.start, ctx.start+ctx.count-1)
		if appendNode, destOff, err = ctx.driver.ApplyAppend(bat, 0, ctx.count, tbl.store.txn); err != nil {
			panic(err)
		}
		ctx.driver.Close()
		id := ctx.driver.GetID()
		info := ctx.node.AddApplyInfo(ctx.start, ctx.count, destOff, ctx.count, id)
		logutil.Debugf(info.String())
		appendNode.PrepareCommit()
		tbl.txnEntries = append(tbl.txnEntries, appendNode)
	}
	if tbl.tableHandle != nil {
		tbl.entry.GetTableData().ApplyHandle(tbl.tableHandle)
	}
}

func (tbl *txnTable) prepareAppend(node InsertNode) (err error) {
	tableData := tbl.entry.GetTableData()
	if tbl.tableHandle == nil {
		tbl.tableHandle = tableData.GetHandle()
	}
	appended := uint32(0)
	for appended < node.RowsWithoutDeletes() {
		appender, err := tbl.tableHandle.GetAppender()
		if err == data.ErrAppendableSegmentNotFound {
			seg, err := tbl.CreateSegment()
			if err != nil {
				return err
			}
			blk, err := seg.CreateBlock()
			if err != nil {
				return err
			}
			appender = tbl.tableHandle.SetAppender(blk.Fingerprint())
		} else if err == data.ErrAppendableBlockNotFound {
			id := appender.GetID()
			blk, err := tbl.CreateBlock(id.SegmentID)
			if err != nil {
				return err
			}
			appender = tbl.tableHandle.SetAppender(blk.Fingerprint())
		}
		toAppend, err := appender.PrepareAppend(node.RowsWithoutDeletes() - appended)
		toAppendWithDeletes := node.LengthWithDeletes(appended, toAppend)
		ctx := &appendCtx{
			driver: appender,
			node:   node,
			start:  appended,
			count:  toAppendWithDeletes,
		}
		id := appender.GetID()
		tbl.store.warChecker.ReadBlock(id)
		tbl.appends = append(tbl.appends, ctx)
		logutil.Debugf("%s: toAppend %d, appended %d, blks=%d", id.String(), toAppend, appended, len(tbl.appends))
		appended += toAppend
		if appended == node.Rows() {
			break
		}
	}
	return
}

func (tbl *txnTable) PreCommit() (err error) {
	for _, node := range tbl.inodes {
		if err = tbl.prepareAppend(node); err != nil {
			break
		}
	}
	return
}

func (tbl *txnTable) PrepareCommit() (err error) {
	for _, node := range tbl.txnEntries {
		if err = node.PrepareCommit(); err != nil {
			break
		}
	}
	return
}

func (tbl *txnTable) ApplyCommit() (err error) {
	csn := tbl.csnStart
	for _, node := range tbl.txnEntries {
		if err = node.ApplyCommit(tbl.store.cmdMgr.MakeLogIndex(csn)); err != nil {
			break
		}
		csn++
	}
	return
}

func (tbl *txnTable) ApplyRollback() (err error) {
	for _, node := range tbl.txnEntries {
		if err = node.ApplyRollback(); err != nil {
			break
		}
	}
	return
}

func (tbl *txnTable) buildCommitCmd(cmdSeq *uint32) (cmd txnif.TxnCmd, entries []wal.LogEntry, err error) {
	composedCmd := txnbase.NewComposedCmd()

	for i, inode := range tbl.inodes {
		h := tbl.store.nodesMgr.Pin(inode)
		if h == nil {
			panic("not expected")
		}
		forceFlush := (i < len(tbl.inodes)-1)
		cmd, entry, err := inode.MakeCommand(*cmdSeq, forceFlush)
		if err != nil {
			return cmd, entries, err
		}
		*cmdSeq += uint32(1)
		if cmd == nil {
			inode.ToTransient()
			h.Close()
			inode.Close()
			continue
		}
		if entry != nil {
			entries = append(entries, entry)
		}
		composedCmd.AddCmd(cmd)
		h.Close()
	}
	for _, node := range tbl.updateNodes {
		updateCmd, err := node.MakeCommand(*cmdSeq)
		if err != nil {
			return cmd, entries, err
		}
		composedCmd.AddCmd(updateCmd)
		*cmdSeq += uint32(1)
	}
	return composedCmd, entries, err
}
