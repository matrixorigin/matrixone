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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/updates"
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
	PrepareCommit() error
	PrepareRollback() error
	ApplyCommit() error
	ApplyRollback() error

	LogSegmentID(sid uint64)
	LogBlockID(bid uint64)

	WaitSynced()

	SetCreateEntry(txnif.TxnEntry)
	SetDropEntry(txnif.TxnEntry)
	GetMeta() *catalog.TableEntry

	GetValue(id *common.ID, row uint32, col uint16) (interface{}, error)
	GetByFilter(*handle.Filter) (id *common.ID, offset uint32, err error)
	CreateSegment() (handle.Segment, error)
	CreateBlock(sid uint64) (handle.Block, error)
	CollectCmd(*commandManager) error
}

type txnTable struct {
	txn         txnif.AsyncTxn
	createEntry txnif.TxnEntry
	dropEntry   txnif.TxnEntry
	inodes      []InsertNode
	appendable  base.INodeHandle
	updateNodes map[common.ID]*updates.ColumnNode
	deleteNodes map[common.ID]*updates.DeleteNode
	driver      txnbase.NodeDriver
	entry       *catalog.TableEntry
	handle      handle.Relation
	nodesMgr    base.INodeManager
	index       TableIndex
	rows        uint32
	csegs       []*catalog.SegmentEntry
	dsegs       []*catalog.SegmentEntry
	cblks       []*catalog.BlockEntry
	dblks       []*catalog.BlockEntry
	warChecker  *warChecker
	dataFactory *tables.DataFactory
	logs        []txnbase.NodeEntry
	maxSegId    uint64
	maxBlkId    uint64
}

func newTxnTable(txn txnif.AsyncTxn, handle handle.Relation, driver txnbase.NodeDriver, mgr base.INodeManager, checker *warChecker, dataFactory *tables.DataFactory) *txnTable {
	tbl := &txnTable{
		warChecker:  checker,
		txn:         txn,
		inodes:      make([]InsertNode, 0),
		nodesMgr:    mgr,
		handle:      handle,
		entry:       handle.GetMeta().(*catalog.TableEntry),
		driver:      driver,
		index:       NewSimpleTableIndex(),
		updateNodes: make(map[common.ID]*updates.ColumnNode),
		deleteNodes: make(map[common.ID]*updates.DeleteNode),
		csegs:       make([]*catalog.SegmentEntry, 0),
		dsegs:       make([]*catalog.SegmentEntry, 0),
		dataFactory: dataFactory,
		logs:        make([]txnbase.NodeEntry, 0),
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
	for _, seg := range tbl.csegs {
		csn := cmdMgr.GetCSN()
		cmd, err := seg.MakeCommand(uint32(csn))
		if err != nil {
			return err
		}
		cmdMgr.AddCmd(cmd)
	}
	for _, blk := range tbl.cblks {
		csn := cmdMgr.GetCSN()
		cmd, err := blk.MakeCommand(uint32(csn))
		if err != nil {
			return err
		}
		cmdMgr.AddCmd(cmd)
	}
	for i, node := range tbl.inodes {
		h := tbl.nodesMgr.Pin(node)
		if h == nil {
			panic("not expected")
		}
		forceFlush := i < len(tbl.inodes)-1
		csn := cmdMgr.GetCSN()
		cmd, entry, err := node.MakeCommand(uint32(csn), forceFlush)
		if err != nil {
			panic(err)
		}
		if entry != nil {
			tbl.logs = append(tbl.logs, entry)
		}
		node.ToTransient()
		h.Close()
		if cmd != nil {
			cmdMgr.AddCmd(cmd)
		}
	}
	for _, node := range tbl.updateNodes {
		csn := cmdMgr.GetCSN()
		updateCmd, _, err := node.MakeCommand(uint32(csn), false)
		if err != nil {
			panic(err)
		}
		if updateCmd != nil {
			cmdMgr.AddCmd(updateCmd)
		}
	}
	for _, node := range tbl.deleteNodes {
		csn := cmdMgr.GetCSN()
		deleteCmd, _, err := node.MakeCommand(uint32(csn), false)
		if err != nil {
			panic(err)
		}
		if deleteCmd != nil {
			cmdMgr.AddCmd(deleteCmd)
		}
	}
	return nil
}

func (tbl *txnTable) CreateSegment() (seg handle.Segment, err error) {
	var meta *catalog.SegmentEntry
	var factory catalog.SegmentDataFactory
	if tbl.dataFactory != nil {
		factory = tbl.dataFactory.MakeSegmentFactory()
	}
	if meta, err = tbl.entry.CreateSegment(tbl.txn, catalog.ES_Appendable, factory); err != nil {
		return
	}
	seg = newSegment(tbl.txn, meta)
	tbl.csegs = append(tbl.csegs, meta)
	tbl.warChecker.readTableVar(meta.GetTable())
	return
}

func (tbl *txnTable) CreateBlock(sid uint64) (blk handle.Block, err error) {
	var seg *catalog.SegmentEntry
	if seg, err = tbl.entry.GetSegmentByID(sid); err != nil {
		return
	}
	var factory catalog.BlockDataFactory
	if tbl.dataFactory != nil {
		segData := seg.GetSegmentData()
		factory = tbl.dataFactory.MakeBlockFactory(segData.GetSegmentFile())
	}
	meta, err := seg.CreateBlock(tbl.txn, catalog.ES_Appendable, factory)
	if err != nil {
		return
	}
	tbl.cblks = append(tbl.cblks, meta)
	tbl.warChecker.readSegmentVar(seg)
	return newBlock(tbl.txn, meta), err
}

func (tbl *txnTable) SetCreateEntry(e txnif.TxnEntry) {
	if tbl.createEntry != nil {
		panic("logic error")
	}
	tbl.createEntry = e
	tbl.warChecker.readDBVar(tbl.entry.GetDB())
}

func (tbl *txnTable) SetDropEntry(e txnif.TxnEntry) {
	if tbl.dropEntry != nil {
		panic("logic error")
	}
	tbl.dropEntry = e
	tbl.warChecker.readDBVar(tbl.entry.GetDB())
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
	tbl.csegs = nil
	tbl.dsegs = nil
	tbl.cblks = nil
	tbl.dblks = nil
	tbl.warChecker = nil
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
		BlockID:   tbl.txn.GetID(),
	}
	n := NewInsertNode(tbl, tbl.nodesMgr, id, tbl.driver)
	tbl.appendable = tbl.nodesMgr.Pin(n)
	tbl.inodes = append(tbl.inodes, n)
	return nil
}

func (tbl *txnTable) AddDeleteNode(id *common.ID, node *updates.DeleteNode) error {
	nid := *id
	u := tbl.deleteNodes[nid]
	if u != nil {
		return ErrDuplicateNode
	}
	tbl.deleteNodes[nid] = node
	return nil
}

func (tbl *txnTable) AddUpdateNode(node txnif.UpdateNode) error {
	id := *node.GetID()
	u := tbl.updateNodes[id]
	if u != nil {
		return ErrDuplicateNode
	}
	tbl.updateNodes[id] = node.(*updates.ColumnNode)
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
			logutil.Info(tbl.nodesMgr.String())
			panic(err)
		}
		space := n.GetSpace()
		logutil.Debugf("Appended: %d, Space:%d", appended, space)
		start := tbl.rows
		// logrus.Infof("s,offset=%d,appended=%d,start=%d", data.Vecs[tbl.GetSchema().PrimaryKey], offset, appended, start)
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
	} else {
		node := tbl.inodes[first]
		err = node.RangeDelete(firstOffset, txnbase.MaxNodeRows-1)
		node = tbl.inodes[last]
		err = node.RangeDelete(0, lastOffset)
		if last > first+1 {
			for i := first + 1; i < last; i++ {
				node = tbl.inodes[i]
				if err = node.RangeDelete(0, txnbase.MaxNodeRows); err != nil {
					break
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
	node := tbl.deleteNodes[common.ID{TableID: tbl.GetID(), SegmentID: segmentId, BlockID: blockId}]
	if node != nil {
		chain := node.GetChain().(*updates.DeleteChain)
		controller := chain.GetController()
		writeLock := controller.GetExclusiveLock()
		err = controller.CheckNotDeleted(start, end, tbl.txn.GetStartTS())
		if err == nil {
			if err = controller.CheckNotUpdated(start, end, tbl.txn.GetStartTS()); err == nil {
				node.RangeDeleteLocked(start, end)
			}
		}
		writeLock.Unlock()
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
	node2, err := blkData.RangeDelete(tbl.txn, start, end)
	if err == nil {
		tbl.AddDeleteNode(blk.AsCommonID(), node2.(*updates.DeleteNode))
	}
	return
}

func (tbl *txnTable) GetByFilter(filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	offset, err = tbl.index.Find(filter.Val)
	if err == nil {
		id = &common.ID{}
		id.PartID = 1
		err = nil
		return
	}
	blockIt := tbl.handle.MakeBlockIt()
	for blockIt.Valid() {
		h := blockIt.GetBlock()
		block := h.GetMeta().(*catalog.BlockEntry).GetBlockData()
		offset, err = block.GetByFilter(tbl.txn, filter)
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
	return block.GetValue(tbl.txn, row, col)
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
		chain := node.GetChain().(*updates.ColumnChain)
		controller := chain.GetController()
		sharedLock := controller.GetSharedLock()
		err = controller.CheckNotDeleted(row, row, tbl.txn.GetStartTS())
		if err == nil {
			chain.Lock()
			err = chain.TryUpdateNodeLocked(row, v, node)
			chain.Unlock()
		}
		sharedLock.Unlock()
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
	node2, err := blkData.Update(tbl.txn, row, col, v)
	if err == nil {
		tbl.AddUpdateNode(node2)
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
	if tbl.index == nil {
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
		segData := seg.GetSegmentData()
		// TODO: Add a new batch dedup method later
		if err = segData.BatchDedup(tbl.txn, pks); err == data.ErrDuplicate {
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
			// logutil.Infof("%s: %d-%d, %d-%d: %s", tbl.txn.String(), tbl.maxSegId, tbl.maxBlkId, seg.GetID(), blk.GetID(), pks.String())
			blkData := blk.GetBlockData()
			// TODO: Add a new batch dedup method later
			if err = blkData.BatchDedup(tbl.txn, pks); err != nil {
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
	h := tbl.nodesMgr.Pin(n)
	defer h.Close()
	return n.GetValue(int(col), noffset)
}

func (tbl *txnTable) PrepareRollback() (err error) {
	if tbl.createEntry != nil {
		entry := tbl.createEntry.(*catalog.TableEntry)
		if err = entry.GetDB().RemoveEntry(entry); err != nil {
			return
		}
	}
	if tbl.createEntry != nil || tbl.dropEntry != nil {
		if err = tbl.entry.PrepareRollback(); err != nil {
			return
		}
	}
	for _, node := range tbl.updateNodes {
		chain := node.GetChain()
		chain.DeleteNode(node.GetDLNode())
	}
	for _, node := range tbl.deleteNodes {
		chain := node.GetChain()
		chain.Lock()
		chain.RemoveNodeLocked(node)
		chain.Unlock()
	}
	return
}

func (tbl *txnTable) applyAppendInode(node InsertNode) (err error) {
	tableData := tbl.entry.GetTableData()
	appended := uint32(0)
	for appended < node.Rows() {
		id, appender, err := tableData.GetAppender()
		if err == data.ErrAppendableSegmentNotFound {
			seg, err := tbl.CreateSegment()
			if err != nil {
				panic(err)
			}
			blk, err := seg.CreateBlock()
			if err != nil {
				panic(err)
			}
			if appender, err = tableData.SetAppender(blk.Fingerprint()); err != nil {
				panic(err)
			}
		} else if err == data.ErrAppendableBlockNotFound {
			blk, err := tbl.CreateBlock(id.SegmentID)
			if err != nil {
				panic(err)
			}
			if appender, err = tableData.SetAppender(blk.Fingerprint()); err != nil {
				panic(err)
			}
		}
		toAppend, err := appender.PrepareAppend(node.Rows() - appended)
		bat, err := node.Window(appended, appended+toAppend-1)
		var destOff uint32
		if destOff, err = appender.ApplyAppend(bat, 0, toAppend, nil); err != nil {
			panic(err)
		}
		appender.Close()
		info := node.AddApplyInfo(appended, toAppend, destOff, toAppend, appender.GetID())
		logutil.Debug(info.String())
		appended += toAppend
		if appended == node.Rows() {
			break
		}
	}
	return
}

func (tbl *txnTable) PreCommit() (err error) {
	for _, node := range tbl.inodes {
		if err = tbl.applyAppendInode(node); err != nil {
			break
		}
	}
	return
}

func (tbl *txnTable) PrepareCommit() (err error) {
	// TODO: consider committing delete scenario later. Important!!!
	tbl.entry.RLock()
	if tbl.entry.CreateAndDropInSameTxn() {
		tbl.entry.RUnlock()
		// TODO: should remove all inodes and updates
		return
	}
	tbl.entry.RUnlock()
	if tbl.createEntry != nil {
		if err = tbl.createEntry.PrepareCommit(); err != nil {
			return
		}
	} else if tbl.dropEntry != nil {
		if err = tbl.dropEntry.PrepareCommit(); err != nil {
			return
		}
	}

	for _, seg := range tbl.csegs {
		logutil.Debugf("PrepareCommit: %s", seg.String())
		if err = seg.PrepareCommit(); err != nil {
			return
		}
	}
	for _, blk := range tbl.cblks {
		logutil.Debugf("PrepareCommit: %s", blk.String())
		if err = blk.PrepareCommit(); err != nil {
			return
		}
	}
	for _, update := range tbl.updateNodes {
		if err = update.PrepareCommit(); err != nil {
			return
		}
	}
	for _, del := range tbl.deleteNodes {
		if err = del.PrepareCommit(); err != nil {
			return
		}
	}
	return
}

func (tbl *txnTable) ApplyCommit() (err error) {
	tbl.entry.RLock()
	if tbl.entry.CreateAndDropInSameTxn() {
		tbl.entry.RUnlock()
		// TODO: should remove all inodes and updates
		return
	}
	tbl.entry.RUnlock()
	if tbl.createEntry != nil {
		if err = tbl.createEntry.ApplyCommit(); err != nil {
			return
		}
	} else if tbl.dropEntry != nil {
		if err = tbl.dropEntry.ApplyCommit(); err != nil {
			return
		}
	}
	for _, seg := range tbl.csegs {
		if err = seg.ApplyCommit(); err != nil {
			break
		}
	}
	for _, blk := range tbl.cblks {
		if err = blk.ApplyCommit(); err != nil {
			break
		}
	}
	for _, update := range tbl.updateNodes {
		if err = update.ApplyCommit(); err != nil {
			return
		}
	}
	for _, del := range tbl.deleteNodes {
		if err = del.ApplyCommit(); err != nil {
			return
		}
	}
	// TODO
	return
}

func (tbl *txnTable) ApplyRollback() (err error) {
	if tbl.createEntry != nil || tbl.dropEntry != nil {
		if err = tbl.entry.ApplyRollback(); err != nil {
			return
		}
	}
	// TODO: rollback all inserts and updates
	return
}

func (tbl *txnTable) buildCommitCmd(cmdSeq *uint32) (cmd txnif.TxnCmd, entries []txnbase.NodeEntry, err error) {
	composedCmd := txnbase.NewComposedCmd()

	for i, inode := range tbl.inodes {
		h := tbl.nodesMgr.Pin(inode)
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
		updateCmd, _, err := node.MakeCommand(*cmdSeq, false)
		if err != nil {
			return cmd, entries, err
		}
		composedCmd.AddCmd(updateCmd)
		*cmdSeq += uint32(1)
	}
	return composedCmd, entries, err
}
