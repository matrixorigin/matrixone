package txnimpl

import (
	"errors"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/updates"
	"github.com/sirupsen/logrus"
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
	Rows() uint32
	BatchDedupLocal(data *gbat.Batch) error
	BatchDedupLocalByCol(col *gvec.Vector) error
	BatchDedup(col *gvec.Vector) error
	AddUpdateNode(txnif.UpdateNode) error
	IsDeleted() bool
	PreCommit() error
	PrepareCommit() error
	PrepareRollback() error
	ApplyCommit() error
	ApplyRollback() error

	WaitSynced()

	SetCreateEntry(txnif.TxnEntry)
	SetDropEntry(txnif.TxnEntry)
	GetMeta() *catalog.TableEntry

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
	updateNodes map[common.ID]*updates.BlockUpdateNode
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
		updateNodes: make(map[common.ID]*updates.BlockUpdateNode),
		csegs:       make([]*catalog.SegmentEntry, 0),
		dsegs:       make([]*catalog.SegmentEntry, 0),
		dataFactory: dataFactory,
		logs:        make([]txnbase.NodeEntry, 0),
	}
	return tbl
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

func (tbl *txnTable) AddUpdateNode(node txnif.UpdateNode) error {
	id := *node.GetID()
	u := tbl.updateNodes[id]
	if u != nil {
		return ErrDuplicateNode
	}
	tbl.updateNodes[id] = node.(*updates.BlockUpdateNode)
	return nil
}

func (tbl *txnTable) Append(data *batch.Batch) error {
	var err error
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
		size := txnbase.EstimateSize(data, offset, toAppend)
		logrus.Debugf("Offset=%d, ToAppend=%d, EstimateSize=%d", offset, toAppend, size)
		err := n.Expand(size, func() error {
			appended, err = n.Append(data, offset)
			return err
		})
		if err != nil {
			logrus.Info(tbl.nodesMgr.String())
			logrus.Error(err)
			break
		}
		space := n.GetSpace()
		logrus.Debugf("Appended: %d, Space:%d", appended, space)
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

func (tbl *txnTable) BatchDedup(pks *gvec.Vector) (err error) {
	if err = tbl.BatchDedupLocalByCol(pks); err != nil {
		return err
	}
	segIt := tbl.handle.MakeSegmentIt()
	for segIt.Valid() {
		seg := segIt.GetSegment()
		if err = seg.BatchDedup(pks); err != nil {
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
	// TODO: remove all inserts and updates
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
		bat, err := node.Window(0, toAppend-1)
		var destOff uint32
		if destOff, err = appender.ApplyAppend(bat, 0, toAppend, nil); err != nil {
			panic(err)
		}
		appender.Close()
		info := node.AddApplyInfo(appended, toAppend, destOff, toAppend, appender.GetID())
		logrus.Debug(info.String())
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
		logrus.Debugf("PrepareCommit: %s", seg.String())
		if err = seg.PrepareCommit(); err != nil {
			return
		}
	}
	for _, blk := range tbl.cblks {
		logrus.Debugf("PrepareCommit: %s", blk.String())
		if err = blk.PrepareCommit(); err != nil {
			return
		}
	}
	// TODO
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

// func (tbl *txnTable) PrepareCommit() (entry NodeEntry, err error) {
// 	err = tbl.ToCommitting()
// 	if err != nil {
// 		return
// 	}
// 	commitCmd, err := tbl.buildCommitCmd()
// 	if err != nil {
// 		return
// 	}
// 	entry, err = commitCmd.MakeLogEntry()
// 	return
// }

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

// func (tbl *txnTable) PrepareCommit() error {
// 	err := tbl.ToCommitting()
// 	if err != nil {
// 		return err
// 	}
// 	tableInsertEntry := NewTableInsertCommitEntry()
// 	// insertEntries := make([]NodeEntry, 0)
// 	// pendings := make([]*AsyncEntry, 0)
// 	cnt := len(tbl.inodes)
// 	for i, inode := range tbl.inodes {
// 		h := tbl.nodesMgr.Pin(inode)
// 		if h == nil {
// 			panic("not expected")
// 		}
// 		e := inode.MakeCommitEntry()
// 		// Processing last insert node
// 		if i == cnt-1 {
// 			insertEntries = append(insertEntries, e)
// 			inode.ToTransient()
// 			h.Close()
// 			break
// 		}

// 		if e.IsUCPointer() {
// 			insertEntries = append(insertEntries, e)
// 			h.Close()
// 			continue
// 		}
// 		lsn, err := tbl.driver.AppendEntry(GroupUC, e)
// 		if err != nil {
// 			panic(err)
// 		}
// 		asyncE := &AsyncEntry{
// 			lsn:       lsn,
// 			group:     GroupUC,
// 			NodeEntry: e,
// 			seq:       uint32(i),
// 		}
// 		insertEntries = append(insertEntries, asyncE)
// 		pendings = append(pendings, asyncE)
// 		inode.ToTransient()
// 		h.Close()
// 	}
// 	tbl.ToCommitted()
// 	return nil
// }

// func (tbl *txnTable) Commit() error {
// 	return nil
// }
