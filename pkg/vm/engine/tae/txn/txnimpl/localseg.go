package txnimpl

import (
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const LocalSegmentStartID = math.MaxUint64 / 2

var localSegmentIdAlloc *common.IdAlloctor

func init() {
	localSegmentIdAlloc = common.NewIdAlloctor(LocalSegmentStartID)
}

type localSegment struct {
	*txnbase.TxnSegment
	entry       *catalog.SegmentEntry
	appendable  base.INodeHandle
	index       TableIndex
	nodes       []InsertNode
	table       *txnTable
	rows        uint32
	appends     []*appendCtx
	tableHandle data.TableHandle
}

func newLocalSegment(table *txnTable) *localSegment {
	entry := catalog.NewStandaloneSegment(table.entry, localSegmentIdAlloc.Alloc(), table.store.txn.GetStartTS())
	return &localSegment{
		TxnSegment: &txnbase.TxnSegment{
			Txn: table.store.txn,
		},
		entry:   entry,
		nodes:   make([]InsertNode, 0),
		index:   NewSimpleTableIndex(),
		appends: make([]*appendCtx, 0),
		table:   table,
	}
}

func (seg *localSegment) GetLocalPhysicalAxis(row uint32) (int, uint32) {
	npos := int(row) / int(txnbase.MaxNodeRows)
	noffset := row % uint32(txnbase.MaxNodeRows)
	return npos, noffset
}

func (seg *localSegment) registerInsertNode() error {
	if seg.appendable != nil {
		seg.appendable.Close()
	}
	id := seg.entry.AsCommonID()
	id.BlockID = uint64(len(seg.nodes))
	n := NewInsertNode(seg.table, seg.table.store.nodesMgr, *id, seg.table.store.driver)
	seg.appendable = seg.table.store.nodesMgr.Pin(n)
	seg.nodes = append(seg.nodes, n)
	return nil
}

func (seg *localSegment) ApplyAppend() {
	var err error
	for _, ctx := range seg.appends {
		var (
			destOff    uint32
			appendNode txnif.AppendNode
		)
		bat, _ := ctx.node.Window(ctx.start, ctx.start+ctx.count-1)
		if appendNode, destOff, err = ctx.driver.ApplyAppend(bat, 0, ctx.count, seg.Txn); err != nil {
			panic(err)
		}
		ctx.driver.Close()
		id := ctx.driver.GetID()
		info := ctx.node.AddApplyInfo(ctx.start, ctx.count, destOff, ctx.count, seg.table.entry.GetDB().ID, id)
		logutil.Debugf(info.String())
		if err = appendNode.PrepareCommit(); err != nil {
			panic(err)
		}
		seg.table.txnEntries = append(seg.table.txnEntries, appendNode)
	}
	if seg.tableHandle != nil {
		seg.table.entry.GetTableData().ApplyHandle(seg.tableHandle)
	}
}

func (seg *localSegment) PrepareApply() (err error) {
	for _, node := range seg.nodes {
		if err = seg.prepareApplyNode(node); err != nil {
			break
		}
	}
	return
}

func (seg *localSegment) prepareApplyNode(node InsertNode) (err error) {
	tableData := seg.table.entry.GetTableData()
	if seg.tableHandle == nil {
		seg.tableHandle = tableData.GetHandle()
	}
	appended := uint32(0)
	for appended < node.RowsWithoutDeletes() {
		appender, err := seg.tableHandle.GetAppender()
		if err == data.ErrAppendableSegmentNotFound {
			segH, err := seg.table.CreateSegment()
			if err != nil {
				return err
			}
			blk, err := segH.CreateBlock()
			if err != nil {
				return err
			}
			appender = seg.tableHandle.SetAppender(blk.Fingerprint())
		} else if err == data.ErrAppendableBlockNotFound {
			id := appender.GetID()
			blk, err := seg.table.CreateBlock(id.SegmentID)
			if err != nil {
				return err
			}
			appender = seg.tableHandle.SetAppender(blk.Fingerprint())
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
		seg.table.store.warChecker.ReadBlock(seg.table.entry.GetDB().ID, id)
		seg.appends = append(seg.appends, ctx)
		logutil.Debugf("%s: toAppend %d, appended %d, blks=%d", id.String(), toAppend, appended, len(seg.appends))
		appended += toAppend
		if appended == node.Rows() {
			break
		}
	}
	return
}

func (seg *localSegment) Append(data *batch.Batch) (err error) {
	if seg.appendable == nil {
		if err = seg.registerInsertNode(); err != nil {
			return err
		}
	}
	appended := uint32(0)
	offset := uint32(0)
	length := uint32(vector.Length(data.Vecs[0]))
	for {
		h := seg.appendable
		n := h.GetNode().(*insertNode)
		toAppend := n.PrepareAppend(data, offset)
		size := compute.EstimateSize(data, offset, toAppend)
		logutil.Debugf("Offset=%d, ToAppend=%d, EstimateSize=%d", offset, toAppend, size)
		err = n.Expand(size, func() error {
			appended, err = n.Append(data, offset)
			return err
		})
		if err != nil {
			logutil.Info(seg.table.store.nodesMgr.String())
			panic(err)
		}
		space := n.GetSpace()
		logutil.Debugf("Appended: %d, Space:%d", appended, space)
		start := seg.rows
		if err = seg.index.BatchInsert(data.Vecs[seg.table.GetSchema().PrimaryKey], int(offset), int(appended), start, false); err != nil {
			break
		}
		offset += appended
		seg.rows += appended
		if space == 0 {
			if err = seg.registerInsertNode(); err != nil {
				break
			}
		}
		if offset >= length {
			break
		}
	}
	return err
}

func (seg *localSegment) RangeDelete(start, end uint32) error {
	first, firstOffset := seg.GetLocalPhysicalAxis(start)
	last, lastOffset := seg.GetLocalPhysicalAxis(end)
	var err error
	if last == first {
		node := seg.nodes[first]
		err = node.RangeDelete(firstOffset, lastOffset)
		if err == nil {
			for i := firstOffset; i <= lastOffset; i++ {
				v, _ := node.GetValue(int(seg.table.entry.GetSchema().PrimaryKey), i)
				if err = seg.index.Delete(v); err != nil {
					break
				}
			}
		}
	} else {
		node := seg.nodes[first]
		err = node.RangeDelete(firstOffset, txnbase.MaxNodeRows-1)
		node = seg.nodes[last]
		err = node.RangeDelete(0, lastOffset)
		for i := uint32(0); i <= lastOffset; i++ {
			v, _ := node.GetValue(int(seg.table.entry.GetSchema().PrimaryKey), i)
			if err = seg.index.Delete(v); err != nil {
				break
			}
		}
		if last > first+1 && err == nil {
			for i := first + 1; i < last; i++ {
				node = seg.nodes[i]
				if err = node.RangeDelete(0, txnbase.MaxNodeRows); err != nil {
					break
				}
				for i := uint32(0); i <= txnbase.MaxNodeRows; i++ {
					v, _ := node.GetValue(int(seg.table.entry.GetSchema().PrimaryKey), i)
					if err = seg.index.Delete(v); err != nil {
						break
					}
				}
			}
		}
	}
	return err
}

func (seg *localSegment) CollectCmd(cmdMgr *commandManager) (err error) {
	for i, node := range seg.nodes {
		h := seg.table.store.nodesMgr.Pin(node)
		if h == nil {
			panic("not expected")
		}
		forceFlush := i < len(seg.nodes)-1
		csn := uint32(0xffff) // Special cmd
		cmd, entry, err := node.MakeCommand(csn, forceFlush)
		if err != nil {
			panic(err)
		}
		if entry != nil {
			seg.table.logs = append(seg.table.logs, entry)
		}
		node.ToTransient()
		h.Close()
		if cmd != nil {
			cmdMgr.AddInternalCmd(cmd)
		}
	}
	return
}

func (seg *localSegment) DeletesToString() string {
	var s string
	for i, n := range seg.nodes {
		s = fmt.Sprintf("%s\t<INode-%d>: %s\n", s, i, n.PrintDeletes())
	}
	return s
}

func (seg *localSegment) IsDeleted(row uint32) bool {
	npos, noffset := seg.GetLocalPhysicalAxis(row)
	n := seg.nodes[npos]
	return n.IsRowDeleted(noffset)
}

func (seg *localSegment) Update(row uint32, col uint16, value interface{}) error {
	npos, noffset := seg.GetLocalPhysicalAxis(row)
	n := seg.nodes[npos]
	window, err := n.Window(uint32(noffset), uint32(noffset))
	if err != nil {
		return err
	}
	if err = n.RangeDelete(uint32(noffset), uint32(noffset)); err != nil {
		return err
	}
	v, _ := n.GetValue(int(seg.table.entry.GetSchema().PrimaryKey), row)
	if err = seg.index.Delete(v); err != nil {
		panic(err)
	}
	err = seg.Append(window)
	return err
}

func (seg *localSegment) Rows() uint32 {
	cnt := len(seg.nodes)
	if cnt == 0 {
		return 0
	}
	return (uint32(cnt)-1)*txnbase.MaxNodeRows + seg.nodes[cnt-1].Rows()
}

func (seg *localSegment) GetByFilter(filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	offset, err = seg.index.Find(filter.Val)
	if err == nil {
		id = &common.ID{}
		id.PartID = 1
		id.TableID = seg.table.entry.ID
	}
	return
}

func (seg *localSegment) GetPrimaryColumn() *vector.Vector {
	schema := seg.table.entry.GetSchema()
	return seg.index.KeyToVector(schema.ColDefs[schema.PrimaryKey].Type)
}

func (seg *localSegment) BatchDedupByCol(col *vector.Vector) error {
	return seg.index.BatchDedup(col)
}

func (seg *localSegment) GetValue(row uint32, col uint16) (interface{}, error) {
	npos, noffset := seg.GetLocalPhysicalAxis(row)
	n := seg.nodes[npos]
	h := seg.table.store.nodesMgr.Pin(n)
	defer h.Close()
	return n.GetValue(int(col), noffset)
}

func (seg *localSegment) Close() (err error) {
	if seg.appendable != nil {
		if err = seg.appendable.Close(); err != nil {
			return
		}
	}
	for _, node := range seg.nodes {
		if err = node.Close(); err != nil {
			return
		}
	}
	seg.index.Close()
	seg.index = nil
	seg.nodes = nil
	seg.appendable = nil
	return
}
