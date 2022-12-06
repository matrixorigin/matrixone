// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"bytes"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const (
	// Note: Do not edit this id!!!
	LocalSegmentStartID uint64 = 1 << 47
)

var localSegmentIdAlloc *common.IdAlloctor

func init() {
	localSegmentIdAlloc = common.NewIdAlloctor(LocalSegmentStartID)
}

func isLocalSegment(id *common.ID) bool {
	return id.SegmentID >= LocalSegmentStartID
}

func isLocalSegmentByID(id uint64) bool {
	return id >= LocalSegmentStartID
}

type blockMeta struct {
	uuid string
	//sorted primary key if exists.
	pks     containers.Vector
	metaloc string
	//object name on S3/FS
	file string
}

type localSegment struct {
	entry      *catalog.SegmentEntry
	appendable base.INodeHandle
	//index for primary key
	index TableIndex
	//nodes contains memInsertNode and persistedInsertNode.
	nodes []InsertNode
	//the max index of memInsertNode.
	maxIdx uint32
	//meta for non-appendable blocks on S3/FS
	//TODO::will be removed.
	blocks      map[string]*blockMeta
	table       *txnTable
	rows        uint32
	appends     []*appendCtx
	tableHandle data.TableHandle
	//last non-appendable segment into which being added non-appendable blocks
	lastNSeg data.Segment
}

func newLocalSegment(table *txnTable) *localSegment {
	entry := catalog.NewStandaloneSegment(table.entry, localSegmentIdAlloc.Alloc(), table.store.txn.GetStartTS())
	return &localSegment{
		entry:   entry,
		nodes:   make([]InsertNode, 0),
		blocks:  make(map[string]*blockMeta),
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

func (seg *localSegment) registerPersistedInsertNode() {

}

func (seg *localSegment) registerMemInsertNode() {
	var err error
	if seg.appendable != nil {
		seg.appendable.Close()
	}
	meta := catalog.NewStandaloneBlock(seg.entry, uint64(len(seg.nodes)), seg.table.store.txn.GetStartTS())
	seg.entry.AddEntryLocked(meta)
	n := NewMemInsertNode(
		seg.table,
		seg.table.store.dataFactory.Fs,
		seg.table.store.nodesMgr,
		seg.table.store.dataFactory.Scheduler, meta,
		seg.table.store.driver)
	//TODO::will be removed in next PR.
	seg.appendable, err = seg.table.store.nodesMgr.TryPin(n.storage.mnode, time.Second)
	if err != nil {
		panic(err)
	}
	seg.nodes = append(seg.nodes, n)
	seg.maxIdx = uint32(len(seg.nodes))
}

// ApplyAppend applies all the memInsertNodes into appendable blocks
// and un-reference the appendable blocks which had been referenced when PrepareApply.
func (seg *localSegment) ApplyAppend() (err error) {
	var destOff int
	defer func() {
		// Close All unclosed Appends:un-reference the appendable block.
		seg.CloseAppends()
	}()
	for _, ctx := range seg.appends {
		bat, _ := ctx.node.Window(ctx.start, ctx.start+ctx.count)
		defer bat.Close()
		if destOff, err = ctx.driver.ApplyAppend(
			bat,
			seg.table.store.txn); err != nil {
			return
		}
		id := ctx.driver.GetID()
		ctx.node.AddApplyInfo(ctx.start, ctx.count, uint32(destOff), ctx.count, seg.table.entry.GetDB().ID, id)
	}
	if seg.tableHandle != nil {
		seg.table.entry.GetTableData().ApplyHandle(seg.tableHandle)
	}
	return
}

func (seg *localSegment) PrepareApply() (err error) {
	defer func() {
		//un-reference the last non-appendable segment.
		seg.table.entry.GetTableData().CloseLastNonAppendableSeg()
	}()
	defer func() {
		if err != nil {
			// Close All unclosed Appends: un-reference all the appendable blocks.
			seg.CloseAppends()
		}
	}()
	for _, node := range seg.nodes {
		if err = seg.prepareApplyNode(node); err != nil {
			return
		}
	}
	return
}

// TODO::will be removed.
func (seg *localSegment) ApplyBlocksOnFS(blks map[string]*blockMeta) (err error) {
	//create a appendable segment.
	segH, err := seg.table.CreateSegment(true)
	if err != nil {
		return err
	}
	for _, v := range blks {
		_, err := segH.CreateNonAppendableBlockWithMeta(v.metaloc, "")
		if err != nil {
			return err
		}
	}
	return
}

func (seg *localSegment) prepareApplyNode(node InsertNode) (err error) {
	if !node.IsPersisted() {
		tableData := seg.table.entry.GetTableData()
		if seg.tableHandle == nil {
			seg.tableHandle = tableData.GetHandle()
		}
		appended := uint32(0)
		for appended < node.RowsWithoutDeletes() {
			appender, err := seg.tableHandle.GetAppender()
			if moerr.IsMoErrCode(err, moerr.ErrAppendableSegmentNotFound) {
				segH, err := seg.table.CreateSegment(true)
				if err != nil {
					return err
				}
				blk, err := segH.CreateBlock(true)
				if err != nil {
					return err
				}
				appender = seg.tableHandle.SetAppender(blk.Fingerprint())
			} else if moerr.IsMoErrCode(err, moerr.ErrAppendableBlockNotFound) {
				id := appender.GetID()
				blk, err := seg.table.CreateBlock(id.SegmentID, true)
				if err != nil {
					return err
				}
				appender = seg.tableHandle.SetAppender(blk.Fingerprint())
			}
			//PrepareAppend: It is very important that appending a AppendNode into
			// block's MVCCHandle before applying data into block.
			anode, created, toAppend, err := appender.PrepareAppend(
				node.RowsWithoutDeletes()-appended,
				seg.table.store.txn)
			if err != nil {
				return err
			}
			toAppendWithDeletes := node.LengthWithDeletes(appended, toAppend)
			ctx := &appendCtx{
				driver: appender,
				node:   node,
				anode:  anode,
				start:  node.OffsetWithDeletes(appended),
				count:  toAppendWithDeletes,
			}
			if created {
				seg.table.store.IncreateWriteCnt()
				seg.table.txnEntries.Append(anode)
			}
			id := appender.GetID()
			seg.table.store.warChecker.Insert(appender.GetMeta().(*catalog.BlockEntry))
			seg.table.store.txn.GetMemo().AddBlock(seg.table.entry.GetDB().ID, id.TableID, id.SegmentID, id.BlockID)
			seg.appends = append(seg.appends, ctx)
			logutil.Debugf("%s: toAppend %d, appended %d, blks=%d", id.String(), toAppend, appended, len(seg.appends))
			appended += toAppend
			if appended == node.Rows() {
				break
			}
		}
		return
	}
	//prepareApply persistedInsertNode.
	tableData := seg.table.entry.GetTableData()
	segID, err := tableData.GetLastNonAppendableSeg()
	if moerr.IsMoErrCode(err, moerr.ErrNonAppendableSegmentNotFound) {
		segH, err := seg.table.CreateNonAppendableSegment(true)
		if err != nil {
			return err
		}
		tableData.SetLastNonAppendableSeg(segH.GetMeta().(*catalog.SegmentEntry).AsCommonID())
		//create non-appendable block.
		_, err = segH.CreateNonAppendableBlockWithMeta(node.GetMetaLoc())
		if err != nil {
			return err
		}
		return err
	}
	metaLoc, delLoc := node.GetMetaLoc()
	_, err = seg.table.CreateNonAppendableBlockWithMeta(segID.SegmentID, metaLoc, delLoc)
	if err != nil {
		return err
	}
	return
}

// CloseAppends un-reference the appendable blocks
func (seg *localSegment) CloseAppends() {
	for _, ctx := range seg.appends {
		ctx.driver.Close()
	}
}

// Append appends batch of data into memInsetNode.
// TODO::need to refactor.
func (seg *localSegment) Append(data *containers.Batch) (err error) {
	if seg.appendable == nil {
		seg.registerMemInsertNode()
	}
	appended := uint32(0)
	offset := uint32(0)
	length := uint32(data.Length())
	for {
		h := seg.appendable
		n := h.GetNode().(*memoryNode)
		space := n.GetSpace()
		if space == 0 {
			seg.registerMemInsertNode()
			n = h.GetNode().(*memoryNode)
		}
		toAppend := n.PrepareAppend(data, offset)
		size := compute.EstimateSize(data, offset, toAppend)
		logutil.Debugf("Offset=%d, ToAppend=%d, EstimateSize=%d", offset, toAppend, size)
		err = n.Expand(size, func() error {
			appended, err = n.Append(data, offset)
			return err
		})
		if err != nil {
			logutil.Info(seg.table.store.nodesMgr.String())
			break
		}
		logutil.Debugf("Appended: %d, Space:%d", appended, space)
		if seg.table.schema.HasPK() {
			if err = seg.index.BatchInsert(
				data.Attrs[seg.table.schema.GetSingleSortKeyIdx()],
				data.Vecs[seg.table.schema.GetSingleSortKeyIdx()],
				int(offset),
				int(appended),
				seg.rows,
				false); err != nil {
				break
			}
		}
		offset += appended
		seg.rows += appended
		if offset >= length {
			break
		}
	}
	return err
}

// AddBlocksWithMetaLoc transfer blocks with meta location into persistedInsertNodes
// TODO::rename to AddBlocksWithMetaLoc(...)
func (seg *localSegment) AppendBlocksOnFS(
	pkVecs []containers.Vector,
	uuids []string,
	file string,
	metaLocs []string,
	flag int32) (err error) {
	for i, id := range uuids {
		seg.blocks[id] = &blockMeta{
			uuid: id,
			//pks:  nil,
			metaloc: metaLocs[i],
			file:    file,
		}
		if pkVecs != nil {
			seg.blocks[id].pks = pkVecs[i]
			//insert primary keys into seg.index
			if err = seg.index.BatchInsert(
				seg.table.schema.GetSingleSortKey().Name,
				pkVecs[i],
				0,
				pkVecs[i].Length(),
				seg.rows,
				false,
			); err != nil {
				return
			}
			seg.rows += uint32(pkVecs[i].Length())
		}
	}
	return nil
}

// TODO::need to rewrite
func (seg *localSegment) DeleteFromIndex(from, to uint32, node InsertNode) (err error) {
	for i := from; i <= to; i++ {
		v := node.GetValue(seg.table.schema.GetSingleSortKeyIdx(), i)
		if err = seg.index.Delete(v); err != nil {
			break
		}
	}
	return
}

// TODO::
func (seg *localSegment) RangeDelete(start, end uint32) error {
	first, firstOffset := seg.GetLocalPhysicalAxis(start)
	last, lastOffset := seg.GetLocalPhysicalAxis(end)
	var err error
	if last == first {
		node := seg.nodes[first]
		err = node.RangeDelete(firstOffset, lastOffset)
		if err != nil {
			return err
		}
		if !seg.table.schema.HasPK() {
			// If no pk defined
			return err
		}
		err = seg.DeleteFromIndex(firstOffset, lastOffset, node)
		return err
	}

	node := seg.nodes[first]
	if err = node.RangeDelete(firstOffset, txnbase.MaxNodeRows-1); err != nil {
		return err
	}
	node = seg.nodes[last]
	if err = node.RangeDelete(0, lastOffset); err != nil {
		return err
	}
	if err = seg.DeleteFromIndex(0, lastOffset, node); err != nil {
		return err
	}
	if last > first+1 {
		for i := first + 1; i < last; i++ {
			node = seg.nodes[i]
			if err = node.RangeDelete(0, txnbase.MaxNodeRows); err != nil {
				break
			}
			if err = seg.DeleteFromIndex(0, txnbase.MaxNodeRows, node); err != nil {
				break
			}
		}
	}
	return err
}

// CollectCmd collect txnCmd for memInsertNode.
func (seg *localSegment) CollectCmd(cmdMgr *commandManager) (err error) {
	for i, node := range seg.nodes {
		//if node.IsPersisted() {
		//	continue
		//}
		//h, err := seg.table.store.nodesMgr.TryPin(node.(*memInsertNode).storage.mnode, time.Second)
		//if err != nil {
		//	return err
		//}
		//forceFlush := i < len(seg.nodes)-1
		forceFlush := i < int(seg.maxIdx-1)
		csn := uint32(0xffff) // Special cmd
		cmd, entry, err := node.MakeCommand(csn, forceFlush)
		if err != nil {
			panic(err)
		}
		if entry != nil {
			seg.table.logs = append(seg.table.logs, entry)
		}
		//node.ToTransient()
		//h.Close()
		if cmd != nil {
			cmdMgr.AddInternalCmd(cmd)
		}
	}
	return
}

// TODO::
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

func (seg *localSegment) Rows() uint32 {
	cnt := len(seg.nodes)
	if cnt == 0 {
		return 0
	}
	return (uint32(cnt)-1)*txnbase.MaxNodeRows + seg.nodes[cnt-1].Rows()
}

func (seg *localSegment) GetByFilter(filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	id = seg.entry.AsCommonID()
	if !seg.table.schema.HasPK() {
		_, _, offset = model.DecodePhyAddrKeyFromValue(filter.Val)
		return
	}
	if v, ok := filter.Val.([]byte); ok {
		offset, err = seg.index.Search(string(v))
	} else {
		offset, err = seg.index.Search(filter.Val)
	}
	return
}

func (seg *localSegment) GetPKColumn() containers.Vector {
	schema := seg.table.entry.GetSchema()
	return seg.index.KeyToVector(schema.GetSortKeyType())
}

func (seg *localSegment) BatchDedup(key containers.Vector) error {
	return seg.index.BatchDedup(seg.table.GetSchema().GetSingleSortKey().Name, key)
}

// TODO::need to rewrite
func (seg *localSegment) GetColumnDataByIds(
	blk *catalog.BlockEntry,
	colIdxes []int,
	buffers []*bytes.Buffer) (view *model.BlockView, err error) {
	view = model.NewBlockView(seg.table.store.txn.GetStartTS())
	npos := int(blk.ID)
	n := seg.nodes[npos]
	h, err := seg.table.store.nodesMgr.TryPin(n.(*memInsertNode).storage.mnode, time.Second)
	if err != nil {
		return
	}
	err = n.FillBlockView(view, buffers, colIdxes)
	h.Close()
	if err != nil {
		return
	}
	return
}

// TODO::
func (seg *localSegment) GetColumnDataById(
	blk *catalog.BlockEntry,
	colIdx int,
	buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	view = model.NewColumnView(seg.table.store.txn.GetStartTS(), colIdx)
	npos := int(blk.ID)
	n := seg.nodes[npos]
	h, err := seg.table.store.nodesMgr.TryPin(n.(*memInsertNode).storage.mnode, time.Second)
	if err != nil {
		return
	}
	err = n.FillColumnView(view, buffer)
	h.Close()
	if err != nil {
		return
	}
	// view.ApplyDeletes()
	return
}

func (seg *localSegment) GetBlockRows(blk *catalog.BlockEntry) int {
	npos := int(blk.ID)
	n := seg.nodes[npos]
	return int(n.Rows())
}

// TODO::need to refactor:call InsertNode's GetValue.
func (seg *localSegment) GetValue(row uint32, col uint16) (any, error) {
	npos, noffset := seg.GetLocalPhysicalAxis(row)
	n := seg.nodes[npos]
	h, err := seg.table.store.nodesMgr.TryPin(n.(*memInsertNode).storage.mnode, time.Second)
	if err != nil {
		return nil, err
	}
	defer h.Close()
	v := n.GetValue(int(col), noffset)
	return v, nil
}

func (seg *localSegment) Close() (err error) {
	if seg.appendable != nil {
		//unpin the memoryNode.
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
