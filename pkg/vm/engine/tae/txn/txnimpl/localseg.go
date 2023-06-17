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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

const (
	// Note: Do not edit this id!!!
	LocalSegmentStartID uint64 = 1 << 47
)

// var localSegmentIdAlloc *common.IdAlloctor

// func init() {
// 	localSegmentIdAlloc = common.NewIdAlloctor(LocalSegmentStartID)
// }

type localSegment struct {
	entry *catalog.SegmentEntry

	appendable InsertNode
	//index for primary key
	index TableIndex
	//nodes contains anode and node.
	nodes       []InsertNode
	table       *txnTable
	rows        uint32
	appends     []*appendCtx
	tableHandle data.TableHandle
	nseg        handle.Segment
}

func newLocalSegment(table *txnTable) *localSegment {
	return &localSegment{
		entry: catalog.NewStandaloneSegment(
			table.entry,
			table.store.txn.GetStartTS()),
		nodes:   make([]InsertNode, 0),
		index:   NewSimpleTableIndex(),
		appends: make([]*appendCtx, 0),
		table:   table,
	}
}

func (seg *localSegment) GetLocalPhysicalAxis(row uint32) (int, uint32) {
	var sum uint32
	for i, node := range seg.nodes {
		sum += node.Rows()
		if row <= sum-1 {
			return i, node.Rows() - (sum - (row + 1)) - 1
		}
	}
	panic("Invalid row ")
}

// register a non-appendable insertNode.
func (seg *localSegment) registerNode(metaLoc objectio.Location, deltaLoc objectio.Location) {
	sid := metaLoc.Name().SegmentId()
	meta := catalog.NewStandaloneBlockWithLoc(
		nil,
		objectio.NewBlockid(&sid, 0, uint16(len(seg.nodes))),
		seg.table.store.txn.GetStartTS(),
		metaLoc,
		deltaLoc)
	n := newPNode(
		seg.table,
		meta,
	)
	seg.nodes = append(seg.nodes, n)

}

// register an appendable insertNode.
func (seg *localSegment) registerANode() {
	entry := seg.entry
	meta := catalog.NewStandaloneBlock(
		entry,
		objectio.NewBlockid(&entry.ID, 0, uint16(len(seg.nodes))),
		seg.table.store.txn.GetStartTS())
	entry.AddEntryLocked(meta)
	n := NewANode(
		seg.table,
		meta,
	)
	seg.appendable = n
	seg.nodes = append(seg.nodes, n)
}

// ApplyAppend applies all the anodes into appendable blocks
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
		ctx.node.AddApplyInfo(
			ctx.start,
			ctx.count,
			uint32(destOff),
			ctx.count, id)
	}
	if seg.tableHandle != nil {
		seg.table.entry.GetTableData().ApplyHandle(seg.tableHandle)
	}
	return
}

func (seg *localSegment) PrepareApply() (err error) {
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

func (seg *localSegment) prepareApplyANode(node *anode) error {
	node.Compact()
	tableData := seg.table.entry.GetTableData()
	if seg.tableHandle == nil {
		seg.tableHandle = tableData.GetHandle()
	}
	appended := uint32(0)
	vec := seg.table.store.rt.VectorPool.Transient.GetVector(&objectio.RowidType)
	for appended < node.Rows() {
		appender, err := seg.tableHandle.GetAppender()
		if moerr.IsMoErrCode(err, moerr.ErrAppendableSegmentNotFound) {
			segH, err := seg.table.CreateSegment(true)
			if err != nil {
				return err
			}
			blk, err := segH.CreateBlock(true)
			segH.Close()
			if err != nil {
				return err
			}
			appender = seg.tableHandle.SetAppender(blk.Fingerprint())
			blk.Close()
		} else if moerr.IsMoErrCode(err, moerr.ErrAppendableBlockNotFound) {
			id := appender.GetID()
			blk, err := seg.table.CreateBlock(id.SegmentID(), true)
			if err != nil {
				return err
			}
			appender = seg.tableHandle.SetAppender(blk.Fingerprint())
			blk.Close()
		}
		if !appender.IsSameColumns(seg.table.GetLocalSchema()) {
			return moerr.NewInternalErrorNoCtx("schema changed, please rollback and retry")
		}
		//PrepareAppend: It is very important that appending a AppendNode into
		// block's MVCCHandle before applying data into block.
		anode, created, toAppend, err := appender.PrepareAppend(
			node.Rows()-appended,
			seg.table.store.txn)
		if err != nil {
			return err
		}
		blockId := appender.GetMeta().(*catalog.BlockEntry).ID
		col, err := objectio.ConstructRowidColumn(
			&blockId,
			anode.GetMaxRow()-toAppend,
			toAppend,
			common.DefaultAllocator)
		if err != nil {
			return err
		}
		defer col.Free(common.DefaultAllocator)
		if err = vec.ExtendVec(col); err != nil {
			return err
		}
		ctx := &appendCtx{
			driver: appender,
			node:   node,
			anode:  anode,
			start:  appended,
			count:  toAppend,
		}
		if created {
			seg.table.store.IncreateWriteCnt()
			seg.table.txnEntries.Append(anode)
		}
		id := appender.GetID()
		seg.table.store.warChecker.Insert(appender.GetMeta().(*catalog.BlockEntry))
		seg.table.store.txn.GetMemo().AddBlock(seg.table.entry.GetDB().ID,
			id.TableID, &id.BlockID)
		seg.appends = append(seg.appends, ctx)
		// logutil.Debugf("%s: toAppend %d, appended %d, blks=%d",
		// 	id.String(), toAppend, appended, len(seg.appends))
		appended += toAppend
		if appended == node.Rows() {
			break
		}
	}
	node.data.Vecs[seg.table.GetLocalSchema().PhyAddrKey.Idx].Close()
	node.data.Vecs[seg.table.GetLocalSchema().PhyAddrKey.Idx] = vec
	return nil
}

func (seg *localSegment) prepareApplyPNode(node *pnode) (err error) {
	//handle persisted insertNode.
	metaloc, deltaloc := node.GetPersistedLoc()
	blkn := metaloc.ID()
	sid := metaloc.Name().SegmentId()
	filen := metaloc.Name().Num()

	shouldCreateNewSeg := func() bool {
		if seg.nseg == nil {
			return true
		}
		entry := seg.nseg.GetMeta().(*catalog.SegmentEntry)
		return entry.ID != sid
	}

	if shouldCreateNewSeg() {
		seg.nseg, err = seg.table.CreateNonAppendableSegment(true, new(objectio.CreateSegOpt).WithId(&sid))
		seg.nseg.GetMeta().(*catalog.SegmentEntry).SetSorted()
		if err != nil {
			return
		}
	}
	opts := new(objectio.CreateBlockOpt).
		WithMetaloc(metaloc).
		WithDetaloc(deltaloc).
		WithFileIdx(filen).
		WithBlkIdx(uint16(blkn))
	_, err = seg.nseg.CreateNonAppendableBlock(opts)
	if err != nil {
		return
	}
	return
}

func (seg *localSegment) prepareApplyNode(node InsertNode) (err error) {
	if !node.IsPersisted() {
		return seg.prepareApplyANode(node.(*anode))
	}
	return seg.prepareApplyPNode(node.(*pnode))
}

// CloseAppends un-reference the appendable blocks
func (seg *localSegment) CloseAppends() {
	for _, ctx := range seg.appends {
		if ctx.driver != nil {
			ctx.driver.Close()
			ctx.driver = nil
		}
	}
}

// Append appends batch of data into anode.
func (seg *localSegment) Append(data *containers.Batch) (err error) {
	if seg.appendable == nil {
		seg.registerANode()
	}
	appended := uint32(0)
	offset := uint32(0)
	length := uint32(data.Length())
	schema := seg.table.GetLocalSchema()
	for {
		h := seg.appendable
		space := h.GetSpace()
		if space == 0 {
			seg.registerANode()
			h = seg.appendable
		}
		appended, err = h.Append(data, offset)
		if err != nil {
			return
		}
		dedupType := seg.table.store.txn.GetDedupType()
		if schema.HasPK() && dedupType == txnif.FullDedup {
			if err = seg.index.BatchInsert(
				data.Attrs[schema.GetSingleSortKeyIdx()],
				data.Vecs[schema.GetSingleSortKeyIdx()],
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
	return
}

// AddBlksWithMetaLoc transfers blocks with meta location into non-appendable nodes
func (seg *localSegment) AddBlksWithMetaLoc(
	pkVecs []containers.Vector,
	metaLocs []objectio.Location,
) (err error) {
	for i, metaLoc := range metaLocs {
		seg.registerNode(metaLoc, nil)
		dedupType := seg.table.store.txn.GetDedupType()
		//insert primary keys into seg.index
		if pkVecs != nil && dedupType == txnif.FullDedup {
			if err = seg.index.BatchInsert(
				seg.table.GetLocalSchema().GetSingleSortKey().Name,
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

func (seg *localSegment) DeleteFromIndex(from, to uint32, node InsertNode) (err error) {
	schema := seg.table.GetLocalSchema()
	for i := from; i <= to; i++ {
		v, _, err := node.GetValue(schema.GetSingleSortKeyIdx(), i)
		if err != nil {
			return err
		}
		if err = seg.index.Delete(v); err != nil {
			return err
		}
	}
	return
}

// RangeDelete delete rows : [start, end]
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
		if !seg.table.GetLocalSchema().HasPK() {
			// If no pk defined
			return err
		}
		err = seg.DeleteFromIndex(firstOffset, lastOffset, node)
		return err
	}

	node := seg.nodes[first]
	if err = node.RangeDelete(firstOffset, node.Rows()-1); err != nil {

		return err
	}
	if err = seg.DeleteFromIndex(firstOffset, node.Rows()-1, node); err != nil {
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
			if err = node.RangeDelete(0, node.Rows()-1); err != nil {
				break
			}
			if err = seg.DeleteFromIndex(0, node.Rows()-1, node); err != nil {
				break
			}
		}
	}
	return err
}

// CollectCmd collect txnCmd for anode whose data resides in memory.
func (seg *localSegment) CollectCmd(cmdMgr *commandManager) (err error) {
	for _, node := range seg.nodes {
		csn := uint32(0xffff) // Special cmd
		cmd, err := node.MakeCommand(csn)
		if err != nil {
			panic(err)
		}
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

func (seg *localSegment) Rows() (n uint32) {
	for _, node := range seg.nodes {
		n += node.Rows()
	}
	return
}

func (seg *localSegment) GetByFilter(filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	if !seg.table.GetLocalSchema().HasPK() {
		id = seg.table.entry.AsCommonID()
		rid := filter.Val.(types.Rowid)
		id.BlockID, offset = rid.Decode()
		return
	}
	id = seg.entry.AsCommonID()
	if v, ok := filter.Val.([]byte); ok {
		offset, err = seg.index.Search(string(v))
	} else {
		offset, err = seg.index.Search(filter.Val)
	}
	if err != nil {
		return
	}
	return
}

func (seg *localSegment) GetPKColumn() containers.Vector {
	schema := seg.table.entry.GetLastestSchema()
	return seg.index.KeyToVector(schema.GetSingleSortKeyType())
}

func (seg *localSegment) GetPKVecs() []containers.Vector {
	schema := seg.table.entry.GetLastestSchema()
	return seg.index.KeyToVectors(schema.GetSingleSortKeyType())
}

func (seg *localSegment) BatchDedup(key containers.Vector) error {
	return seg.index.BatchDedup(seg.table.GetLocalSchema().GetSingleSortKey().Name, key)
}

func (seg *localSegment) GetColumnDataByIds(
	blk *catalog.BlockEntry,
	colIdxes []int,
) (view *containers.BlockView, err error) {
	_, pos := blk.ID.Offsets()
	n := seg.nodes[int(pos)]
	return n.GetColumnDataByIds(colIdxes)
}

func (seg *localSegment) GetColumnDataById(
	ctx context.Context,
	blk *catalog.BlockEntry,
	colIdx int,
) (view *containers.ColumnView, err error) {
	_, pos := blk.ID.Offsets()
	n := seg.nodes[int(pos)]
	return n.GetColumnDataById(ctx, colIdx)
}

func (seg *localSegment) Prefetch(blk *catalog.BlockEntry, idxes []uint16) error {
	_, pos := blk.ID.Offsets()
	n := seg.nodes[int(pos)]
	return n.Prefetch(idxes)
}

func (seg *localSegment) GetBlockRows(blk *catalog.BlockEntry) int {
	_, pos := blk.ID.Offsets()
	n := seg.nodes[int(pos)]
	return int(n.Rows())
}

func (seg *localSegment) GetValue(row uint32, col uint16) (any, bool, error) {
	npos, noffset := seg.GetLocalPhysicalAxis(row)
	n := seg.nodes[npos]
	return n.GetValue(int(col), noffset)
}

// Close free the resource when transaction commits.
func (seg *localSegment) Close() (err error) {
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
