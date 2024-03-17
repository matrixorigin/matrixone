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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

type tableSpace struct {
	entry *catalog.ObjectEntry

	appendable InsertNode
	//index for primary key
	index TableIndex
	//nodes contains anode and node.
	nodes       []InsertNode
	table       *txnTable
	rows        uint32
	appends     []*appendCtx
	tableHandle data.TableHandle
	nobj        handle.Object

	stats []objectio.ObjectStats
}

func newTableSpace(table *txnTable) *tableSpace {
	return &tableSpace{
		entry: catalog.NewStandaloneObject(
			table.entry,
			table.store.txn.GetStartTS()),
		nodes:   make([]InsertNode, 0),
		index:   NewSimpleTableIndex(),
		appends: make([]*appendCtx, 0),
		table:   table,
	}
}

func (space *tableSpace) GetLocalPhysicalAxis(row uint32) (int, uint32) {
	var sum uint32
	for i, node := range space.nodes {
		sum += node.Rows()
		if row <= sum-1 {
			return i, node.Rows() - (sum - (row + 1)) - 1
		}
	}
	panic("Invalid row ")
}

// register a non-appendable insertNode.
func (space *tableSpace) registerStats(stats objectio.ObjectStats) {
	if space.stats == nil {
		space.stats = make([]objectio.ObjectStats, 0)
	}
	space.stats = append(space.stats, stats)
}

func (space *tableSpace) isStatsExisted(o objectio.ObjectStats) bool {
	for _, stats := range space.stats {
		if stats.ObjectName().Equal(o.ObjectName()) {
			return true
		}
	}
	return false
}

// register an appendable insertNode.
func (space *tableSpace) registerANode() {
	entry := space.entry
	meta := catalog.NewStandaloneBlock(
		entry,
		objectio.NewBlockidWithObjectID(&entry.ID, uint16(len(space.nodes))),
		space.table.store.txn.GetStartTS())
	entry.AddEntryLocked(meta)
	n := NewANode(
		space.table,
		meta,
	)
	space.appendable = n
	space.nodes = append(space.nodes, n)
}

// ApplyAppend applies all the anodes into appendable blocks
// and un-reference the appendable blocks which had been referenced when PrepareApply.
func (space *tableSpace) ApplyAppend() (err error) {
	var destOff int
	defer func() {
		// Close All unclosed Appends:un-reference the appendable block.
		space.CloseAppends()
	}()
	for _, ctx := range space.appends {
		bat, _ := ctx.node.Window(ctx.start, ctx.start+ctx.count)
		defer bat.Close()
		if destOff, err = ctx.driver.ApplyAppend(
			bat,
			space.table.store.txn); err != nil {
			return
		}
		id := ctx.driver.GetID()
		ctx.node.AddApplyInfo(
			ctx.start,
			ctx.count,
			uint32(destOff),
			ctx.count, id)
	}
	if space.tableHandle != nil {
		space.table.entry.GetTableData().ApplyHandle(space.tableHandle)
	}
	return
}

func (space *tableSpace) PrepareApply() (err error) {
	defer func() {
		if err != nil {
			// Close All unclosed Appends: un-reference all the appendable blocks.
			space.CloseAppends()
		}
	}()
	for _, node := range space.nodes {
		if err = space.prepareApplyNode(node); err != nil {
			return
		}
	}
	for _, stats := range space.stats {
		if err = space.prepareApplyObjectStats(stats); err != nil {
			return
		}
	}
	return
}

func (space *tableSpace) prepareApplyANode(node *anode) error {
	node.Compact()
	tableData := space.table.entry.GetTableData()
	if space.tableHandle == nil {
		space.tableHandle = tableData.GetHandle()
	}
	appended := uint32(0)
	vec := space.table.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	for appended < node.Rows() {
		appender, err := space.tableHandle.GetAppender()
		if moerr.IsMoErrCode(err, moerr.ErrAppendableObjectNotFound) {
			objH, err := space.table.CreateObject(true)
			if err != nil {
				return err
			}
			blk, err := objH.CreateBlock(true)
			objH.Close()
			if err != nil {
				return err
			}
			appender = space.tableHandle.SetAppender(blk.Fingerprint())
			blk.Close()
		} else if moerr.IsMoErrCode(err, moerr.ErrAppendableBlockNotFound) {
			id := appender.GetID()
			blk, err := space.table.CreateBlock(id.ObjectID(), true)
			if err != nil {
				return err
			}
			appender = space.tableHandle.SetAppender(blk.Fingerprint())
			blk.Close()
		}
		if !appender.IsSameColumns(space.table.GetLocalSchema()) {
			return moerr.NewInternalErrorNoCtx("schema changed, please rollback and retry")
		}

		// see more notes in flushtabletail.go
		/// ----------- Choose a ablock ---------
		appender.LockFreeze()
		// no one can touch freeze for now, check it
		if appender.CheckFreeze() {
			// freezed, try to find another ablock
			appender.UnlockFreeze()
			continue
		}

		// hold freezelock to attach AppendNode
		//PrepareAppend: It is very important that appending a AppendNode into
		// block's MVCCHandle before applying data into block.
		anode, created, toAppend, err := appender.PrepareAppend(
			node.Rows()-appended,
			space.table.store.txn)
		if err != nil {
			appender.UnlockFreeze()
			return err
		}
		appender.UnlockFreeze()
		/// ------- Attach AppendNode Successfully -----

		blockId := appender.GetMeta().(*catalog.BlockEntry).ID
		col := space.table.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
		defer col.Close()
		if err = objectio.ConstructRowidColumnTo(
			col.GetDownstreamVector(),
			&blockId,
			anode.GetMaxRow()-toAppend,
			toAppend,
			col.GetAllocator(),
		); err != nil {
			return err
		}
		if err = vec.ExtendVec(col.GetDownstreamVector()); err != nil {
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
			space.table.store.IncreateWriteCnt()
			space.table.txnEntries.Append(anode)
		}
		id := appender.GetID()
		space.table.store.warChecker.Insert(appender.GetMeta().(*catalog.BlockEntry))
		space.table.store.txn.GetMemo().AddBlock(space.table.entry.GetDB().ID,
			id.TableID, &id.BlockID)
		space.appends = append(space.appends, ctx)
		// logutil.Debugf("%s: toAppend %d, appended %d, blks=%d",
		// 	id.String(), toAppend, appended, len(space.appends))
		appended += toAppend
		if appended == node.Rows() {
			break
		}
	}
	node.data.Vecs[space.table.GetLocalSchema().PhyAddrKey.Idx].Close()
	node.data.Vecs[space.table.GetLocalSchema().PhyAddrKey.Idx] = vec
	return nil
}

func (space *tableSpace) prepareApplyObjectStats(stats objectio.ObjectStats) (err error) {
	sid := stats.ObjectName().ObjectId()
	shouldCreateNewObj := func() bool {
		if space.nobj == nil {
			return true
		}
		entry := space.nobj.GetMeta().(*catalog.ObjectEntry)
		return !entry.ID.Eq(*sid)
	}

	if shouldCreateNewObj() {
		space.nobj, err = space.table.CreateNonAppendableObject(true, new(objectio.CreateObjOpt).WithId(sid))
		if err != nil {
			return
		}
		space.nobj.GetMeta().(*catalog.ObjectEntry).SetSorted()
		err = space.nobj.UpdateStats(stats)
		if err != nil {
			return
		}
	}

	num := stats.ObjectName().Num()
	blkCount := stats.BlkCnt()
	totalRow := stats.Rows()
	blkMaxRows := space.table.schema.BlockMaxRows
	for i := uint16(0); i < uint16(blkCount); i++ {
		var blkRow uint32
		if totalRow > blkMaxRows {
			blkRow = blkMaxRows
		} else {
			blkRow = totalRow
		}
		totalRow -= blkRow
		metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)

		opts := new(objectio.CreateBlockOpt).
			WithMetaloc(metaloc).
			WithFileIdx(num).
			WithBlkIdx(i)
		_, err = space.nobj.CreateNonAppendableBlock(opts)
		if err != nil {
			return
		}
	}
	return
}

func (space *tableSpace) prepareApplyNode(node InsertNode) (err error) {
	if !node.IsPersisted() {
		return space.prepareApplyANode(node.(*anode))
	}
	return nil
}

// CloseAppends un-reference the appendable blocks
func (space *tableSpace) CloseAppends() {
	for _, ctx := range space.appends {
		if ctx.driver != nil {
			ctx.driver.Close()
			ctx.driver = nil
		}
	}
}

// Append appends batch of data into anode.
func (space *tableSpace) Append(data *containers.Batch) (err error) {
	if space.appendable == nil {
		space.registerANode()
	}
	appended := uint32(0)
	offset := uint32(0)
	length := uint32(data.Length())
	schema := space.table.GetLocalSchema()
	for {
		h := space.appendable
		if h.GetSpace() == 0 {
			space.registerANode()
			h = space.appendable
		}
		appended, err = h.Append(data, offset)
		if err != nil {
			return
		}
		dedupType := space.table.store.txn.GetDedupType()
		if schema.HasPK() && dedupType == txnif.FullDedup {
			if err = space.index.BatchInsert(
				data.Attrs[schema.GetSingleSortKeyIdx()],
				data.Vecs[schema.GetSingleSortKeyIdx()],
				int(offset),
				int(appended),
				space.rows,
				false); err != nil {
				break
			}
		}
		offset += appended
		space.rows += appended
		if offset >= length {
			break
		}
	}
	return
}

// AddBlksWithMetaLoc transfers blocks with meta location into non-appendable nodes
func (space *tableSpace) AddBlksWithMetaLoc(
	pkVecs []containers.Vector,
	stats objectio.ObjectStats,
) (err error) {
	space.registerStats(stats)
	for i := range pkVecs {
		dedupType := space.table.store.txn.GetDedupType()
		//insert primary keys into space.index
		if pkVecs != nil && dedupType == txnif.FullDedup {
			if err = space.index.BatchInsert(
				space.table.GetLocalSchema().GetSingleSortKey().Name,
				pkVecs[i],
				0,
				pkVecs[i].Length(),
				space.rows,
				false,
			); err != nil {
				return
			}
			space.rows += uint32(pkVecs[i].Length())
		}
	}
	return nil
}

func (space *tableSpace) DeleteFromIndex(from, to uint32, node InsertNode) (err error) {
	schema := space.table.GetLocalSchema()
	for i := from; i <= to; i++ {
		v, _, err := node.GetValue(schema.GetSingleSortKeyIdx(), i)
		if err != nil {
			return err
		}
		if err = space.index.Delete(v); err != nil {
			return err
		}
	}
	return
}

// RangeDelete delete rows : [start, end]
func (space *tableSpace) RangeDelete(start, end uint32) error {
	first, firstOffset := space.GetLocalPhysicalAxis(start)
	last, lastOffset := space.GetLocalPhysicalAxis(end)
	var err error
	if last == first {
		node := space.nodes[first]
		err = node.RangeDelete(firstOffset, lastOffset)
		if err != nil {
			return err
		}
		if !space.table.GetLocalSchema().HasPK() {
			// If no pk defined
			return err
		}
		err = space.DeleteFromIndex(firstOffset, lastOffset, node)
		return err
	}

	node := space.nodes[first]
	if err = node.RangeDelete(firstOffset, node.Rows()-1); err != nil {

		return err
	}
	if err = space.DeleteFromIndex(firstOffset, node.Rows()-1, node); err != nil {
		return err
	}
	node = space.nodes[last]
	if err = node.RangeDelete(0, lastOffset); err != nil {
		return err
	}
	if err = space.DeleteFromIndex(0, lastOffset, node); err != nil {
		return err
	}
	if last > first+1 {
		for i := first + 1; i < last; i++ {
			node = space.nodes[i]
			if err = node.RangeDelete(0, node.Rows()-1); err != nil {
				break
			}
			if err = space.DeleteFromIndex(0, node.Rows()-1, node); err != nil {
				break
			}
		}
	}
	return err
}

// CollectCmd collect txnCmd for anode whose data resides in memory.
func (space *tableSpace) CollectCmd(cmdMgr *commandManager) (err error) {
	for _, node := range space.nodes {
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

func (space *tableSpace) DeletesToString() string {
	var s string
	for i, n := range space.nodes {
		s = fmt.Sprintf("%s\t<INode-%d>: %s\n", s, i, n.PrintDeletes())
	}
	return s
}

func (space *tableSpace) IsDeleted(row uint32) bool {
	npos, noffset := space.GetLocalPhysicalAxis(row)
	n := space.nodes[npos]
	return n.IsRowDeleted(noffset)
}

func (space *tableSpace) Rows() (n uint32) {
	for _, node := range space.nodes {
		n += node.Rows()
	}
	return
}

func (space *tableSpace) GetByFilter(filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	if !space.table.GetLocalSchema().HasPK() {
		id = space.table.entry.AsCommonID()
		rid := filter.Val.(types.Rowid)
		id.BlockID, offset = rid.Decode()
		return
	}
	id = space.entry.AsCommonID()
	if v, ok := filter.Val.([]byte); ok {
		offset, err = space.index.Search(string(v))
	} else {
		offset, err = space.index.Search(filter.Val)
	}
	if err != nil {
		return
	}
	return
}

func (space *tableSpace) GetPKColumn() containers.Vector {
	schema := space.table.entry.GetLastestSchema()
	return space.index.KeyToVector(schema.GetSingleSortKeyType())
}

func (space *tableSpace) GetPKVecs() []containers.Vector {
	schema := space.table.entry.GetLastestSchema()
	return space.index.KeyToVectors(schema.GetSingleSortKeyType())
}

func (space *tableSpace) BatchDedup(key containers.Vector) error {
	return space.index.BatchDedup(space.table.GetLocalSchema().GetSingleSortKey().Name, key)
}

func (space *tableSpace) GetColumnDataByIds(
	blk *catalog.BlockEntry,
	colIdxes []int,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {
	_, pos := blk.ID.Offsets()
	n := space.nodes[int(pos)]
	return n.GetColumnDataByIds(colIdxes, mp)
}

func (space *tableSpace) GetColumnDataById(
	ctx context.Context,
	blk *catalog.BlockEntry,
	colIdx int,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	_, pos := blk.ID.Offsets()
	n := space.nodes[int(pos)]
	return n.GetColumnDataById(ctx, colIdx, mp)
}

func (space *tableSpace) Prefetch(blk *catalog.BlockEntry, idxes []uint16) error {
	_, pos := blk.ID.Offsets()
	n := space.nodes[int(pos)]
	return n.Prefetch(idxes)
}

func (space *tableSpace) GetBlockRows(blk *catalog.BlockEntry) int {
	_, pos := blk.ID.Offsets()
	n := space.nodes[int(pos)]
	return int(n.Rows())
}

func (space *tableSpace) GetValue(row uint32, col uint16) (any, bool, error) {
	npos, noffset := space.GetLocalPhysicalAxis(row)
	n := space.nodes[npos]
	return n.GetValue(int(col), noffset)
}

// Close free the resource when transaction commits.
func (space *tableSpace) Close() (err error) {
	for _, node := range space.nodes {
		if err = node.Close(); err != nil {
			return
		}
	}
	space.index.Close()
	space.index = nil
	space.nodes = nil
	space.appendable = nil
	return
}
