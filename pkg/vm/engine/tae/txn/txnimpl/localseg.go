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

const (
	// Note: Do not edit this id!!!
	LocalObjectStartID uint64 = 1 << 47
)

// var localObjectIdAlloc *common.IdAlloctor

// func init() {
// 	localObjectIdAlloc = common.NewIdAlloctor(LocalObjectStartID)
// }

type localObject struct {
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

func newLocalObject(table *txnTable) *localObject {
	return &localObject{
		entry: catalog.NewStandaloneObject(
			table.entry,
			table.store.txn.GetStartTS()),
		nodes:   make([]InsertNode, 0),
		index:   NewSimpleTableIndex(),
		appends: make([]*appendCtx, 0),
		table:   table,
	}
}

func (obj *localObject) GetLocalPhysicalAxis(row uint32) (int, uint32) {
	var sum uint32
	for i, node := range obj.nodes {
		sum += node.Rows()
		if row <= sum-1 {
			return i, node.Rows() - (sum - (row + 1)) - 1
		}
	}
	panic("Invalid row ")
}

// register a non-appendable insertNode.
func (obj *localObject) registerStats(stats objectio.ObjectStats) {
	if obj.stats == nil {
		obj.stats = make([]objectio.ObjectStats, 0)
	}
	obj.stats = append(obj.stats, stats)
}

func (obj *localObject) isStatsExisted(o objectio.ObjectStats) bool {
	for _, stats := range obj.stats {
		if stats.ObjectName().Equal(o.ObjectName()) {
			return true
		}
	}
	return false
}

// register an appendable insertNode.
func (obj *localObject) registerANode() {
	entry := obj.entry
	meta := catalog.NewStandaloneBlock(
		entry,
		objectio.NewBlockidWithObjectID(&entry.ID, uint16(len(obj.nodes))),
		obj.table.store.txn.GetStartTS())
	entry.AddEntryLocked(meta)
	n := NewANode(
		obj.table,
		meta,
	)
	obj.appendable = n
	obj.nodes = append(obj.nodes, n)
}

// ApplyAppend applies all the anodes into appendable blocks
// and un-reference the appendable blocks which had been referenced when PrepareApply.
func (obj *localObject) ApplyAppend() (err error) {
	var destOff int
	defer func() {
		// Close All unclosed Appends:un-reference the appendable block.
		obj.CloseAppends()
	}()
	for _, ctx := range obj.appends {
		bat, _ := ctx.node.Window(ctx.start, ctx.start+ctx.count)
		defer bat.Close()
		if destOff, err = ctx.driver.ApplyAppend(
			bat,
			obj.table.store.txn); err != nil {
			return
		}
		id := ctx.driver.GetID()
		ctx.node.AddApplyInfo(
			ctx.start,
			ctx.count,
			uint32(destOff),
			ctx.count, id)
	}
	if obj.tableHandle != nil {
		obj.table.entry.GetTableData().ApplyHandle(obj.tableHandle)
	}
	return
}

func (obj *localObject) PrepareApply() (err error) {
	defer func() {
		if err != nil {
			// Close All unclosed Appends: un-reference all the appendable blocks.
			obj.CloseAppends()
		}
	}()
	for _, node := range obj.nodes {
		if err = obj.prepareApplyNode(node); err != nil {
			return
		}
	}
	for _, stats := range obj.stats {
		if err = obj.prepareApplyObjectStats(stats); err != nil {
			return
		}
	}
	return
}

func (obj *localObject) prepareApplyANode(node *anode) error {
	node.Compact()
	tableData := obj.table.entry.GetTableData()
	if obj.tableHandle == nil {
		obj.tableHandle = tableData.GetHandle()
	}
	appended := uint32(0)
	vec := obj.table.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	for appended < node.Rows() {
		appender, err := obj.tableHandle.GetAppender()
		if moerr.IsMoErrCode(err, moerr.ErrAppendableObjectNotFound) {
			objH, err := obj.table.CreateObject(true)
			if err != nil {
				return err
			}
			blk, err := objH.CreateBlock(true)
			objH.Close()
			if err != nil {
				return err
			}
			appender = obj.tableHandle.SetAppender(blk.Fingerprint())
			blk.Close()
		} else if moerr.IsMoErrCode(err, moerr.ErrAppendableBlockNotFound) {
			id := appender.GetID()
			blk, err := obj.table.CreateBlock(id.ObjectID(), true)
			if err != nil {
				return err
			}
			appender = obj.tableHandle.SetAppender(blk.Fingerprint())
			blk.Close()
		}
		if !appender.IsSameColumns(obj.table.GetLocalSchema()) {
			return moerr.NewInternalErrorNoCtx("schema changed, please rollback and retry")
		}
		//PrepareAppend: It is very important that appending a AppendNode into
		// block's MVCCHandle before applying data into block.
		anode, created, toAppend, err := appender.PrepareAppend(
			node.Rows()-appended,
			obj.table.store.txn)
		if err != nil {
			return err
		}
		blockId := appender.GetMeta().(*catalog.BlockEntry).ID
		col := obj.table.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
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
			obj.table.store.IncreateWriteCnt()
			obj.table.txnEntries.Append(anode)
		}
		id := appender.GetID()
		obj.table.store.warChecker.Insert(appender.GetMeta().(*catalog.BlockEntry))
		obj.table.store.txn.GetMemo().AddBlock(obj.table.entry.GetDB().ID,
			id.TableID, &id.BlockID)
		obj.appends = append(obj.appends, ctx)
		// logutil.Debugf("%s: toAppend %d, appended %d, blks=%d",
		// 	id.String(), toAppend, appended, len(obj.appends))
		appended += toAppend
		if appended == node.Rows() {
			break
		}
	}
	node.data.Vecs[obj.table.GetLocalSchema().PhyAddrKey.Idx].Close()
	node.data.Vecs[obj.table.GetLocalSchema().PhyAddrKey.Idx] = vec
	return nil
}

func (obj *localObject) prepareApplyObjectStats(stats objectio.ObjectStats) (err error) {
	sid := stats.ObjectName().ObjectId()
	shouldCreateNewObj := func() bool {
		if obj.nobj == nil {
			return true
		}
		entry := obj.nobj.GetMeta().(*catalog.ObjectEntry)
		return !entry.ID.Eq(*sid)
	}

	if shouldCreateNewObj() {
		obj.nobj, err = obj.table.CreateNonAppendableObject(true, new(objectio.CreateObjOpt).WithId(sid))
		if err != nil {
			return
		}
		obj.nobj.GetMeta().(*catalog.ObjectEntry).SetSorted()
		err = obj.nobj.UpdateStats(stats)
		if err != nil {
			return
		}
	}

	num := stats.ObjectName().Num()
	blkCount := stats.BlkCnt()
	totalRow := stats.Rows()
	blkMaxRows := obj.table.schema.BlockMaxRows
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
		_, err = obj.nobj.CreateNonAppendableBlock(opts)
		if err != nil {
			return
		}
	}
	return
}

func (obj *localObject) prepareApplyNode(node InsertNode) (err error) {
	if !node.IsPersisted() {
		return obj.prepareApplyANode(node.(*anode))
	}
	return nil
}

// CloseAppends un-reference the appendable blocks
func (obj *localObject) CloseAppends() {
	for _, ctx := range obj.appends {
		if ctx.driver != nil {
			ctx.driver.Close()
			ctx.driver = nil
		}
	}
}

// Append appends batch of data into anode.
func (obj *localObject) Append(data *containers.Batch) (err error) {
	if obj.appendable == nil {
		obj.registerANode()
	}
	appended := uint32(0)
	offset := uint32(0)
	length := uint32(data.Length())
	schema := obj.table.GetLocalSchema()
	for {
		h := obj.appendable
		space := h.GetSpace()
		if space == 0 {
			obj.registerANode()
			h = obj.appendable
		}
		appended, err = h.Append(data, offset)
		if err != nil {
			return
		}
		dedupType := obj.table.store.txn.GetDedupType()
		if schema.HasPK() && dedupType == txnif.FullDedup {
			if err = obj.index.BatchInsert(
				data.Attrs[schema.GetSingleSortKeyIdx()],
				data.Vecs[schema.GetSingleSortKeyIdx()],
				int(offset),
				int(appended),
				obj.rows,
				false); err != nil {
				break
			}
		}
		offset += appended
		obj.rows += appended
		if offset >= length {
			break
		}
	}
	return
}

// AddBlksWithMetaLoc transfers blocks with meta location into non-appendable nodes
func (obj *localObject) AddBlksWithMetaLoc(
	pkVecs []containers.Vector,
	stats objectio.ObjectStats,
) (err error) {
	obj.registerStats(stats)
	for i := range pkVecs {
		dedupType := obj.table.store.txn.GetDedupType()
		//insert primary keys into obj.index
		if pkVecs != nil && dedupType == txnif.FullDedup {
			if err = obj.index.BatchInsert(
				obj.table.GetLocalSchema().GetSingleSortKey().Name,
				pkVecs[i],
				0,
				pkVecs[i].Length(),
				obj.rows,
				false,
			); err != nil {
				return
			}
			obj.rows += uint32(pkVecs[i].Length())
		}
	}
	return nil
}

func (obj *localObject) DeleteFromIndex(from, to uint32, node InsertNode) (err error) {
	schema := obj.table.GetLocalSchema()
	for i := from; i <= to; i++ {
		v, _, err := node.GetValue(schema.GetSingleSortKeyIdx(), i)
		if err != nil {
			return err
		}
		if err = obj.index.Delete(v); err != nil {
			return err
		}
	}
	return
}

// RangeDelete delete rows : [start, end]
func (obj *localObject) RangeDelete(start, end uint32) error {
	first, firstOffset := obj.GetLocalPhysicalAxis(start)
	last, lastOffset := obj.GetLocalPhysicalAxis(end)
	var err error
	if last == first {
		node := obj.nodes[first]
		err = node.RangeDelete(firstOffset, lastOffset)
		if err != nil {
			return err
		}
		if !obj.table.GetLocalSchema().HasPK() {
			// If no pk defined
			return err
		}
		err = obj.DeleteFromIndex(firstOffset, lastOffset, node)
		return err
	}

	node := obj.nodes[first]
	if err = node.RangeDelete(firstOffset, node.Rows()-1); err != nil {

		return err
	}
	if err = obj.DeleteFromIndex(firstOffset, node.Rows()-1, node); err != nil {
		return err
	}
	node = obj.nodes[last]
	if err = node.RangeDelete(0, lastOffset); err != nil {
		return err
	}
	if err = obj.DeleteFromIndex(0, lastOffset, node); err != nil {
		return err
	}
	if last > first+1 {
		for i := first + 1; i < last; i++ {
			node = obj.nodes[i]
			if err = node.RangeDelete(0, node.Rows()-1); err != nil {
				break
			}
			if err = obj.DeleteFromIndex(0, node.Rows()-1, node); err != nil {
				break
			}
		}
	}
	return err
}

// CollectCmd collect txnCmd for anode whose data resides in memory.
func (obj *localObject) CollectCmd(cmdMgr *commandManager) (err error) {
	for _, node := range obj.nodes {
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

func (obj *localObject) DeletesToString() string {
	var s string
	for i, n := range obj.nodes {
		s = fmt.Sprintf("%s\t<INode-%d>: %s\n", s, i, n.PrintDeletes())
	}
	return s
}

func (obj *localObject) IsDeleted(row uint32) bool {
	npos, noffset := obj.GetLocalPhysicalAxis(row)
	n := obj.nodes[npos]
	return n.IsRowDeleted(noffset)
}

func (obj *localObject) Rows() (n uint32) {
	for _, node := range obj.nodes {
		n += node.Rows()
	}
	return
}

func (obj *localObject) GetByFilter(filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	if !obj.table.GetLocalSchema().HasPK() {
		id = obj.table.entry.AsCommonID()
		rid := filter.Val.(types.Rowid)
		id.BlockID, offset = rid.Decode()
		return
	}
	id = obj.entry.AsCommonID()
	if v, ok := filter.Val.([]byte); ok {
		offset, err = obj.index.Search(string(v))
	} else {
		offset, err = obj.index.Search(filter.Val)
	}
	if err != nil {
		return
	}
	return
}

func (obj *localObject) GetPKColumn() containers.Vector {
	schema := obj.table.entry.GetLastestSchema()
	return obj.index.KeyToVector(schema.GetSingleSortKeyType())
}

func (obj *localObject) GetPKVecs() []containers.Vector {
	schema := obj.table.entry.GetLastestSchema()
	return obj.index.KeyToVectors(schema.GetSingleSortKeyType())
}

func (obj *localObject) BatchDedup(key containers.Vector) error {
	return obj.index.BatchDedup(obj.table.GetLocalSchema().GetSingleSortKey().Name, key)
}

func (obj *localObject) GetColumnDataByIds(
	blk *catalog.BlockEntry,
	colIdxes []int,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {
	_, pos := blk.ID.Offsets()
	n := obj.nodes[int(pos)]
	return n.GetColumnDataByIds(colIdxes, mp)
}

func (obj *localObject) GetColumnDataById(
	ctx context.Context,
	blk *catalog.BlockEntry,
	colIdx int,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	_, pos := blk.ID.Offsets()
	n := obj.nodes[int(pos)]
	return n.GetColumnDataById(ctx, colIdx, mp)
}

func (obj *localObject) Prefetch(blk *catalog.BlockEntry, idxes []uint16) error {
	_, pos := blk.ID.Offsets()
	n := obj.nodes[int(pos)]
	return n.Prefetch(idxes)
}

func (obj *localObject) GetBlockRows(blk *catalog.BlockEntry) int {
	_, pos := blk.ID.Offsets()
	n := obj.nodes[int(pos)]
	return int(n.Rows())
}

func (obj *localObject) GetValue(row uint32, col uint16) (any, bool, error) {
	npos, noffset := obj.GetLocalPhysicalAxis(row)
	n := obj.nodes[npos]
	return n.GetValue(int(col), noffset)
}

// Close free the resource when transaction commits.
func (obj *localObject) Close() (err error) {
	for _, node := range obj.nodes {
		if err = node.Close(); err != nil {
			return
		}
	}
	obj.index.Close()
	obj.index = nil
	obj.nodes = nil
	obj.appendable = nil
	return
}
