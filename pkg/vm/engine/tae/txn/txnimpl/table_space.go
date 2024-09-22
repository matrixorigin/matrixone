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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

type tableSpace struct {
	entry       *catalog.ObjectEntry
	isTombstone bool

	appendable *anode
	//index for primary key
	index TableIndex
	//nodes contains anode and node.
	node        *anode
	table       *txnTable
	rows        uint32
	appends     []*appendCtx
	tableHandle data.TableHandle
	nobj        handle.Object

	stats []objectio.ObjectStats
	// for tombstone table space
	objs []*objectio.ObjectId
}

func newTableSpace(table *txnTable, isTombstone bool) *tableSpace {
	space := &tableSpace{
		index:       NewSimpleTableIndex(),
		appends:     make([]*appendCtx, 0),
		table:       table,
		isTombstone: isTombstone,
	}
	space.entry = catalog.NewStandaloneObject(
		table.entry,
		table.store.txn.GetStartTS(),
		isTombstone)
	return space
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
	n := NewANode(
		space.table,
		space.entry,
		space.isTombstone,
	)
	space.appendable = n
	space.node = n
}

func (tbl *txnTable) NeedRollback() bool {
	return tbl.createEntry != nil && tbl.dropEntry != nil
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
		space.table.entry.GetTableData().ApplyHandle(space.tableHandle, space.isTombstone)
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
	if space.node != nil {
		if err = space.prepareApplyNode(space.node); err != nil {
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

func (space *tableSpace) prepareApplyANode(node *anode, startOffset uint32) error {
	node.Compact()
	tableData := space.table.entry.GetTableData()
	if space.tableHandle == nil {
		space.tableHandle = tableData.GetHandle(space.isTombstone)
	}
	appended := startOffset
	var vec containers.Vector
	if startOffset == 0 {
		vec = space.table.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	} else {
		vec = node.data.Vecs[space.table.GetLocalSchema(space.isTombstone).PhyAddrKey.Idx]
	}
	for appended < node.Rows() {
		appender, err := space.tableHandle.GetAppender()
		if moerr.IsMoErrCode(err, moerr.ErrAppendableObjectNotFound) {
			objH, err := space.table.CreateObject(space.isTombstone)
			if err != nil {
				return err
			}
			appender = space.tableHandle.SetAppender(objH.Fingerprint())
			objH.Close()
		}
		if !appender.IsSameColumns(space.table.GetLocalSchema(space.isTombstone)) {
			return moerr.NewInternalErrorNoCtx("schema changed, please rollback and retry")
		}

		// see more notes in flushtabletail.go
		/// ----------- Choose a ablock ---------
		appender.LockFreeze()
		// no one can touch freeze for now, check it
		if appender.CheckFreeze() {
			// freezed, try to find another ablock
			appender.UnlockFreeze()
			// Unref the appender, otherwise it can't be PrepareCompact(ed) successfully
			appender.Close()
			continue
		}

		// hold freezelock to attach AppendNode
		//PrepareAppend: It is very important that appending a AppendNode into
		// block's MVCCHandle before applying data into block.
		anode, created, toAppend, err := appender.PrepareAppend(
			node.isMergeCompact,
			node.Rows()-appended,
			space.table.store.txn)
		if err != nil {
			appender.UnlockFreeze()
			return err
		}
		appender.UnlockFreeze()
		/// ------- Attach AppendNode Successfully -----

		objID := appender.GetMeta().(*catalog.ObjectEntry).ID()
		col := space.table.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
		defer col.Close()
		blkID := objectio.NewBlockidWithObjectID(objID, 0)
		if err = objectio.ConstructRowidColumnTo(
			col.GetDownstreamVector(),
			blkID,
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
		space.table.store.warChecker.Insert(appender.GetMeta().(*catalog.ObjectEntry))
		space.table.store.txn.GetMemo().AddObject(space.table.entry.GetDB().ID,
			id.TableID, id.ObjectID(), space.isTombstone)
		space.appends = append(space.appends, ctx)
		// logutil.Debugf("%s: toAppend %d, appended %d, blks=%d",
		// 	id.String(), toAppend, appended, len(space.appends))
		appended += toAppend
		if appended == node.Rows() {
			break
		}
	}
	if startOffset == 0 {
		node.data.Vecs[space.table.GetLocalSchema(space.isTombstone).PhyAddrKey.Idx].Close()
		node.data.Vecs[space.table.GetLocalSchema(space.isTombstone).PhyAddrKey.Idx] = vec
	}
	return nil
}

func (space *tableSpace) prepareApplyObjectStats(stats objectio.ObjectStats) (err error) {
	sid := stats.ObjectName().ObjectId()
	shouldCreateNewObj := func() bool {
		if space.nobj == nil {
			return true
		}
		entry := space.nobj.GetMeta().(*catalog.ObjectEntry)
		return !entry.ID().EQ(sid)
	}

	if shouldCreateNewObj() {
		space.nobj, err = space.table.CreateNonAppendableObject(
			&objectio.CreateObjOpt{Stats: &stats, IsTombstone: space.isTombstone})
		if err != nil {
			return
		}
	}

	return
}

func (space *tableSpace) prepareApplyNode(node *anode) (err error) {
	return space.prepareApplyANode(node, 0)
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
func (space *tableSpace) Append(data *containers.Batch) (dur float64, err error) {
	if space.appendable == nil {
		space.registerANode()
	}
	appended := uint32(0)
	offset := uint32(0)
	length := uint32(data.Length())
	schema := space.table.GetLocalSchema(space.isTombstone)
	for {
		h := space.appendable
		appended, err = h.Append(data, offset)
		if err != nil {
			return
		}
		dedupType := space.table.store.txn.GetDedupType()
		if schema.HasPK() && !dedupType.SkipWorkSpace() {
			now := time.Now()
			if err = space.index.BatchInsert(
				data.Attrs[schema.GetSingleSortKeyIdx()],
				data.Vecs[schema.GetSingleSortKeyIdx()],
				int(offset),
				int(appended),
				space.rows,
				false); err != nil {
				break
			}
			dur += time.Since(now).Seconds()
		}
		offset += appended
		space.rows += appended
		if offset >= length {
			break
		}
	}
	return
}

// AddObjsWithMetaLoc transfers blocks with meta location into non-appendable nodes
func (space *tableSpace) AddObjsWithMetaLoc(
	pkVecs []containers.Vector,
	stats objectio.ObjectStats,
) (err error) {
	for i := range pkVecs {
		dedupType := space.table.store.txn.GetDedupType()
		//insert primary keys into space.index
		if pkVecs != nil && !dedupType.SkipWorkSpace() {
			if err = space.index.BatchInsert(
				space.table.GetLocalSchema(space.isTombstone).GetSingleSortKey().Name,
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
	space.registerStats(stats)
	return nil
}

func (space *tableSpace) DeleteFromIndex(from, to uint32, node *anode) (err error) {
	schema := space.table.GetLocalSchema(space.isTombstone)
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
	err := space.node.RangeDelete(start, end)
	if err != nil {
		return err
	}
	if !space.table.GetLocalSchema(space.isTombstone).HasPK() {
		// If no pk defined
		return err
	}
	err = space.DeleteFromIndex(start, start, space.node)
	return err
}

// CollectCmd collect txnCmd for anode whose data resides in memory.
func (space *tableSpace) CollectCmd(cmdMgr *commandManager) (err error) {
	if space.node == nil {
		return
	}
	csn := uint32(0xffff) // Special cmd
	cmd, err := space.node.MakeCommand(csn)
	if err != nil {
		panic(err)
	}
	if cmd != nil {
		cmdMgr.AddInternalCmd(cmd)
	}
	return
}

func (space *tableSpace) DeletesToString() string {
	var s string
	s = fmt.Sprintf("%s\t<INode>: %s\n", s, space.node.PrintDeletes())
	return s
}

func (space *tableSpace) IsDeleted(row uint32) bool {
	return space.node.IsRowDeleted(row)
}

func (space *tableSpace) Rows() (n uint32) {
	return space.node.Rows()
}

func (space *tableSpace) GetByFilter(filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	if !space.table.GetLocalSchema(space.isTombstone).HasPK() {
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

func (space *tableSpace) BatchDedup(key containers.Vector) error {
	return space.index.BatchDedup(space.table.GetLocalSchema(space.isTombstone).GetSingleSortKey().Name, key)
}

func (space *tableSpace) Scan(
	ctx context.Context, bat **containers.Batch, colIdxes []int, mp *mpool.MPool,
) {
	n := space.node
	n.Scan(ctx, bat, colIdxes, mp)
}

func (space *tableSpace) HybridScan(
	ctx context.Context, bat **containers.Batch, colIdxes []int, mp *mpool.MPool,
) {
	space.node.Scan(ctx, bat, colIdxes, mp)
	if (*bat).Deletes == nil {
		(*bat).Deletes = &nulls.Nulls{}
	}
	(*bat).Deletes.Or(space.node.data.Deletes)
}

func (space *tableSpace) Prefetch(obj *catalog.ObjectEntry) error {
	n := space.node
	return n.Prefetch()
}

func (space *tableSpace) GetValue(row uint32, col uint16) (any, bool, error) {
	return space.node.GetValue(int(col), row)
}

// Close free the resource when transaction commits.
func (space *tableSpace) Close() (err error) {
	if space.node != nil {
		space.node.Close()
	}
	space.index.Close()
	space.index = nil
	space.node = nil
	space.appendable = nil
	return
}
