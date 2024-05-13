// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

type BlockT[T common.IRef] interface {
	common.IRef
	Pin() *common.PinnedItem[T]
	GetID() *common.ID
}

func DefaultTOmbstoneFactory(meta *catalog.ObjectEntry) data.Tombstone {
	return updates.NewObjectMVCCHandle(meta)
}

type baseObject struct {
	common.RefHelper
	*sync.RWMutex
	rt         *dbutils.Runtime
	meta       *catalog.ObjectEntry
	appendMVCC *updates.AppendMVCCHandle
	impl       data.Object

	node atomic.Pointer[Node]
}

func newBaseObject(
	impl data.Object,
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) *baseObject {
	blk := &baseObject{
		impl:       impl,
		rt:         rt,
		meta:       meta,
		appendMVCC: updates.NewAppendMVCCHandle(meta),
	}
	blk.appendMVCC.SetAppendListener(blk.OnApplyAppend)
	blk.RWMutex = meta.RWMutex
	return blk
}

func (blk *baseObject) OnApplyAppend(n txnif.AppendNode) (err error) {
	blk.meta.GetTable().AddRows(
		uint64(n.GetMaxRow() - n.GetStartRow()),
	)
	return
}
func (blk *baseObject) Close() {
	// TODO
}

func (blk *baseObject) GetRuntime() *dbutils.Runtime {
	return blk.rt
}

func (blk *baseObject) PinNode() *Node {
	n := blk.node.Load()
	// if ref fails, reload.
	// Note: avoid bad case where releasing happens before Ref()
	for ; !n.RefIfHasRef(); n = blk.node.Load() {
	}
	return n
}
func (blk *baseObject) tryGetMVCC() *updates.ObjectMVCCHandle {
	tombstone := blk.meta.GetTable().TryGetTombstone(blk.meta.ID)
	if tombstone == nil {
		return nil
	}
	return tombstone.(*updates.ObjectMVCCHandle)
}
func (blk *baseObject) getOrCreateMVCC() *updates.ObjectMVCCHandle {
	return blk.meta.GetTable().GetOrCreateTombstone(blk.meta, DefaultTOmbstoneFactory).(*updates.ObjectMVCCHandle)
}

func (blk *baseObject) GCInMemeoryDeletesByTSForTest(ts types.TS) {
	blk.Lock()
	defer blk.Unlock()
	mvcc := blk.tryGetMVCC()
	if mvcc == nil {
		return
	}
	mvcc.UpgradeDeleteChainByTSLocked(ts)
}

func (blk *baseObject) UpgradeAllDeleteChain() {
	blk.Lock()
	defer blk.Unlock()
	mvcc := blk.tryGetMVCC()
	if mvcc == nil {
		return
	}
	mvcc.UpgradeAllDeleteChain()
}

func (blk *baseObject) Rows() (int, error) {
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		blk.RLock()
		defer blk.RUnlock()
		rows, err := node.Rows()
		return int(rows), err
	} else {
		rows, err := node.Rows()
		return int(rows), err
	}
}
func (blk *baseObject) Foreach(
	ctx context.Context,
	readSchema any,
	blkID uint16,
	colIdx int,
	op func(v any, isNull bool, row int) error,
	sels []uint32,
	mp *mpool.MPool,
) error {
	node := blk.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	if !node.IsPersisted() {
		blk.RLock()
		defer blk.RUnlock()
		return node.MustMNode().Foreach(schema, blkID, colIdx, op, sels, mp)
	} else {
		return node.MustPNode().Foreach(ctx, schema, blkID, colIdx, op, sels, mp)
	}
}

func (blk *baseObject) TryUpgrade() (err error) {
	node := blk.node.Load()
	if node.IsPersisted() {
		return
	}
	pnode := newPersistedNode(blk)
	nnode := NewNode(pnode)
	nnode.Ref()

	if !blk.node.CompareAndSwap(node, nnode) {
		nnode.Unref()
	} else {
		node.Unref()
	}
	return
}

func (blk *baseObject) GetMeta() any { return blk.meta }
func (blk *baseObject) CheckFlushTaskRetry(startts types.TS) bool {
	if !blk.meta.IsAppendable() {
		panic("not support")
	}
	if blk.meta.HasDropCommitted() {
		panic("not support")
	}
	blk.RLock()
	defer blk.RUnlock()
	x := blk.appendMVCC.GetLatestAppendPrepareTSLocked()
	return x.Greater(&startts)
}
func (blk *baseObject) GetFs() *objectio.ObjectFS { return blk.rt.Fs }
func (blk *baseObject) GetID() *common.ID         { return blk.meta.AsCommonID() }

func (blk *baseObject) fillInMemoryDeletesLocked(
	txn txnif.TxnReader,
	blkID uint16,
	view *containers.BaseView,
	rwlocker *sync.RWMutex,
) (err error) {
	mvcc := blk.tryGetMVCC()
	if mvcc == nil {
		return
	}
	deleteHandle := mvcc.TryGetDeleteChain(blkID)
	if deleteHandle == nil {
		return
	}
	chain := deleteHandle.GetDeleteChain()
	deletes, err := chain.CollectDeletesLocked(txn, rwlocker)
	if err != nil || deletes.IsEmpty() {
		return
	}
	if view.DeleteMask == nil {
		view.DeleteMask = deletes
	} else {
		view.DeleteMask.Or(deletes)
	}
	return
}

func (blk *baseObject) buildMetalocation(bid uint16) (objectio.Location, error) {
	if !blk.meta.ObjectPersisted() {
		return nil, nil
	}
	stats, err := blk.meta.MustGetObjectStats()
	if err != nil {
		return nil, err
	}
	blkMaxRows := blk.meta.GetSchema().BlockMaxRows
	blkRow := blkMaxRows
	if bid == uint16(blk.meta.BlockCnt())-1 {
		blkRow = stats.Rows() - uint32(bid)*blkMaxRows
	}
	metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, bid)
	return metaloc, nil
}

func (blk *baseObject) LoadPersistedCommitTS(bid uint16) (vec containers.Vector, err error) {
	if !blk.meta.IsAppendable() {
		return
	}
	location, err := blk.buildMetalocation(bid)
	if err != nil {
		return
	}
	if location.IsEmpty() {
		return
	}
	//Extend lifetime of vectors is without the function.
	//need to copy. closeFunc will be nil.
	vectors, _, err := blockio.LoadColumns2(
		context.Background(),
		[]uint16{objectio.SEQNUM_COMMITTS},
		nil,
		blk.rt.Fs.Service,
		location,
		fileservice.Policy(0),
		true,
		blk.rt.VectorPool.Transient,
	)
	if err != nil {
		return
	}
	if vectors[0].GetType().Oid != types.T_TS {
		panic(fmt.Sprintf("%s: bad commits layout", blk.meta.ID.String()))
	}
	vec = vectors[0]
	return
}

func (blk *baseObject) LoadPersistedColumnData(
	ctx context.Context, schema *catalog.Schema, colIdx int, mp *mpool.MPool, blkID uint16,
) (vec containers.Vector, err error) {
	def := schema.ColDefs[colIdx]
	location, err := blk.buildMetalocation(blkID)
	if err != nil {
		return nil, err
	}
	id := blk.meta.AsCommonID()
	id.SetBlockOffset(blkID)
	return LoadPersistedColumnData(
		ctx,
		blk.rt,
		id,
		def,
		location,
		mp,
	)
}

func (blk *baseObject) loadLatestPersistedDeletes(
	ctx context.Context,
	blkID uint16,
	txn txnif.TxnReader,
	mp *mpool.MPool,
) (bat *containers.Batch, persistedByCN bool, deltalocCommitTS types.TS, visible bool, release func(), err error) {
	blk.RLock()
	mvcc := blk.tryGetMVCC()
	if mvcc == nil {
		blk.RUnlock()
		return
	}
	node := mvcc.GetLatestMVCCNode(blkID)
	if node == nil {
		blk.RUnlock()
		return
	}
	location := node.BaseNode.DeltaLoc
	if location.IsEmpty() {
		blk.RUnlock()
		return
	}
	deltalocCommitTS = node.End
	visible = node.IsVisible(txn)
	blk.RUnlock()
	pkName := blk.meta.GetSchema().GetPrimaryKey().Name
	bat, persistedByCN, release, err = LoadPersistedDeletes(
		ctx,
		pkName,
		blk.rt.Fs,
		location,
		mp,
	)
	return
}
func (blk *baseObject) FillPersistedDeletes(
	ctx context.Context,
	blkID uint16,
	txn txnif.TxnReader,
	view *containers.BaseView,
	mp *mpool.MPool,
) (err error) {
	return blk.foreachPersistedDeletesVisibleByTxn(
		ctx,
		txn,
		blkID,
		true,
		func(i int, rowIdVec *vector.Vector) {
			rowid := vector.GetFixedAt[types.Rowid](rowIdVec, i)
			row := rowid.GetRowOffset()
			if view.DeleteMask == nil {
				view.DeleteMask = nulls.NewWithSize(int(row) + 1)
			}
			view.DeleteMask.Add(uint64(row))
		},
		nil,
		mp,
	)
}

func (blk *baseObject) fillPersistedDeletesInRange(
	ctx context.Context,
	blkID uint16,
	start, end types.TS,
	view *containers.BaseView,
	mp *mpool.MPool,
) (err error) {
	err = blk.foreachPersistedDeletesCommittedInRange(
		ctx,
		start,
		end,
		blkID,
		true,
		func(i int, rowIdVec *vector.Vector) {
			rowid := vector.GetFixedAt[types.Rowid](rowIdVec, i)
			row := rowid.GetRowOffset()
			if view.DeleteMask == nil {
				view.DeleteMask = nulls.NewWithSize(int(row) + 1)
			}
			view.DeleteMask.Add(uint64(row))
		},
		nil,
		mp,
	)
	return err
}

func (blk *baseObject) persistedCollectDeleteMaskInRange(
	ctx context.Context,
	blkID uint16,
	start, end types.TS,
	mp *mpool.MPool,
) (deletes *nulls.Nulls, err error) {
	err = blk.foreachPersistedDeletesCommittedInRange(
		ctx,
		start,
		end,
		blkID,
		true,
		func(i int, rowIdVec *vector.Vector) {
			rowid := vector.GetFixedAt[types.Rowid](rowIdVec, i)
			row := rowid.GetRowOffset()
			if deletes == nil {
				deletes = nulls.NewWithSize(int(row) + 1)
			}
			deletes.Add(uint64(row))
		},
		nil,
		mp,
	)
	return
}

// for each deletes in [start,end]
func (blk *baseObject) foreachPersistedDeletesCommittedInRange(
	ctx context.Context,
	start, end types.TS,
	blkID uint16,
	skipAbort bool,
	loopOp func(int, *vector.Vector),
	postOp func(*containers.Batch),
	mp *mpool.MPool,
) (err error) {
	loadFn := func() (bat *containers.Batch, persistedByCN bool, commitTS types.TS, visible bool, release func(), err error) {
		mvcc := blk.tryGetMVCC()
		if mvcc == nil {
			return
		}
		// commitTS of deltalocation is the commitTS of deletes persisted by CN batches
		location, deltalocCommitTS, deltalocStartTS := mvcc.GetDeltaLocAndCommitTS(blkID)
		if location.IsEmpty() {
			return
		}

		// quick check for early return.
		persistedByCN, err = blockio.IsPersistedByCN(ctx, location, blk.rt.Fs.Service)
		if err != nil {
			return
		}
		if persistedByCN {
			if deltalocCommitTS.Equal(&txnif.UncommitTS) {
				return
			}
			if deltalocCommitTS.Less(&start) || deltalocCommitTS.Greater(&end) {
				return
			}
		} else if deltalocStartTS.Less(&start) {
			return
		}

		// IO
		visible = true
		pkName := blk.meta.GetSchema().GetPrimaryKey().Name
		bat, release, err = LoadPersistedDeletesBySchema(
			ctx,
			pkName,
			blk.rt.Fs,
			location,
			persistedByCN,
			mp,
		)
		return
	}
	return blk.foreachPersistedDeletes(ctx, start, end, blkID, skipAbort, loadFn, loopOp, postOp, mp)
}

// for each deletes in [start,end]
func (blk *baseObject) foreachPersistedDeletesVisibleByTxn(
	ctx context.Context,
	txn txnif.TxnReader,
	blkID uint16,
	skipAbort bool,
	loopOp func(int, *vector.Vector),
	postOp func(*containers.Batch),
	mp *mpool.MPool,
) (err error) {
	loadFn := func() (deletes *containers.Batch, persistedByCN bool, commitTS types.TS, visible bool, release func(), err error) {
		// commitTS of deltalocation is the commitTS of deletes persisted by CN batches
		return blk.loadLatestPersistedDeletes(ctx, blkID, txn, mp)
	}
	return blk.foreachPersistedDeletes(ctx, types.TS{}, txn.GetStartTS(), blkID, skipAbort, loadFn, loopOp, postOp, mp)
}
func (blk *baseObject) foreachPersistedDeletes(
	ctx context.Context,
	start, end types.TS,
	blkID uint16,
	skipAbort bool,
	loadFn func() (bat *containers.Batch, persistedByCN bool, commitTS types.TS, visible bool, release func(), err error),
	loopOp func(int, *vector.Vector),
	postOp func(*containers.Batch),
	mp *mpool.MPool,
) (err error) {
	// commitTS of deltalocation is the commitTS of deletes persisted by CN batches
	deletes, persistedByCN, deltalocCommitTS, visible, release, err := loadFn()
	if deletes == nil || err != nil {
		return
	}
	defer release()
	defer deletes.Close()
	if persistedByCN {
		if !visible {
			return
		}
		rowIdVec := deletes.Vecs[0].GetDownstreamVector()
		for i := 0; i < deletes.Length(); i++ {
			loopOp(i, rowIdVec)
		}
		commitTSVec := containers.NewConstFixed[types.TS](types.T_TS.ToType(), deltalocCommitTS, deletes.Length())
		abortVec := containers.NewConstFixed[bool](types.T_bool.ToType(), false, deletes.Length())
		deletes.AddVector(catalog.AttrCommitTs, commitTSVec)
		deletes.AddVector(catalog.AttrAborted, abortVec)
	} else {
		abortVec := deletes.Vecs[3].GetDownstreamVector()
		commitTsVec := deletes.Vecs[1].GetDownstreamVector()
		rowIdVec := deletes.Vecs[0].GetDownstreamVector()

		rstart, rend := blockio.FindIntervalForBlock(vector.MustFixedCol[types.Rowid](rowIdVec), objectio.NewBlockidWithObjectID(&blk.meta.ID, blkID))
		for i := rstart; i < rend; i++ {
			if skipAbort {
				abort := vector.GetFixedAt[bool](abortVec, i)
				if abort {
					continue
				}
			}
			commitTS := vector.GetFixedAt[types.TS](commitTsVec, i)
			if commitTS.GreaterEq(&start) && commitTS.LessEq(&end) {
				loopOp(i, rowIdVec)
			}
		}
	}
	if postOp != nil {
		postOp(deletes)
	}
	return
}
func (blk *baseObject) Prefetch(idxes []uint16, blkID uint16) error {
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return nil
	} else {
		key, err := blk.buildMetalocation(blkID)
		if err != nil {
			return err
		}
		return blockio.Prefetch(idxes, []uint16{key.ID()}, blk.rt.Fs.Service, key)
	}
}

func (blk *baseObject) ResolvePersistedColumnDatas(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkID uint16,
	colIdxs []int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {

	view = containers.NewBlockView()
	location, err := blk.buildMetalocation(blkID)
	if err != nil {
		return nil, err
	}
	id := blk.meta.AsCommonID()
	id.SetBlockOffset(blkID)
	vecs, err := LoadPersistedColumnDatas(
		ctx, readSchema, blk.rt, id, colIdxs, location, mp,
	)
	if err != nil {
		return nil, err
	}
	for i, vec := range vecs {
		view.SetData(colIdxs[i], vec)
	}

	if skipDeletes {
		return
	}

	defer func() {
		if err != nil {
			view.Close()
		}
	}()

	blk.RLock()
	err = blk.fillInMemoryDeletesLocked(txn, blkID, view.BaseView, blk.RWMutex)
	blk.RUnlock()

	if err = blk.FillPersistedDeletes(ctx, blkID, txn, view.BaseView, mp); err != nil {
		return
	}
	return
}

func (blk *baseObject) ResolvePersistedColumnData(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkID uint16,
	colIdx int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	view = containers.NewColumnView(colIdx)
	vec, err := blk.LoadPersistedColumnData(context.Background(), readSchema, colIdx, mp, blkID)
	if err != nil {
		return
	}
	view.SetData(vec)

	if skipDeletes {
		return
	}

	defer func() {
		if err != nil {
			view.Close()
		}
	}()

	if err = blk.FillPersistedDeletes(ctx, blkID, txn, view.BaseView, mp); err != nil {
		return
	}

	blk.RLock()
	err = blk.fillInMemoryDeletesLocked(txn, blkID, view.BaseView, blk.RWMutex)
	blk.RUnlock()
	return
}

func (blk *baseObject) dedupWithLoad(
	ctx context.Context,
	txn txnif.TxnReader,
	keys containers.Vector,
	sels *nulls.Bitmap,
	rowmask *roaring.Bitmap,
	blkOffset uint16,
	isAblk bool,
	mp *mpool.MPool,
) (err error) {
	schema := blk.meta.GetSchema()
	def := schema.GetSingleSortKey()
	view, err := blk.ResolvePersistedColumnData(
		ctx,
		txn,
		schema,
		blkOffset,
		def.Idx,
		false,
		mp,
	)
	if err != nil {
		return err
	}
	if rowmask != nil {
		if view.DeleteMask == nil {
			view.DeleteMask = common.RoaringToMOBitmap(rowmask)
		} else {
			common.MOOrRoaringBitmap(view.DeleteMask, rowmask)
		}
	}
	defer view.Close()
	var dedupFn any
	if isAblk {
		dedupFn = containers.MakeForeachVectorOp(
			keys.GetType().Oid, dedupAlkFunctions, view.GetData(), view.DeleteMask, def, blk.LoadPersistedCommitTS, txn,
		)
	} else {
		dedupFn = containers.MakeForeachVectorOp(
			keys.GetType().Oid, dedupNABlkFunctions, view.GetData(), view.DeleteMask, def,
		)
	}
	err = containers.ForeachVector(keys, dedupFn, sels)
	return
}

func (blk *baseObject) PersistedBatchDedup(
	ctx context.Context,
	txn txnif.TxnReader,
	isCommitting bool,
	keys containers.Vector,
	keysZM index.ZM,
	rowmask *roaring.Bitmap,
	isAblk bool,
	bf objectio.BloomFilter,
	mp *mpool.MPool,
) (err error) {
	pkIndex, err := MakeImmuIndex(
		ctx,
		blk.meta,
		bf,
		blk.rt,
	)
	if err != nil {
		return
	}
	for i := 0; i < blk.meta.BlockCnt(); i++ {
		sels, err := pkIndex.BatchDedup(
			ctx,
			keys,
			keysZM,
			blk.rt,
			uint32(i),
		)
		if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
			continue
		}
		err = blk.dedupWithLoad(ctx, txn, keys, sels, rowmask, uint16(i), isAblk, mp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (blk *baseObject) getPersistedValue(
	ctx context.Context,
	txn txnif.TxnReader,
	schema *catalog.Schema,
	blkID uint16,
	row, col int,
	skipMemory bool,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	view := containers.NewColumnView(col)
	if err = blk.FillPersistedDeletes(ctx, blkID, txn, view.BaseView, mp); err != nil {
		return
	}
	if !skipMemory {
		blk.RLock()
		err = blk.fillInMemoryDeletesLocked(txn, blkID, view.BaseView, blk.RWMutex)
		blk.RUnlock()
		if err != nil {
			return
		}
	}
	if view.DeleteMask.Contains(uint64(row)) {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	view2, err := blk.ResolvePersistedColumnData(ctx, txn, schema, blkID, col, true, mp)
	if err != nil {
		return
	}
	defer view2.Close()
	v, isNull = view2.GetValue(row)
	return
}

func (blk *baseObject) DeletesInfo() string {
	blk.RLock()
	defer blk.RUnlock()
	mvcc := blk.tryGetMVCC()
	if mvcc == nil {
		return ""
	}
	return mvcc.StringLocked(1, 0, "")
}

func (blk *baseObject) RangeDelete(
	txn txnif.AsyncTxn,
	blkID uint16,
	start, end uint32,
	pk containers.Vector,
	dt handle.DeleteType) (node txnif.DeleteNode, err error) {
	blk.Lock()
	defer blk.Unlock()
	blkMVCC := blk.getOrCreateMVCC().GetOrCreateDeleteChainLocked(blkID)
	if err = blkMVCC.CheckNotDeleted(start, end, txn.GetStartTS()); err != nil {
		return
	}
	node = blkMVCC.CreateDeleteNode(txn, dt)
	node.RangeDeleteLocked(start, end, pk, common.MutMemAllocator)
	return
}

func (blk *baseObject) TryDeleteByDeltaloc(
	txn txnif.AsyncTxn,
	blkID uint16,
	deltaLoc objectio.Location) (node txnif.TxnEntry, ok bool, err error) {
	if blk.meta.IsAppendable() {
		return
	}
	blk.Lock()
	defer blk.Unlock()
	blkMVCC := blk.getOrCreateMVCC().GetOrCreateDeleteChainLocked(blkID)
	return blkMVCC.TryDeleteByDeltalocLocked(txn, deltaLoc, true)
}

func (blk *baseObject) PPString(level common.PPLevel, depth int, prefix string, blkid int) string {
	rows, err := blk.Rows()
	if err != nil {
		logutil.Warnf("get object rows failed, obj: %v, err: %v", blk.meta.ID.String(), err)
	}
	s := fmt.Sprintf("%s | [Rows=%d]", blk.meta.PPString(level, depth, prefix), rows)
	if level >= common.PPL1 {
		blk.RLock()
		var appendstr, deletestr string
		if blk.appendMVCC != nil {
			appendstr = blk.appendMVCC.StringLocked()
		}
		if mvcc := blk.tryGetMVCC(); mvcc != nil {
			if blkid >= 0 {
				deletestr = mvcc.StringBlkLocked(level, 0, "", blkid)
			} else {
				deletestr = mvcc.StringLocked(level, 0, "")
			}
		}
		blk.RUnlock()
		if appendstr != "" {
			s = fmt.Sprintf("%s\n Appends: %s", s, appendstr)
		}
		if deletestr != "" {
			s = fmt.Sprintf("%s\n Deletes: %s", s, deletestr)
		}
	}
	return s
}

func (blk *baseObject) HasDeleteIntentsPreparedIn(from, to types.TS) (found, isPersist bool) {
	blk.RLock()
	defer blk.RUnlock()
	mvcc := blk.tryGetMVCC()
	if mvcc == nil {
		return
	}
	return mvcc.HasDeleteIntentsPreparedIn(from, to)
}
func (blk *baseObject) HasDeleteIntentsPreparedInByBlock(blkID uint16, from, to types.TS) (found, isPersist bool) {
	blk.RLock()
	defer blk.RUnlock()
	mvcc := blk.tryGetMVCC()
	if mvcc == nil {
		return
	}
	return mvcc.HasInMemoryDeleteIntentsPreparedInByBlock(blkID, from, to)
}
func (blk *baseObject) CollectChangesInRange(
	ctx context.Context,
	blkID uint16,
	startTs, endTs types.TS,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {
	view = containers.NewBlockView()
	view.DeleteMask, err = blk.inMemoryCollectDeletesInRange(blkID, startTs, endTs)
	blk.fillPersistedDeletesInRange(ctx, blkID, startTs, endTs, view.BaseView, mp)
	return
}

func (blk *baseObject) inMemoryCollectDeletesInRange(blkID uint16, start, end types.TS) (deletes *nulls.Bitmap, err error) {
	blk.RLock()
	defer blk.RUnlock()
	mvcc := blk.tryGetMVCC()
	if mvcc == nil {
		return
	}
	blkMvcc := mvcc.TryGetDeleteChain(blkID)
	if blkMvcc == nil {
		return
	}
	deleteChain := blkMvcc.GetDeleteChain()
	deletes, err =
		deleteChain.CollectDeletesInRangeWithLock(start, end, blk.RWMutex)
	return
}

func (blk *baseObject) CollectDeleteInRange(
	ctx context.Context,
	start, end types.TS,
	withAborted bool,
	mp *mpool.MPool,
) (bat *containers.Batch, emtpyDelBlkIdx *bitmap.Bitmap, err error) {
	emtpyDelBlkIdx = &bitmap.Bitmap{}
	emtpyDelBlkIdx.InitWithSize(int64(blk.meta.BlockCnt()))
	for blkID := uint16(0); blkID < uint16(blk.meta.BlockCnt()); blkID++ {
		deletes, err := blk.CollectDeleteInRangeByBlock(ctx, blkID, start, end, withAborted, mp)
		if err != nil {
			return nil, nil, err
		}
		if deletes == nil || deletes.Length() == 0 {
			emtpyDelBlkIdx.Add(uint64(blkID))
		} else {
			if bat == nil {
				bat = containers.NewBatch()
				bat.AddVector(catalog.AttrRowID, containers.MakeVector(types.T_Rowid.ToType(), mp))
				bat.AddVector(catalog.AttrCommitTs, containers.MakeVector(types.T_TS.ToType(), mp))
				bat.AddVector(catalog.AttrPKVal, containers.MakeVector(*deletes.GetVectorByName(catalog.AttrPKVal).GetType(), mp))
				if withAborted {
					bat.AddVector(catalog.AttrAborted, containers.MakeVector(types.T_bool.ToType(), mp))
				}
			}
			bat.Extend(deletes)
			deletes.Close()
		}
	}
	return
}

func (blk *baseObject) CollectDeleteInRangeByBlock(
	ctx context.Context,
	blkID uint16,
	start, end types.TS,
	withAborted bool,
	mp *mpool.MPool) (*containers.Batch, error) {
	deletes, minTS, _, err := blk.inMemoryCollectDeleteInRange(
		ctx,
		blkID,
		start,
		end,
		withAborted,
		mp,
	)
	if err != nil {
		if deletes != nil {
			deletes.Close()
		}
		return nil, err
	}
	currentEnd := end
	if !minTS.IsEmpty() && currentEnd.Greater(&minTS) {
		currentEnd = minTS.Prev()
	}
	deletes, err = blk.PersistedCollectDeleteInRange(
		ctx,
		deletes,
		blkID,
		start,
		currentEnd,
		withAborted,
		mp,
	)
	if err != nil {
		if deletes != nil {
			deletes.Close()
		}
		return nil, err
	}
	return deletes, nil
}

func (blk *baseObject) inMemoryCollectDeleteInRange(
	ctx context.Context,
	blkID uint16,
	start, end types.TS,
	withAborted bool,
	mp *mpool.MPool,
) (bat *containers.Batch, minTS, persistedTS types.TS, err error) {
	blk.RLock()
	objMVCC := blk.tryGetMVCC()
	if objMVCC == nil {
		blk.RUnlock()
		return
	}
	mvcc := objMVCC.TryGetDeleteChain(blkID)
	blk.RUnlock()
	if mvcc == nil {
		return
	}
	return mvcc.InMemoryCollectDeleteInRange(ctx, start, end, withAborted, mp)
}

// collect the row if its committs is in [start,end]
func (blk *baseObject) PersistedCollectDeleteInRange(
	ctx context.Context,
	b *containers.Batch,
	blkID uint16,
	start, end types.TS,
	withAborted bool,
	mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	if b != nil {
		bat = b
	}
	t := types.T_int32.ToType()
	sels := blk.rt.VectorPool.Transient.GetVector(&t)
	defer sels.Close()
	selsVec := sels.GetDownstreamVector()
	blk.foreachPersistedDeletesCommittedInRange(
		ctx,
		start, end,
		blkID,
		!withAborted,
		func(row int, rowIdVec *vector.Vector) {
			_ = vector.AppendFixed[int32](selsVec, int32(row), false, mp)
		},
		func(delBat *containers.Batch) {
			if sels.Length() == 0 {
				return
			}
			if bat == nil {
				bat = containers.NewBatchWithCapacity(len(delBat.Attrs))
				for i, name := range delBat.Attrs {
					if !withAborted && name == catalog.AttrAborted {
						continue
					}
					bat.AddVector(
						name,
						blk.rt.VectorPool.Transient.GetVector(delBat.Vecs[i].GetType()),
					)
				}
			}
			for _, name := range bat.Attrs {
				retVec := bat.GetVectorByName(name)
				srcVec := delBat.GetVectorByName(name)
				retVec.PreExtend(sels.Length())
				retVec.GetDownstreamVector().Union(
					srcVec.GetDownstreamVector(),
					vector.MustFixedCol[int32](sels.GetDownstreamVector()),
					retVec.GetAllocator(),
				)
			}
		},
		mp,
	)
	return bat, nil
}

func (blk *baseObject) OnReplayDelete(blkID uint16, node txnif.DeleteNode) (err error) {
	blk.getOrCreateMVCC().GetOrCreateDeleteChainLocked(blkID).OnReplayDeleteNode(node)
	err = node.OnApply()
	return
}

func (blk *baseObject) OnReplayAppend(_ txnif.AppendNode) (err error) {
	panic("not supported")
}

func (blk *baseObject) OnReplayAppendPayload(_ *containers.Batch) (err error) {
	panic("not supported")
}

func (blk *baseObject) MakeAppender() (appender data.ObjectAppender, err error) {
	panic("not supported")
}

func (blk *baseObject) GetTotalChanges() int {
	blk.RLock()
	defer blk.RUnlock()
	objMVCC := blk.tryGetMVCC()
	if objMVCC == nil {
		return 0
	}
	return int(objMVCC.GetDeleteCnt())
}

func (blk *baseObject) IsAppendable() bool { return false }

func (blk *baseObject) MutationInfo() string {
	rows, err := blk.Rows()
	blk.RLock()
	defer blk.RUnlock()
	if err != nil {
		logutil.Warnf("get object rows failed, obj: %v, err %v", blk.meta.ID.String(), err)
	}
	objMVCC := blk.tryGetMVCC()
	var deleteCnt uint32
	if objMVCC != nil {
		deleteCnt = objMVCC.GetDeleteCnt()
	}
	s := fmt.Sprintf("Block %s Mutation Info: Changes=%d/%d",
		blk.meta.AsCommonID().BlockString(),
		deleteCnt,
		rows)
	return s
}

func (blk *baseObject) CollectAppendInRange(
	start, end types.TS, withAborted bool, mp *mpool.MPool,
) (*containers.BatchWithVersion, error) {
	return nil, nil
}

func (blk *baseObject) UpdateDeltaLoc(txn txnif.TxnReader, blkID uint16, deltaLoc objectio.Location) (bool, txnif.TxnEntry, error) {
	blk.Lock()
	defer blk.Unlock()
	mvcc := blk.getOrCreateMVCC().GetOrCreateDeleteChainLocked(blkID)
	return mvcc.UpdateDeltaLocLocked(txn, deltaLoc, false)
}

func (blk *baseObject) GetDeltaPersistedTS() types.TS {
	blk.RLock()
	defer blk.RUnlock()
	objMVCC := blk.tryGetMVCC()
	if objMVCC == nil {
		return types.TS{}
	}
	return objMVCC.GetDeltaPersistedTS()
}
