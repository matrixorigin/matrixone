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
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type BlockT[T common.IRef] interface {
	common.IRef
	Pin() *common.PinnedItem[T]
	GetID() *common.ID
}

type baseBlock struct {
	common.RefHelper
	*sync.RWMutex
	rt   *dbutils.Runtime
	meta *catalog.BlockEntry
	mvcc *updates.MVCCHandle
	ttl  time.Time
	impl data.Block

	node atomic.Pointer[Node]
}

func newBaseBlock(
	impl data.Block,
	meta *catalog.BlockEntry,
	rt *dbutils.Runtime,
) *baseBlock {
	blk := &baseBlock{
		impl: impl,
		rt:   rt,
		meta: meta,
		ttl:  time.Now(),
	}
	blk.mvcc = updates.NewMVCCHandle(meta)
	blk.RWMutex = blk.mvcc.RWMutex
	return blk
}

func (blk *baseBlock) Close() {
	// TODO
}

func (blk *baseBlock) PinNode() *Node {
	n := blk.node.Load()
	// if ref fails, reload.
	// Note: avoid bad case where releasing happens before Ref()
	for ; !n.RefIfHasRef(); n = blk.node.Load() {
	}
	return n
}

func (blk *baseBlock) GCInMemeoryDeletesByTS(ts types.TS) {
	blk.mvcc.UpgradeDeleteChainByTS(ts)
}

func (blk *baseBlock) Rows() int {
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		blk.RLock()
		defer blk.RUnlock()
		return int(node.Rows())
	} else {
		return int(node.Rows())
	}
}

func (blk *baseBlock) Foreach(ctx context.Context, colIdx int, op func(v any, isNull bool, row int) error, sels *nulls.Bitmap) error {
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		blk.RLock()
		defer blk.RUnlock()
		return node.MustMNode().Foreach(colIdx, op, sels)
	} else {
		return node.MustPNode().Foreach(ctx, blk.meta.GetSchema(), colIdx, op, sels)
	}
}

func (blk *baseBlock) TryUpgrade() (err error) {
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

func (blk *baseBlock) GetMeta() any              { return blk.meta }
func (blk *baseBlock) GetFs() *objectio.ObjectFS { return blk.rt.Fs }
func (blk *baseBlock) GetID() *common.ID         { return blk.meta.AsCommonID() }

func (blk *baseBlock) FillInMemoryDeletesLocked(
	txn txnif.TxnReader,
	view *containers.BaseView,
	rwlocker *sync.RWMutex) (err error) {
	chain := blk.mvcc.GetDeleteChain()
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

func (blk *baseBlock) LoadPersistedCommitTS() (vec containers.Vector, err error) {
	if !blk.meta.IsAppendable() {
		return
	}
	location := blk.meta.GetMetaLoc()
	if location.IsEmpty() {
		return
	}
	bat, err := blockio.LoadColumns(
		context.Background(),
		[]uint16{objectio.SEQNUM_COMMITTS},
		nil,
		blk.rt.Fs.Service,
		location,
		nil,
	)
	if err != nil {
		return
	}
	if bat.Vecs[0].GetType().Oid != types.T_TS {
		panic(fmt.Sprintf("%s: bad commits layout", blk.meta.ID.String()))
	}
	vec = containers.ToDNVector(bat.Vecs[0])
	return
}

// func (blk *baseBlock) LoadPersistedData() (bat *containers.Batch, err error) {
// 	schema := blk.meta.GetSchema()
// 	bat = containers.NewBatch()
// 	defer func() {
// 		if err != nil {
// 			bat.Close()
// 		}
// 	}()
// 	var vec containers.Vector
// 	for i, col := range schema.ColDefs {
// 		vec, err = blk.LoadPersistedColumnData(i)
// 		if err != nil {
// 			return
// 		}
// 		bat.AddVector(col.Name, vec)
// 	}
// 	return
// }

func (blk *baseBlock) LoadPersistedColumnData(ctx context.Context, schema *catalog.Schema, colIdx int) (
	vec containers.Vector, err error) {
	def := schema.ColDefs[colIdx]
	location := blk.meta.GetMetaLoc()
	return LoadPersistedColumnData(
		ctx,
		blk.rt,
		blk.meta.AsCommonID(),
		def,
		location)
}

func (blk *baseBlock) LoadPersistedDeletes(ctx context.Context) (bat *containers.Batch, err error) {
	location := blk.meta.GetDeltaLoc()
	if location.IsEmpty() {
		return
	}
	pkName := blk.meta.GetSchema().GetPrimaryKey().Name
	return LoadPersistedDeletes(
		ctx,
		pkName,
		blk.rt.Fs,
		location)
}

func (blk *baseBlock) FillPersistedDeletes(
	ctx context.Context,
	txn txnif.TxnReader,
	view *containers.BaseView) (err error) {
	blk.fillPersistedDeletesInRange(
		ctx,
		types.TS{},
		txn.GetStartTS(),
		view)
	return nil
}

func (blk *baseBlock) fillPersistedDeletesInRange(
	ctx context.Context,
	start, end types.TS,
	view *containers.BaseView) (err error) {
	blk.foreachPersistedDeletesCommittedInRange(
		ctx,
		start,
		end,
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
	)
	return nil
}

func (blk *baseBlock) foreachPersistedDeletesCommittedInRange(
	ctx context.Context,
	start, end types.TS,
	skipAbort bool,
	loopOp func(int, *vector.Vector),
	postOp func(*containers.Batch),
) (err error) {
	deletes, err := blk.LoadPersistedDeletes(ctx)
	if deletes == nil || err != nil {
		return
	}
	abortVec := deletes.Vecs[3].GetDownstreamVector()
	commitTsVec := deletes.Vecs[1].GetDownstreamVector()
	rowIdVec := deletes.Vecs[0].GetDownstreamVector()
	for i := 0; i < deletes.Length(); i++ {
		if skipAbort {
			abort := vector.GetFixedAt[bool](abortVec, i)
			if abort {
				continue
			}
		}
		commitTS := vector.GetFixedAt[types.TS](commitTsVec, i)
		if commitTS.GreaterEq(start) && commitTS.LessEq(end) {
			loopOp(i, rowIdVec)
		}
	}
	if postOp != nil {
		postOp(deletes)
	}
	return
}

func (blk *baseBlock) Prefetch(idxes []uint16) error {
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return nil
	} else {
		key := blk.meta.GetMetaLoc()
		return blockio.Prefetch(idxes, []uint16{key.ID()}, blk.rt.Fs.Service, key)
	}
}

func (blk *baseBlock) ResolvePersistedColumnDatas(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	colIdxs []int,
	skipDeletes bool) (view *containers.BlockView, err error) {

	view = containers.NewBlockView()
	for _, colIdx := range colIdxs {
		vec, err := blk.LoadPersistedColumnData(ctx, readSchema, colIdx)
		if err != nil {
			return nil, err
		}
		view.SetData(colIdx, vec)
	}

	if skipDeletes {
		return
	}

	defer func() {
		if err != nil {
			view.Close()
		}
	}()

	if err = blk.FillPersistedDeletes(ctx, txn, view.BaseView); err != nil {
		return
	}

	blk.RLock()
	err = blk.FillInMemoryDeletesLocked(txn, view.BaseView, blk.RWMutex)
	blk.RUnlock()
	return
}

func (blk *baseBlock) ResolvePersistedColumnData(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	colIdx int,
	skipDeletes bool) (view *containers.ColumnView, err error) {
	view = containers.NewColumnView(colIdx)
	vec, err := blk.LoadPersistedColumnData(context.Background(), readSchema, colIdx)
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

	if err = blk.FillPersistedDeletes(ctx, txn, view.BaseView); err != nil {
		return
	}

	blk.RLock()
	err = blk.FillInMemoryDeletesLocked(txn, view.BaseView, blk.RWMutex)
	blk.RUnlock()
	return
}

func (blk *baseBlock) dedupWithLoad(
	ctx context.Context,
	txn txnif.TxnReader,
	keys containers.Vector,
	sels *nulls.Bitmap,
	rowmask *roaring.Bitmap,
	isAblk bool,
) (err error) {
	schema := blk.meta.GetSchema()
	def := schema.GetSingleSortKey()
	view, err := blk.ResolvePersistedColumnData(
		ctx,
		txn,
		schema,
		def.Idx,
		false)
	if err != nil {
		return
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

func (blk *baseBlock) PersistedBatchDedup(
	ctx context.Context,
	txn txnif.TxnReader,
	isCommitting bool,
	keys containers.Vector,
	keysZM index.ZM,
	rowmask *roaring.Bitmap,
	isAblk bool,
	bf objectio.BloomFilter,
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
	sels, err := pkIndex.BatchDedup(
		ctx,
		keys,
		keysZM,
		blk.rt,
	)
	if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
		return
	}
	return blk.dedupWithLoad(ctx, txn, keys, sels, rowmask, isAblk)
}

func (blk *baseBlock) getPersistedValue(
	ctx context.Context,
	txn txnif.TxnReader,
	schema *catalog.Schema,
	row, col int,
	skipMemory bool) (v any, isNull bool, err error) {
	view := containers.NewColumnView(col)
	if err = blk.FillPersistedDeletes(ctx, txn, view.BaseView); err != nil {
		return
	}
	if !skipMemory {
		blk.RLock()
		err = blk.FillInMemoryDeletesLocked(txn, view.BaseView, blk.RWMutex)
		blk.RUnlock()
		if err != nil {
			return
		}
	}
	if view.DeleteMask.Contains(uint64(row)) {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	view2, err := blk.ResolvePersistedColumnData(ctx, txn, schema, col, true)
	if err != nil {
		return
	}
	defer view2.Close()
	v, isNull = view2.GetValue(row)
	return
}

func (blk *baseBlock) DeletesInfo() string {
	blk.RLock()
	defer blk.RUnlock()
	return blk.mvcc.GetDeleteChain().StringLocked()
}

func (blk *baseBlock) RangeDelete(
	txn txnif.AsyncTxn,
	start, end uint32,
	dt handle.DeleteType) (node txnif.DeleteNode, err error) {
	blk.Lock()
	defer blk.Unlock()
	if err = blk.mvcc.CheckNotDeleted(start, end, txn.GetStartTS()); err != nil {
		return
	}
	node = blk.mvcc.CreateDeleteNode(txn, dt)
	node.RangeDeleteLocked(start, end)
	return
}

func (blk *baseBlock) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s | [Rows=%d]", blk.meta.PPString(level, depth, prefix), blk.Rows())
	if level >= common.PPL1 {
		blk.RLock()
		s2 := blk.mvcc.StringLocked()
		blk.RUnlock()
		if s2 != "" {
			s = fmt.Sprintf("%s\n%s", s, s2)
		}
	}
	return s
}

func (blk *baseBlock) HasDeleteIntentsPreparedIn(from, to types.TS) (found bool) {
	blk.RLock()
	defer blk.RUnlock()
	found = blk.mvcc.GetDeleteChain().HasDeleteIntentsPreparedInLocked(from, to)
	return
}

func (blk *baseBlock) CollectChangesInRange(ctx context.Context, startTs, endTs types.TS) (view *containers.BlockView, err error) {
	view = containers.NewBlockView()
	view.DeleteMask, err = blk.inMemoryCollectDeletesInRange(startTs, endTs)
	blk.fillPersistedDeletesInRange(ctx, startTs, endTs, view.BaseView)
	return
}

func (blk *baseBlock) inMemoryCollectDeletesInRange(start, end types.TS) (deletes *nulls.Bitmap, err error) {
	blk.RLock()
	defer blk.RUnlock()
	deleteChain := blk.mvcc.GetDeleteChain()
	deletes, err =
		deleteChain.CollectDeletesInRange(start, end, blk.RWMutex)
	return
}

func (blk *baseBlock) CollectDeleteInRange(
	ctx context.Context,
	start, end types.TS,
	withAborted bool) (bat *containers.Batch, err error) {
	bat, persistedTS, err := blk.inMemoryCollectDeleteInRange(
		ctx,
		start,
		end,
		withAborted)
	if err != nil {
		return
	}
	if end.Greater(persistedTS) {
		end = persistedTS
	}
	bat, err = blk.persistedCollectDeleteInRange(
		ctx,
		bat,
		start,
		end,
		withAborted)
	return
}

func (blk *baseBlock) inMemoryCollectDeleteInRange(
	ctx context.Context,
	start, end types.TS,
	withAborted bool) (bat *containers.Batch, persistedTS types.TS, err error) {
	blk.RLock()
	persistedTS = blk.mvcc.GetDeletesPersistedTS()
	if persistedTS.GreaterEq(end) {
		blk.RUnlock()
		return
	}
	rowID, ts, abort, abortedMap, deletes := blk.mvcc.CollectDeleteLocked(start, end)
	blk.RUnlock()
	if rowID == nil {
		return
	}
	pkDef := blk.meta.GetSchema().GetPrimaryKey()
	pkVec := containers.MakeVector(pkDef.Type)
	pkIdx := pkDef.Idx
	blk.Foreach(ctx, pkIdx, func(v any, isNull bool, row int) error {
		pkVec.Append(v, false)
		return nil
	}, deletes)
	// batch: rowID, ts, pkVec, abort
	bat = containers.NewBatch()
	bat.AddVector(catalog.PhyAddrColumnName, rowID)
	bat.AddVector(catalog.AttrCommitTs, ts)
	bat.AddVector(pkDef.Name, pkVec)
	if withAborted {
		bat.AddVector(catalog.AttrAborted, abort)
	} else {
		bat.Deletes = abortedMap
		bat.Compact()
	}
	return
}

// collect the row if its committs is in [start,end]
func (blk *baseBlock) persistedCollectDeleteInRange(
	ctx context.Context,
	b *containers.Batch,
	start, end types.TS,
	withAborted bool,
) (bat *containers.Batch, err error) {
	if b != nil {
		bat = b
	}
	t := types.T_int32.ToType()
	sels := blk.rt.VectorPool.Transient.GetVector(&t)
	defer sels.Close()
	selsVec := sels.GetDownstreamVector()
	mp := sels.GetAllocator()
	blk.foreachPersistedDeletesCommittedInRange(
		ctx,
		start, end,
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
					bat.AddVector(
						name,
						blk.rt.VectorPool.Transient.GetVector(delBat.Vecs[i].GetType()),
					)
				}
			}
			for _, name := range delBat.Attrs {
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
	)
	return bat, nil
}

func (blk *baseBlock) adjustScore(
	rawScoreFn func() (int, bool),
	ttl time.Duration,
	force bool) int {
	score, dropped := rawScoreFn()
	if dropped {
		return 0
	}
	if force {
		score = 100
	}
	if score == 0 || score > 1 {
		return score
	}
	var ratio float32
	if blk.meta.IsAppendable() {
		currRows := uint32(blk.Rows())
		ratio = float32(currRows) / float32(blk.meta.GetSchema().BlockMaxRows)
		if ratio >= 0 && ratio < 0.2 {
			ttl = 3*ttl - ttl/2
		} else if ratio >= 0.2 && ratio < 0.4 {
			ttl = 2 * ttl
		} else if ratio >= 0.4 && ratio < 0.6 {
			ttl = 2*ttl - ttl/2
		}
	}

	deleteCnt := blk.mvcc.GetDeleteCnt()
	ratio = float32(deleteCnt) / float32(blk.meta.GetSchema().BlockMaxRows)
	if ratio <= 1 && ratio > 0.5 {
		ttl /= 10
	} else if ratio <= 0.5 && ratio > 0.3 {
		ttl /= 5
	} else if ratio <= 0.3 && ratio > 0.2 {
		ttl /= 3
	} else if ratio <= 0.2 && ratio > 0.1 {
		ttl /= 2
	}

	if time.Now().After(blk.ttl.Add(ttl)) {
		return 100
	}
	return 1
}

func (blk *baseBlock) OnReplayDelete(node txnif.DeleteNode) (err error) {
	blk.mvcc.OnReplayDeleteNode(node)
	err = node.OnApply()
	return
}

func (blk *baseBlock) OnReplayAppend(_ txnif.AppendNode) (err error) {
	panic("not supported")
}

func (blk *baseBlock) OnReplayAppendPayload(_ *containers.Batch) (err error) {
	panic("not supported")
}

func (blk *baseBlock) MakeAppender() (appender data.BlockAppender, err error) {
	panic("not supported")
}

func (blk *baseBlock) GetRowsOnReplay() uint64 {
	rows := uint64(blk.mvcc.GetTotalRow())
	metaLoc := blk.meta.GetMetaLoc()
	if metaLoc.IsEmpty() {
		return rows
	}
	fileRows := uint64(metaLoc.Rows())
	if rows > fileRows {
		return rows
	}
	return fileRows
}

func (blk *baseBlock) GetTotalChanges() int {
	return int(blk.mvcc.GetChangeNodeCnt())
}

func (blk *baseBlock) IsAppendable() bool { return false }

func (blk *baseBlock) MutationInfo() string {
	rows := blk.Rows()
	totalChanges := blk.mvcc.GetChangeNodeCnt()
	s := fmt.Sprintf("Block %s Mutation Info: Changes=%d/%d",
		blk.meta.AsCommonID().BlockString(),
		totalChanges,
		rows)
	if totalChanges == 0 {
		return s
	}
	deleteCnt := blk.mvcc.GetDeleteCnt()
	if deleteCnt != 0 {
		s = fmt.Sprintf("%s, Del:%d/%d", s, deleteCnt, rows)
	}
	return s
}

func (blk *baseBlock) BuildCompactionTaskFactory() (
	factory tasks.TxnTaskFactory,
	taskType tasks.TaskType,
	scopes []common.ID,
	err error) {

	if !blk.impl.PrepareCompact() {
		return
	}

	factory = jobs.CompactBlockTaskFactory(blk.meta, blk.rt)
	taskType = tasks.DataCompactionTask
	scopes = append(scopes, *blk.meta.AsCommonID())
	return
}

func (blk *baseBlock) CollectAppendInRange(start, end types.TS, withAborted bool) (*containers.BatchWithVersion, error) {
	return nil, nil
}
