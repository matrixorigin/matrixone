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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

type BlockT[T common.IRef] interface {
	common.IRef
	Pin() *common.PinnedItem[T]
	GetID() *common.ID
}

type baseObject struct {
	common.RefHelper
	*sync.RWMutex
	rt         *dbutils.Runtime
	meta       atomic.Pointer[catalog.ObjectEntry]
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
		appendMVCC: updates.NewAppendMVCCHandle(meta),
	}
	blk.meta.Store(meta)
	blk.appendMVCC.SetAppendListener(blk.OnApplyAppend)
	blk.RWMutex = blk.appendMVCC.RWMutex
	return blk
}

func (blk *baseObject) GetMutex() *sync.RWMutex { return blk.RWMutex }
func (blk *baseObject) UpdateMeta(meta any) {
	blk.meta.Store(meta.(*catalog.ObjectEntry))
}

func (blk *baseObject) OnApplyAppend(n txnif.AppendNode) (err error) {
	if n.IsTombstone() {
		blk.meta.Load().GetTable().RemoveRows(
			uint64(n.GetMaxRow() - n.GetStartRow()),
		)
		return
	}
	blk.meta.Load().GetTable().AddRows(
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

func (blk *baseObject) GetMeta() any { return blk.meta.Load() }
func (blk *baseObject) CheckFlushTaskRetry(startts types.TS) bool {
	if !blk.meta.Load().IsAppendable() {
		panic("not support")
	}
	if blk.meta.Load().HasDropCommitted() {
		panic("not support")
	}
	blk.RLock()
	defer blk.RUnlock()
	x := blk.appendMVCC.GetLatestAppendPrepareTSLocked()
	return x.Greater(&startts)
}
func (blk *baseObject) GetFs() *objectio.ObjectFS { return blk.rt.Fs }
func (blk *baseObject) GetID() *common.ID         { return blk.meta.Load().AsCommonID() }

func (blk *baseObject) buildMetalocation(bid uint16) (objectio.Location, error) {
	if !blk.meta.Load().ObjectPersisted() {
		panic("logic error")
	}
	stats, err := blk.meta.Load().MustGetObjectStats()
	if err != nil {
		return nil, err
	}
	blkMaxRows := blk.meta.Load().GetSchema().BlockMaxRows
	return catalog.BuildLocation(stats, bid, blkMaxRows), nil
}

func (blk *baseObject) LoadPersistedCommitTS(bid uint16) (vec containers.Vector, err error) {
	if !blk.meta.Load().IsAppendable() {
		panic("not support")
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
		panic(fmt.Sprintf("%s: bad commits layout", blk.meta.Load().ID().String()))
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
	id := blk.meta.Load().AsCommonID()
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
		return blockio.Prefetch(blk.rt.SID(), idxes, []uint16{key.ID()}, blk.rt.Fs.Service, key)
	}
}

func (blk *baseObject) ResolvePersistedColumnData(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkOffset uint16,
	col int,
	mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	err = blk.Scan(ctx, &bat, txn, readSchema, blkOffset, []int{col}, mp)
	return
}

func (blk *baseObject) getDuplicateRowsWithLoad(
	ctx context.Context,
	txn txnif.TxnReader,
	keys containers.Vector,
	sels *nulls.Bitmap,
	rowIDs containers.Vector,
	blkOffset uint16,
	isAblk bool,
	skipCommittedBeforeTxnForAblk bool,
	maxVisibleRow uint32,
	mp *mpool.MPool,
) (err error) {
	schema := blk.meta.Load().GetSchema()
	def := schema.GetSingleSortKey()
	view, err := blk.ResolvePersistedColumnData(
		ctx,
		txn,
		schema,
		blkOffset,
		def.Idx,
		mp,
	)
	if err != nil {
		return
	}
	defer view.Close()
	blkID := objectio.NewBlockidWithObjectID(blk.meta.Load().ID(), blkOffset)
	var dedupFn any
	if isAblk {
		dedupFn = containers.MakeForeachVectorOp(
			keys.GetType().Oid, getRowIDAlkFunctions, view.Vecs[0], rowIDs, blkID, maxVisibleRow, blk.LoadPersistedCommitTS, txn, skipCommittedBeforeTxnForAblk,
		)
	} else {
		dedupFn = containers.MakeForeachVectorOp(
			keys.GetType().Oid, getDuplicatedRowIDNABlkFunctions, view.Vecs[0], rowIDs, blkID,
		)
	}
	err = containers.ForeachVector(keys, dedupFn, sels)
	return
}

func (blk *baseObject) containsWithLoad(
	ctx context.Context,
	txn txnif.TxnReader,
	keys containers.Vector,
	sels *nulls.Bitmap,
	blkOffset uint16,
	isAblk bool,
	isCommitting bool,
	mp *mpool.MPool,
) (err error) {
	schema := blk.meta.Load().GetSchema()
	def := schema.GetSingleSortKey()
	view, err := blk.ResolvePersistedColumnData(
		ctx,
		txn,
		schema,
		blkOffset,
		def.Idx,
		mp,
	)
	if err != nil {
		return
	}
	var dedupFn any
	if isAblk {
		dedupFn = containers.MakeForeachVectorOp(
			keys.GetType().Oid, containsAlkFunctions, view.Vecs[0], keys, blk.LoadPersistedCommitTS, txn,
		)
	} else {
		dedupFn = containers.MakeForeachVectorOp(
			keys.GetType().Oid, containsNABlkFunctions, view.Vecs[0], keys,
		)
	}
	err = containers.ForeachVector(keys, dedupFn, sels)
	return
}

func (blk *baseObject) persistedGetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	skipCommittedBeforeTxnForAblk bool,
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
	isAblk bool,
	maxVisibleRow uint32,
	mp *mpool.MPool,
) (err error) {
	pkIndex, err := MakeImmuIndex(
		ctx,
		blk.meta.Load(),
		nil,
		blk.rt,
	)
	if err != nil {
		return
	}
	for i := 0; i < blk.meta.Load().BlockCnt(); i++ {
		sels, err := pkIndex.BatchDedup(
			ctx,
			keys,
			keysZM,
			blk.rt,
			blk.meta.Load().IsTombstone,
			uint32(i),
		)
		if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
			continue
		}
		err = blk.getDuplicateRowsWithLoad(ctx, txn, keys, sels, rowIDs, uint16(i), isAblk, skipCommittedBeforeTxnForAblk, maxVisibleRow, mp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (blk *baseObject) persistedContains(
	ctx context.Context,
	txn txnif.TxnReader,
	isCommitting bool,
	keys containers.Vector,
	keysZM index.ZM,
	isAblk bool,
	mp *mpool.MPool,
) (err error) {
	pkIndex, err := MakeImmuIndex(
		ctx,
		blk.meta.Load(),
		nil,
		blk.rt,
	)
	if err != nil {
		return
	}
	for i := 0; i < blk.meta.Load().BlockCnt(); i++ {
		sels, err := pkIndex.BatchDedup(
			ctx,
			keys,
			keysZM,
			blk.rt,
			true,
			uint32(i),
		)
		if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
			continue
		}
		err = blk.containsWithLoad(ctx, txn, keys, sels, uint16(i), isAblk, isCommitting, mp)
		if err != nil {
			return err
		}
	}
	return nil
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
	return int(blk.meta.Load().GetDeleteCount())
}

func (blk *baseObject) IsAppendable() bool { return false }

func (blk *baseObject) PPString(level common.PPLevel, depth int, prefix string, blkid int) string {
	rows, err := blk.Rows()
	if err != nil {
		logutil.Warnf("get object rows failed, obj: %v, err: %v", blk.meta.Load().ID().String(), err)
	}
	s := fmt.Sprintf("%s | [Rows=%d]", blk.meta.Load().PPString(level, depth, prefix), rows)
	if level >= common.PPL1 {
		blk.RLock()
		var appendstr, deletestr string
		if blk.appendMVCC != nil {
			appendstr = blk.appendMVCC.StringLocked()
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
func (blk *baseObject) Scan(
	ctx context.Context,
	bat **containers.Batch,
	txn txnif.TxnReader,
	readSchema any,
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (err error) {
	node := blk.PinNode()
	defer node.Unref()
	return node.Scan(ctx, bat, txn, readSchema.(*catalog.Schema), blkID, colIdxes, mp)
}

func (blk *baseObject) FillBlockTombstones(
	ctx context.Context,
	txn txnif.TxnReader,
	blkID *objectio.Blockid,
	deletes **nulls.Nulls,
	mp *mpool.MPool) error {
	node := blk.PinNode()
	defer node.Unref()
	if !blk.meta.Load().IsTombstone {
		panic("logic err")
	}
	return node.FillBlockTombstones(ctx, txn, blkID, deletes, mp)
}

func (blk *baseObject) ScanInMemory(
	ctx context.Context,
	batches map[uint32]*containers.BatchWithVersion,
	start, end types.TS,
	mp *mpool.MPool,
) (err error) {
	node := blk.PinNode()
	defer node.Unref()
	if node.IsPersisted() {
		return nil
	}
	mnode := node.MustMNode()
	return mnode.getDataWindowOnWriteSchema(ctx, batches, start, end, mp)
}

func (blk *baseObject) CollectObjectTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	objID *types.Objectid,
	bat **containers.Batch,
	mp *mpool.MPool,
	vpool *containers.VectorPool,
) (err error) {
	if !blk.meta.Load().IsTombstone {
		panic("logic err")
	}
	node := blk.PinNode()
	defer node.Unref()
	return node.CollectObjectTombstoneInRange(ctx, start, end, objID, bat, mp, vpool)
}

// TODO: equal filter
func (obj *baseObject) GetValue(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	blkOffset uint16,
	row, col int,
	skipCheckDelete bool,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	if !obj.meta.Load().IsTombstone && !skipCheckDelete {
		var bat *containers.Batch
		blkID := objectio.NewBlockidWithObjectID(obj.meta.Load().ID(), blkOffset)
		err = HybridScanByBlock(ctx, obj.meta.Load().GetTable(), txn, &bat, readSchema.(*catalog.Schema), []int{col}, blkID, mp)
		if err != nil {
			return
		}
		err = txn.GetStore().FillInWorkspaceDeletes(obj.meta.Load().AsCommonID(), &bat.Deletes)
		if err != nil {
			return
		}
		if bat.Deletes != nil && bat.Deletes.Contains(uint64(row)) {
			err = moerr.NewNotFoundNoCtx()
			return
		}
		isNull = bat.Vecs[0].IsNull(row)
		if !isNull {
			v = bat.Vecs[0].Get(row)
		}
		return
	}
	var bat *containers.Batch
	err = obj.Scan(ctx, &bat, txn, readSchema, blkOffset, []int{col}, mp)
	if err != nil {
		return
	}
	isNull = bat.Vecs[0].IsNull(row)
	if !isNull {
		v = bat.Vecs[0].Get(row)
	}
	return
}
