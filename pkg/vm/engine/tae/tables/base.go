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
	obj := &baseObject{
		impl:       impl,
		rt:         rt,
		appendMVCC: updates.NewAppendMVCCHandle(meta),
	}
	obj.meta.Store(meta)
	obj.appendMVCC.SetAppendListener(obj.OnApplyAppend)
	obj.RWMutex = obj.appendMVCC.RWMutex
	return obj
}

func (obj *baseObject) GetMutex() *sync.RWMutex { return obj.RWMutex }
func (obj *baseObject) UpdateMeta(meta any) {
	obj.meta.Store(meta.(*catalog.ObjectEntry))
}

func (obj *baseObject) OnApplyAppend(n txnif.AppendNode) (err error) {
	if n.IsTombstone() {
		obj.meta.Load().GetTable().RemoveRows(
			uint64(n.GetMaxRow() - n.GetStartRow()),
		)
		return
	}
	obj.meta.Load().GetTable().AddRows(
		uint64(n.GetMaxRow() - n.GetStartRow()),
	)
	return
}
func (obj *baseObject) Close() {
	// TODO
}

func (obj *baseObject) GetRuntime() *dbutils.Runtime {
	return obj.rt
}

func (obj *baseObject) PinNode() *Node {
	n := obj.node.Load()
	// if ref fails, reload.
	// Note: avoid bad case where releasing happens before Ref()
	for ; !n.RefIfHasRef(); n = obj.node.Load() {
	}
	return n
}

func (obj *baseObject) Rows() (int, error) {
	node := obj.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		obj.RLock()
		defer obj.RUnlock()
		rows, err := node.Rows()
		return int(rows), err
	} else {
		rows, err := node.Rows()
		return int(rows), err
	}
}

func (obj *baseObject) TryUpgrade() (err error) {
	node := obj.node.Load()
	if node.IsPersisted() {
		return
	}
	pnode := newPersistedNode(obj)
	nnode := NewNode(pnode)
	nnode.Ref()

	if !obj.node.CompareAndSwap(node, nnode) {
		nnode.Unref()
	} else {
		node.Unref()
	}
	return
}

func (obj *baseObject) GetMeta() any { return obj.meta.Load() }
func (obj *baseObject) CheckFlushTaskRetry(startts types.TS) bool {
	if !obj.meta.Load().IsAppendable() {
		panic("not support")
	}
	if obj.meta.Load().HasDropCommitted() {
		panic("not support")
	}
	obj.RLock()
	defer obj.RUnlock()
	x := obj.appendMVCC.GetLatestAppendPrepareTSLocked()
	return x.Greater(&startts)
}
func (obj *baseObject) GetFs() *objectio.ObjectFS { return obj.rt.Fs }
func (obj *baseObject) GetID() *common.ID         { return obj.meta.Load().AsCommonID() }

func (obj *baseObject) buildMetalocation(bid uint16) (objectio.Location, error) {
	if !obj.meta.Load().ObjectPersisted() {
		panic("logic error")
	}
	stats, err := obj.meta.Load().MustGetObjectStats()
	if err != nil {
		return nil, err
	}
	blkMaxRows := obj.meta.Load().GetSchema().BlockMaxRows
	return catalog.BuildLocation(stats, bid, blkMaxRows), nil
}

func (obj *baseObject) LoadPersistedCommitTS(bid uint16) (vec containers.Vector, err error) {
	if !obj.meta.Load().IsAppendable() {
		panic("not support")
	}
	location, err := obj.buildMetalocation(bid)
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
		obj.rt.Fs.Service,
		location,
		fileservice.Policy(0),
		true,
		obj.rt.VectorPool.Transient,
	)
	if err != nil {
		return
	}
	if vectors[0].GetType().Oid != types.T_TS {
		panic(fmt.Sprintf("%s: bad commits layout", obj.meta.Load().ID().String()))
	}
	vec = vectors[0]
	return
}

// func (obj *baseObject) LoadPersistedColumnData(
// 	ctx context.Context, schema *catalog.Schema, colIdx int, mp *mpool.MPool, blkID uint16,
// ) (vec containers.Vector, err error) {
// 	def := schema.ColDefs[colIdx]
// 	location, err := obj.buildMetalocation(blkID)
// 	if err != nil {
// 		return nil, err
// 	}
// 	id := obj.meta.Load().AsCommonID()
// 	id.SetBlockOffset(blkID)
// 	return LoadPersistedColumnData(
// 		ctx,
// 		obj.rt,
// 		id,
// 		def,
// 		location,
// 		mp,
// 	)
// }

func (obj *baseObject) Prefetch(idxes []uint16, blkID uint16) error {
	node := obj.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return nil
	} else {
		key, err := obj.buildMetalocation(blkID)
		if err != nil {
			return err
		}
		return blockio.Prefetch(obj.rt.SID(), idxes, []uint16{key.ID()}, obj.rt.Fs.Service, key)
	}
}

func (obj *baseObject) ResolvePersistedColumnData(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkOffset uint16,
	col int,
	mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	err = obj.Scan(ctx, &bat, txn, readSchema, blkOffset, []int{col}, mp)
	return
}

func (obj *baseObject) getDuplicateRowsWithLoad(
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
	schema := obj.meta.Load().GetSchema()
	def := schema.GetSingleSortKey()
	view, err := obj.ResolvePersistedColumnData(
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
	blkID := objectio.NewBlockidWithObjectID(obj.meta.Load().ID(), blkOffset)
	var dedupFn any
	if isAblk {
		dedupFn = containers.MakeForeachVectorOp(
			keys.GetType().Oid, getRowIDAlkFunctions, view.Vecs[0], rowIDs, blkID, maxVisibleRow, obj.LoadPersistedCommitTS, txn, skipCommittedBeforeTxnForAblk,
		)
	} else {
		dedupFn = containers.MakeForeachVectorOp(
			keys.GetType().Oid, getDuplicatedRowIDNABlkFunctions, view.Vecs[0], rowIDs, blkID,
		)
	}
	err = containers.ForeachVector(keys, dedupFn, sels)
	return
}

func (obj *baseObject) containsWithLoad(
	ctx context.Context,
	txn txnif.TxnReader,
	keys containers.Vector,
	sels *nulls.Bitmap,
	blkOffset uint16,
	isAblk bool,
	isCommitting bool,
	mp *mpool.MPool,
) (err error) {
	schema := obj.meta.Load().GetSchema()
	def := schema.GetSingleSortKey()
	view, err := obj.ResolvePersistedColumnData(
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
			keys.GetType().Oid, containsAlkFunctions, view.Vecs[0], keys, obj.LoadPersistedCommitTS, txn,
		)
	} else {
		dedupFn = containers.MakeForeachVectorOp(
			keys.GetType().Oid, containsNABlkFunctions, view.Vecs[0], keys,
		)
	}
	err = containers.ForeachVector(keys, dedupFn, sels)
	return
}

func (obj *baseObject) persistedGetDuplicatedRows(
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
		obj.meta.Load(),
		nil,
		obj.rt,
	)
	if err != nil {
		return
	}
	for i := 0; i < obj.meta.Load().BlockCnt(); i++ {
		sels, err := pkIndex.BatchDedup(
			ctx,
			keys,
			keysZM,
			obj.rt,
			obj.meta.Load().IsTombstone,
			uint32(i),
		)
		if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
			continue
		}
		err = obj.getDuplicateRowsWithLoad(ctx, txn, keys, sels, rowIDs, uint16(i), isAblk, skipCommittedBeforeTxnForAblk, maxVisibleRow, mp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (obj *baseObject) persistedContains(
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
		obj.meta.Load(),
		nil,
		obj.rt,
	)
	if err != nil {
		return
	}
	for i := 0; i < obj.meta.Load().BlockCnt(); i++ {
		sels, err := pkIndex.BatchDedup(
			ctx,
			keys,
			keysZM,
			obj.rt,
			true,
			uint32(i),
		)
		if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
			continue
		}
		err = obj.containsWithLoad(ctx, txn, keys, sels, uint16(i), isAblk, isCommitting, mp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (obj *baseObject) OnReplayAppend(_ txnif.AppendNode) (err error) {
	panic("not supported")
}

func (obj *baseObject) OnReplayAppendPayload(_ *containers.Batch) (err error) {
	panic("not supported")
}

func (obj *baseObject) MakeAppender() (appender data.ObjectAppender, err error) {
	panic("not supported")
}

func (obj *baseObject) GetTotalChanges() int {
	return int(obj.meta.Load().GetDeleteCount())
}

func (obj *baseObject) IsAppendable() bool { return false }

func (obj *baseObject) PPString(level common.PPLevel, depth int, prefix string, blkid int) string {
	rows, err := obj.Rows()
	if err != nil {
		logutil.Warnf("get object rows failed, obj: %v, err: %v", obj.meta.Load().ID().String(), err)
	}
	s := fmt.Sprintf("%s | [Rows=%d]", obj.meta.Load().PPString(level, depth, prefix), rows)
	if level >= common.PPL1 {
		obj.RLock()
		var appendstr, deletestr string
		if obj.appendMVCC != nil {
			appendstr = obj.appendMVCC.StringLocked()
		}
		obj.RUnlock()
		if appendstr != "" {
			s = fmt.Sprintf("%s\n Appends: %s", s, appendstr)
		}
		if deletestr != "" {
			s = fmt.Sprintf("%s\n Deletes: %s", s, deletestr)
		}
	}
	return s
}
func (obj *baseObject) Scan(
	ctx context.Context,
	bat **containers.Batch,
	txn txnif.TxnReader,
	readSchema any,
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (err error) {
	node := obj.PinNode()
	defer node.Unref()
	return node.Scan(ctx, bat, txn, readSchema.(*catalog.Schema), blkID, colIdxes, mp)
}

func (obj *baseObject) FillBlockTombstones(
	ctx context.Context,
	txn txnif.TxnReader,
	blkID *objectio.Blockid,
	deletes **nulls.Nulls,
	mp *mpool.MPool) error {
	node := obj.PinNode()
	defer node.Unref()
	if !obj.meta.Load().IsTombstone {
		panic("logic err")
	}
	return node.FillBlockTombstones(ctx, txn, blkID, deletes, mp)
}

func (obj *baseObject) ScanInMemory(
	ctx context.Context,
	batches map[uint32]*containers.BatchWithVersion,
	start, end types.TS,
	mp *mpool.MPool,
) (err error) {
	node := obj.PinNode()
	defer node.Unref()
	if node.IsPersisted() {
		return nil
	}
	mnode := node.MustMNode()
	return mnode.getDataWindowOnWriteSchema(ctx, batches, start, end, mp)
}

func (obj *baseObject) CollectObjectTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	objID *types.Objectid,
	bat **containers.Batch,
	mp *mpool.MPool,
	vpool *containers.VectorPool,
) (err error) {
	if !obj.meta.Load().IsTombstone {
		panic("logic err")
	}
	node := obj.PinNode()
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
