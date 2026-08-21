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
	"slices"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
)

var _ NodeT = (*memoryNode)(nil)

type memoryNode struct {
	common.RefHelper
	object      *baseObject
	writeSchema *catalog.Schema
	data        *containers.Batch

	//index for primary key : Art tree + ZoneMap.
	pkIndex *indexwrapper.MutIndex
}

func newMemoryNode(object *baseObject, isTombstone bool) *memoryNode {
	impl := new(memoryNode)
	impl.object = object

	// Get the lastest schema, it will not be modified, so just keep the pointer
	schema := object.meta.Load().GetTable().GetLastestSchemaLocked(isTombstone)
	impl.writeSchema = schema
	// impl.data = containers.BuildBatchWithPool(
	// 	schema.AllNames(), schema.AllTypes(), 0, object.rt.VectorPool.Memtable,
	// )
	impl.initPKIndex(schema)
	impl.OnZeroCB = impl.close
	return impl
}

func (node *memoryNode) mustData() *containers.Batch {
	if node.data != nil {
		return node.data
	}
	schema := node.writeSchema
	opts := containers.Options{
		Allocator: common.MutMemAllocator,
	}
	node.data = containers.BuildBatch(
		schema.AllNames(), schema.AllTypes(), opts,
	)
	return node.data
}

func (node *memoryNode) initPKIndex(schema *catalog.Schema) {
	if !schema.HasPK() {
		return
	}
	pkDef := schema.GetSingleSortKey()
	node.pkIndex = indexwrapper.NewMutIndex(pkDef.Type)
}

func (node *memoryNode) close() {
	mvcc := node.object.appendMVCC
	logutil.Debugf("Releasing Memorynode BLK-%s", node.object.meta.Load().ID().String())
	if node.data != nil {
		node.data.Close()
		node.data = nil
	}
	if node.pkIndex != nil {
		node.pkIndex.Close()
		node.pkIndex = nil
	}
	node.object = nil
	mvcc.ReleaseAppends()
}

func (node *memoryNode) IsPersisted() bool { return false }

func (node *memoryNode) Contains(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	txn txnif.TxnReader,
	mp *mpool.MPool,
) (err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	blkID := objectio.NewBlockidWithObjectID(node.object.meta.Load().ID(), 0)
	return node.pkIndex.Contains(ctx, keys.GetDownstreamVector(), keysZM, &blkID, node.checkConflictLocked(txn), mp)
}
func (node *memoryNode) getDuplicatedRowsLocked(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
	getRowSelection func() (index.RowSelection, error),
	skipFn func(uint32) error,
	mp *mpool.MPool,
) (err error) {
	blkID := objectio.NewBlockidWithObjectID(node.object.meta.Load().ID(), 0)
	return node.pkIndex.GetDuplicatedRows(
		ctx,
		keys.GetDownstreamVector(),
		keysZM,
		&blkID,
		rowIDs.GetDownstreamVector(),
		getRowSelection,
		skipFn,
		mp)
}

func (node *memoryNode) Rows() (uint32, error) {
	if node.data == nil {
		return 0, nil
	}
	return uint32(node.data.Length()), nil
}

func (node *memoryNode) EstimateMemSizeLocked() int {
	if node.data == nil {
		return 0
	}
	return node.data.ApproxSize()
}

func (node *memoryNode) getDataWindowOnWriteSchema(
	ctx context.Context,
	batches map[uint32]*containers.BatchWithVersion,
	start, end types.TS, mp *mpool.MPool,
) (err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	if node.data == nil {
		return nil
	}
	selection, commitTSVec, abort :=
		node.object.appendMVCC.CollectAppendLocked(start, end, mp)
	if commitTSVec == nil {
		return nil
	}
	var inner *containers.Batch
	selection.ForEachRange(func(from, to uint32) bool {
		window := node.data.CloneWindowWithPool(int(from), int(to-from), node.object.rt.VectorPool.Transient)
		if inner == nil {
			inner = window
		} else {
			inner.Extend(window)
			window.Close()
		}
		return true
	})
	if inner == nil {
		commitTSVec.Close()
		abort.Close()
		return nil
	}
	dest, ok := batches[node.writeSchema.Version]
	persistAbort := node.object.rt.PersistedAObjectAbortSupported()
	if ok {
		// One range collection may visit multiple objects while the rollout gate
		// changes. Keep the schema chosen by the first batch internally stable.
		_, persistAbort = dest.Nameidx[objectio.TombstoneAttr_Abort_Attr]
	}
	inner.AddVector(objectio.TombstoneAttr_CommitTs_Attr, commitTSVec)
	if persistAbort {
		inner.AddVector(objectio.TombstoneAttr_Abort_Attr, abort)
	} else {
		// Old readers interpret every non-rowid/non-TS physical column as user
		// data. Keep their commitTS-only layout, preserve the original physical
		// row coordinates, and encode rollback holes as uncommitted rows so old
		// readers filter them by commit timestamp.
		aborts := vector.MustFixedColWithTypeCheck[bool](abort.GetDownstreamVector())
		for row, aborted := range aborts {
			if !aborted {
				continue
			}
			commitTSVec.Update(row, txnif.UncommitTS, false)
			for _, colDef := range node.writeSchema.ColDefs {
				if colDef.IsRealPrimary() && !node.writeSchema.IsSecondaryIndexTable() {
					inner.Vecs[colDef.Idx].Update(row, nil, true)
				}
			}
		}
		abort.Close()
	}
	if inner.Length() == 0 {
		inner.Close()
		return nil
	}
	if ok {
		dest.Extend(inner)
		inner.Close()
	} else {
		batWithVer := &containers.BatchWithVersion{
			Version:    node.writeSchema.Version,
			NextSeqnum: uint16(node.writeSchema.Extra.NextColSeqnum),
			Seqnums:    node.writeSchema.AllSeqnums(),
			Batch:      inner,
		}
		batWithVer.Seqnums = append(batWithVer.Seqnums, objectio.SEQNUM_COMMITTS)
		if persistAbort {
			batWithVer.Seqnums = append(batWithVer.Seqnums, objectio.SEQNUM_ABORT)
		}
		batches[node.writeSchema.Version] = batWithVer
	}
	return
}

func (node *memoryNode) getDataWindowLocked(
	bat **containers.Batch,
	readSchema *catalog.Schema,
	colIdxes []int,
	from, to uint32,
	mp *mpool.MPool,
) (err error) {
	if node.data == nil {
		return
	}

	if node.data.Deletes != nil {
		panic("not expect")
		// bat.Deletes = bat.WindowDeletes(int(from), int(to-from), false)
	}
	if *bat == nil {
		*bat = containers.NewBatchWithCapacity(len(colIdxes))
		for _, colIdx := range colIdxes {
			if colIdx == objectio.SEQNUM_COMMITTS {
				typ := types.T_TS.ToType()
				vec := node.object.rt.VectorPool.Transient.GetVector(&typ)
				(*bat).AddVector(objectio.TombstoneAttr_CommitTs_Attr, vec)
				continue
			}
			if colIdx == objectio.SEQNUM_ABORT {
				typ := types.T_bool.ToType()
				vec := node.object.rt.VectorPool.Transient.GetVector(&typ)
				(*bat).AddVector(objectio.TombstoneAttr_Abort_Attr, vec)
				continue
			}
			colDef := readSchema.ColDefs[colIdx]
			idx, ok := node.writeSchema.SeqnumMap[colDef.SeqNum]
			var vec containers.Vector
			if !ok {
				vec = node.object.rt.VectorPool.Transient.GetVector(&colDef.Type) // TODO
				for i := from; i < to; i++ {
					vec.Append(nil, true)
				}
			} else {
				vec = node.data.Vecs[idx].CloneWindowWithPool(int(from), int(to-from), node.object.rt.VectorPool.Transient)
			}
			(*bat).AddVector(colDef.Name, vec)
		}
	} else {
		for _, colIdx := range colIdxes {
			if colIdx == objectio.SEQNUM_COMMITTS || colIdx == objectio.SEQNUM_ABORT {
				continue
			}
			colDef := readSchema.ColDefs[colIdx]
			idx, ok := node.writeSchema.SeqnumMap[colDef.SeqNum]
			var vec containers.Vector
			if !ok {
				vec = containers.NewConstNullVector(colDef.Type, int(to-from), mp)
				(*bat).GetVectorByName(colDef.Name).Extend(vec) // TODO
			} else {
				(*bat).GetVectorByName(colDef.Name).Extend(node.data.Vecs[idx])
			}
		}
	}
	return
}

func (node *memoryNode) ApplyAppendLocked(
	bat *containers.Batch,
) (from int, err error) {
	schema := node.writeSchema
	from = int(node.mustData().Length())
	rows := bat.Length()
	for srcPos, attr := range bat.Attrs {
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec := node.data.Vecs[def.Idx]
		destVec.Extend(bat.Vecs[srcPos])
	}
	// RelLogicalID COMPAT
	if node.object.meta.Load().GetTable().ID == 2 && len(node.data.Vecs) > 10 /*not tombstone*/ {
		in_logical_idx := slices.Index(bat.Attrs, pkgcatalog.SystemRelAttr_LogicalID)
		if in_logical_idx == -1 {
			desc := node.data.GetVectorByName(pkgcatalog.SystemRelAttr_LogicalID)
			desc.Extend(bat.GetVectorByName(pkgcatalog.SystemRelAttr_ID))
		}
	}
	// Upgrade compat: replayed WAL batches may be encoded with an older schema
	// and omit columns introduced later. Pad those missing columns with NULLs
	// for the appended rows so every in-memory vector stays aligned.
	expectedLen := from + rows
	for _, destVec := range node.data.Vecs {
		for destVec.Length() < expectedLen {
			destVec.Append(nil, true)
		}
	}
	return
}

func (node *memoryNode) ApplyAppendAtLocked(bat *containers.Batch, offset uint32) (err error) {
	schema := node.writeSchema
	data := node.mustData()
	end := int(offset) + bat.Length()
	for _, destVec := range data.Vecs {
		for destVec.Length() < end {
			destVec.Append(nil, true)
		}
	}
	for srcPos, attr := range bat.Attrs {
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec := data.Vecs[def.Idx]
		for row := 0; row < bat.Length(); row++ {
			destVec.Update(int(offset)+row, bat.Vecs[srcPos].Get(row), bat.Vecs[srcPos].IsNull(row))
		}
	}
	return nil
}

func (node *memoryNode) ReserveRowsLocked(rows uint32) {
	data := node.mustData()
	for _, vec := range data.Vecs {
		for vec.Length() < int(rows) {
			vec.Append(nil, true)
		}
	}
}

func (node *memoryNode) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	getRowSelection func() (index.RowSelection, error),
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
	mp *mpool.MPool,
) (err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	checkFn := node.checkConflictLocked(txn)
	err = node.getDuplicatedRowsLocked(ctx, keys, keysZM, rowIDs, getRowSelection, checkFn, mp)

	return
}

func (node *memoryNode) checkConflictLocked(
	txn txnif.TxnReader,
) func(row uint32) error {
	return func(row uint32) error {
		appendnode := node.object.appendMVCC.GetAppendNodeByRowLocked(row)
		if appendnode == nil {
			return index.ErrNotFound
		}
		if appendnode.IsAborted() {
			return index.ErrNotFound
		}
		// IMPORTANT: THIS RELIES ON THE PREPARE-QUEUE SERIALIZATION CONTRACT.
		//
		// PrePrepare dedup and PrepareTS assignment run in the same worker. The
		// worker finishes both for one transaction before processing the next,
		// so transactions cannot interleave between those two operations. Thus,
		// every transaction ahead of the current one in the queue must already
		// have a non-UncommitTS PrepareTS. An UncommitTS append belongs to a
		// transaction that has not reached its queue slot yet. Skip that candidate
		// entirely: merely suppressing its write-write conflict would still let it
		// participate in snapshot dedup and point lookup. If queue processing stops
		// being serial, this assumption and the dedup protocol must be revisited
		// together.
		if appendnode.GetPrepare() == txnif.UncommitTS {
			return index.ErrNotFound
		}
		// Deletes generated by merge/flush is ignored when check w-w in batchDedup
		if appendnode.IsMergeCompact() {
			return nil
		}
		if appendnode.IsActive() {
			if appendnode.IsSameTxn(txn) {
				return nil
			}
			return moerr.NewTxnWWConflictNoCtx(0, "")
		}
		return appendnode.CheckConflict(txn)
	}
}

func (node *memoryNode) allRowsCommittedBefore(ts types.TS) bool {
	node.object.RLock()
	defer node.object.RUnlock()
	return node.object.appendMVCC.AllAppendsCommittedBeforeLocked(ts)
}

func (node *memoryNode) Scan(
	ctx context.Context,
	bat **containers.Batch,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (err error) {
	if blkID != 0 {
		panic("logic err")
	}
	node.object.RLock()
	defer node.object.RUnlock()
	selection, visible, err := node.object.appendMVCC.GetVisibleRowLocked(ctx, txn)
	if !visible || err != nil {
		// blk.RUnlock()
		return
	}
	rowOffset := 0
	if *bat != nil {
		rowOffset = (*bat).Length()
	}
	err = node.getDataWindowLocked(
		bat,
		readSchema,
		colIdxes,
		0,
		selection.MaxRow,
		mp,
	)
	for _, idx := range colIdxes {
		if idx == objectio.SEQNUM_COMMITTS {
			node.object.appendMVCC.FillInCommitTSVecLocked(
				(*bat).GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr), selection.MaxRow, mp)
		}
	}
	if selection.Holes != nil {
		selection.Holes.Foreach(func(row uint64) bool {
			(*bat).Delete(rowOffset + int(row))
			return true
		})
	}
	return
}

func (node *memoryNode) CollectObjectTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	objID *types.Objectid,
	bat **containers.Batch,
	mp *mpool.MPool,
	vpool *containers.VectorPool,
) (err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	selection, commitTSVec, abort :=
		node.object.appendMVCC.CollectAppendLocked(start, end, mp)
	if commitTSVec == nil {
		return nil
	}
	defer commitTSVec.Close()
	defer abort.Close()
	rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](
		node.data.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).GetDownstreamVector())
	commitTSs := vector.MustFixedColWithTypeCheck[types.TS](commitTSVec.GetDownstreamVector())
	aborts := vector.MustFixedColWithTypeCheck[bool](abort.GetDownstreamVector())
	pkVec := node.data.GetVectorByName(objectio.TombstoneAttr_PK_Attr)
	selectedOffset := 0
	for i := selection.MinRow; i < selection.MaxRow; i++ {
		if !selection.Contains(i) {
			continue
		}
		if aborts[selectedOffset] {
			selectedOffset++
			continue
		}
		if types.PrefixCompare(rowIDs[i][:], objID[:]) == 0 {
			if *bat == nil {
				*bat = catalog.NewTombstoneBatchByPKType(*pkVec.GetType(), mp)
			}
			(*bat).GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).Append(rowIDs[i], false)
			(*bat).GetVectorByName(objectio.TombstoneAttr_PK_Attr).Append(pkVec.Get(int(i)), false)
			(*bat).GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).Append(commitTSs[selectedOffset], false)
		}
		selectedOffset++
	}
	return
}

func (node *memoryNode) FillBlockTombstones(
	ctx context.Context,
	txn txnif.TxnReader,
	blkID *objectio.Blockid,
	deletes **nulls.Nulls,
	deleteStartOffset uint64,
	mp *mpool.MPool) error {
	node.object.RLock()
	defer node.object.RUnlock()
	selection, visible, err := node.object.appendMVCC.GetVisibleRowLocked(ctx, txn)
	if !visible || err != nil {
		// blk.RUnlock()
		return err
	}
	rowIDVec := node.data.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr)
	rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](rowIDVec.GetDownstreamVector())
	for i := 0; i < int(selection.MaxRow); i++ {
		if !selection.Contains(uint32(i)) {
			continue
		}
		rowID := rowIDs[i]
		if types.PrefixCompare(rowID[:], blkID[:]) == 0 {
			if *deletes == nil {
				*deletes = &nulls.Nulls{}
			}
			offset := rowID.GetRowOffset()
			(*deletes).Add(uint64(offset) + deleteStartOffset)
		}
	}
	return nil
}
