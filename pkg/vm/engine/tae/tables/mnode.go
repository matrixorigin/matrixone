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

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
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
	lower, to types.TS,
	skipFn func(uint32) error,
	mp *mpool.MPool,
) (err error) {
	blkID := objectio.NewBlockidWithObjectID(node.object.meta.Load().ID(), 0)
	// ART narrows the work to rows with matching keys. AppendNode Prepare and
	// its physical row range are stable while the caller holds object.RLock.
	rowInWindow := func(row uint32) bool {
		appendnode := node.object.appendMVCC.GetAppendNodeByRowLocked(row)
		if appendnode == nil || appendnode.IsAborted() {
			return false
		}
		return appendnode.Prepare.GT(&lower) && appendnode.Prepare.LE(&to)
	}
	return node.pkIndex.GetDuplicatedRows(
		ctx,
		keys.GetDownstreamVector(),
		keysZM,
		&blkID,
		rowIDs.GetDownstreamVector(),
		rowInWindow,
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
	var selection index.RowSelection
	var commitTSVec, abort containers.Vector
	preserveOffsets := preservePhysicalRows(ctx)
	if preserveOffsets {
		selection, commitTSVec, abort = node.object.appendMVCC.CollectAppendSnapshotLocked(end, mp)
	} else {
		selection, commitTSVec, abort = node.object.appendMVCC.CollectAppendLocked(start, end, mp)
	}
	if commitTSVec == nil {
		return nil
	}
	var inner *containers.Batch
	if preserveOffsets {
		inner = node.data.CloneWindowWithPool(0, int(selection.MaxRow), node.object.rt.VectorPool.Transient)
	} else {
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
	}
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
	spec, err := readSchema.ColumnReadSpec()
	if err != nil {
		return err
	}
	var maxSeqnum uint16
	if spec != nil {
		maxSeqnum = node.writeSchema.MaxSeqnum()
	}
	fresh := *bat == nil
	if fresh {
		*bat = containers.NewBatchWithCapacity(len(colIdxes))
		defer func() {
			if err != nil {
				(*bat).Close()
				*bat = nil
			}
		}()
	}
	pool := node.object.rt.VectorPool.Transient
	rows := int(to - from)
	for _, colIdx := range colIdxes {
		switch colIdx {
		case objectio.SEQNUM_COMMITTS:
			if fresh {
				typ := types.T_TS.ToType()
				(*bat).AddVector(objectio.TombstoneAttr_CommitTs_Attr, pool.GetVector(&typ))
			}
			continue
		case objectio.SEQNUM_ABORT:
			if fresh {
				typ := types.T_bool.ToType()
				(*bat).AddVector(objectio.TombstoneAttr_Abort_Attr, pool.GetVector(&typ))
			}
			continue
		}
		col := readSchema.ColDefs[colIdx]
		idx, present := node.writeSchema.SeqnumMap[col.SeqNum]
		switch spec.Classify(col.SeqNum, maxSeqnum, present) {
		case objectio.ColPhysical:
			src := node.data.Vecs[idx]
			if fresh {
				(*bat).AddVector(col.Name, src.CloneWindowWithPool(int(from), rows, pool))
			} else {
				(*bat).GetVectorByName(col.Name).ExtendWithOffset(src, int(from), rows)
			}
		case objectio.ColVirtualDefault:
			if fresh {
				(*bat).AddVector(col.Name, pool.GetVector(&col.Type))
			}
			dst := (*bat).GetVectorByName(col.Name)
			if err = spec.AppendMissingRows(col.SeqNum, dst.GetDownstreamVector(), rows, dst.GetAllocator()); err != nil {
				return err
			}
		case objectio.ColCorruptedMissing:
			return moerr.NewInternalErrorNoCtxf("column sequence %d is missing within memory node layout ending at %d", col.SeqNum, maxSeqnum)
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
	if err = node.appendMissingWALColumns(bat, rows); err != nil {
		return
	}
	for srcPos, attr := range bat.Attrs {
		idx := schema.GetColIdx(attr)
		if idx < 0 {
			continue
		}
		destVec := node.data.Vecs[idx]
		destVec.Extend(bat.Vecs[srcPos])
	}
	if logicalID := node.legacyLogicalID(bat); logicalID != nil {
		node.data.GetVectorByName(pkgcatalog.SystemRelAttr_LogicalID).Extend(logicalID)
	}
	// Align remaining legacy columns that have no evolution metadata.
	node.ReserveRowsLocked(uint32(from + rows))
	return
}

func (node *memoryNode) missingWALColumns(bat *containers.Batch) (*objectio.ColumnReadSpec, []*catalog.ColDef, error) {
	schema := node.writeSchema
	if !features.IsSchemaEvolved(schema.Extra.FeatureFlag) {
		return nil, nil, nil
	}
	var missing []*catalog.ColDef
	var maxSeqnum uint16
	for _, col := range schema.ColDefs {
		if col.PhyAddr {
			continue
		}
		if _, present := bat.Nameidx[col.Name]; present {
			maxSeqnum = max(maxSeqnum, col.SeqNum)
		} else {
			missing = append(missing, col)
		}
	}
	if len(missing) == 0 {
		return nil, nil, nil
	}
	spec, err := schema.ColumnReadSpec()
	if err != nil {
		return nil, nil, err
	}
	for _, col := range missing {
		if spec.Classify(col.SeqNum, maxSeqnum, false) == objectio.ColCorruptedMissing {
			return nil, nil, moerr.NewInternalErrorNoCtxf("column sequence %d is missing within append layout ending at %d", col.SeqNum, maxSeqnum)
		}
	}
	return spec, missing, nil
}

// appendMissingWALColumns fills absent columns before mutating supplied
// values. Roll back the fills if an allocation fails.
func (node *memoryNode) appendMissingWALColumns(bat *containers.Batch, rows int) error {
	spec, missing, err := node.missingWALColumns(bat)
	if err != nil || len(missing) == 0 {
		return err
	}
	checkpoints := make([]vector.AppendCheckpoint, len(missing))
	for i, col := range missing {
		vec := node.data.Vecs[col.Idx]
		dst := vec.GetDownstreamVector()
		checkpoints[i] = dst.MakeAppendCheckpoint()
		if err = spec.AppendMissingRows(col.SeqNum, dst, rows, vec.GetAllocator()); err != nil {
			for j, filled := range missing[:i+1] {
				err = vector.JoinAppendRollbackError(err, node.data.Vecs[filled.Idx].GetDownstreamVector().RollbackAppend(checkpoints[j], rows))
			}
			return err
		}
	}
	return nil
}

func (node *memoryNode) ApplyAppendAtLocked(bat *containers.Batch, offset uint32) (err error) {
	schema := node.writeSchema
	data := node.mustData()
	rows := bat.Length()
	spec, missing, err := node.missingWALColumns(bat)
	if err != nil {
		return err
	}
	if len(missing) != 0 {
		if err = fillMissingWALColumnsAt(data, spec, missing, int(offset), rows); err != nil {
			return err
		}
	}
	node.ReserveRowsLocked(offset + uint32(rows))
	for srcPos, attr := range bat.Attrs {
		idx := schema.GetColIdx(attr)
		if idx < 0 {
			continue
		}
		destVec := data.Vecs[idx]
		for row := range rows {
			destVec.CopyFrom(bat.Vecs[srcPos], int(offset)+row, row)
		}
	}
	if logicalID := node.legacyLogicalID(bat); logicalID != nil {
		destVec := data.GetVectorByName(pkgcatalog.SystemRelAttr_LogicalID)
		for row := range rows {
			destVec.CopyFrom(logicalID, int(offset)+row, row)
		}
	}
	return nil
}

// Prepare positional fills separately because replay can overwrite existing rows.
// Publish only after every missing column has been filled successfully.
func fillMissingWALColumnsAt(data *containers.Batch, spec *objectio.ColumnReadSpec, missing []*catalog.ColDef, offset, rows int) error {
	staged := make([]containers.Vector, 0, len(missing))
	defer func() {
		for _, vec := range staged {
			vec.Close()
		}
	}()
	for _, col := range missing {
		current := data.Vecs[col.Idx]
		mp := current.GetAllocator()
		next, err := containers.CloneVector(current.GetDownstreamVector(), mp, nil)
		if err != nil {
			return err
		}
		staged = append(staged, next)
		dst := next.GetDownstreamVector()
		if grow := offset + rows - dst.Length(); grow > 0 {
			if err := vector.AppendMultiFixed(dst, byte(0), true, grow, mp); err != nil {
				return err
			}
		}
		src, err := spec.MaterializeMissingVector(col.SeqNum, col.Type, 1, mp)
		if err != nil {
			return err
		}
		for row := range rows {
			if err = dst.Copy(src, int64(offset+row), 0, mp); err != nil {
				break
			}
		}
		src.Free(mp)
		if err != nil {
			return err
		}
	}
	for i, col := range missing {
		data.Vecs[col.Idx], staged[i] = staged[i], data.Vecs[col.Idx]
	}
	return nil
}

// legacyLogicalID preserves the pre-logical-ID mo_tables WAL contract for
// both sequential and positional installation. An explicit column, even NULL,
// is authoritative; only an absent legacy column is derived from rel_id.
func (node *memoryNode) legacyLogicalID(bat *containers.Batch) containers.Vector {
	if node.writeSchema.GetColIdx(pkgcatalog.SystemRelAttr_LogicalID) < 0 ||
		node.object.meta.Load().GetTable().ID != pkgcatalog.MO_TABLES_ID {
		return nil
	}
	if _, supplied := bat.Nameidx[pkgcatalog.SystemRelAttr_LogicalID]; supplied {
		return nil
	}
	return bat.GetVectorByName(pkgcatalog.SystemRelAttr_ID)
}

func (node *memoryNode) ReserveRowsLocked(rows uint32) {
	data := node.mustData()
	for _, vec := range data.Vecs {
		missing := int(rows) - vec.Length()
		if missing == 1 {
			vec.Append(nil, true)
		} else if missing > 1 {
			// Memory-node vectors own their mutable backing. Reuse vector's
			// bulk growth and metadata accounting without a temporary vector.
			if err := vector.AppendMultiFixed(vec.GetDownstreamVector(), 0, true, missing, vec.GetAllocator()); err != nil {
				panic(err)
			}
		}
	}
}

func (node *memoryNode) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	lower, to types.TS,
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
	mp *mpool.MPool,
) (err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	checkFn := node.checkConflictLocked(txn)
	err = node.getDuplicatedRowsLocked(ctx, keys, keysZM, rowIDs, lower, to, checkFn, mp)

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
	if err = node.getDataWindowLocked(bat, readSchema, colIdxes, 0, selection.MaxRow, mp); err != nil {
		return err
	}
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
