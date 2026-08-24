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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
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

func lookupTNBatchVector(bat *containers.Batch, attr string) containers.Vector {
	if bat == nil {
		return nil
	}
	pos, ok := bat.Nameidx[attr]
	if !ok || pos < 0 || pos >= len(bat.Vecs) || pos >= len(bat.Attrs) ||
		bat.Attrs[pos] != attr {
		return nil
	}
	return bat.Vecs[pos]
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
	getRowOffset func() (min, max int32, err error),
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
		getRowOffset,
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
	if node == nil || node.object == nil || node.object.appendMVCC == nil ||
		node.writeSchema == nil {
		return moerr.NewInternalErrorNoCtx(
			"append-window collection has no object, append state, or schema",
		)
	}
	if ctx == nil || batches == nil || mp == nil {
		return moerr.NewInvalidInputNoCtx(
			"append-window collection requires context, destination, and mpool",
		)
	}
	if start.GT(&end) {
		return moerr.NewInvalidInputNoCtx(
			"append-window collection start timestamp is after end timestamp",
		)
	}
	if node.object.rt == nil || node.object.rt.VectorPool.Transient == nil {
		return moerr.NewInternalErrorNoCtx(
			"append-window collection has no runtime or transient vector pool",
		)
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	node.object.RLock()
	defer node.object.RUnlock()
	if node.data == nil {
		return nil
	}
	from, to, commitTSVec, abort, _ :=
		node.object.appendMVCC.CollectAppendLocked(start, end, mp)
	if commitTSVec == nil {
		if abort != nil {
			abort.Close()
		}
		return nil
	}
	if abort == nil {
		commitTSVec.Close()
		return moerr.NewInternalErrorNoCtx("append MVCC returned commit timestamps without abort state")
	}
	commitOwned, abortOwned := true, abort != nil
	defer func() {
		if commitOwned {
			commitTSVec.Close()
		}
		if abortOwned {
			abort.Close()
		}
	}()
	if to < from || uint64(to) > uint64(node.data.Length()) {
		return moerr.NewInternalErrorNoCtxf(
			"append MVCC returned invalid row window [%d,%d) for %d rows",
			from, to, node.data.Length(),
		)
	}
	windowRows := int(to - from)
	if _, validateErr := ioutil.ValidateTombstoneCommitTSColumn(
		windowRows, commitTSVec.GetDownstreamVector(),
	); validateErr != nil {
		return validateErr
	}
	validatedAborts, validateErr := ioutil.ValidateTombstoneAbortColumn(
		windowRows, abort.GetDownstreamVector(),
	)
	if validateErr != nil {
		return validateErr
	}
	dest, ok := batches[node.writeSchema.Version]
	persistAbort := node.object.rt.PersistedAObjectAbortSupported()
	if ok {
		if dest == nil || dest.Batch == nil {
			return moerr.NewInternalErrorNoCtx(
				"append-window destination batch is nil",
			)
		}
		// One range collection may visit multiple objects while the rollout gate
		// changes. Keep the schema chosen by the first batch internally stable.
		_, persistAbort = dest.Nameidx[objectio.TombstoneAttr_Abort_Attr]
	}
	inner := node.data.CloneWindowWithPool(
		int(from), windowRows, node.object.rt.VectorPool.Transient)
	innerOwned := true
	defer func() {
		if innerOwned {
			inner.Close()
		}
	}()
	inner.AddVector(objectio.TombstoneAttr_CommitTs_Attr, commitTSVec)
	commitOwned = false
	if persistAbort {
		inner.AddVector(objectio.TombstoneAttr_Abort_Attr, abort)
		abortOwned = false
	} else {
		// Old readers interpret every non-rowid/non-TS physical column as user
		// data. Keep their commitTS-only layout, preserve the original physical
		// row coordinates, and encode rollback holes as uncommitted rows so old
		// readers filter them by commit timestamp.
		for row := 0; row < windowRows; row++ {
			if validatedAborts.IsPresent() && validatedAborts.At(row) {
				commitTSVec.Update(row, txnif.UncommitTS, false)
			}
		}
	}
	if inner.Length() == 0 {
		return nil
	}
	if ok {
		if _, err = appendTNBatchVectorsAtomic(dest.Batch, inner.Attrs, inner.Vecs, mp); err != nil {
			return err
		}
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
		innerOwned = false
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
		return moerr.NewInternalErrorNoCtx("in-memory scan has no object data")
	}
	if from > to || int(to) > node.data.Length() {
		return moerr.NewInternalErrorNoCtxf(
			"in-memory scan window [%d,%d) exceeds %d rows", from, to, node.data.Length(),
		)
	}
	if node.data.Deletes != nil {
		return moerr.NewInternalErrorNoCtx(
			"in-memory object data unexpectedly contains a delete mask",
		)
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
			if colDef == nil {
				return moerr.NewInternalErrorNoCtxf(
					"in-memory scan schema column %d is nil", colIdx,
				)
			}
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
			target := lookupTNBatchVector(*bat, colDef.Name)
			if target == nil || *target.GetType() != colDef.Type {
				return moerr.NewInternalErrorNoCtxf(
					"in-memory scan output column %q is missing or incompatible", colDef.Name,
				)
			}
			if !ok {
				vec := containers.NewConstNullVector(colDef.Type, int(to-from), mp)
				target.Extend(vec)
				vec.Close()
			} else {
				target.ExtendWithOffset(node.data.Vecs[idx], int(from), int(to-from))
			}
		}
	}
	return
}

func (node *memoryNode) ApplyAppendLocked(
	bat *containers.Batch,
) (from int, err error) {
	if node == nil || node.object == nil || node.writeSchema == nil || bat == nil ||
		len(bat.Attrs) == 0 || len(bat.Attrs) != len(bat.Vecs) || bat.Vecs[0] == nil {
		return 0, moerr.NewInvalidInputNoCtx(
			"in-memory append requires object, schema, and a rectangular source batch",
		)
	}
	schema := node.writeSchema
	rows := bat.Length()
	dest := node.data
	if dest != nil {
		from = int(dest.Length())
		for pos, destVec := range dest.Vecs {
			if destVec == nil || destVec.Length() != from {
				return 0, moerr.NewInternalErrorNoCtxf(
					"in-memory append destination column %d is not rectangular", pos,
				)
			}
		}
	}
	seenDestinations := make(map[int]struct{}, len(bat.Attrs))
	for srcPos, attr := range bat.Attrs {
		colIdx := schema.GetColIdx(attr)
		if colIdx < 0 || colIdx >= len(schema.ColDefs) || bat.Vecs[srcPos] == nil ||
			bat.Vecs[srcPos].Length() != rows {
			return 0, moerr.NewInvalidInputNoCtxf(
				"in-memory append source column %q is missing or malformed", attr,
			)
		}
		def := schema.ColDefs[colIdx]
		if def == nil {
			return 0, moerr.NewInternalErrorNoCtxf(
				"in-memory append schema column %d is nil", colIdx,
			)
		}
		if def.Idx < 0 || def.Idx >= len(schema.ColDefs) ||
			def.Type != *bat.Vecs[srcPos].GetType() {
			return 0, moerr.NewInvalidInputNoCtxf(
				"in-memory append source column %q is incompatible", attr,
			)
		}
		if dest != nil && (def.Idx >= len(dest.Vecs) || dest.Vecs[def.Idx] == nil ||
			*dest.Vecs[def.Idx].GetType() != def.Type) {
			return 0, moerr.NewInternalErrorNoCtxf(
				"in-memory append destination column %q is incompatible", attr,
			)
		}
		if _, duplicate := seenDestinations[def.Idx]; duplicate {
			return 0, moerr.NewInvalidInputNoCtxf(
				"in-memory append maps multiple columns to %q", attr,
			)
		}
		seenDestinations[def.Idx] = struct{}{}
	}
	logicalIDCompat := false
	if meta := node.object.meta.Load(); meta != nil && meta.GetTable() != nil &&
		meta.GetTable().ID == 2 && len(schema.ColDefs) > 10 {
		logicalIDCompat = slices.Index(bat.Attrs, pkgcatalog.SystemRelAttr_LogicalID) == -1
		if logicalIDCompat {
			idSource := lookupTNBatchVector(bat, pkgcatalog.SystemRelAttr_ID)
			logicalIdx := schema.GetColIdx(pkgcatalog.SystemRelAttr_LogicalID)
			if idSource == nil || idSource.Length() != rows || logicalIdx < 0 ||
				logicalIdx >= len(schema.ColDefs) || schema.ColDefs[logicalIdx] == nil ||
				*idSource.GetType() != schema.ColDefs[logicalIdx].Type {
				return 0, moerr.NewInvalidInputNoCtx(
					"system relation logical-id compatibility requires a valid relation id",
				)
			}
		}
	}
	if dest == nil {
		dest = node.mustData()
		if dest == nil || len(dest.Vecs) != len(schema.ColDefs) {
			return 0, moerr.NewInternalErrorNoCtx(
				"in-memory append could not initialize its destination",
			)
		}
		for pos, destVec := range dest.Vecs {
			if destVec == nil || destVec.Length() != 0 {
				return 0, moerr.NewInternalErrorNoCtxf(
					"in-memory append destination column %d is not empty", pos,
				)
			}
		}
	}
	if logicalIDCompat {
		idSource := lookupTNBatchVector(bat, pkgcatalog.SystemRelAttr_ID)
		logicalDest := lookupTNBatchVector(dest, pkgcatalog.SystemRelAttr_LogicalID)
		if logicalDest == nil || *idSource.GetType() != *logicalDest.GetType() {
			return 0, moerr.NewInternalErrorNoCtx(
				"system relation logical-id destination is missing or incompatible",
			)
		}
	}
	for srcPos, attr := range bat.Attrs {
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec := dest.Vecs[def.Idx]
		destVec.Extend(bat.Vecs[srcPos])
	}
	// RelLogicalID COMPAT
	if logicalIDCompat {
		desc := lookupTNBatchVector(dest, pkgcatalog.SystemRelAttr_LogicalID)
		desc.Extend(lookupTNBatchVector(bat, pkgcatalog.SystemRelAttr_ID))
	}
	// Upgrade compat: replayed WAL batches may be encoded with an older schema
	// and omit columns introduced later. Pad those missing columns with NULLs
	// for the appended rows so every in-memory vector stays aligned.
	expectedLen := from + rows
	for _, destVec := range dest.Vecs {
		for destVec.Length() < expectedLen {
			destVec.Append(nil, true)
		}
		if destVec.Length() != expectedLen {
			return 0, moerr.NewInternalErrorNoCtx(
				"in-memory append produced a non-rectangular destination",
			)
		}
	}
	return
}

func (node *memoryNode) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	getRowOffset func() (min, max int32, err error),
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
	mp *mpool.MPool,
) (err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	checkFn := node.checkConflictLocked(txn)
	err = node.getDuplicatedRowsLocked(ctx, keys, keysZM, rowIDs, getRowOffset, checkFn, mp)

	return
}

func (node *memoryNode) checkConflictLocked(
	txn txnif.TxnReader,
) func(row uint32) error {
	return func(row uint32) error {
		appendnode := node.object.appendMVCC.GetAppendNodeByRowLocked(row)
		if appendnode.IsAborted() {
			return index.ErrNotFound
		}
		// Deletes generated by merge/flush is ignored when check w-w in batchDedup
		if appendnode.IsMergeCompact() {
			return nil
		}
		if appendnode.IsActive() {
			return moerr.NewInternalErrorNoCtx(
				"cannot check write conflict against an active append",
			)
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
	if node == nil || node.object == nil || node.object.appendMVCC == nil ||
		node.writeSchema == nil {
		return moerr.NewInternalErrorNoCtx(
			"in-memory scan has no object, append state, or write schema",
		)
	}
	if ctx == nil || bat == nil || txn == nil || readSchema == nil || mp == nil {
		return moerr.NewInvalidInputNoCtx(
			"in-memory scan requires context, output, transaction, schema, and mpool",
		)
	}
	if blkID != 0 {
		return moerr.NewInvalidInputNoCtxf(
			"in-memory object has only block 0, cannot scan block %d", blkID,
		)
	}
	if len(colIdxes) == 0 {
		return moerr.NewInvalidInputNoCtx("in-memory scan requires at least one column")
	}
	if node.object.rt == nil || node.object.rt.VectorPool.Transient == nil {
		return moerr.NewInternalErrorNoCtx("in-memory scan has no transient vector pool")
	}
	node.object.RLock()
	defer node.object.RUnlock()
	maxRow, visible, holes, err := node.object.appendMVCC.GetVisibleRowLocked(ctx, txn)
	if !visible || err != nil {
		return
	}
	if node.data == nil || int(maxRow) > node.data.Length() {
		return moerr.NewInternalErrorNoCtx("visible in-memory row range exceeds object data")
	}
	seenAttrs := make(map[string]struct{}, len(colIdxes))
	for _, colIdx := range colIdxes {
		var attr string
		var typ types.Type
		switch colIdx {
		case objectio.SEQNUM_COMMITTS:
			attr = objectio.TombstoneAttr_CommitTs_Attr
			typ = types.T_TS.ToType()
		case objectio.SEQNUM_ABORT:
			return moerr.NewInvalidInputNoCtx(
				"transaction-visible in-memory scan does not expose the abort column",
			)
		default:
			if colIdx < 0 || colIdx >= len(readSchema.ColDefs) {
				return moerr.NewInvalidInputNoCtxf(
					"in-memory scan column %d is outside schema with %d columns",
					colIdx, len(readSchema.ColDefs),
				)
			}
			def := readSchema.ColDefs[colIdx]
			if def == nil {
				return moerr.NewInternalErrorNoCtxf(
					"in-memory scan schema column %d is nil", colIdx,
				)
			}
			attr, typ = def.Name, def.Type
			if sourcePos, ok := node.writeSchema.SeqnumMap[def.SeqNum]; ok &&
				(sourcePos < 0 || sourcePos >= len(node.data.Vecs) ||
					node.data.Vecs[sourcePos] == nil ||
					*node.data.Vecs[sourcePos].GetType() != def.Type) {
				return moerr.NewInternalErrorNoCtxf(
					"in-memory source column %q is missing or incompatible", def.Name,
				)
			}
		}
		if _, exists := seenAttrs[attr]; exists {
			return moerr.NewInvalidInputNoCtxf(
				"in-memory scan requests duplicate column %q", attr,
			)
		}
		seenAttrs[attr] = struct{}{}
		if *bat != nil {
			target := lookupTNBatchVector(*bat, attr)
			if target == nil || *target.GetType() != typ {
				return moerr.NewInvalidInputNoCtxf(
					"in-memory scan output column %q is missing or incompatible", attr,
				)
			}
		}
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
		maxRow,
		mp,
	)
	if err != nil {
		return err
	}
	for _, idx := range colIdxes {
		if idx == objectio.SEQNUM_COMMITTS {
			commitTSVec := lookupTNBatchVector(
				*bat, objectio.TombstoneAttr_CommitTs_Attr,
			)
			if commitTSVec == nil {
				return moerr.NewInternalErrorNoCtx(
					"in-memory scan output is missing the commit-ts column",
				)
			}
			node.object.appendMVCC.FillInCommitTSVecLocked(
				commitTSVec, maxRow, mp)
		}
	}
	if !holes.IsEmpty() {
		holes.Foreach(func(row uint64) bool {
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
	if node == nil || node.object == nil || node.object.appendMVCC == nil || node.data == nil {
		return moerr.NewInternalErrorNoCtx("tombstone range scan has no in-memory object data")
	}
	if ctx == nil || objID == nil || bat == nil || mp == nil {
		return moerr.NewInvalidInputNoCtx(
			"tombstone range scan requires context, object id, output batch, and mpool",
		)
	}
	if start.GT(&end) {
		return moerr.NewInvalidInputNoCtx("tombstone range scan start timestamp is after end timestamp")
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	initialBatch := *bat
	var appendCheckpoint *tombstoneResultAppendCheckpoint
	defer func() {
		if err == nil || *bat == nil {
			return
		}
		if initialBatch == nil {
			(*bat).Close()
			*bat = nil
			return
		}
		if appendCheckpoint != nil {
			appendCheckpoint.Rollback()
		}
	}()
	node.object.RLock()
	defer node.object.RUnlock()
	minRow, maxRow, commitTSVec, abort, _ :=
		node.object.appendMVCC.CollectAppendLocked(start, end, mp)
	if commitTSVec == nil {
		if abort != nil {
			abort.Close()
		}
		return nil
	}
	if abort == nil {
		commitTSVec.Close()
		return moerr.NewInternalErrorNoCtx("append MVCC returned commit timestamps without abort state")
	}
	defer commitTSVec.Close()
	defer abort.Close()
	if maxRow < minRow || node.data == nil || len(node.data.Vecs) == 0 ||
		int(maxRow) > node.data.Length() {
		return moerr.NewInternalErrorNoCtx("append MVCC returned an invalid tombstone row range")
	}
	rowCount := int(maxRow - minRow)
	rowIDVec := lookupTNBatchVector(node.data, objectio.TombstoneAttr_Rowid_Attr)
	if rowIDVec == nil || rowIDVec.GetDownstreamVector() == nil {
		return moerr.NewInternalErrorNoCtx("in-memory tombstone rowid column is missing")
	}
	rowIDs, validateErr := ioutil.ValidateTombstoneRowIDColumn(
		node.data.Length(), rowIDVec.GetDownstreamVector(),
	)
	if validateErr != nil {
		return validateErr
	}
	commitTSs, validateErr := ioutil.ValidateTombstoneCommitTSColumn(
		rowCount, commitTSVec.GetDownstreamVector(),
	)
	if validateErr != nil {
		return validateErr
	}
	aborts, abortErr := ioutil.ValidateTombstoneAbortColumn(rowCount, abort.GetDownstreamVector())
	if abortErr != nil {
		return abortErr
	}
	pkVec := lookupTNBatchVector(node.data, objectio.TombstoneAttr_PK_Attr)
	if pkVec == nil || pkVec.GetDownstreamVector() == nil ||
		pkVec.Length() != node.data.Length() {
		return moerr.NewInternalErrorNoCtx("in-memory tombstone primary-key row count is invalid")
	}
	var appender *tombstoneResultAppender
	for i := minRow; i < maxRow; i++ {
		if (i-minRow)&1023 == 0 {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			default:
			}
		}
		commitTS := commitTSs.At(int(i - minRow))
		if (aborts.IsPresent() && aborts.At(int(i-minRow))) ||
			commitTS.Equal(&txnif.UncommitTS) ||
			commitTS.LT(&start) || commitTS.GT(&end) {
			continue
		}
		if types.PrefixCompare(rowIDs[i][:], objID[:]) == 0 {
			if *bat == nil {
				*bat = catalog.NewTombstoneBatchByPKType(*pkVec.GetType(), mp)
			}
			if appender == nil {
				appender, err = newTombstoneResultAppender(*bat, pkVec.GetType(), mp)
				if err != nil {
					return err
				}
				appendCheckpoint = appender.MakeCheckpoint()
			}
			if err = appender.Append(
				rowIDs[i], pkVec, int(i), commitTS,
			); err != nil {
				return err
			}
		}
	}
	return
}

func (node *memoryNode) FillBlockTombstones(
	ctx context.Context,
	txn txnif.TxnReader,
	blkID *objectio.Blockid,
	deletes **nulls.Nulls,
	deleteStartOffset uint64,
	deleteEndOffset uint64,
	mp *mpool.MPool) error {
	if node == nil || node.object == nil || node.object.appendMVCC == nil || node.data == nil {
		return moerr.NewInternalErrorNoCtx("tombstone fill has no in-memory object data")
	}
	if ctx == nil || txn == nil || blkID == nil || deletes == nil || mp == nil {
		return moerr.NewInvalidInputNoCtx(
			"tombstone fill requires context, transaction, block id, delete mask, and mpool",
		)
	}
	if deleteEndOffset < deleteStartOffset {
		return moerr.NewInvalidInputNoCtx("tombstone fill has a reversed output row range")
	}
	node.object.RLock()
	defer node.object.RUnlock()
	maxRow, visible, holes, err := node.object.appendMVCC.GetVisibleRowLocked(ctx, txn)
	if !visible || err != nil {
		// blk.RUnlock()
		return err
	}
	if node.data == nil || len(node.data.Vecs) == 0 || int(maxRow) > node.data.Length() {
		return moerr.NewInternalErrorNoCtx("append MVCC returned an invalid tombstone row range")
	}
	rowIDVec := lookupTNBatchVector(node.data, objectio.TombstoneAttr_Rowid_Attr)
	if rowIDVec == nil || rowIDVec.GetDownstreamVector() == nil {
		return moerr.NewInternalErrorNoCtx("in-memory tombstone rowid column is missing")
	}
	rowIDs, err := ioutil.ValidateTombstoneRowIDColumn(
		node.data.Length(), rowIDVec.GetDownstreamVector(),
	)
	if err != nil {
		return err
	}
	pendingDeletes := &nulls.Nulls{}
	for i := 0; i < int(maxRow); i++ {
		if i&1023 == 0 {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			default:
			}
		}
		if holes.Contains(uint64(i)) {
			continue
		}
		rowID := rowIDs[i]
		if types.PrefixCompare(rowID[:], blkID[:]) == 0 {
			offset, offsetErr := checkedDeleteOffset(
				rowID.GetRowOffset(), deleteStartOffset, deleteEndOffset,
			)
			if offsetErr != nil {
				return offsetErr
			}
			pendingDeletes.Add(offset)
		}
	}
	if !pendingDeletes.IsEmpty() {
		if *deletes == nil {
			*deletes = &nulls.Nulls{}
		}
		pendingDeletes.Foreach(func(offset uint64) bool {
			(*deletes).Add(offset)
			return true
		})
	}
	return nil
}
