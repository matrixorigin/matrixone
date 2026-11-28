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
	return node.pkIndex.Contains(ctx, keys, keysZM, &blkID, node.checkConflictLocked(txn), mp)
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
		keys,
		keysZM,
		&blkID,
		rowIDs,
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
		selection, commitTSVec, abort, err = node.object.appendMVCC.CollectAppendSnapshotLocked(end, mp)
	} else {
		selection, commitTSVec, abort, err = node.object.appendMVCC.CollectAppendLocked(start, end, mp)
	}
	if err != nil {
		return
	}
	defer selection.Release()
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
		containers.ReleaseVector(commitTSVec)
		containers.ReleaseVector(abort)
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
		aborts := vector.MustFixedColWithTypeCheck[bool](abort)
		for row, aborted := range aborts {
			if !aborted {
				continue
			}
			if err = vector.SetFixedAtNoTypeCheck(commitTSVec, row, txnif.UncommitTS); err != nil {
				containers.ReleaseVector(abort)
				inner.Close()
				return
			}
			for _, colDef := range node.writeSchema.ColDefs {
				if colDef.IsRealPrimary() && !node.writeSchema.IsSecondaryIndexTable() {
					inner.Vecs[colDef.Idx].SetNull(uint64(row))
				}
			}
		}
		containers.ReleaseVector(abort)
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
	spec := readSchema.ColumnReadSpec()
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
				vec, cloneErr := pool.CloneWindow(src, int(from), rows)
				if cloneErr != nil {
					return cloneErr
				}
				(*bat).AddVector(col.Name, vec)
			} else {
				dst := (*bat).GetVectorByName(col.Name)
				if err = dst.UnionBatch(src, int64(from), rows, nil, dst.MPool()); err != nil {
					return err
				}
			}
		case objectio.ColVirtualDefault:
			if fresh {
				(*bat).AddVector(col.Name, pool.GetVector(&col.Type))
			}
			dst := (*bat).GetVectorByName(col.Name)
			if err = spec.AppendMissingRows(col.SeqNum, dst, rows, dst.MPool()); err != nil {
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
	source *catalog.Schema,
) (from int, err error) {
	if source == nil {
		source = node.writeSchema
	}
	spec, missing, err := node.validateAppendLayout(bat, source)
	if err != nil {
		return 0, err
	}
	from = int(node.mustData().Length())
	rows := bat.Length()
	if err = node.appendMissingWALColumns(spec, missing, rows); err != nil {
		return
	}
	for srcPos, attr := range bat.Attrs {
		idx := node.appendDestination(source, attr)
		if idx < 0 {
			continue
		}
		destVec, src := node.data.Vecs[idx], bat.Vecs[srcPos]
		if err = destVec.UnionBatch(src, 0, src.Length(), nil, destVec.MPool()); err != nil {
			return
		}
	}
	if logicalID := node.legacyLogicalID(bat); logicalID != nil {
		destVec := node.data.GetVectorByName(pkgcatalog.SystemRelAttr_LogicalID)
		if err = destVec.UnionBatch(logicalID, 0, logicalID.Length(), nil, destVec.MPool()); err != nil {
			return
		}
	}
	// Align remaining legacy columns that have no evolution metadata.
	err = node.ReserveRowsLocked(uint32(from + rows))
	return
}

// validateAppendLayout checks the entire source layout before modifying data or
// the index. A subset of names cannot establish a WAL layout's high-water mark.
func (node *memoryNode) validateAppendLayout(bat *containers.Batch, source *catalog.Schema) (*objectio.ColumnReadSpec, []*catalog.ColDef, error) {
	schema := node.writeSchema
	if len(bat.Attrs) == 0 || len(bat.Attrs) != len(bat.Vecs) || bat.Vecs[0] == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("invalid append payload shape")
	}
	rows := bat.Length()
	required := 0
	for pos, name := range bat.Attrs {
		idx := source.GetColIdx(name)
		if idx < 0 || source.ColDefs[idx].Name != name {
			return nil, nil, moerr.NewInternalErrorNoCtxf("unknown append column %q in source schema version %d", name, source.Version)
		}
		col := source.ColDefs[idx]
		vec := bat.Vecs[pos]
		if mapped, ok := bat.Nameidx[name]; !ok || mapped != pos || vec == nil ||
			vec.Length() != rows {
			return nil, nil, moerr.NewInternalErrorNoCtxf("invalid append vector for column %q", name)
		}
		if !col.Type.Eq(*vec.GetType()) {
			return nil, nil, moerr.NewInternalErrorNoCtxf("invalid append vector for column %q: expected type %#v, got %#v", name, col.Type, *vec.GetType())
		}
		if !col.PhyAddr {
			required++
		}
		if source != schema {
			if dst, present := schema.SeqnumMap[col.SeqNum]; present {
				if !schema.ColDefs[dst].Type.Eq(col.Type) {
					return nil, nil, moerr.NewInternalErrorNoCtxf("append column %q changed type at sequence %d", name, col.SeqNum)
				}
			} else if !col.PhyAddr && uint32(col.SeqNum) >= schema.Extra.NextColSeqnum {
				return nil, nil, moerr.NewInternalErrorNoCtxf("append column %q at sequence %d was never allocated in the destination schema", name, col.SeqNum)
			}
		}
	}
	expected := len(source.ColDefs)
	if source.PhyAddrKey != nil {
		expected--
	}
	if required != expected {
		for _, col := range source.ColDefs {
			if col.PhyAddr {
				continue
			}
			if pos, present := bat.Nameidx[col.Name]; present && pos >= 0 && pos < len(bat.Attrs) && bat.Attrs[pos] == col.Name {
				continue
			}
			if col.Name == pkgcatalog.SystemRelAttr_LogicalID && node.legacyLogicalID(bat) != nil {
				continue
			}
			return nil, nil, moerr.NewInternalErrorNoCtxf("append column %q is required by source schema version %d", col.Name, source.Version)
		}
	}
	if source == schema {
		return nil, nil, nil
	}
	spec := schema.ColumnReadSpec()
	var missing []*catalog.ColDef
	for _, col := range schema.ColDefs {
		if col.PhyAddr {
			continue
		}
		if _, present := source.SeqnumMap[col.SeqNum]; present {
			continue
		}
		if uint32(col.SeqNum) < source.Extra.NextColSeqnum || col.IsRealPrimary() {
			return nil, nil, moerr.NewInternalErrorNoCtxf("append column %q at sequence %d is missing inside the source layout", col.Name, col.SeqNum)
		}
		if spec != nil {
			missing = append(missing, col)
		}
	}
	return spec, missing, nil
}

// appendDestination is called only for columns validated against source.
func (node *memoryNode) appendDestination(source *catalog.Schema, name string) int {
	idx := source.GetColIdx(name)
	if source == node.writeSchema {
		return idx
	}
	if dst, present := node.writeSchema.SeqnumMap[source.ColDefs[idx].SeqNum]; present {
		return dst
	}
	return -1
}

// appendMissingWALColumns fills absent columns before mutating supplied
// values. Roll back the fills if an allocation fails.
func (node *memoryNode) appendMissingWALColumns(spec *objectio.ColumnReadSpec, missing []*catalog.ColDef, rows int) error {
	if len(missing) == 0 {
		return nil
	}
	checkpoints := make([]vector.AppendCheckpoint, len(missing))
	for i, col := range missing {
		vec := node.data.Vecs[col.Idx]
		checkpoints[i] = vec.MakeAppendCheckpoint()
		if err := spec.AppendMissingRows(col.SeqNum, vec, rows, vec.MPool()); err != nil {
			for j, filled := range missing[:i+1] {
				err = vector.JoinAppendRollbackError(err, node.data.Vecs[filled.Idx].RollbackAppend(checkpoints[j], rows))
			}
			return err
		}
	}
	return nil
}

func (node *memoryNode) ApplyAppendAtLocked(bat *containers.Batch, offset uint32, source *catalog.Schema, beforeOverwrite func() error) error {
	if source == nil {
		source = node.writeSchema
	}
	spec, missing, err := node.validateAppendLayout(bat, source)
	if err != nil {
		return err
	}
	data := node.mustData()
	rows := bat.Length()
	if rows == 0 {
		return nil
	}
	start, end := int(offset), int(offset)+rows

	// Keep allocations and other fallible vector writes ahead of the PK change.
	// A reserved column can be patched in place when its copy cannot allocate.
	var staged []containers.Vector
	defer func() {
		for _, vec := range staged {
			if vec != nil {
				containers.ReleaseVector(vec)
			}
		}
	}()
	stage := func(idx int) (containers.Vector, error) {
		current := data.Vecs[idx]
		next, err := containers.CloneVector(current, current.MPool(), nil)
		if err != nil {
			return nil, err
		}
		if staged == nil {
			staged = make([]containers.Vector, len(data.Vecs))
		}
		staged[idx] = next
		if err := extendAppendVector(next, end); err != nil {
			return nil, err
		}
		return next, nil
	}
	for _, col := range missing {
		if err := spec.ValidateMissingColumn(col.SeqNum); err != nil {
			return err
		}
		next, err := stage(col.Idx)
		if err != nil {
			return err
		}
		nsp := next.GetNulls()
		if err := nulls.TryExpand(nsp, end); err != nil {
			return err
		}
		nsp.AddRange(uint64(start), uint64(end))
	}

	legacyID := node.legacyLogicalID(bat)
	sourceVector := func(col *catalog.ColDef) containers.Vector {
		if pos, ok := source.SeqnumMap[col.SeqNum]; ok {
			if src, ok := bat.Nameidx[source.ColDefs[pos].Name]; ok {
				return bat.Vecs[src]
			}
		}
		if col.Name == pkgcatalog.SystemRelAttr_LogicalID {
			return legacyID
		}
		return nil
	}
	for idx, col := range node.writeSchema.ColDefs {
		if staged != nil && staged[idx] != nil {
			continue
		}
		src := sourceVector(col)
		if canInstallAppendInPlace(data.Vecs[idx], src, end) {
			continue
		}
		next, err := stage(idx)
		if err != nil {
			return err
		}
		if src != nil {
			if err := copyAppendRows(next, src, start); err != nil {
				return err
			}
		}
	}
	if beforeOverwrite != nil {
		if err := beforeOverwrite(); err != nil {
			return err
		}
	}
	for idx, col := range node.writeSchema.ColDefs {
		if staged != nil && staged[idx] != nil {
			dst, next := data.Vecs[idx], staged[idx]
			*dst, *next = *next, *dst
			continue
		}
		dest := data.Vecs[idx]
		if err := extendAppendVector(dest, end); err != nil {
			return err
		}
		if src := sourceVector(col); src != nil {
			if err := copyAppendRows(dest, src, start); err != nil {
				return err
			}
		}
	}
	return nil
}

func extendAppendVector(dest containers.Vector, end int) error {
	if grow := end - dest.Length(); grow > 0 {
		return vector.AppendMultiFixed(dest, byte(0), true, grow, dest.MPool())
	}
	return nil
}

// A patch can use the live vector if its row slots, NULL bitmap and varlena
// area already have enough capacity. The payload owns separate source backing.
func canInstallAppendInPlace(dst *vector.Vector, source containers.Vector, end int) bool {
	hasBitmapRoom := dst.GetNulls().GetBitmap().CapWords() >= (end+63)/64
	if end > dst.Length() && (dst.Capacity() < end || !hasBitmapRoom) {
		return false
	}
	if source == nil {
		return true
	}
	src := source
	if !hasBitmapRoom && (src.IsConstNull() || src.GetNulls().Any()) {
		return false
	}
	if len(src.GetArea()) == 0 {
		return true
	}
	area := dst.GetArea()
	remaining := cap(area) - len(area)
	if src.IsConst() {
		return len(src.GetBytesAt(0)) <= remaining/src.Length()
	}
	for row := range src.Length() {
		if !src.IsNull(uint64(row)) {
			if size := len(src.GetBytesAt(row)); size > types.VarlenaInlineSize {
				remaining -= size
				if remaining < 0 {
					return false
				}
			}
		}
	}
	return true
}

func copyAppendRows(dest, source containers.Vector, offset int) error {
	dst, src := dest, source
	if src.IsConstNull() || src.GetNulls().Any() {
		if err := nulls.TryExpand(dst.GetNulls(), offset+src.Length()); err != nil {
			return err
		}
	}
	for row := range src.Length() {
		if err := dst.Copy(src, int64(offset+row), int64(row), dest.MPool()); err != nil {
			return err
		}
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
	if pos, ok := bat.Nameidx[pkgcatalog.SystemRelAttr_ID]; ok && pos >= 0 && pos < len(bat.Attrs) &&
		bat.Attrs[pos] == pkgcatalog.SystemRelAttr_ID {
		return bat.Vecs[pos]
	}
	return nil
}

// ReserveRowsLocked pads every vector with NULLs up to rows.
func (node *memoryNode) ReserveRowsLocked(rows uint32) error {
	data := node.mustData()
	for _, vec := range data.Vecs {
		missing := int(rows) - vec.Length()
		if missing == 1 {
			if err := vector.AppendAny(vec, nil, true, vec.MPool()); err != nil {
				return err
			}
		} else if missing > 1 {
			// Memory-node vectors own their mutable backing. Reuse vector's
			// bulk growth and metadata accounting without a temporary vector.
			if err := vector.AppendMultiFixed(vec, 0, true, missing, vec.MPool()); err != nil {
				return err
			}
		}
	}
	return nil
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
	defer selection.Release()
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
			if err = node.object.appendMVCC.FillInCommitTSVecLocked(
				(*bat).GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr), selection.MaxRow, mp); err != nil {
				return
			}
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
	selection, commitTSVec, abort, err :=
		node.object.appendMVCC.CollectAppendLocked(start, end, mp)
	if err != nil {
		return err
	}
	defer selection.Release()
	if commitTSVec == nil {
		return nil
	}
	defer containers.ReleaseVector(commitTSVec)
	defer containers.ReleaseVector(abort)
	rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](
		node.data.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr))
	commitTSs := vector.MustFixedColWithTypeCheck[types.TS](commitTSVec)
	aborts := vector.MustFixedColWithTypeCheck[bool](abort)
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
			if err = appendTombstoneRow(*bat, rowIDs[i], pkVec, int(i), commitTSs[selectedOffset]); err != nil {
				return
			}
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
	defer selection.Release()
	if !visible || err != nil {
		// blk.RUnlock()
		return err
	}
	rowIDVec := node.data.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr)
	rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](rowIDVec)
	for i := 0; i < int(selection.MaxRow); i++ {
		if !selection.Contains(uint32(i)) {
			continue
		}
		rowID := rowIDs[i]
		if types.PrefixCompare(rowID[:], blkID[:]) == 0 {
			if *deletes == nil {
				*deletes = &nulls.Nulls{}
				(*deletes).Init(mp)
			}
			offset := rowID.GetRowOffset()
			(*deletes).Add(uint64(offset) + deleteStartOffset)
		}
	}
	return nil
}
