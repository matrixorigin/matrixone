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
	"math"
	"slices"
	"sync"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type scanNoCopyKey struct{}

// WithScanNoCopy signals the scan chain to use zero-copy mode (needCopy=false).
// Vectors will wrap fileservice buffers directly; the caller must hold the
// returned DataRelease on the batch until the data is fully consumed.
func WithScanNoCopy(ctx context.Context) context.Context {
	return context.WithValue(ctx, scanNoCopyKey{}, true)
}

func isScanNoCopy(ctx context.Context) bool {
	v, _ := ctx.Value(scanNoCopyKey{}).(bool)
	return v
}

var _ NodeT = (*persistedNode)(nil)

type persistedNode struct {
	common.RefHelper
	object                    *baseObject
	tombstoneLayoutOnce       sync.Once
	tombstoneLayoutBlockCount int
	persistedByCN             bool
	tombstoneLayoutErr        error
}

const maxPersistedObjectBlockCount = 1 << 16

func validatePersistedObjectBlockCount(count int) error {
	if count <= 0 || count > maxPersistedObjectBlockCount {
		return moerr.NewInternalErrorNoCtxf(
			"invalid persisted object block count %d; expected 1..%d",
			count, maxPersistedObjectBlockCount,
		)
	}
	return nil
}

func objectLifetimeOverlapsRange(createAt, deleteAt, start, end types.TS) bool {
	return !createAt.GT(&end) && (deleteAt.IsEmpty() || !deleteAt.LT(&start))
}

func checkedDeleteOffset(rowOffset uint32, base, end uint64) (uint64, error) {
	if end < base {
		return 0, moerr.NewInvalidInputNoCtxf(
			"delete offset range [%d,%d) is reversed", base, end,
		)
	}
	if base > math.MaxUint64-uint64(rowOffset) {
		return 0, moerr.NewInvalidInputNoCtxf(
			"delete offset base %d overflows with row offset %d", base, rowOffset,
		)
	}
	offset := base + uint64(rowOffset)
	if offset >= end {
		return 0, moerr.NewInvalidInputNoCtxf(
			"delete offset %d is outside output row range [%d,%d)", offset, base, end,
		)
	}
	return offset, nil
}

func classifyPersistedTombstoneBlocks(
	dataMeta objectio.ObjectDataMeta,
	expectedBlockCount int,
) (persistedByCN bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = moerr.NewInternalErrorNoCtxf(
				"persisted tombstone block metadata is malformed: %v", recovered,
			)
		}
	}()
	if dataMeta == nil {
		return false, moerr.NewInternalErrorNoCtx(
			"persisted tombstone object has no data metadata",
		)
	}
	if expectedBlockCount <= 0 || dataMeta.BlockCount() != uint32(expectedBlockCount) {
		return false, moerr.NewInternalErrorNoCtxf(
			"persisted tombstone object reports %d blocks but metadata contains %d",
			expectedBlockCount, dataMeta.BlockCount(),
		)
	}
	startID := uint32(dataMeta.BlockHeader().StartID())
	for blockOffset := 0; blockOffset < expectedBlockCount; blockOffset++ {
		block := dataMeta.GetBlockMeta(startID + uint32(blockOffset))
		cnCreated := isCNCreatedTombstoneBlock(block)
		if blockOffset == 0 {
			persistedByCN = cnCreated
			continue
		}
		if cnCreated != persistedByCN {
			return false, moerr.NewInternalErrorNoCtxf(
				"persisted tombstone object mixes CN-created and TN-created block layouts at block %d",
				blockOffset,
			)
		}
	}
	return persistedByCN, nil
}

func getPersistedBlockBloomFilter(
	bf objectio.BloomFilter,
	expectedBlockCount int,
	blockOffset int,
) (buf []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			buf = nil
			err = moerr.NewInternalErrorNoCtxf(
				"persisted bloom filter metadata is malformed: %v", recovered,
			)
		}
	}()
	// ObjectIO stores one additional object-level bloom filter after the
	// per-block filters.
	expectedFilterCount := uint32(expectedBlockCount) + 1
	if expectedBlockCount <= 0 || blockOffset < 0 || blockOffset >= expectedBlockCount ||
		bf.BlockCount() != expectedFilterCount {
		return nil, moerr.NewInternalErrorNoCtxf(
			"persisted bloom filter has %d entries; expected %d and requested block %d",
			bf.BlockCount(), expectedFilterCount, blockOffset,
		)
	}
	return bf.GetBloomFilter(uint32(blockOffset)), nil
}

func (node *persistedNode) classifyTombstoneBlocks(
	dataMeta objectio.ObjectDataMeta,
	expectedBlockCount int,
) (bool, error) {
	if node == nil {
		return false, moerr.NewInternalErrorNoCtx("persisted tombstone node is nil")
	}
	node.tombstoneLayoutOnce.Do(func() {
		node.tombstoneLayoutBlockCount = expectedBlockCount
		node.persistedByCN, node.tombstoneLayoutErr =
			classifyPersistedTombstoneBlocks(dataMeta, expectedBlockCount)
	})
	if node.tombstoneLayoutBlockCount != expectedBlockCount {
		return false, moerr.NewInternalErrorNoCtxf(
			"persisted tombstone block count changed from %d to %d",
			node.tombstoneLayoutBlockCount, expectedBlockCount,
		)
	}
	return node.persistedByCN, node.tombstoneLayoutErr
}

func newPersistedNode(object *baseObject) *persistedNode {
	node := &persistedNode{
		object: object,
	}
	node.OnZeroCB = node.close
	return node
}

func (node *persistedNode) close() {}

func (node *persistedNode) Rows() (uint32, error) {
	if node == nil || node.object == nil {
		return 0, moerr.NewInternalErrorNoCtx("persisted row count has no object")
	}
	meta := node.object.meta.Load()
	if meta == nil {
		return 0, moerr.NewInternalErrorNoCtx("persisted row count has no object metadata")
	}
	return meta.GetObjectStats().Rows(), nil
}

func (node *persistedNode) Contains(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	txn txnif.TxnReader,
	mp *mpool.MPool,
) (err error) {
	return moerr.NewInternalErrorNoCtx("persisted primary-key Contains is unsupported")
}
func (node *persistedNode) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	getRowOffset func() (min, max int32, err error),
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
	mp *mpool.MPool,
) (err error) {
	return moerr.NewInternalErrorNoCtx("persisted duplicate-row lookup is unsupported")
}

func (node *persistedNode) GetDataWindow(
	readSchema *catalog.Schema, colIdxes []int, from, to uint32, mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	return nil, moerr.NewInternalErrorNoCtx("persisted data-window lookup is unsupported")
}

func (node *persistedNode) IsPersisted() bool { return true }

type persistedScanAppendState struct {
	destIndex   int
	original    containers.Vector
	vec         *vector.Vector
	allocator   *mpool.MPool
	checkpoint  vector.AppendCheckpoint
	replacement bool
}

// appendTNBatchVectorsAtomic appends one source batch to an existing TN
// batch atomically. A destination may be a const compatibility vector or a
// borrowed zero-copy vector; both must be materialized before append. Owned
// vectors are protected by append checkpoints so an OOM in a later column
// cannot leave the batch with different column lengths.
func appendTNBatchVectorsAtomic(
	dst *containers.Batch,
	attrs []string,
	sources []containers.Vector,
	fallbackMP *mpool.MPool,
) (appendOffset int, err error) {
	if dst == nil || len(attrs) == 0 || len(attrs) != len(sources) ||
		len(dst.Vecs) != len(attrs) || len(dst.Attrs) != len(dst.Vecs) ||
		len(dst.Nameidx) != len(dst.Vecs) || len(dst.Vecs) == 0 {
		return 0, moerr.NewInternalErrorNoCtx("persisted scan append schema is inconsistent")
	}
	if dst.Vecs[0] == nil || sources[0] == nil {
		return 0, moerr.NewInternalErrorNoCtx("persisted scan append has a nil leading column")
	}
	appendOffset = dst.Vecs[0].Length()
	for pos, dest := range dst.Vecs {
		if dest == nil || dest.GetDownstreamVector() == nil || dest.Length() != appendOffset {
			return 0, moerr.NewInternalErrorNoCtxf(
				"persisted scan destination column %d has inconsistent length", pos,
			)
		}
	}
	appendRows := sources[0].Length()

	const inlineColumns = 16
	var inline [inlineColumns]persistedScanAppendState
	states := inline[:]
	if len(attrs) <= inlineColumns {
		states = states[:len(attrs)]
	} else {
		states = make([]persistedScanAppendState, len(attrs))
	}
	cleanReplacements := func() {
		for i := range states {
			if states[i].replacement && states[i].vec != nil {
				states[i].vec.Free(states[i].allocator)
				states[i].vec = nil
			}
		}
	}
	const inlineSeenColumns = 64
	var inlineSeen [inlineSeenColumns]bool
	seenDestinations := inlineSeen[:]
	if len(dst.Vecs) <= inlineSeenColumns {
		seenDestinations = seenDestinations[:len(dst.Vecs)]
	} else {
		seenDestinations = make([]bool, len(dst.Vecs))
	}
	seenCount := 0
	for i, attr := range attrs {
		source := sources[i]
		if source == nil || source.GetDownstreamVector() == nil || source.Length() != appendRows {
			cleanReplacements()
			return 0, moerr.NewInternalErrorNoCtxf(
				"persisted scan source column %q has inconsistent length", attr,
			)
		}
		destIndex, ok := dst.Nameidx[attr]
		if !ok || destIndex < 0 || destIndex >= len(dst.Vecs) ||
			dst.Attrs[destIndex] != attr {
			cleanReplacements()
			return 0, moerr.NewInternalErrorNoCtxf(
				"persisted scan destination is missing column %q", attr,
			)
		}
		if seenDestinations[destIndex] {
			cleanReplacements()
			return 0, moerr.NewInternalErrorNoCtxf(
				"persisted scan maps multiple sources to column %q", attr,
			)
		}
		seenDestinations[destIndex] = true
		seenCount++
		dest := dst.Vecs[destIndex]
		if *dest.GetType() != *source.GetType() ||
			dest.GetDownstreamVector() == source.GetDownstreamVector() {
			cleanReplacements()
			return 0, moerr.NewInternalErrorNoCtxf(
				"persisted scan column %q has incompatible source", attr,
			)
		}
		allocator := dest.GetAllocator()
		if allocator == nil {
			allocator = fallbackMP
		}
		if allocator == nil {
			cleanReplacements()
			return 0, moerr.NewInternalErrorNoCtxf(
				"persisted scan column %q has no allocator", attr,
			)
		}
		states[i] = persistedScanAppendState{
			destIndex: destIndex,
			original:  dest,
			vec:       dest.GetDownstreamVector(),
			allocator: allocator,
		}
		if !states[i].vec.IsConst() && !states[i].vec.NeedDup() {
			continue
		}
		materialized := vector.NewOffHeapVecWithType(*dest.GetType())
		if selection := states[i].vec.AllocationAccountSelection(); selection != nil {
			if err = materialized.SetAllocationAccount(selection); err != nil {
				materialized.Free(allocator)
				cleanReplacements()
				return 0, err
			}
		}
		if err = materialized.UnionBatch(
			states[i].vec, 0, appendOffset, nil, allocator,
		); err != nil {
			materialized.Free(allocator)
			cleanReplacements()
			return 0, err
		}
		states[i].vec = materialized
		states[i].replacement = true
	}
	if seenCount != len(dst.Vecs) {
		cleanReplacements()
		return 0, moerr.NewInternalErrorNoCtx("persisted scan append does not cover every destination column")
	}
	if appendRows == 0 {
		cleanReplacements()
		return appendOffset, nil
	}
	for i := range states {
		states[i].checkpoint = states[i].vec.MakeAppendCheckpoint()
	}
	for i := range states {
		if err = states[i].vec.UnionBatch(
			sources[i].GetDownstreamVector(), 0, appendRows, nil, states[i].allocator,
		); err != nil {
			for pos := range states {
				states[pos].vec.RollbackAppend(states[pos].checkpoint, appendRows)
			}
			cleanReplacements()
			return 0, err
		}
	}
	for i := range states {
		if !states[i].replacement {
			continue
		}
		states[i].original.Close()
		dst.Vecs[states[i].destIndex] = containers.ToTNVector(
			states[i].vec, states[i].allocator,
		)
		states[i].vec = nil
	}
	return appendOffset, nil
}

type tombstoneResultAppender struct {
	vectors    [3]*vector.Vector
	allocators [3]*mpool.MPool
}

type tombstoneResultAppendCheckpoint struct {
	vectors     [3]*vector.Vector
	checkpoints [3]vector.AppendCheckpoint
	rowCount    int
}

func newTombstoneResultAppender(
	dst *containers.Batch,
	pkType *types.Type,
	fallbackMP *mpool.MPool,
) (*tombstoneResultAppender, error) {
	if dst == nil || pkType == nil || len(dst.Vecs) != 3 ||
		len(dst.Attrs) != 3 || len(dst.Nameidx) != 3 {
		return nil, moerr.NewInternalErrorNoCtx("invalid tombstone result schema")
	}
	names := [...]string{
		objectio.TombstoneAttr_Rowid_Attr,
		objectio.TombstoneAttr_PK_Attr,
		objectio.TombstoneAttr_CommitTs_Attr,
	}
	appender := new(tombstoneResultAppender)
	var positions [len(names)]int
	rowCount := -1
	for i, name := range names {
		pos, ok := dst.Nameidx[name]
		if !ok || pos < 0 || pos >= len(dst.Vecs) || dst.Attrs[pos] != name ||
			dst.Vecs[pos] == nil {
			return nil, moerr.NewInternalErrorNoCtxf("tombstone result is missing column %q", name)
		}
		for previous := 0; previous < i; previous++ {
			if positions[previous] == pos {
				return nil, moerr.NewInternalErrorNoCtx("tombstone result columns overlap")
			}
		}
		positions[i] = pos
		appender.vectors[i] = dst.Vecs[pos].GetDownstreamVector()
		if appender.vectors[i] == nil || appender.vectors[i].IsConst() || appender.vectors[i].NeedDup() {
			return nil, moerr.NewInternalErrorNoCtxf("tombstone result column %q is not appendable", name)
		}
		if rowCount < 0 {
			rowCount = appender.vectors[i].Length()
		} else if appender.vectors[i].Length() != rowCount {
			return nil, moerr.NewInternalErrorNoCtx("tombstone result columns have inconsistent lengths")
		}
		appender.allocators[i] = dst.Vecs[pos].GetAllocator()
		if appender.allocators[i] == nil {
			appender.allocators[i] = fallbackMP
		}
		if appender.allocators[i] == nil {
			return nil, moerr.NewInternalErrorNoCtxf("tombstone result column %q has no allocator", name)
		}
	}
	if appender.vectors[0].GetType().Oid != types.T_Rowid ||
		*appender.vectors[1].GetType() != *pkType ||
		appender.vectors[2].GetType().Oid != types.T_TS {
		return nil, moerr.NewInternalErrorNoCtx("tombstone result schema is incompatible")
	}
	return appender, nil
}

func (a *tombstoneResultAppender) Append(
	rowID types.Rowid,
	pkSource containers.Vector,
	pkRow int,
	commitTS types.TS,
) error {
	if a == nil || pkSource == nil || pkSource.GetDownstreamVector() == nil ||
		pkRow < 0 || pkRow >= pkSource.Length() ||
		*a.vectors[1].GetType() != *pkSource.GetType() {
		return moerr.NewInternalErrorNoCtx("invalid tombstone result row source")
	}
	rowCount := a.vectors[0].Length()
	if a.vectors[1].Length() != rowCount || a.vectors[2].Length() != rowCount {
		return moerr.NewInternalErrorNoCtx("tombstone result columns have inconsistent lengths")
	}
	var checkpoints [len(a.vectors)]vector.AppendCheckpoint
	for i := range a.vectors {
		checkpoints[i] = a.vectors[i].MakeAppendCheckpoint()
	}
	rollback := func() {
		for i := range a.vectors {
			a.vectors[i].RollbackAppend(checkpoints[i], 1)
		}
	}
	if err := vector.AppendFixed(a.vectors[0], rowID, false, a.allocators[0]); err != nil {
		rollback()
		return err
	}
	if err := a.vectors[1].UnionOne(
		pkSource.GetDownstreamVector(), int64(pkRow), a.allocators[1],
	); err != nil {
		rollback()
		return err
	}
	if err := vector.AppendFixed(a.vectors[2], commitTS, false, a.allocators[2]); err != nil {
		rollback()
		return err
	}
	return nil
}

func (a *tombstoneResultAppender) MakeCheckpoint() *tombstoneResultAppendCheckpoint {
	checkpoint := &tombstoneResultAppendCheckpoint{
		vectors:  a.vectors,
		rowCount: a.vectors[0].Length(),
	}
	for i := range a.vectors {
		checkpoint.checkpoints[i] = a.vectors[i].MakeAppendCheckpoint()
	}
	return checkpoint
}

func (c *tombstoneResultAppendCheckpoint) Rollback() {
	if c == nil {
		return
	}
	attemptedRows := c.vectors[0].Length() - c.rowCount
	for i := range c.vectors {
		c.vectors[i].RollbackAppend(c.checkpoints[i], attemptedRows)
	}
}

func (node *persistedNode) Scan(
	ctx context.Context,
	bat **containers.Batch,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (err error) {
	if node == nil || node.object == nil {
		return moerr.NewInternalErrorNoCtx("persisted scan has no object")
	}
	if ctx == nil || bat == nil || readSchema == nil || mp == nil {
		return moerr.NewInvalidInputNoCtx("persisted scan requires context, output, schema, and mpool")
	}
	if len(colIdxes) == 0 {
		return moerr.NewInvalidInputNoCtx("persisted scan requires at least one column")
	}
	attrs := make([]string, len(colIdxes))
	seenAttrs := make(map[string]struct{}, len(colIdxes))
	idPos := -1
	for i, idx := range colIdxes {
		switch idx {
		case objectio.SEQNUM_COMMITTS:
			attrs[i] = objectio.TombstoneAttr_CommitTs_Attr
		case objectio.SEQNUM_ABORT:
			attrs[i] = objectio.TombstoneAttr_Abort_Attr
		default:
			if idx < 0 || idx >= len(readSchema.ColDefs) || readSchema.ColDefs[idx] == nil {
				return moerr.NewInvalidInputNoCtxf(
					"persisted scan column %d is outside the read schema", idx,
				)
			}
			attrs[i] = readSchema.ColDefs[idx].Name
			if attrs[i] == pkgcatalog.SystemRelAttr_ID {
				idPos = i
			}
		}
		if attrs[i] == "" {
			return moerr.NewInvalidInputNoCtxf(
				"persisted scan column %d has an empty attribute", idx,
			)
		}
		if _, exists := seenAttrs[attrs[i]]; exists {
			return moerr.NewInvalidInputNoCtxf(
				"persisted scan requests duplicate column %q", attrs[i],
			)
		}
		seenAttrs[attrs[i]] = struct{}{}
	}
	meta := node.object.meta.Load()
	if meta == nil {
		return moerr.NewInternalErrorNoCtx("persisted scan has no object metadata")
	}
	blockCount := meta.BlockCnt()
	if err = validatePersistedObjectBlockCount(blockCount); err != nil {
		return err
	}
	if int(blkID) >= blockCount {
		return moerr.NewInvalidInputNoCtxf(
			"persisted scan block %d is outside object with %d blocks", blkID, blockCount,
		)
	}
	if node.object.rt == nil || node.object.rt.Fs == nil ||
		node.object.rt.VectorPool.Transient == nil {
		return moerr.NewInternalErrorNoCtx(
			"persisted scan has no runtime, file service, or transient vector pool",
		)
	}
	if meta.IsAppendable() && txn == nil {
		return moerr.NewInvalidInputNoCtx("appendable persisted scan requires a transaction reader")
	}
	id := meta.AsCommonID()
	id.SetBlockOffset(uint16(blkID))
	location, err := node.object.buildMetalocation(uint16(blkID))
	if err != nil {
		return err
	}
	var tsForAppendable *types.TS
	if meta.IsAppendable() {
		ts := txn.GetStartTS()
		tsForAppendable = &ts
	}
	vecs, deletes, release, err := LoadPersistedColumnData(
		ctx, readSchema, node.object.rt, id, colIdxes, location, mp, tsForAppendable,
		!isScanNoCopy(ctx), meta.IsTombstone,
	)
	if err != nil {
		return err
	}
	defer func() {
		closePersistedVectors(vecs)
		if release != nil {
			release()
		}
	}()
	if len(vecs) != len(colIdxes) {
		return moerr.NewInternalErrorNoCtxf(
			"persisted scan returned %d columns, expected %d", len(vecs), len(colIdxes),
		)
	}
	sourceRows := -1
	for pos, vec := range vecs {
		if vec == nil || vec.GetDownstreamVector() == nil {
			return moerr.NewInternalErrorNoCtxf(
				"persisted scan returned a nil vector at column %d", pos,
			)
		}
		if sourceRows < 0 {
			sourceRows = vec.Length()
		} else if vec.Length() != sourceRows {
			return moerr.NewInternalErrorNoCtxf(
				"persisted scan column %d has %d rows, expected %d",
				pos, vec.Length(), sourceRows,
			)
		}
	}

	replaceCommitTS := func(i int) error {
		createTS := meta.GetCreatedAt()
		length := vecs[i].Length()
		replacement := node.object.rt.VectorPool.Transient.GetVector(&objectio.TSType)
		allocator := replacement.GetAllocator()
		if allocator == nil {
			allocator = mp
		}
		if err := vector.AppendMultiFixed(
			replacement.GetDownstreamVector(), createTS, false, length, allocator,
		); err != nil {
			replacement.Close()
			return err
		}
		vecs[i].Close()
		vecs[i] = replacement
		return nil
	}

	for i, idx := range colIdxes {
		switch idx {
		case objectio.SEQNUM_COMMITTS:
			if vecs[i].IsConstNull() {
				if err = replaceCommitTS(i); err != nil {
					return err
				}
			}
		}
	}

	// TODO: check visibility
	if *bat == nil {
		for i, attr := range attrs {
			// RelLogicalID COMPAT
			if attr == pkgcatalog.SystemRelAttr_LogicalID && vecs[i].IsConstNull() {
				if idPos < 0 {
					return moerr.NewInternalErrorNoCtx(
						"system relation logical-id compatibility requires relation id",
					)
				}
				dup, err := vecs[idPos].GetDownstreamVector().Dup(mp)
				if err != nil {
					return err
				}
				vecs[i].Close()
				vecs[i] = containers.ToTNVector(dup, mp)
			}
		}

		result := containers.NewBatch()
		result.Deletes = deletes
		result.DataRelease = release
		release = nil
		for i, attr := range attrs {
			result.AddVector(attr, vecs[i])
			vecs[i] = nil
		}
		*bat = result
		return nil
	}

	sources := make([]containers.Vector, len(attrs))
	for i, attr := range attrs {
		source := vecs[i]
		// RelLogicalID COMPAT
		if attr == pkgcatalog.SystemRelAttr_LogicalID && source.IsConstNull() {
			if idPos < 0 {
				return moerr.NewInternalErrorNoCtx(
					"system relation logical-id compatibility requires relation id",
				)
			}
			source = vecs[idPos]
		}
		sources[i] = source
	}
	appendRows := sources[0].Length()
	if !deletes.IsEmpty() {
		var invalidDelete bool
		deletes.Foreach(func(i uint64) bool {
			if i >= uint64(appendRows) {
				invalidDelete = true
				return false
			}
			return true
		})
		if invalidDelete {
			return moerr.NewInternalErrorNoCtx("persisted scan delete offset exceeds source rows")
		}
	}
	appendOffset, err := appendTNBatchVectorsAtomic(*bat, attrs, sources, mp)
	if err != nil {
		return err
	}
	if (*bat).DataRelease != nil {
		allMaterialized := true
		for _, vec := range (*bat).Vecs {
			if vec != nil && vec.GetDownstreamVector() != nil &&
				vec.GetDownstreamVector().NeedDup() {
				allMaterialized = false
				break
			}
		}
		if allMaterialized {
			(*bat).DataRelease()
			(*bat).DataRelease = nil
		}
	}
	if !deletes.IsEmpty() {
		if (*bat).Deletes == nil {
			(*bat).Deletes = nulls.NewWithSize(appendOffset + appendRows)
		}
		deletes.Foreach(func(i uint64) bool {
			(*bat).Deletes.Add(i + uint64(appendOffset))
			return true
		})
	}
	return
}

func (node *persistedNode) CollectObjectTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	objID *types.Objectid,
	bat **containers.Batch,
	mp *mpool.MPool,
	vpool *containers.VectorPool,
) (err error) {
	if node == nil || node.object == nil {
		return moerr.NewInternalErrorNoCtx("tombstone range scan has no persisted object")
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
	meta := node.object.meta.Load()
	if meta == nil {
		return moerr.NewInternalErrorNoCtx("tombstone range scan has no object metadata")
	}
	if node.object.rt == nil || node.object.rt.Fs == nil ||
		node.object.rt.VectorPool.Transient == nil {
		return moerr.NewInternalErrorNoCtx(
			"tombstone range scan has no runtime, file service, or transient vector pool",
		)
	}
	initialBatch := *bat
	var appender *tombstoneResultAppender
	var appendCheckpoint *tombstoneResultAppendCheckpoint
	ensureAppender := func(pkType *types.Type) error {
		if appender != nil {
			if *appender.vectors[1].GetType() != *pkType {
				return moerr.NewInternalErrorNoCtx("tombstone range scan primary-key type changed")
			}
			return nil
		}
		if *bat == nil {
			*bat = catalog.NewTombstoneBatchByPKType(*pkType, mp)
		}
		var appenderErr error
		appender, appenderErr = newTombstoneResultAppender(*bat, pkType, mp)
		if appenderErr == nil {
			appendCheckpoint = appender.MakeCheckpoint()
		}
		return appenderErr
	}
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
	if !meta.IsTombstone {
		return moerr.NewInternalErrorNoCtx("cannot collect tombstones from a data object")
	}
	colIdxes := append(
		slices.Clone(objectio.TombstoneColumns_TN_Created),
		objectio.SEQNUM_ABORT,
	)
	table := meta.GetTable()
	if table == nil {
		return moerr.NewInternalErrorNoCtx("tombstone range scan has no table metadata")
	}
	readSchema := table.GetLastestSchema(true)
	if readSchema == nil {
		return moerr.NewInternalErrorNoCtx("tombstone range scan has no table schema")
	}
	var startTS types.TS
	if meta.IsAppendable() {
		createAt := meta.GetCreatedAt()
		deleteAt := meta.GetDeleteAt()
		if !objectLifetimeOverlapsRange(createAt, deleteAt, start, end) {
			return
		}
	}
	blockCount := meta.BlockCnt()
	if err = validatePersistedObjectBlockCount(blockCount); err != nil {
		return err
	}
	id := meta.AsCommonID()

	objLocation := meta.GetLocation()
	objDataMeta, err := objectio.FastLoadObjectMeta(ctx, &objLocation, false, node.object.GetFs())
	if err != nil {
		return err
	}
	dataMeta, err := ioutil.GetDataMetaForLocation(objDataMeta, objLocation)
	if err != nil {
		return err
	}
	persistedByCN, err := node.classifyTombstoneBlocks(dataMeta, blockCount)
	if err != nil {
		return err
	}
	if !meta.IsAppendable() && persistedByCN {
		// A CN-created tombstone object commits all rows at object creation.
		// TN-created and backup tombstones persist per-row commit timestamps,
		// so object creation time cannot prune their range scan.
		startTS = meta.GetCreatedAt()
		if startTS.LT(&start) || startTS.GT(&end) {
			return nil
		}
	}
	var bf objectio.BloomFilter
	if bf, err = objectio.FastLoadBF(
		ctx,
		meta.GetLocation(),
		false,
		node.object.rt.Fs,
	); err != nil {
		return
	}
	for blkID := 0; blkID < blockCount; blkID++ {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}
		buf, bloomErr := getPersistedBlockBloomFilter(bf, blockCount, blkID)
		if bloomErr != nil {
			return bloomErr
		}
		bfIndex := index.NewEmptyBloomFilterWithType(index.HBF)
		if err = index.DecodeBloomFilter(bfIndex, buf); err != nil {
			return
		}
		containes, err := bfIndex.PrefixMayContainsKey(objID[:], index.PrefixFnID_Object, 1)
		if err != nil {
			return err
		}
		if !containes {
			continue
		}
		id.SetBlockOffset(uint16(blkID))
		location, err := node.object.buildMetalocation(uint16(blkID))
		if err != nil {
			return err
		}
		vecs, _, release, err := LoadPersistedColumnData(
			ctx, readSchema, node.object.rt, id, colIdxes, location, mp, nil,
			true, true,
		)
		if err != nil {
			return err
		}
		if err = func() error {
			defer func() {
				closePersistedVectors(vecs)
				if release != nil {
					release()
				}
			}()
			rowCount := vecs[0].Length()
			rowIDs, validateErr := ioutil.ValidateTombstoneRowIDColumn(
				rowCount, vecs[0].GetDownstreamVector(),
			)
			if validateErr != nil {
				return validateErr
			}
			if persistedByCN {
				for i := 0; i < rowCount; i++ {
					if i&1023 == 0 {
						select {
						case <-ctx.Done():
							return context.Cause(ctx)
						default:
						}
					}
					if types.PrefixCompare(rowIDs[i][:], objID[:]) != 0 { // TODO
						continue
					}
					if validateErr = ensureAppender(vecs[1].GetType()); validateErr != nil {
						return validateErr
					}
					if appendErr := appender.Append(rowIDs[i], vecs[1], i, startTS); appendErr != nil {
						return appendErr
					}
				}
				return nil
			}

			commitTSs, validateErr := ioutil.ValidateTombstoneCommitTSColumn(
				rowCount, vecs[2].GetDownstreamVector(),
			)
			if validateErr != nil {
				return validateErr
			}
			aborts, validateErr := ioutil.ValidateTombstoneAbortColumn(
				rowCount, vecs[3].GetDownstreamVector(),
			)
			if validateErr != nil {
				return validateErr
			}
			for i := 0; i < rowCount; i++ {
				if i&1023 == 0 {
					select {
					case <-ctx.Done():
						return context.Cause(ctx)
					default:
					}
				}
				if aborts.IsPresent() && aborts.At(i) {
					continue
				}
				commitTS := commitTSs.At(i)
				if !commitTS.Equal(&txnif.UncommitTS) &&
					commitTS.GE(&start) && commitTS.LE(&end) &&
					types.PrefixCompare(rowIDs[i][:], objID[:]) == 0 { // TODO
					if validateErr = ensureAppender(vecs[1].GetType()); validateErr != nil {
						return validateErr
					}
					if appendErr := appender.Append(rowIDs[i], vecs[1], i, commitTS); appendErr != nil {
						return appendErr
					}
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return
}

func (node *persistedNode) FillBlockTombstones(
	ctx context.Context,
	txn txnif.TxnReader,
	blkID *objectio.Blockid,
	deletes **nulls.Nulls,
	deleteStartOffset uint64,
	deleteEndOffset uint64,
	mp *mpool.MPool) error {
	if node == nil || node.object == nil {
		return moerr.NewInternalErrorNoCtx("tombstone fill has no persisted object")
	}
	if ctx == nil || txn == nil || blkID == nil || deletes == nil || mp == nil {
		return moerr.NewInvalidInputNoCtx(
			"tombstone fill requires context, transaction, block id, delete mask, and mpool",
		)
	}
	if deleteEndOffset < deleteStartOffset {
		return moerr.NewInvalidInputNoCtx("tombstone fill has a reversed output row range")
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	meta := node.object.meta.Load()
	if meta == nil {
		return moerr.NewInternalErrorNoCtx("tombstone fill has no object metadata")
	}
	if !meta.IsTombstone {
		return moerr.NewInternalErrorNoCtx("cannot fill tombstones from a data object")
	}
	if node.object.rt == nil || node.object.rt.Fs == nil ||
		node.object.rt.VectorPool.Transient == nil {
		return moerr.NewInternalErrorNoCtx(
			"tombstone fill has no runtime, file service, or transient vector pool",
		)
	}
	startTS := txn.GetStartTS()
	blockCount := meta.BlockCnt()
	if err := validatePersistedObjectBlockCount(blockCount); err != nil {
		return err
	}
	id := meta.AsCommonID()
	table := meta.GetTable()
	if table == nil {
		return moerr.NewInternalErrorNoCtx("tombstone fill has no table metadata")
	}
	readSchema := table.GetLastestSchema(true)
	if readSchema == nil {
		return moerr.NewInternalErrorNoCtx("tombstone fill has no table schema")
	}
	objLocation := meta.GetLocation()
	objDataMeta, err := objectio.FastLoadObjectMeta(
		ctx, &objLocation, false, node.object.GetFs(),
	)
	if err != nil {
		return err
	}
	dataMeta, err := ioutil.GetDataMetaForLocation(objDataMeta, objLocation)
	if err != nil {
		return err
	}
	persistedByCN, err := node.classifyTombstoneBlocks(dataMeta, blockCount)
	if err != nil {
		return err
	}
	if !meta.IsAppendable() && persistedByCN {
		createAt := meta.GetCreatedAt()
		if createAt.GT(&startTS) {
			return nil
		}
	}
	var bf objectio.BloomFilter
	if bf, err = objectio.FastLoadBF(
		ctx,
		meta.GetLocation(),
		false,
		node.object.rt.Fs,
	); err != nil {
		return err
	}
	colIdxs := []int{0}
	var snapshotTS *types.TS
	if meta.IsAppendable() {
		colIdxs = append(colIdxs, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT)
		snapshotTS = &startTS
	} else {
		// Non-appendable tombstones can contain rows committed at different
		// timestamps, including legacy backup objects.
		colIdxs = append(colIdxs, objectio.SEQNUM_COMMITTS)
	}
	pendingDeletes := &nulls.Nulls{}
	for tombstoneBlkID := 0; tombstoneBlkID < blockCount; tombstoneBlkID++ {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}
		buf, bloomErr := getPersistedBlockBloomFilter(
			bf, blockCount, tombstoneBlkID,
		)
		if bloomErr != nil {
			return bloomErr
		}
		bfIndex := index.NewEmptyBloomFilterWithType(index.HBF)
		if err := index.DecodeBloomFilter(bfIndex, buf); err != nil {
			return err
		}
		containes, err := bfIndex.PrefixMayContainsKey(blkID[:], index.PrefixFnID_Block, 2)
		if err != nil {
			return err
		}
		if !containes {
			continue
		}
		id.SetBlockOffset(uint16(tombstoneBlkID))
		location, err := node.object.buildMetalocation(uint16(tombstoneBlkID))
		if err != nil {
			return err
		}
		vecs, visibilityDeletes, release, err := LoadPersistedColumnData(
			ctx, readSchema, node.object.rt, id, colIdxs, location, mp, snapshotTS,
			true, true,
		)
		if err != nil {
			return err
		}
		if err = func() error {
			defer func() {
				closePersistedVectors(vecs)
				if release != nil {
					release()
				}
			}()
			rowCount := vecs[0].Length()
			rowIDs, validateErr := ioutil.ValidateTombstoneRowIDColumn(
				rowCount, vecs[0].GetDownstreamVector(),
			)
			if validateErr != nil {
				return validateErr
			}
			var commitTSs ioutil.TombstoneCommitTSColumn
			if !meta.IsAppendable() && !vecs[1].IsConstNull() {
				commitTSs, err = ioutil.ValidateTombstoneCommitTSColumn(
					rowCount, vecs[1].GetDownstreamVector(),
				)
				if err != nil {
					return err
				}
			}
			for i := 0; i < len(rowIDs); i++ {
				if i&1023 == 0 {
					select {
					case <-ctx.Done():
						return context.Cause(ctx)
					default:
					}
				}
				if visibilityDeletes != nil && visibilityDeletes.Contains(uint64(i)) {
					continue
				}
				if commitTSs.IsPresent() {
					commitTS := commitTSs.At(i)
					if commitTS.Equal(&txnif.UncommitTS) || commitTS.GT(&startTS) {
						continue
					}
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
			return nil
		}(); err != nil {
			return err
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
