// Copyright 2026 Matrix Origin
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

package disttae

import (
	"bytes"
	"context"
	"encoding/binary"
	"reflect"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type visibleStateRow struct {
	pk    []byte
	rowID []byte
	row   []byte
}

// VisibleStateChangesHandle reconstructs the net effect of [start, end] from
// two visible snapshots:
//   - the visible row set at start.Prev()
//   - the visible row set at end
//
// This handle does not attempt to replay every historical intermediate change.
// Instead it emits the final net delta expected by exact-diff style callers:
// inserts are returned as data rows, deletes as tombstones, and updates as a
// tombstone plus a data row for the same primary key.
//
// The emitted commit ts is synthetic and always set to end. Callers using this
// handle only need a stable ordering key for the current logical range; they do
// not rely on replaying the original per-row commit timestamps.
type VisibleStateChangesHandle struct {
	tbl          *txnTable
	start        types.TS
	end          types.TS
	skipDeletes  bool
	coarseMaxRow uint32
	mp           *mpool.MPool

	// scan* describes the full visible snapshot scan, including the primary key
	// column that is later encoded into a deterministic row key.
	scanAttrs []string
	scanTypes []types.Type

	// data* describes the output data batch. It excludes hidden rowid columns and
	// mirrors the row layout expected by the existing diff/hash logic.
	dataScanIdxes []int
	dataAttrs     []string
	dataTypes     []types.Type
	pkScanIdx     int
	pkType        types.Type
	rowIDScanIdx  int
	rowIDType     types.Type
	retainRowID   bool

	// beforeRows contains the visible row set at start.Prev(), keyed by encoded
	// primary key. Rows are deleted from this map once they are matched against
	// the end snapshot. Anything left in the map after the end snapshot is fully
	// consumed represents a delete in the requested range.
	beforeRows map[string]visibleStateRow

	// afterReaders streams the visible row set at end so recovery materializes
	// only the before-side map plus one end-side batch at a time.
	afterReaders   []engine.Reader
	afterReaderIdx int
	currentAfter   *batch.Batch
	currentRow     int
	afterDone      bool

	// pendingDeletes materializes the leftovers from beforeRows only after the
	// end snapshot has been fully scanned. This lets Next() keep the same
	// chunked-batch contract as other ChangesHandle implementations.
	pendingDeletes []visibleStateRow
	deleteIdx      int
}

// NewVisibleStateChangesHandle prepares the two boundary snapshots needed to
// compute the net diff of [start, end]. start.Prev() is used deliberately so
// that rows committed exactly at start are treated as part of the requested
// range, matching the existing inclusive CollectChanges semantics.
func NewVisibleStateChangesHandle(
	ctx context.Context,
	tbl *txnTable,
	start, end types.TS,
	skipDeletes bool,
	coarseMaxRow uint32,
	mp *mpool.MPool,
) (*VisibleStateChangesHandle, error) {
	if end.IsEmpty() || (!start.IsEmpty() && start.GT(&end)) {
		return nil, moerr.NewInternalErrorNoCtx("invalid timestamp")
	}
	effectiveMP := mp
	if effectiveMP == nil && tbl != nil {
		if proc := tbl.proc.Load(); proc != nil {
			effectiveMP = proc.Mp()
		}
	}
	if effectiveMP == nil {
		return nil, moerr.NewInternalErrorNoCtx("visible-state changes handle requires a non-nil mpool")
	}
	if tbl == nil {
		return nil, moerr.NewInternalErrorNoCtx("visible-state changes handle requires a table")
	}
	h := &VisibleStateChangesHandle{
		tbl:          tbl,
		start:        start,
		end:          end,
		skipDeletes:  skipDeletes,
		coarseMaxRow: coarseMaxRow,
		mp:           effectiveMP,
		beforeRows:   make(map[string]visibleStateRow),
		pkScanIdx:    -1,
		rowIDScanIdx: -1,
		retainRowID:  engine.RetainRowIDFromContext(ctx),
	}
	if err := h.initSchema(ctx); err != nil {
		return nil, err
	}
	if !start.IsEmpty() {
		if err := h.loadVisibleRows(ctx, start.Prev()); err != nil {
			return nil, err
		}
	}
	readers, err := h.buildSnapshotReaders(ctx, end)
	if err != nil {
		return nil, err
	}
	h.afterReaders = readers
	return h, nil
}

// Next emits chunked net changes derived from the two visible snapshots. The
// output is intentionally shaped like a normal change stream so existing diff
// consumers can reuse the same downstream hashing and merge logic.
func (h *VisibleStateChangesHandle) Next(ctx context.Context, _ *mpool.MPool) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	hint = engine.ChangesHandle_Tail_done
	data = h.newDataBatch()
	if !h.skipDeletes {
		tombstone = h.newTombstoneBatch()
	}

	for !h.afterDone {
		if err = h.ensureAfterBatch(ctx); err != nil {
			if data != nil {
				data.Clean(h.mp)
			}
			if tombstone != nil {
				tombstone.Clean(h.mp)
			}
			return nil, nil, hint, err
		}
		if h.afterDone {
			break
		}
		for h.currentRow < h.currentAfter.RowCount() {
			pkBytes, rowBytes := h.encodeSnapshotRow(h.currentAfter, h.currentRow)
			key := string(pkBytes)
			if prev, ok := h.beforeRows[key]; !ok {
				// Present only in the end snapshot: insert in [start, end].
				if err = h.appendDataRow(data, h.currentAfter, h.currentRow); err != nil {
					if data != nil {
						data.Clean(h.mp)
					}
					if tombstone != nil {
						tombstone.Clean(h.mp)
					}
					return nil, nil, hint, err
				}
			} else {
				delete(h.beforeRows, key)
				if !bytes.Equal(prev.row, rowBytes) {
					// Present in both snapshots but with different visible row
					// payloads: model it as delete + insert so downstream exact-diff
					// code observes the same update shape it already understands.
					if !h.skipDeletes {
						if err = h.appendTombstoneRow(tombstone, prev); err != nil {
							if data != nil {
								data.Clean(h.mp)
							}
							if tombstone != nil {
								tombstone.Clean(h.mp)
							}
							return nil, nil, hint, err
						}
					}
					if err = h.appendDataRow(data, h.currentAfter, h.currentRow); err != nil {
						if data != nil {
							data.Clean(h.mp)
						}
						if tombstone != nil {
							tombstone.Clean(h.mp)
						}
						return nil, nil, hint, err
					}
				}
			}
			h.currentRow++
			if h.isBatchFull(data, tombstone) {
				data, tombstone = h.normalizeOutput(data, tombstone)
				return data, tombstone, hint, nil
			}
		}
		h.currentAfter.Clean(h.mp)
		h.currentAfter = nil
		h.currentRow = 0
	}

	if !h.skipDeletes {
		if h.pendingDeletes == nil {
			// Rows still left in beforeRows were visible before the range but not
			// visible at end, so they are deletes in [start, end].
			h.pendingDeletes = make([]visibleStateRow, 0, len(h.beforeRows))
			for _, row := range h.beforeRows {
				h.pendingDeletes = append(h.pendingDeletes, row)
			}
			sort.Slice(h.pendingDeletes, func(i, j int) bool {
				return bytes.Compare(h.pendingDeletes[i].pk, h.pendingDeletes[j].pk) < 0
			})
			h.beforeRows = nil
		}
		for h.deleteIdx < len(h.pendingDeletes) && tombstone.RowCount() < int(h.coarseMaxRow) {
			if err = h.appendTombstoneRow(tombstone, h.pendingDeletes[h.deleteIdx]); err != nil {
				if data != nil {
					data.Clean(h.mp)
				}
				if tombstone != nil {
					tombstone.Clean(h.mp)
				}
				return nil, nil, hint, err
			}
			h.deleteIdx++
		}
	}

	data, tombstone = h.normalizeOutput(data, tombstone)
	return data, tombstone, hint, nil
}

func (h *VisibleStateChangesHandle) Close() error {
	if h == nil {
		return nil
	}
	if h.currentAfter != nil {
		// Query cancellation may close a partially initialized handle. Keep Close
		// panic-free even if mp is unexpectedly nil.
		if h.mp != nil {
			h.currentAfter.Clean(h.mp)
		} else {
			h.currentAfter.CleanOnlyData()
		}
		h.currentAfter = nil
	}
	for _, reader := range h.afterReaders {
		closeEngineReader(reader)
	}
	h.afterReaders = nil
	h.beforeRows = nil
	h.pendingDeletes = nil
	return nil
}

// closeEngineReader skips typed-nil interfaces and keeps Close idempotent.
func closeEngineReader(reader engine.Reader) {
	if isNilEngineReader(reader) {
		return
	}
	_ = reader.Close()
}

func isNilEngineReader(reader engine.Reader) bool {
	if reader == nil {
		return true
	}
	value := reflect.ValueOf(reader)
	switch value.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		return value.IsNil()
	default:
		return false
	}
}

func (h *VisibleStateChangesHandle) initSchema(ctx context.Context) error {
	tblDef := h.tbl.GetTableDef(ctx)
	for _, col := range tblDef.Cols {
		colType := plan2.ExprType2Type(&col.Typ)
		h.scanAttrs = append(h.scanAttrs, col.Name)
		h.scanTypes = append(h.scanTypes, colType)
		if colType.Oid == types.T_Rowid {
			h.rowIDScanIdx = len(h.scanAttrs) - 1
			h.rowIDType = colType
			continue
		}
		if col.Name == tblDef.Pkey.PkeyColName {
			h.pkScanIdx = len(h.scanAttrs) - 1
			h.pkType = colType
		}
		h.dataScanIdxes = append(h.dataScanIdxes, len(h.scanAttrs)-1)
		h.dataAttrs = append(h.dataAttrs, col.Name)
		h.dataTypes = append(h.dataTypes, colType)
	}
	if h.pkScanIdx < 0 {
		return moerr.NewInternalErrorNoCtx("primary key column not found")
	}
	if h.retainRowID && h.rowIDScanIdx < 0 {
		return moerr.NewInternalErrorNoCtx("rowid column not found")
	}
	return nil
}

// loadVisibleRows materializes the visible row set at a snapshot boundary into
// beforeRows. This is only done for the "before" side; the "after" side stays
// streaming to avoid loading both snapshots into memory at once.
func (h *VisibleStateChangesHandle) loadVisibleRows(ctx context.Context, at types.TS) error {
	readers, err := h.buildSnapshotReaders(ctx, at)
	if err != nil {
		return err
	}
	defer func() {
		for _, reader := range readers {
			closeEngineReader(reader)
		}
	}()
	for _, reader := range readers {
		for {
			readBatch := h.newScanBatch()
			isEnd, err := reader.Read(ctx, h.scanAttrs, nil, h.mp, readBatch)
			if err != nil {
				readBatch.Clean(h.mp)
				return err
			}
			if isEnd {
				readBatch.Clean(h.mp)
				break
			}
			for row := 0; row < readBatch.RowCount(); row++ {
				pkBytes, rowBytes := h.encodeSnapshotRow(readBatch, row)
				stored := visibleStateRow{pk: pkBytes, row: rowBytes}
				if h.retainRowID {
					if readBatch.Vecs[h.rowIDScanIdx].IsNull(uint64(row)) {
						readBatch.Clean(h.mp)
						return moerr.NewInternalErrorNoCtx("visible-state snapshot row has null rowid")
					}
					stored.rowID = h.encodeValue(readBatch.Vecs[h.rowIDScanIdx], row)
				}
				h.beforeRows[string(pkBytes)] = stored
			}
			readBatch.Clean(h.mp)
		}
	}
	return nil
}

// buildSnapshotReaders opens readers at a specific visible snapshot. Snapshot
// relation lookup errors are returned unchanged: treating an unavailable
// historical table as empty would turn every end row into a false insert.
func (h *VisibleStateChangesHandle) buildSnapshotReaders(ctx context.Context, at types.TS) ([]engine.Reader, error) {
	rel, err := h.getRelationAt(ctx, at)
	if err != nil {
		return nil, err
	}
	if rel == nil {
		return nil, moerr.NewInternalErrorNoCtxf(
			"table %d resolved to nil at snapshot %s",
			h.tbl.tableId,
			at.ToString(),
		)
	}
	relData, err := rel.Ranges(ctx, engine.DefaultRangesParam)
	if err != nil {
		return nil, err
	}
	readers, err := rel.BuildReaders(
		ctx,
		h.tbl.proc.Load(),
		nil,
		relData,
		1,
		0,
		false,
		engine.Policy_CheckCommittedOnly,
		engine.FilterHint{},
	)
	if err != nil {
		for _, reader := range readers {
			closeEngineReader(reader)
		}
		return nil, err
	}
	return readers, nil
}

// getRelationAt resolves the table handle at a snapshot timestamp. All errors
// must propagate: GetRelationById currently uses the same error shape for an
// actually absent table and for unavailable historical catalog state, so it is
// unsafe to infer an empty visible row set here.
func (h *VisibleStateChangesHandle) getRelationAt(ctx context.Context, at types.TS) (engine.Relation, error) {
	_, _, rel, err := h.tbl.eng.GetRelationById(
		ctx,
		h.tbl.db.op.CloneSnapshotOp(at.ToTimestamp()),
		h.tbl.tableId,
	)
	return rel, err
}

// ensureAfterBatch advances the streaming end snapshot reader lazily, one batch
// at a time.
func (h *VisibleStateChangesHandle) ensureAfterBatch(ctx context.Context) error {
	for h.currentAfter == nil {
		if h.afterReaderIdx >= len(h.afterReaders) {
			h.afterDone = true
			return nil
		}
		readBatch := h.newScanBatch()
		isEnd, err := h.afterReaders[h.afterReaderIdx].Read(ctx, h.scanAttrs, nil, h.mp, readBatch)
		if err != nil {
			readBatch.Clean(h.mp)
			return err
		}
		if isEnd {
			readBatch.Clean(h.mp)
			h.afterReaderIdx++
			continue
		}
		h.currentAfter = readBatch
		h.currentRow = 0
	}
	return nil
}

// encodeSnapshotRow builds stable comparison keys from the visible snapshot
// output. The primary key is encoded separately for map lookup, while the whole
// visible row payload is encoded for update detection.
func (h *VisibleStateChangesHandle) encodeSnapshotRow(src *batch.Batch, row int) ([]byte, []byte) {
	pkBytes := h.encodeValue(src.Vecs[h.pkScanIdx], row)
	rowBytes := make([]byte, 0, 128)
	for _, idx := range h.dataScanIdxes {
		rowBytes = h.encodeField(rowBytes, src.Vecs[idx], row)
	}
	return pkBytes, rowBytes
}

// encodeField uses an explicit null marker and length prefix so different value
// layouts cannot alias to the same byte sequence when we compare visible rows.
func (h *VisibleStateChangesHandle) encodeField(dst []byte, vec *vector.Vector, row int) []byte {
	if vec.IsNull(uint64(row)) {
		return append(dst, 0)
	}
	dst = append(dst, 1)
	valBytes := h.encodeValue(vec, row)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(valBytes)))
	dst = append(dst, lenBuf[:]...)
	dst = append(dst, valBytes...)
	return dst
}

func (h *VisibleStateChangesHandle) encodeValue(vec *vector.Vector, row int) []byte {
	if vec.IsNull(uint64(row)) {
		return nil
	}
	val := vector.GetAny(vec, row, true)
	encoded := types.EncodeValue(val, vec.GetType().Oid)
	return append([]byte(nil), encoded...)
}

// appendDataRow appends one visible row from the end snapshot and assigns the
// synthetic commit ts of the logical range end.
func (h *VisibleStateChangesHandle) appendDataRow(dst *batch.Batch, src *batch.Batch, row int) error {
	outOffset := 0
	if h.retainRowID {
		rowIDVec := src.Vecs[h.rowIDScanIdx]
		if rowIDVec.IsNull(uint64(row)) {
			return moerr.NewInternalErrorNoCtx("visible-state snapshot row has null rowid")
		}
		if err := vector.AppendAny(
			dst.Vecs[0],
			vector.GetAny(rowIDVec, row, true),
			rowIDVec.IsNull(uint64(row)),
			h.mp,
		); err != nil {
			return err
		}
		outOffset = 1
	}
	for outIdx, srcIdx := range h.dataScanIdxes {
		vec := src.Vecs[srcIdx]
		var val any
		if !vec.IsNull(uint64(row)) {
			val = vector.GetAny(vec, row, true)
		}
		if err := vector.AppendAny(dst.Vecs[outIdx+outOffset], val, vec.IsNull(uint64(row)), h.mp); err != nil {
			return err
		}
	}
	if err := vector.AppendFixed(dst.Vecs[len(dst.Vecs)-1], h.end, false, h.mp); err != nil {
		return err
	}
	dst.SetRowCount(dst.Vecs[0].Length())
	return nil
}

// appendTombstoneRow appends a delete for the given primary key and uses the
// same synthetic commit ts as appendDataRow so updates stay ordered within one
// logical range.
func (h *VisibleStateChangesHandle) appendTombstoneRow(dst *batch.Batch, visibleRow visibleStateRow) error {
	if dst == nil {
		return nil
	}
	pkIdx := 0
	if h.retainRowID {
		if len(visibleRow.rowID) != types.RowidSize {
			return moerr.NewInternalErrorNoCtx("visible-state tombstone has invalid rowid")
		}
		rowID := types.DecodeValue(visibleRow.rowID, h.rowIDType.Oid)
		if err := vector.AppendAny(dst.Vecs[0], rowID, false, h.mp); err != nil {
			return err
		}
		pkIdx = 1
	}
	val := types.DecodeValue(visibleRow.pk, h.pkType.Oid)
	if err := vector.AppendAny(dst.Vecs[pkIdx], val, false, h.mp); err != nil {
		return err
	}
	if err := vector.AppendFixed(dst.Vecs[pkIdx+1], h.end, false, h.mp); err != nil {
		return err
	}
	dst.SetRowCount(dst.Vecs[0].Length())
	return nil
}

func (h *VisibleStateChangesHandle) isBatchFull(data, tombstone *batch.Batch) bool {
	if data != nil && data.RowCount() >= int(h.coarseMaxRow) {
		return true
	}
	return tombstone != nil && tombstone.RowCount() >= int(h.coarseMaxRow)
}

func (h *VisibleStateChangesHandle) normalizeOutput(data, tombstone *batch.Batch) (*batch.Batch, *batch.Batch) {
	if data != nil && data.RowCount() == 0 {
		data.Clean(h.mp)
		data = nil
	}
	if tombstone != nil && tombstone.RowCount() == 0 {
		tombstone.Clean(h.mp)
		tombstone = nil
	}
	return data, tombstone
}

func (h *VisibleStateChangesHandle) newScanBatch() *batch.Batch {
	bat := batch.NewWithSize(len(h.scanAttrs))
	bat.SetAttributes(h.scanAttrs)
	for i := range h.scanAttrs {
		bat.Vecs[i] = vector.NewVec(h.scanTypes[i])
	}
	return bat
}

func (h *VisibleStateChangesHandle) newDataBatch() *batch.Batch {
	leading := 0
	attrs := make([]string, 0, len(h.dataAttrs)+2)
	if h.retainRowID {
		leading = 1
		attrs = append(attrs, catalog.Row_ID)
	}
	attrs = append(attrs, h.dataAttrs...)
	attrs = append(attrs, objectio.DefaultCommitTS_Attr)
	bat := batch.NewWithSize(len(attrs))
	bat.SetAttributes(attrs)
	if h.retainRowID {
		bat.Vecs[0] = vector.NewVec(h.rowIDType)
	}
	for i := range h.dataTypes {
		bat.Vecs[i+leading] = vector.NewVec(h.dataTypes[i])
	}
	bat.Vecs[len(bat.Vecs)-1] = vector.NewVec(types.T_TS.ToType())
	return bat
}

func (h *VisibleStateChangesHandle) newTombstoneBatch() *batch.Batch {
	attrs := []string{objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
	if h.retainRowID {
		attrs = append([]string{catalog.Row_ID}, attrs...)
	}
	bat := batch.NewWithSize(len(attrs))
	bat.SetAttributes(attrs)
	idx := 0
	if h.retainRowID {
		bat.Vecs[0] = vector.NewVec(h.rowIDType)
		idx = 1
	}
	bat.Vecs[idx] = vector.NewVec(h.pkType)
	bat.Vecs[idx+1] = vector.NewVec(types.T_TS.ToType())
	return bat
}
