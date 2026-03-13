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
	"strings"

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
	pk  []byte
	row []byte
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

	// beforeRows contains the visible row set at start.Prev(), keyed by encoded
	// primary key. Rows are deleted from this map once they are matched against
	// the end snapshot. Anything left in the map after the end snapshot is fully
	// consumed represents a delete in the requested range.
	beforeRows map[string]visibleStateRow

	// afterReaders streams the visible row set at end. Streaming keeps the memory
	// footprint bounded even when the end snapshot is large.
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
	h := &VisibleStateChangesHandle{
		tbl:          tbl,
		start:        start,
		end:          end,
		skipDeletes:  skipDeletes,
		coarseMaxRow: coarseMaxRow,
		mp:           effectiveMP,
		beforeRows:   make(map[string]visibleStateRow),
		pkScanIdx:    -1,
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
func (h *VisibleStateChangesHandle) Next(ctx context.Context, mp *mpool.MPool) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	hint = engine.ChangesHandle_Tail_done
	data = h.newDataBatch()
	if !h.skipDeletes {
		tombstone = h.newTombstoneBatch()
	}

	for !h.afterDone {
		if err = h.ensureAfterBatch(ctx); err != nil {
			if data != nil {
				data.Clean(mp)
			}
			if tombstone != nil {
				tombstone.Clean(mp)
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
						data.Clean(mp)
					}
					if tombstone != nil {
						tombstone.Clean(mp)
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
						if err = h.appendTombstoneRow(tombstone, prev.pk); err != nil {
							if data != nil {
								data.Clean(mp)
							}
							if tombstone != nil {
								tombstone.Clean(mp)
							}
							return nil, nil, hint, err
						}
					}
					if err = h.appendDataRow(data, h.currentAfter, h.currentRow); err != nil {
						if data != nil {
							data.Clean(mp)
						}
						if tombstone != nil {
							tombstone.Clean(mp)
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
		h.currentAfter.Clean(mp)
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
			h.beforeRows = nil
		}
		for h.deleteIdx < len(h.pendingDeletes) && tombstone.RowCount() < int(h.coarseMaxRow) {
			if err = h.appendTombstoneRow(tombstone, h.pendingDeletes[h.deleteIdx].pk); err != nil {
				if data != nil {
					data.Clean(mp)
				}
				if tombstone != nil {
					tombstone.Clean(mp)
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
				h.beforeRows[string(pkBytes)] = visibleStateRow{
					pk:  pkBytes,
					row: rowBytes,
				}
			}
			readBatch.Clean(h.mp)
		}
	}
	return nil
}

// buildSnapshotReaders opens readers at a specific visible snapshot. Missing
// tables at the requested timestamp are treated as an empty visible row set by
// getRelationAt.
func (h *VisibleStateChangesHandle) buildSnapshotReaders(ctx context.Context, at types.TS) ([]engine.Reader, error) {
	rel, err := h.getRelationAt(ctx, at)
	if err != nil || rel == nil {
		return nil, err
	}
	relData, err := rel.Ranges(ctx, engine.DefaultRangesParam)
	if err != nil {
		return nil, err
	}
	return rel.BuildReaders(
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
}

// getRelationAt resolves the table handle at a snapshot timestamp. If the table
// does not exist at that snapshot, the caller observes an empty row set rather
// than an error, which is exactly what visible-state diff needs at a boundary.
func (h *VisibleStateChangesHandle) getRelationAt(ctx context.Context, at types.TS) (engine.Relation, error) {
	_, _, rel, err := h.tbl.eng.GetRelationById(ctx, h.tbl.db.op.CloneSnapshotOp(at.ToTimestamp()), h.tbl.tableId)
	if err != nil {
		if strings.Contains(err.Error(), "can not find table by id") {
			return nil, nil
		}
		return nil, err
	}
	return rel, nil
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
	for outIdx, srcIdx := range h.dataScanIdxes {
		vec := src.Vecs[srcIdx]
		var val any
		if !vec.IsNull(uint64(row)) {
			val = vector.GetAny(vec, row, true)
		}
		if err := vector.AppendAny(dst.Vecs[outIdx], val, vec.IsNull(uint64(row)), h.mp); err != nil {
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
func (h *VisibleStateChangesHandle) appendTombstoneRow(dst *batch.Batch, pkBytes []byte) error {
	if dst == nil {
		return nil
	}
	val := types.DecodeValue(pkBytes, h.pkType.Oid)
	if err := vector.AppendAny(dst.Vecs[0], val, false, h.mp); err != nil {
		return err
	}
	if err := vector.AppendFixed(dst.Vecs[1], h.end, false, h.mp); err != nil {
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
	bat := batch.NewWithSize(len(h.dataAttrs) + 1)
	bat.SetAttributes(append(append([]string{}, h.dataAttrs...), objectio.DefaultCommitTS_Attr))
	for i := range h.dataTypes {
		bat.Vecs[i] = vector.NewVec(h.dataTypes[i])
	}
	bat.Vecs[len(bat.Vecs)-1] = vector.NewVec(types.T_TS.ToType())
	return bat
}

func (h *VisibleStateChangesHandle) newTombstoneBatch() *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{
		objectio.TombstoneAttr_PK_Attr,
		objectio.DefaultCommitTS_Attr,
	})
	bat.Vecs[0] = vector.NewVec(h.pkType)
	bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	return bat
}
