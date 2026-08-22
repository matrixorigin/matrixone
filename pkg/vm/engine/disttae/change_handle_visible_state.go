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

type visibleStateSnapshotScan struct {
	attrs        []string
	types        []types.Type
	compareIdxes []int
	pkIdx        int
	rowIDIdx     int
	readers      []engine.Reader
	readerIdx    int
}

type visibleStateRow struct {
	pk    []byte
	rowID []byte
	row   []byte
}

// VisibleStateChangesHandle reconstructs the net effect of [start, end] from
// the visible row sets at start.Prev() and end. Each boundary is read with its
// own historical schema; only columns with the same physical sequence number
// and type participate in update comparison.
type VisibleStateChangesHandle struct {
	tbl          *txnTable
	start        types.TS
	end          types.TS
	skipDeletes  bool
	coarseMaxRow uint32
	mp           *mpool.MPool
	retainRowID  bool

	beforeScan visibleStateSnapshotScan
	afterScan  visibleStateSnapshotScan

	dataScanIdxes []int
	dataAttrs     []string
	dataTypes     []types.Type
	pkType        types.Type
	rowIDType     types.Type

	beforeRows engine.VisibleStateStore

	currentAfter *batch.Batch
	currentRow   int
	afterDone    bool
}

// NewVisibleStateChangesHandle prepares the two boundary snapshots needed to
// compute the net diff. start.Prev() preserves inclusive CollectChanges
// semantics for rows committed exactly at start.
func NewVisibleStateChangesHandle(
	ctx context.Context,
	tbl *txnTable,
	start, end types.TS,
	skipDeletes bool,
	coarseMaxRow uint32,
	mp *mpool.MPool,
	resources engine.VisibleStateRecoveryResources,
	startRelation engine.Relation,
) (_ *VisibleStateChangesHandle, err error) {
	if end.IsEmpty() || (!start.IsEmpty() && start.GT(&end)) {
		return nil, moerr.NewInternalErrorNoCtx("invalid timestamp")
	}
	if tbl == nil {
		return nil, moerr.NewInternalErrorNoCtx("visible-state changes handle requires a table")
	}
	if resources == nil {
		return nil, moerr.NewInternalErrorNoCtx("visible-state changes handle requires bounded recovery resources")
	}
	effectiveMP := mp
	if effectiveMP == nil {
		if proc := tbl.proc.Load(); proc != nil {
			effectiveMP = proc.Mp()
		}
	}
	if effectiveMP == nil {
		return nil, moerr.NewInternalErrorNoCtx("visible-state changes handle requires a non-nil mpool")
	}

	h := &VisibleStateChangesHandle{
		tbl: tbl, start: start, end: end, skipDeletes: skipDeletes,
		coarseMaxRow: coarseMaxRow, mp: effectiveMP,
		retainRowID: engine.RetainRowIDFromContext(ctx),
		beforeScan:  visibleStateSnapshotScan{pkIdx: -1, rowIDIdx: -1},
		afterScan:   visibleStateSnapshotScan{pkIdx: -1, rowIDIdx: -1},
	}
	defer func() {
		if err != nil {
			_ = h.Close()
		}
	}()

	var beforeRel engine.Relation
	if !start.IsEmpty() {
		beforeRel = startRelation
		if beforeRel == nil {
			if beforeRel, err = h.getRelationAt(ctx, start.Prev()); err != nil {
				return nil, err
			}
		}
		if beforeRel == nil {
			return nil, moerr.NewInternalErrorNoCtxf(
				"table %d resolved to nil at snapshot %s", tbl.tableId, start.Prev().ToString())
		}
	}
	afterRel, err := h.getRelationAt(ctx, end)
	if err != nil {
		return nil, err
	}
	if afterRel == nil {
		return nil, moerr.NewInternalErrorNoCtxf(
			"table %d resolved to nil at snapshot %s", tbl.tableId, end.ToString())
	}

	var beforeDef *plan2.TableDef
	if beforeRel != nil {
		beforeDef = beforeRel.GetTableDef(ctx)
	}
	if err = h.initSchema(beforeDef, afterRel.GetTableDef(ctx)); err != nil {
		return nil, err
	}
	if h.beforeRows, err = resources.NewVisibleStateStore(); err != nil {
		return nil, err
	}
	if beforeRel != nil {
		if h.beforeScan.readers, err = h.buildSnapshotReaders(ctx, beforeRel); err != nil {
			return nil, err
		}
		if err = h.loadVisibleRows(ctx); err != nil {
			return nil, err
		}
	}
	if h.afterScan.readers, err = h.buildSnapshotReaders(ctx, afterRel); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *VisibleStateChangesHandle) Next(ctx context.Context, _ *mpool.MPool) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	hint = engine.ChangesHandle_Tail_done
	data = h.newDataBatch()
	if !h.skipDeletes {
		tombstone = h.newTombstoneBatch()
	}
	cleanOutput := func() {
		if data != nil {
			data.Clean(h.mp)
		}
		if tombstone != nil {
			tombstone.Clean(h.mp)
		}
	}

	for !h.afterDone {
		if err = h.ensureAfterBatch(ctx); err != nil {
			cleanOutput()
			return nil, nil, hint, err
		}
		if h.afterDone {
			break
		}
		for h.currentRow < h.currentAfter.RowCount() {
			pkBytes, rowBytes := h.encodeSnapshotRow(h.currentAfter, h.currentRow, &h.afterScan)
			stored, ok, popErr := h.beforeRows.Pop(pkBytes)
			if popErr != nil {
				cleanOutput()
				return nil, nil, hint, popErr
			}
			if !ok {
				if err = h.appendDataRow(data, h.currentAfter, h.currentRow); err != nil {
					cleanOutput()
					return nil, nil, hint, err
				}
			} else {
				prev, decodeErr := decodeVisibleStateRow(pkBytes, stored)
				if decodeErr != nil {
					cleanOutput()
					return nil, nil, hint, decodeErr
				}
				if !bytes.Equal(prev.row, rowBytes) {
					if !h.skipDeletes {
						if err = h.appendTombstoneRow(tombstone, prev); err != nil {
							cleanOutput()
							return nil, nil, hint, err
						}
					}
					if err = h.appendDataRow(data, h.currentAfter, h.currentRow); err != nil {
						cleanOutput()
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

	if h.skipDeletes {
		if err = h.closeBeforeRows(); err != nil {
			cleanOutput()
			return nil, nil, hint, err
		}
	} else if h.beforeRows != nil && h.beforeRows.Len() > 0 {
		pending := make([]visibleStateRow, 0, h.coarseMaxRow)
		_, err = h.beforeRows.Drain(int(h.coarseMaxRow), func(key, value []byte) error {
			row, decodeErr := decodeVisibleStateRow(key, value)
			if decodeErr != nil {
				return decodeErr
			}
			pending = append(pending, row)
			return nil
		})
		if err != nil {
			cleanOutput()
			return nil, nil, hint, err
		}
		sort.Slice(pending, func(i, j int) bool {
			return bytes.Compare(pending[i].pk, pending[j].pk) < 0
		})
		for i := range pending {
			if err = h.appendTombstoneRow(tombstone, pending[i]); err != nil {
				cleanOutput()
				return nil, nil, hint, err
			}
		}
	}
	if h.beforeRows != nil && h.beforeRows.Len() == 0 {
		if err = h.closeBeforeRows(); err != nil {
			cleanOutput()
			return nil, nil, hint, err
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
		if h.mp != nil {
			h.currentAfter.Clean(h.mp)
		} else {
			h.currentAfter.CleanOnlyData()
		}
		h.currentAfter = nil
	}
	for _, scan := range []*visibleStateSnapshotScan{&h.beforeScan, &h.afterScan} {
		for _, reader := range scan.readers {
			closeEngineReader(reader)
		}
		scan.readers = nil
	}
	return h.closeBeforeRows()
}

func (h *VisibleStateChangesHandle) closeBeforeRows() error {
	if h.beforeRows == nil {
		return nil
	}
	err := h.beforeRows.Close()
	h.beforeRows = nil
	return err
}

func closeEngineReader(reader engine.Reader) {
	if !isNilEngineReader(reader) {
		_ = reader.Close()
	}
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

func (h *VisibleStateChangesHandle) initSchema(beforeDef, afterDef *plan2.TableDef) error {
	if afterDef == nil || afterDef.Pkey == nil {
		return moerr.NewInternalErrorNoCtx("visible-state end snapshot has no primary key definition")
	}
	afterBySeqnum := make(map[uint32]int, len(afterDef.Cols))
	for _, col := range afterDef.Cols {
		colType := plan2.ExprType2Type(&col.Typ)
		idx := len(h.afterScan.attrs)
		h.afterScan.attrs = append(h.afterScan.attrs, col.Name)
		h.afterScan.types = append(h.afterScan.types, colType)
		afterBySeqnum[col.Seqnum] = idx
		if colType.Oid == types.T_Rowid {
			h.afterScan.rowIDIdx = idx
			h.rowIDType = colType
			continue
		}
		if col.Name == afterDef.Pkey.PkeyColName {
			h.afterScan.pkIdx = idx
			h.pkType = colType
		}
		h.dataScanIdxes = append(h.dataScanIdxes, idx)
		h.dataAttrs = append(h.dataAttrs, col.Name)
		h.dataTypes = append(h.dataTypes, colType)
	}
	if h.afterScan.pkIdx < 0 {
		return moerr.NewInternalErrorNoCtx("primary key column not found in end snapshot")
	}
	if h.retainRowID && h.afterScan.rowIDIdx < 0 {
		return moerr.NewInternalErrorNoCtx("rowid column not found in end snapshot")
	}
	if beforeDef == nil {
		h.afterScan.compareIdxes = append(h.afterScan.compareIdxes, h.dataScanIdxes...)
		return nil
	}
	if beforeDef.Pkey == nil {
		return moerr.NewInternalErrorNoCtx("visible-state start snapshot has no primary key definition")
	}
	beforePKSeqnum := uint32(0)
	afterPKSeqnum := afterDef.Cols[h.afterScan.pkIdx].Seqnum
	for _, col := range beforeDef.Cols {
		colType := plan2.ExprType2Type(&col.Typ)
		idx := len(h.beforeScan.attrs)
		h.beforeScan.attrs = append(h.beforeScan.attrs, col.Name)
		h.beforeScan.types = append(h.beforeScan.types, colType)
		if colType.Oid == types.T_Rowid {
			h.beforeScan.rowIDIdx = idx
			continue
		}
		if col.Name == beforeDef.Pkey.PkeyColName {
			h.beforeScan.pkIdx = idx
			beforePKSeqnum = col.Seqnum
		}
		afterIdx, ok := afterBySeqnum[col.Seqnum]
		if !ok || h.afterScan.types[afterIdx].Oid == types.T_Rowid || h.afterScan.types[afterIdx] != colType {
			continue
		}
		h.beforeScan.compareIdxes = append(h.beforeScan.compareIdxes, idx)
		h.afterScan.compareIdxes = append(h.afterScan.compareIdxes, afterIdx)
	}
	if h.beforeScan.pkIdx < 0 {
		return moerr.NewInternalErrorNoCtx("primary key column not found in start snapshot")
	}
	if beforePKSeqnum != afterPKSeqnum || h.beforeScan.types[h.beforeScan.pkIdx] != h.pkType {
		return moerr.NewInternalErrorNoCtx("primary key lineage changed across visible-state snapshots")
	}
	if h.retainRowID && h.beforeScan.rowIDIdx < 0 {
		return moerr.NewInternalErrorNoCtx("rowid column not found in start snapshot")
	}
	return nil
}

func (h *VisibleStateChangesHandle) loadVisibleRows(ctx context.Context) error {
	defer func() {
		for _, reader := range h.beforeScan.readers {
			closeEngineReader(reader)
		}
		h.beforeScan.readers = nil
	}()
	for _, reader := range h.beforeScan.readers {
		for {
			readBatch := newVisibleStateScanBatch(&h.beforeScan)
			isEnd, err := reader.Read(ctx, h.beforeScan.attrs, nil, h.mp, readBatch)
			if err != nil {
				readBatch.Clean(h.mp)
				return err
			}
			if isEnd {
				readBatch.Clean(h.mp)
				break
			}
			entries := make([]engine.VisibleStateEntry, readBatch.RowCount())
			for row := 0; row < readBatch.RowCount(); row++ {
				pkBytes, rowBytes := h.encodeSnapshotRow(readBatch, row, &h.beforeScan)
				var rowID []byte
				if h.retainRowID {
					if readBatch.Vecs[h.beforeScan.rowIDIdx].IsNull(uint64(row)) {
						readBatch.Clean(h.mp)
						return moerr.NewInternalErrorNoCtx("visible-state snapshot row has null rowid")
					}
					rowID = h.encodeValue(readBatch.Vecs[h.beforeScan.rowIDIdx], row)
				}
				entries[row] = engine.VisibleStateEntry{Key: pkBytes, Value: encodeVisibleStateRow(rowID, rowBytes)}
			}
			if err = h.beforeRows.PutBatch(entries); err != nil {
				readBatch.Clean(h.mp)
				return err
			}
			readBatch.Clean(h.mp)
		}
	}
	return nil
}

func encodeVisibleStateRow(rowID, row []byte) []byte {
	encoded := make([]byte, 4+len(rowID)+len(row))
	binary.LittleEndian.PutUint32(encoded[:4], uint32(len(rowID)))
	copy(encoded[4:], rowID)
	copy(encoded[4+len(rowID):], row)
	return encoded
}

func decodeVisibleStateRow(pk, encoded []byte) (visibleStateRow, error) {
	if len(encoded) < 4 {
		return visibleStateRow{}, moerr.NewInternalErrorNoCtx("visible-state row payload is truncated")
	}
	rowIDLen := int(binary.LittleEndian.Uint32(encoded[:4]))
	if rowIDLen > len(encoded)-4 {
		return visibleStateRow{}, moerr.NewInternalErrorNoCtx("visible-state rowid payload is truncated")
	}
	return visibleStateRow{pk: pk, rowID: encoded[4 : 4+rowIDLen], row: encoded[4+rowIDLen:]}, nil
}

func (h *VisibleStateChangesHandle) buildSnapshotReaders(ctx context.Context, rel engine.Relation) ([]engine.Reader, error) {
	relData, err := rel.Ranges(ctx, engine.DefaultRangesParam)
	if err != nil {
		return nil, err
	}
	readers, err := rel.BuildReaders(ctx, h.tbl.proc.Load(), nil, relData, 1, 0, false,
		engine.Policy_CheckCommittedOnly, engine.FilterHint{})
	if err != nil {
		for _, reader := range readers {
			closeEngineReader(reader)
		}
		return nil, err
	}
	return readers, nil
}

func (h *VisibleStateChangesHandle) getRelationAt(ctx context.Context, at types.TS) (engine.Relation, error) {
	_, _, rel, err := h.tbl.eng.GetRelationById(
		ctx, h.tbl.db.op.CloneSnapshotOp(at.ToTimestamp()), h.tbl.tableId)
	return rel, err
}

func (h *VisibleStateChangesHandle) ensureAfterBatch(ctx context.Context) error {
	for h.currentAfter == nil {
		if h.afterScan.readerIdx >= len(h.afterScan.readers) {
			h.afterDone = true
			return nil
		}
		readBatch := newVisibleStateScanBatch(&h.afterScan)
		isEnd, err := h.afterScan.readers[h.afterScan.readerIdx].Read(
			ctx, h.afterScan.attrs, nil, h.mp, readBatch)
		if err != nil {
			readBatch.Clean(h.mp)
			return err
		}
		if isEnd {
			readBatch.Clean(h.mp)
			h.afterScan.readerIdx++
			continue
		}
		h.currentAfter = readBatch
		h.currentRow = 0
	}
	return nil
}

func (h *VisibleStateChangesHandle) encodeSnapshotRow(src *batch.Batch, row int, scan *visibleStateSnapshotScan) ([]byte, []byte) {
	pkBytes := h.encodeValue(src.Vecs[scan.pkIdx], row)
	rowBytes := make([]byte, 0, 128)
	for _, idx := range scan.compareIdxes {
		rowBytes = h.encodeField(rowBytes, src.Vecs[idx], row)
	}
	return pkBytes, rowBytes
}

func (h *VisibleStateChangesHandle) encodeField(dst []byte, vec *vector.Vector, row int) []byte {
	if vec.IsNull(uint64(row)) {
		return append(dst, 0)
	}
	dst = append(dst, 1)
	valBytes := h.encodeValue(vec, row)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(valBytes)))
	dst = append(dst, lenBuf[:]...)
	return append(dst, valBytes...)
}

func (h *VisibleStateChangesHandle) encodeValue(vec *vector.Vector, row int) []byte {
	if vec.IsNull(uint64(row)) {
		return nil
	}
	val := vector.GetAny(vec, row, true)
	return append([]byte(nil), types.EncodeValue(val, vec.GetType().Oid)...)
}

func (h *VisibleStateChangesHandle) appendDataRow(dst, src *batch.Batch, row int) error {
	outOffset := 0
	if h.retainRowID {
		rowIDVec := src.Vecs[h.afterScan.rowIDIdx]
		if rowIDVec.IsNull(uint64(row)) {
			return moerr.NewInternalErrorNoCtx("visible-state snapshot row has null rowid")
		}
		if err := vector.AppendAny(dst.Vecs[0], vector.GetAny(rowIDVec, row, true), false, h.mp); err != nil {
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

func (h *VisibleStateChangesHandle) appendTombstoneRow(dst *batch.Batch, row visibleStateRow) error {
	if dst == nil {
		return nil
	}
	pkIdx := 0
	if h.retainRowID {
		if len(row.rowID) != types.RowidSize {
			return moerr.NewInternalErrorNoCtx("visible-state tombstone has invalid rowid")
		}
		if err := vector.AppendAny(dst.Vecs[0], types.DecodeValue(row.rowID, h.rowIDType.Oid), false, h.mp); err != nil {
			return err
		}
		pkIdx = 1
	}
	if err := vector.AppendAny(dst.Vecs[pkIdx], types.DecodeValue(row.pk, h.pkType.Oid), false, h.mp); err != nil {
		return err
	}
	if err := vector.AppendFixed(dst.Vecs[pkIdx+1], h.end, false, h.mp); err != nil {
		return err
	}
	dst.SetRowCount(dst.Vecs[0].Length())
	return nil
}

func (h *VisibleStateChangesHandle) isBatchFull(data, tombstone *batch.Batch) bool {
	return data != nil && data.RowCount() >= int(h.coarseMaxRow) ||
		tombstone != nil && tombstone.RowCount() >= int(h.coarseMaxRow)
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

func newVisibleStateScanBatch(scan *visibleStateSnapshotScan) *batch.Batch {
	bat := batch.NewWithSize(len(scan.attrs))
	bat.SetAttributes(scan.attrs)
	for i := range scan.attrs {
		bat.Vecs[i] = vector.NewVec(scan.types[i])
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
