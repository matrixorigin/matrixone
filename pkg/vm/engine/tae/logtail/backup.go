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

package logtail

import (
	"context"
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"go.uber.org/zap"
)

type objData struct {
	stats      *objectio.ObjectStats
	data       []*batch.Batch
	sortKey    uint16
	ckpRow     int
	tid        uint64
	appendable bool
	dataType   objectio.DataMetaType
	// tombstoneRowsSorted records a one-time validation of every loaded block.
	// GetTombstonesByBlockId uses binary search, while Shrink preserves order.
	tombstoneRowsSorted bool
}

type BackupDeltaLocDataSource struct {
	ctx         context.Context
	fs          fileservice.FileService
	ts          types.TS
	ds          map[string]*objData
	tombstones  []objectio.ObjectStats
	needShrink  bool
	owned       map[*batch.Batch]struct{}
	loadedNames map[string]*objData
	closed      bool
}

const invalidBackupSpecialColumnPosition = math.MaxUint16

// backupSpecialColumnLayout keeps the 4.2 backup helpers compatible with both
// the legacy layout resolver and the v10 special-column metadata.
type backupSpecialColumnLayout struct {
	PhysicalAddr uint16
	CommitTS     uint16
	Abort        uint16
}

func (layout backupSpecialColumnLayout) Resolve(seqnum uint16) (uint16, bool) {
	switch seqnum {
	case objectio.SEQNUM_COMMITTS:
		return layout.CommitTS, layout.CommitTS != invalidBackupSpecialColumnPosition
	case objectio.SEQNUM_ABORT:
		return layout.Abort, layout.Abort != invalidBackupSpecialColumnPosition
	default:
		return invalidBackupSpecialColumnPosition, false
	}
}

func resolveBackupSpecialColumnLayout(block objectio.BlockObject) backupSpecialColumnLayout {
	resolved := objectio.ResolveSpecialColumnLayout(block)
	layout := backupSpecialColumnLayout{
		PhysicalAddr: resolved.PhysicalAddr,
		CommitTS:     resolved.CommitTS,
		Abort:        resolved.Abort,
	}
	if layout.CommitTS == invalidBackupSpecialColumnPosition {
		if legacyCommitTS, ok := ioutil.ResolveLegacyBackupTombstoneCommitTS(block); ok {
			layout.CommitTS = legacyCommitTS
		}
	}
	return layout
}

func loadSpecialColumnLayout(
	ctx context.Context,
	fs fileservice.FileService,
	location objectio.Location,
	allowLegacyTombstone bool,
) (backupSpecialColumnLayout, error) {
	objectMeta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
	if err != nil {
		return backupSpecialColumnLayout{}, err
	}
	dataMeta, err := ioutil.GetDataMetaForLocation(objectMeta, location)
	if err != nil {
		return backupSpecialColumnLayout{}, err
	}
	blockMeta := dataMeta.GetBlockMeta(uint32(location.ID()))
	resolved := objectio.ResolveSpecialColumnLayout(blockMeta)
	physical := backupSpecialColumnLayout{
		PhysicalAddr: resolved.PhysicalAddr,
		CommitTS:     resolved.CommitTS,
		Abort:        resolved.Abort,
	}
	if allowLegacyTombstone && physical.CommitTS == invalidBackupSpecialColumnPosition {
		if legacyCommitTS, ok := ioutil.ResolveLegacyBackupTombstoneCommitTS(blockMeta); ok {
			physical.CommitTS = legacyCommitTS
		}
	}
	translate := func(seqnum uint16) (uint16, error) {
		if seqnum == invalidBackupSpecialColumnPosition {
			return invalidBackupSpecialColumnPosition, nil
		}
		column := blockMeta.ColumnMeta(seqnum)
		position := column.Idx()
		if column.DataType() == uint8(types.T_any) || position >= blockMeta.GetColumnCount() {
			return invalidBackupSpecialColumnPosition, moerr.NewInternalErrorNoCtxf(
				"backup special column %d has invalid writer position %d", seqnum, position,
			)
		}
		return position, nil
	}
	layout := backupSpecialColumnLayout{}
	if layout.PhysicalAddr, err = translate(physical.PhysicalAddr); err != nil {
		return backupSpecialColumnLayout{}, err
	}
	if layout.CommitTS, err = translate(physical.CommitTS); err != nil {
		return backupSpecialColumnLayout{}, err
	}
	if layout.Abort, err = translate(physical.Abort); err != nil {
		return backupSpecialColumnLayout{}, err
	}
	return layout, nil
}

func loadOneBlockWithBackupLayout(
	ctx context.Context,
	fs fileservice.FileService,
	location objectio.Location,
) (*batch.Batch, uint16, backupSpecialColumnLayout, error) {
	bat, sortKey, resolved, _, err := ioutil.LoadOneBlockWithColumnLayout(
		ctx, fs, location, objectio.SchemaData,
	)
	if err != nil {
		return nil, sortKey, backupSpecialColumnLayout{}, err
	}
	layout := backupSpecialColumnLayout{
		PhysicalAddr: resolved.PhysicalAddr,
		CommitTS:     resolved.CommitTS,
		Abort:        resolved.Abort,
	}
	if layout.CommitTS == invalidBackupSpecialColumnPosition {
		layout, err = loadSpecialColumnLayout(ctx, fs, location, true)
		if err != nil {
			bat.Clean(common.DebugAllocator)
			return nil, sortKey, backupSpecialColumnLayout{}, err
		}
	}
	return bat, sortKey, layout, nil
}

func visibleAppendableRows(
	ctx context.Context,
	bat *batch.Batch,
	layout backupSpecialColumnLayout,
	ts *types.TS,
) ([]int64, error) {
	commitPos, err := validateBackupTombstoneBatch(ctx, bat, layout)
	if err != nil {
		return nil, err
	}
	rowCount := bat.Vecs[objectio.TombstoneAttr_Rowid_Idx].Length()
	commitTSs, err := ioutil.ValidateTombstoneCommitTSColumn(rowCount, bat.Vecs[commitPos])
	if err != nil {
		return nil, moerr.NewInternalErrorf(
			ctx, "appendable tombstone has invalid commit timestamp column: %v", err,
		)
	}
	var aborts ioutil.TombstoneAbortColumn
	if abortPos, ok := layout.Resolve(objectio.SEQNUM_ABORT); ok {
		aborts, err = ioutil.ValidateTombstoneAbortColumn(rowCount, bat.Vecs[abortPos])
		if err != nil {
			return nil, moerr.NewInternalErrorf(
				ctx, "appendable tombstone has invalid abort column: %v", err,
			)
		}
	}
	rows := make([]int64, 0, rowCount)
	for row := 0; row < rowCount; row++ {
		commitTS := commitTSs.At(row)
		if (aborts.IsPresent() && aborts.At(row)) ||
			commitTS.Equal(&txnif.UncommitTS) || (ts != nil && commitTS.GT(ts)) {
			continue
		}
		rows = append(rows, int64(row))
	}
	return rows, nil
}

func validateBackupTombstoneBatch(
	ctx context.Context,
	bat *batch.Batch,
	layout backupSpecialColumnLayout,
) (uint16, error) {
	if bat == nil || len(bat.Vecs) < len(objectio.TombstoneSeqnums_DN_Created) {
		return 0, moerr.NewInternalError(ctx, "appendable tombstone has too few columns")
	}
	if bat.Vecs[objectio.TombstoneAttr_PK_Idx] == nil {
		return 0, moerr.NewInternalError(ctx, "appendable tombstone has no primary-key column")
	}

	rowCount := bat.RowCount()
	if _, err := ioutil.ValidateTombstoneRowIDColumn(
		rowCount, bat.Vecs[objectio.TombstoneAttr_Rowid_Idx],
	); err != nil {
		return 0, moerr.NewInternalErrorf(
			ctx, "appendable tombstone has invalid rowid column: %v", err,
		)
	}
	for pos, vec := range bat.Vecs {
		if vec == nil {
			return 0, moerr.NewInternalErrorf(ctx, "appendable tombstone column %d is nil", pos)
		}
		if vec.Length() != rowCount {
			return 0, moerr.NewInternalErrorf(
				ctx,
				"appendable tombstone column %d has %d rows, expected %d",
				pos,
				vec.Length(),
				rowCount,
			)
		}
	}

	commitPos, ok := layout.Resolve(objectio.SEQNUM_COMMITTS)
	if !ok || int(commitPos) >= len(bat.Vecs) ||
		int(commitPos) < len(objectio.TombstoneSeqnums_CN_Created) {
		return 0, moerr.NewInternalError(ctx, "appendable tombstone has no commit timestamp")
	}
	if _, err := ioutil.ValidateTombstoneCommitTSColumn(rowCount, bat.Vecs[commitPos]); err != nil {
		return 0, moerr.NewInternalErrorf(
			ctx, "appendable tombstone has invalid commit timestamp column: %v", err,
		)
	}

	physicalPos := layout.PhysicalAddr
	hasPhysicalAddr := physicalPos != invalidBackupSpecialColumnPosition &&
		physicalPos != objectio.TombstoneAttr_Rowid_Idx
	if hasPhysicalAddr {
		// Position zero is already the mandatory semantic tombstone rowid.
		// Legacy hand-built layouts leave PhysicalAddr at its Go zero value;
		// do not mistake that for a second physical-address column.
		if int(physicalPos) < len(objectio.TombstoneSeqnums_CN_Created) ||
			physicalPos == commitPos || int(physicalPos) >= len(bat.Vecs) ||
			bat.Vecs[physicalPos] == nil {
			return 0, moerr.NewInternalError(ctx, "appendable tombstone has invalid physical rowid column")
		}
		if _, err := ioutil.ValidateTombstoneRowIDColumn(
			rowCount, bat.Vecs[physicalPos],
		); err != nil {
			return 0, moerr.NewInternalErrorf(
				ctx, "appendable tombstone has invalid physical rowid column: %v", err,
			)
		}
	}
	if abortPos, ok := layout.Resolve(objectio.SEQNUM_ABORT); ok {
		if int(abortPos) < len(objectio.TombstoneSeqnums_CN_Created) ||
			abortPos == commitPos || (hasPhysicalAddr && abortPos == physicalPos) ||
			int(abortPos) >= len(bat.Vecs) || bat.Vecs[abortPos] == nil ||
			bat.Vecs[abortPos].IsConstNull() {
			return 0, moerr.NewInternalError(ctx, "appendable tombstone has invalid abort column")
		}
		if _, err := ioutil.ValidateTombstoneAbortColumn(rowCount, bat.Vecs[abortPos]); err != nil {
			return 0, moerr.NewInternalErrorf(
				ctx, "appendable tombstone has invalid abort column: %v", err,
			)
		}
	}
	return commitPos, nil
}

// canonicalizeBackupTombstone converts an appendable tombstone batch to the
// non-appendable TN tombstone layout written into a backup. The caller has
// already removed aborted and too-new rows, so only rowid, PK, and commitTS
// remain. On success this function consumes data; on error data stays owned by
// the caller.
func canonicalizeBackupTombstone(
	ctx context.Context,
	data *batch.Batch,
	layout backupSpecialColumnLayout,
) (*batch.Batch, error) {
	commitPos, err := validateBackupTombstoneBatch(ctx, data, layout)
	if err != nil {
		return nil, err
	}
	commitVec := data.Vecs[commitPos]
	if commitVec.IsConst() {
		materialized := vector.NewVec(*commitVec.GetType())
		if err = materialized.UnionBatch(
			commitVec, 0, commitVec.Length(), nil, common.DebugAllocator,
		); err != nil {
			materialized.Free(common.DebugAllocator)
			return nil, err
		}
		commitVec.Free(common.DebugAllocator)
		data.Vecs[commitPos] = materialized
		commitVec = materialized
	}
	result := batch.NewWithSize(len(objectio.TombstoneSeqnums_DN_Created))
	result.Vecs[objectio.TombstoneAttr_Rowid_Idx] = data.Vecs[objectio.TombstoneAttr_Rowid_Idx]
	result.Vecs[objectio.TombstoneAttr_PK_Idx] = data.Vecs[objectio.TombstoneAttr_PK_Idx]
	result.Vecs[objectio.TombstoneAttr_NA_CommitTs_Idx] = commitVec
	result.SetRowCount(result.Vecs[objectio.TombstoneAttr_Rowid_Idx].Length())
	for pos, vec := range data.Vecs {
		if pos == objectio.TombstoneAttr_Rowid_Idx ||
			pos == objectio.TombstoneAttr_PK_Idx ||
			pos == int(commitPos) {
			continue
		}
		if vec != nil {
			vec.Free(common.DebugAllocator)
		}
	}
	data.Vecs = nil
	data.Attrs = nil
	data.SetRowCount(0)
	return formatData(result), nil
}

func NewBackupDeltaLocDataSource(
	ctx context.Context,
	fs fileservice.FileService,
	ts types.TS,
	ds map[string]*objData,
) *BackupDeltaLocDataSource {
	if ds == nil {
		ds = make(map[string]*objData)
	}
	return &BackupDeltaLocDataSource{
		ctx:        ctx,
		fs:         fs,
		ts:         ts,
		ds:         ds,
		needShrink: true,
		owned:      make(map[*batch.Batch]struct{}),
	}
}

func (d *BackupDeltaLocDataSource) String() string {
	return "BackupDeltaLocDataSource"
}

func (d *BackupDeltaLocDataSource) SetTS(
	ts types.TS,
) {
	if d == nil {
		return
	}
	if len(d.loadedNames) > 0 && !d.ts.Equal(&ts) {
		d.releaseOwnedTombstones()
	}
	d.ts = ts
}

func (d *BackupDeltaLocDataSource) Next(
	_ context.Context,
	_ []string,
	_ []types.Type,
	_ []uint16,
	_ int32,
	_ any,
	_ *mpool.MPool,
	_ *batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {
	return nil, engine.Persisted, nil
}

func (d *BackupDeltaLocDataSource) Close() {
	if d == nil || d.closed {
		return
	}
	d.closed = true
	d.releaseOwnedTombstones()
	d.owned = nil
	d.loadedNames = nil
}

func (d *BackupDeltaLocDataSource) releaseOwnedTombstones() {
	for name, ownedObject := range d.loadedNames {
		// The datasource map is supplied by the caller and can outlive this
		// reader. Remove only the exact entry installed by the lazy loader; a
		// caller replacement with the same name remains caller-owned.
		if d.ds[name] == ownedObject {
			delete(d.ds, name)
		}
	}
	for bat := range d.owned {
		if bat != nil {
			bat.Clean(common.DebugAllocator)
		}
	}
	d.owned = make(map[*batch.Batch]struct{})
	d.loadedNames = nil
}

func (d *BackupDeltaLocDataSource) ApplyTombstones(
	_ context.Context,
	_ *objectio.Blockid,
	_ []int64,
	_ engine.TombstoneApplyPolicy,
) ([]int64, error) {
	panic("Not Support ApplyTombstones")
}
func (d *BackupDeltaLocDataSource) SetOrderBy(orderby []*plan.OrderBySpec) {
	panic("Not Support order by")
}

func (d *BackupDeltaLocDataSource) GetOrderBy() []*plan.OrderBySpec {
	panic("Not Support order by")
}

func (d *BackupDeltaLocDataSource) SetFilterZM(zm objectio.ZoneMap) {
	panic("Not Support order by")
}

func ForeachTombstoneObject(
	onTombstone func(tombstone *objData) (next bool, err error),
	ds map[string]*objData,
) error {
	if onTombstone == nil {
		return moerr.NewInvalidInputNoCtx("tombstone object iteration requires a callback")
	}
	for _, d := range ds {
		if d == nil || !d.appendable {
			continue
		}
		if next, err := onTombstone(d); !next || err != nil {
			return err
		}
	}
	return nil
}

func buildDS(
	onTombstone func(tombstone objectio.ObjectStats) (next bool, err error),
	ds []objectio.ObjectStats,
) error {
	if onTombstone == nil {
		return moerr.NewInvalidInputNoCtx("tombstone datasource build requires a callback")
	}
	for _, d := range ds {
		if next, err := onTombstone(d); !next || err != nil {
			return err
		}
	}
	return nil
}

func GetTombstonesByBlockId(
	ctx context.Context,
	bid *objectio.Blockid,
	deleteMask *objectio.Bitmap,
	scanOp func(func(tombstone *objData) (bool, error)) error,
	needShrink bool,
	maxRows int,
) (err error) {
	if ctx == nil || bid == nil || deleteMask == nil || scanOp == nil || maxRows < -1 {
		return moerr.NewInvalidInputNoCtx(
			"tombstone scan requires context, block id, delete mask, callback, and a valid row bound",
		)
	}
	type matchedTombstoneRows struct {
		batch   *batch.Batch
		start   int
		end     int
		offsets []uint64
	}
	matches := make([]matchedTombstoneRows, 0)
	matchesByBatch := make(map[*batch.Batch]matchedTombstoneRows)

	onTombstone := func(oData *objData) (bool, error) {
		select {
		case <-ctx.Done():
			return false, context.Cause(ctx)
		default:
		}
		if oData == nil || oData.stats == nil {
			return false, moerr.NewInternalErrorNoCtx("appendable tombstone metadata is missing")
		}
		obj := oData.stats
		if !oData.appendable {
			return true, nil
		}
		if oData.dataType != objectio.SchemaTombstone {
			return false, moerr.NewInternalErrorNoCtxf(
				"appendable object %s has data type %d, expected tombstone",
				obj.ObjectName().String(), oData.dataType,
			)
		}
		blockCount, validateErr := validateObjectStatsBlockCount(ctx, *obj)
		if validateErr != nil {
			return false, validateErr
		}
		if len(oData.data) != blockCount {
			return false, moerr.NewInternalErrorNoCtxf(
				"appendable tombstone %s has %d loaded blocks, expected %d",
				obj.ObjectName().String(), len(oData.data), blockCount,
			)
		}
		if !obj.ZMIsEmpty() {
			objZM := obj.SortKeyZoneMap()
			if skip := !objZM.RowidPrefixEq(bid[:]); skip {
				return true, nil
			}
		}

		for idx := 0; idx < blockCount; idx++ {
			select {
			case <-ctx.Done():
				return false, context.Cause(ctx)
			default:
			}
			if oData.data[idx] == nil ||
				len(oData.data[idx].Vecs) == 0 || oData.data[idx].Vecs[0] == nil {
				return false, moerr.NewInternalErrorNoCtxf(
					"appendable tombstone %s block %d/%d is missing data",
					obj.ObjectName().String(), idx, obj.BlkCnt(),
				)
			}
			rowIDVec := oData.data[idx].Vecs[0]
			rowids, validateErr := ioutil.ValidateTombstoneRowIDColumn(
				oData.data[idx].RowCount(), rowIDVec,
			)
			if validateErr != nil {
				return false, moerr.NewInternalErrorNoCtxf(
					"appendable tombstone %s block %d has invalid rowid column: %v",
					obj.ObjectName().String(), idx, validateErr,
				)
			}
			if !oData.tombstoneRowsSorted {
				for row := 1; row < len(rowids); row++ {
					if row&1023 == 0 {
						select {
						case <-ctx.Done():
							return false, context.Cause(ctx)
						default:
						}
					}
					if rowids[row].LT(&rowids[row-1]) {
						return false, moerr.NewInternalErrorNoCtxf(
							"appendable tombstone %s block %d rowids are not sorted at row %d",
							obj.ObjectName().String(), idx, row,
						)
					}
				}
			}
			start, end := ioutil.FindStartEndOfBlockFromSortedRowids(rowids, bid)
			if start == end {
				continue
			}
			offsets := make([]uint64, 0, end-start)
			for row := start; row < end; row++ {
				offset := uint64(rowids[row].GetRowOffset())
				if maxRows >= 0 && offset >= uint64(maxRows) {
					return false, moerr.NewInternalErrorNoCtxf(
						"tombstone row offset %d exceeds target block row count %d",
						offset, maxRows,
					)
				}
				offsets = append(offsets, offset)
			}
			match := matchedTombstoneRows{
				batch:   oData.data[idx],
				start:   start,
				end:     end,
				offsets: offsets,
			}
			if previous, exists := matchesByBatch[match.batch]; exists {
				if previous.start != match.start || previous.end != match.end {
					return false, moerr.NewInternalErrorNoCtx(
						"appendable tombstone batch is referenced with inconsistent row ranges",
					)
				}
				// A caller-owned datasource map can contain aliases to the same
				// object data. Consume a physical batch once; otherwise the first
				// Shrink invalidates the staged offsets of the second alias.
				continue
			}
			matchesByBatch[match.batch] = match
			matches = append(matches, match)
		}
		oData.tombstoneRowsSorted = true
		return true, nil
	}

	if err = scanOp(onTombstone); err != nil {
		return err
	}
	// Publish both the result and destructive consumption only after every
	// candidate batch has passed validation. A failed scan is therefore safe to
	// retry with the same data source.
	for _, match := range matches {
		for _, offset := range match.offsets {
			deleteMask.Add(offset)
		}
		if needShrink {
			// The matched tombstones have been applied and must not be emitted into
			// the rewritten backup object.
			rows := make([]int64, match.end-match.start)
			for i := range rows {
				rows[i] = int64(match.start + i)
			}
			match.batch.Shrink(rows, true)
		}
	}
	return nil
}

func (d *BackupDeltaLocDataSource) GetTombstones(
	ctx context.Context, bid *objectio.Blockid,
) (deletedRows objectio.Bitmap, err error) {
	return d.getTombstones(ctx, bid, -1)
}

// GetTombstonesWithRowCount applies an exact target-block bound before any
// delete bitmap is grown. Persisted rowids are untrusted input here: allowing a
// malformed uint32 row offset to reach Bitmap.Add can otherwise request
// gigabytes of dense bitmap memory before the caller can reject it.
func (d *BackupDeltaLocDataSource) GetTombstonesWithRowCount(
	ctx context.Context,
	bid *objectio.Blockid,
	rowCount int,
) (deletedRows objectio.Bitmap, err error) {
	if rowCount < 0 {
		return objectio.NullBitmap, moerr.NewInvalidInputNoCtx(
			"backup tombstone target row count cannot be negative",
		)
	}
	return d.getTombstones(ctx, bid, rowCount)
}

func (d *BackupDeltaLocDataSource) getTombstones(
	ctx context.Context,
	bid *objectio.Blockid,
	maxRows int,
) (deletedRows objectio.Bitmap, err error) {
	// PXU TODO: temp use GetNoReuseBitmap here
	deletedRows = objectio.GetNoReuseBitmap()
	if d == nil || ctx == nil || bid == nil {
		deletedRows.Release()
		return objectio.NullBitmap, moerr.NewInvalidInputNoCtx(
			"backup tombstone read requires data source, context, and block id",
		)
	}
	if d.closed {
		deletedRows.Release()
		return objectio.NullBitmap, moerr.NewInternalError(ctx, "backup data source is closed")
	}
	if len(d.tombstones) > 0 {
		if d.fs == nil {
			deletedRows.Release()
			return objectio.NullBitmap, moerr.NewInternalError(ctx, "backup tombstone file service is nil")
		}
		loadedObjects := make(map[string]*objData, len(d.tombstones))
		loadedBatches := make([]*batch.Batch, 0)
		if err = buildDS(
			func(tombstone objectio.ObjectStats) (bool, error) {
				name := tombstone.ObjectName()
				nameKey := name.String()
				if _, ok := loadedObjects[nameKey]; ok {
					return true, nil
				}
				if _, ok := d.loadedNames[nameKey]; ok {
					return true, nil
				}
				if existing, ok := d.ds[nameKey]; ok {
					// Caller-owned object data wins over the lazy loader. Reusing a
					// valid entry avoids replacing it with owned state that Close or
					// SetTS would later delete. A conflicting non-tombstone entry is a
					// malformed datasource and must not silently suppress deletes.
					if existing == nil || !existing.appendable ||
						existing.dataType != objectio.SchemaTombstone {
						return false, moerr.NewInternalErrorf(
							ctx, "backup datasource has conflicting object %s", nameKey,
						)
					}
					return true, nil
				}
				if !tombstone.ZMIsEmpty() && !tombstone.SortKeyZoneMap().PrefixEq(bid[:]) {
					return true, nil
				}
				logutil.Infof("[GetSnapshot] tombstone object: %v, block count: %d", name.String(), tombstone.BlkCnt())
				blockCount, validateErr := validateObjectStatsBlockCount(ctx, tombstone)
				if validateErr != nil {
					return false, validateErr
				}
				objectData := &objData{
					stats:      &tombstone,
					dataType:   objectio.SchemaTombstone,
					sortKey:    uint16(math.MaxUint16),
					data:       make([]*batch.Batch, 0, blockCount),
					appendable: true,
				}
				if err := forEachObjectBlockLocation(ctx, tombstone, func(location objectio.Location) error {
					bat, _, layout, err := loadOneBlockWithBackupLayout(ctx, d.fs, location)
					if err != nil {
						return err
					}
					if !tombstone.GetCNCreated() {
						visibleRows, err := visibleAppendableRows(ctx, bat, layout, &d.ts)
						if err != nil {
							bat.Clean(common.DebugAllocator)
							return err
						}
						if len(visibleRows) != bat.Vecs[0].Length() {
							bat.Shrink(visibleRows, false)
						}
					}
					objectData.data = append(objectData.data, bat)
					loadedBatches = append(loadedBatches, bat)
					return nil
				}); err != nil {
					return false, err
				}
				loadedObjects[nameKey] = objectData
				return true, nil
			},
			d.tombstones,
		); err != nil {
			for _, bat := range loadedBatches {
				bat.Clean(common.DebugAllocator)
			}
			deletedRows.Release()
			return objectio.NullBitmap, err
		}
		for name, objectData := range loadedObjects {
			if d.ds == nil {
				d.ds = make(map[string]*objData)
			}
			if d.owned == nil {
				d.owned = make(map[*batch.Batch]struct{})
			}
			d.ds[name] = objectData
			if d.loadedNames == nil {
				d.loadedNames = make(map[string]*objData, len(loadedObjects))
			}
			d.loadedNames[name] = objectData
			for _, bat := range objectData.data {
				d.owned[bat] = struct{}{}
			}
		}
	}
	scanOp := func(onTombstone func(tombstone *objData) (bool, error)) (err error) {
		return ForeachTombstoneObject(onTombstone, d.ds)
	}

	if err = GetTombstonesByBlockId(
		ctx,
		bid,
		&deletedRows,
		scanOp,
		d.needShrink,
		maxRows,
	); err != nil {
		deletedRows.Release()
		return objectio.NullBitmap, err
	}
	return
}

func GetCheckpointReader(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	location objectio.Location,
	version uint32,
) (*CKPReader, error) {
	if ctx == nil || fs == nil || location.IsEmpty() {
		return nil, moerr.NewInvalidInputNoCtx(
			"checkpoint reader requires context, file service, and location",
		)
	}
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	default:
	}
	reader := NewCKPReader(version, location, common.CheckpointAllocator, fs)
	if err := reader.ReadMeta(ctx); err != nil {
		return nil, err
	}
	return reader, nil
}

func addObjectToObjectData(
	stats *objectio.ObjectStats,
	isABlk bool,
	row int, tid uint64,
	blockType objectio.DataMetaType,
	objectsData *map[string]*objData,
) error {
	if stats == nil || objectsData == nil || *objectsData == nil {
		return moerr.NewInvalidInputNoCtx("backup object metadata destination is unavailable")
	}
	name := stats.ObjectName().String()
	if _, exists := (*objectsData)[name]; exists {
		return moerr.NewInternalErrorNoCtxf("backup object %s appears more than once", name)
	}
	object := &objData{
		stats:      stats,
		appendable: isABlk,
		tid:        tid,
		dataType:   blockType,
		sortKey:    uint16(math.MaxUint16),
	}
	(*objectsData)[name] = object
	(*objectsData)[name].ckpRow = row
	return nil
}

func trimTombstoneData(
	ctx context.Context,
	fs fileservice.FileService,
	ts types.TS,
	objectsData *map[string]*objData,
) error {
	if fs == nil || objectsData == nil || *objectsData == nil {
		return moerr.NewInvalidInputNoCtx("backup tombstone trim requires file service and object metadata")
	}
	for name := range *objectsData {
		objectData := (*objectsData)[name]
		if objectData == nil || objectData.stats == nil {
			return moerr.NewInternalErrorNoCtxf("backup tombstone %s has no metadata", name)
		}
		if !objectData.appendable {
			continue
		}
		if objectData.dataType != objectio.SchemaTombstone {
			return moerr.NewInternalErrorNoCtxf(
				"backup object %s has data type %d, expected tombstone", name, objectData.dataType,
			)
		}
		blockCount, err := validateObjectStatsBlockCount(ctx, *objectData.stats)
		if err != nil {
			return err
		}
		loadedData := make([]*batch.Batch, 0, blockCount)
		loadedSortKey := uint16(math.MaxUint16)
		sortKeySet := false
		if err = forEachObjectBlockLocation(ctx, *objectData.stats, func(location objectio.Location) error {
			bat, sortKey, layout, err := loadOneBlockWithBackupLayout(ctx, fs, location)
			if err != nil {
				return err
			}
			visibleRows, err := visibleAppendableRows(ctx, bat, layout, &ts)
			if err != nil {
				bat.Clean(common.DebugAllocator)
				return err
			}
			if len(visibleRows) != bat.Vecs[0].Length() {
				bat.Shrink(visibleRows, false)
			}
			// Keep one canonical batch per source block, including empty
			// batches. GetTombstonesByBlockId indexes these batches by the
			// source block ordinal; dropping an empty middle block would shift
			// every following block and either reject valid metadata or apply
			// tombstones from the wrong block.
			canonical, err := canonicalizeBackupTombstone(ctx, bat, layout)
			if err != nil {
				bat.Clean(common.DebugAllocator)
				return err
			}
			if !sortKeySet {
				loadedSortKey = sortKey
				sortKeySet = true
			} else if loadedSortKey != sortKey {
				canonical.Clean(common.DebugAllocator)
				return moerr.NewInternalErrorNoCtxf(
					"backup tombstone %s changes sort-key position from %d to %d",
					name, loadedSortKey, sortKey,
				)
			}
			loadedData = append(loadedData, canonical)
			return nil
		}); err != nil {
			for _, bat := range loadedData {
				bat.Clean(common.DebugAllocator)
			}
			return err
		}
		for _, bat := range objectData.data {
			if bat != nil {
				bat.Clean(common.DebugAllocator)
			}
		}
		objectData.data = loadedData
		objectData.sortKey = loadedSortKey
	}
	return nil
}

func projectBackupSortKey(
	sortKey uint16,
	specialPositions map[uint16]struct{},
	sourceColumnCount int,
) (uint16, error) {
	if sortKey == uint16(math.MaxUint16) {
		return sortKey, nil
	}
	if int(sortKey) >= sourceColumnCount {
		return 0, moerr.NewInternalErrorNoCtxf(
			"backup sort-key position %d exceeds %d source columns", sortKey, sourceColumnCount,
		)
	}
	if _, special := specialPositions[sortKey]; special {
		return 0, moerr.NewInternalErrorNoCtxf(
			"backup sort-key position %d refers to a hidden column", sortKey,
		)
	}
	projected := sortKey
	for special := range specialPositions {
		if special < sortKey {
			projected--
		}
	}
	if int(projected) >= sourceColumnCount-len(specialPositions) {
		return 0, moerr.NewInternalErrorNoCtxf(
			"backup projected sort-key position %d exceeds %d user columns",
			projected, sourceColumnCount-len(specialPositions),
		)
	}
	return projected, nil
}

func backupReplacementIsSorted(object *objData) bool {
	return object != nil &&
		(object.dataType == objectio.SchemaTombstone || object.sortKey != math.MaxUint16)
}

func appendValToBatch(
	account uint32,
	db, tbl uint64,
	objType int8,
	id objectio.ObjectStats,
	create, delete types.TS,
	encoder *types.Packer,
	dst *batch.Batch,
	mp *mpool.MPool,
) (err error) {
	if err = vector.AppendFixed(
		dst.Vecs[ckputil.TableObjectsAttr_Accout_Idx], account, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed(
		dst.Vecs[ckputil.TableObjectsAttr_DB_Idx], db, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed(
		dst.Vecs[ckputil.TableObjectsAttr_Table_Idx], tbl, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendBytes(
		dst.Vecs[ckputil.TableObjectsAttr_ID_Idx], id[:], false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed(
		dst.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx], objType, false, mp,
	); err != nil {
		return
	}
	encoder.Reset()
	ckputil.EncodeCluser(encoder, tbl, objType, id.ObjectName().ObjectId(), delete.IsEmpty())
	if err = vector.AppendBytes(
		dst.Vecs[ckputil.TableObjectsAttr_Cluster_Idx], encoder.Bytes(), false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed(
		dst.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], create, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed(
		dst.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], delete, false, mp,
	); err != nil {
		return
	}
	dst.SetRowCount(dst.Vecs[0].Length())
	return
}

// Need to format the loaded batch, otherwise panic may occur when WriteBatch.
func formatData(data *batch.Batch) *batch.Batch {
	data.Attrs = make([]string, 0)
	for i := range data.Vecs {
		att := fmt.Sprintf("col_%d", i)
		data.Attrs = append(data.Attrs, att)
	}
	if data.Vecs[0].Length() > 0 {
		tmp := containers.ToTNBatch(data, common.CheckpointAllocator)
		data = containers.ToCNBatch(tmp)
	}
	return data
}

func LoadCheckpointEntriesFromKey(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	location objectio.Location,
	version uint32,
	softDeletes *map[string]bool,
	baseTS *types.TS,
) ([]*objectio.BackupObject, *CKPReader, error) {
	if fs == nil || baseTS == nil {
		return nil, nil, moerr.NewInvalidInputNoCtx(
			"checkpoint backup entry load requires file service and base timestamp",
		)
	}
	locations := make([]*objectio.BackupObject, 0)
	pendingSoftDeletes := make([]string, 0)
	ckpReader, err := GetCheckpointReader(ctx, sid, fs, location, version)
	if err != nil {
		return nil, nil, err
	}

	locations = append(locations, &objectio.BackupObject{
		Location: location,
		NeedCopy: true,
	})

	for _, location = range ckpReader.GetLocations() {
		locations = append(locations, &objectio.BackupObject{
			Location: location,
			NeedCopy: true,
		})
	}

	if err = ckpReader.ForEachRow(
		ctx,
		func(
			account uint32,
			dbid, tid uint64,
			objectType int8,
			objectStats objectio.ObjectStats,
			createAt, deletedAt types.TS,
			rowID types.Rowid,
		) error {
			commitAt := createAt
			if !deletedAt.IsEmpty() {
				commitAt = deletedAt
			}
			isAblk := objectStats.GetAppendable()
			if objectStats.Extent().End() == 0 {
				// tn obj is in the batch too
				return nil
			}

			if deletedAt.IsEmpty() && isAblk {
				// no flush, no need to copy
				return nil
			}

			bo := &objectio.BackupObject{
				Location: objectStats.ObjectLocation(),
				CrateTS:  createAt,
				DropTS:   deletedAt,
			}
			if baseTS.IsEmpty() || (!baseTS.IsEmpty() &&
				(createAt.GE(baseTS) || commitAt.GE(baseTS))) {
				bo.NeedCopy = true
			}
			locations = append(locations, bo)
			if !deletedAt.IsEmpty() && softDeletes != nil {
				pendingSoftDeletes = append(
					pendingSoftDeletes, objectStats.ObjectName().String(),
				)
			}
			return nil
		},
	); err != nil {
		return nil, nil, err
	}
	if softDeletes != nil {
		if *softDeletes == nil {
			*softDeletes = make(map[string]bool, len(pendingSoftDeletes))
		}
		for _, name := range pendingSoftDeletes {
			(*softDeletes)[name] = true
		}
	}
	return locations, ckpReader, nil
}

func ReWriteCheckpointAndBlockFromKey(
	ctx context.Context,
	sid string,
	fs, dstFs fileservice.FileService,
	loc objectio.Location,
	lastCkpData *CKPReader,
	version uint32, ts types.TS,
) (objectio.Location, objectio.Location, []string, error) {
	if fs == nil || dstFs == nil || lastCkpData == nil {
		return nil, nil, nil, moerr.NewInvalidInputNoCtx(
			"checkpoint rewrite requires source, destination, and previous checkpoint data",
		)
	}
	logutil.Info("[Start]", common.OperationField("ReWrite Checkpoint"),
		common.OperandField(loc.String()),
		common.OperandField(ts.ToString()))
	phaseNumber := 0
	var err error
	defer func() {
		if err != nil {
			logutil.Error("[DoneWithErr]", common.OperationField("ReWrite Checkpoint"),
				common.AnyField("error", err),
				common.AnyField("phase", phaseNumber),
			)
		}
	}()
	objectsData := make(map[string]*objData, 0)
	tombstonesData := make(map[string]*objData, 0)
	// tombstonesData2 is the tombstone recorded in the last checkpoint,
	// only used when cutting aobject, and does not need to modify itself
	tombstonesData2 := make(map[string]*objData, 0)

	defer func() {
		released := make(map[*objData]struct{})
		cleanup := func(data map[string]*objData) {
			for _, objectData := range data {
				if objectData == nil || objectData.data == nil {
					continue
				}
				if _, ok := released[objectData]; ok {
					continue
				}
				released[objectData] = struct{}{}
				for _, bat := range objectData.data {
					if bat != nil {
						bat.Clean(common.DebugAllocator)
					}
				}
				objectData.data = nil
			}
		}
		cleanup(objectsData)
		cleanup(tombstonesData)
		cleanup(tombstonesData2)
	}()
	phaseNumber = 1
	// Load checkpoint
	ckpReader, err := GetCheckpointReader(ctx, sid, fs, loc, version)
	if err != nil {
		return nil, nil, nil, err
	}

	phaseNumber = 2
	// Analyze checkpoint to get the object file
	var files []string

	initData := func(
		od *map[string]*objData,
		objectType int8,
		dataType objectio.DataMetaType,
	) error {
		i := 0
		return ckpReader.ForEachRow(
			ctx,
			func(
				account uint32,
				dbid, tid uint64,
				objectType2 int8,
				stats objectio.ObjectStats,
				createAt, deleteAt types.TS,
				rowID types.Rowid,
			) error {
				if objectType == objectType2 {
					appendable := stats.GetAppendable()
					commitTS := createAt
					if !deleteAt.IsEmpty() {
						commitTS = deleteAt
					}
					if commitTS.LT(&ts) {
						return moerr.NewInternalErrorf(
							ctx, "checkpoint object commit timestamp %s precedes backup timestamp %s",
							commitTS.ToString(), ts.ToString(),
						)
					}
					if deleteAt.IsEmpty() {
						i++
						return nil
					}
					if createAt.GE(&ts) {
						return moerr.NewInternalErrorf(
							ctx, "checkpoint object create timestamp %s does not precede backup timestamp %s",
							createAt.ToString(), ts.ToString(),
						)
					}
					if err := addObjectToObjectData(&stats, appendable, i, tid, dataType, od); err != nil {
						return err
					}
					i++
				}
				return nil
			},
		)
	}

	initData2 := func(
		od *map[string]*objData,
		objectType int8,
		dataType objectio.DataMetaType,
	) error {
		i := 0
		return lastCkpData.ForEachRow(
			ctx,
			func(
				account uint32,
				dbid, tid uint64,
				objectType2 int8,
				stats objectio.ObjectStats,
				create, deleteAt types.TS,
				rowID types.Rowid,
			) error {
				if objectType2 == objectType {
					appendable := stats.GetAppendable()
					if deleteAt.IsEmpty() {
						i++
						return nil
					}
					if !appendable {
						i++
						return nil
					}
					if err := addObjectToObjectData(&stats, appendable, i, tid, dataType, od); err != nil {
						return err
					}
					i++
				}
				return nil
			},
		)
	}

	if err = initData(&objectsData, ckputil.ObjectType_Data, objectio.SchemaData); err != nil {
		return nil, nil, nil, err
	}
	if err = initData(
		&tombstonesData, ckputil.ObjectType_Tombstone, objectio.SchemaTombstone,
	); err != nil {
		return nil, nil, nil, err
	}
	if err = initData2(
		&tombstonesData2, ckputil.ObjectType_Tombstone, objectio.SchemaTombstone,
	); err != nil {
		return nil, nil, nil, err
	}

	phaseNumber = 3

	// Trim tombstone files based on timestamp
	err = trimTombstoneData(ctx, fs, ts, &tombstonesData)
	if err != nil {
		return nil, nil, nil, err
	}
	// Trim tombstone files based on timestamp
	err = trimTombstoneData(ctx, fs, ts, &tombstonesData2)
	if err != nil {
		return nil, nil, nil, err
	}

	backupPool := dbutils.MakeDefaultSmallPool("backup-vector-pool")
	defer backupPool.Destory()
	insertObjBatch := make(map[uint64][]*objData)

	phaseNumber = 4

	insertBatchFun := func(
		objsData map[string]*objData,
		initData func(*objData, *ioutil.BlockWriter) (bool, error),
	) error {
		for _, objectData := range objsData {
			if objectData == nil || objectData.stats == nil {
				return moerr.NewInternalErrorNoCtx("backup rewrite object metadata is missing")
			}
			if insertObjBatch[objectData.tid] == nil {
				insertObjBatch[objectData.tid] = make([]*objData, 0)
			}
			if !objectData.appendable {
				insertObjBatch[objectData.tid] = append(insertObjBatch[objectData.tid], objectData)
				continue
			}
			objectName := objectData.stats.ObjectName()
			if objectName.Num() > math.MaxUint16-1000 {
				return moerr.NewInternalErrorNoCtxf(
					"backup object file number %d cannot be offset safely", objectName.Num(),
				)
			}
			fileNum := uint16(1000) + objectName.Num()
			segment := objectName.SegmentId()
			name := objectio.BuildObjectName(&segment, fileNum)
			var writer *ioutil.BlockWriter
			if objectData.dataType == objectio.SchemaTombstone {
				writer, err = ioutil.NewBlockWriterNew(
					dstFs, name, 0, objectio.TombstoneSeqnums_DN_Created, true,
				)
			} else {
				writer, err = ioutil.NewBlockWriter(dstFs, name.String())
			}
			if err != nil {
				return err
			}
			var isEmpty bool
			if isEmpty, err = initData(objectData, writer); err != nil {
				return err
			}
			if isEmpty {
				continue
			}
			if len(objectData.data) == 0 {
				return moerr.NewInternalErrorNoCtxf(
					"backup object %s reported data but produced no blocks", objectName.String(),
				)
			}

			writtenBlocks := 0
			for block, objectBatch := range objectData.data {
				if objectBatch == nil || len(objectBatch.Vecs) == 0 || objectBatch.Vecs[0] == nil {
					return moerr.NewInternalErrorNoCtxf(
						"backup object %s block %d is malformed", objectName.String(), block,
					)
				}
				if objectBatch.RowCount() == 0 {
					continue
				}
				sortData := containers.ToTNBatch(objectBatch, common.DebugAllocator)
				if objectData.sortKey != math.MaxUint16 {
					if int(objectData.sortKey) >= len(sortData.Vecs) {
						return moerr.NewInternalErrorNoCtxf(
							"backup object %s has invalid sort-key position %d",
							objectName.String(), objectData.sortKey,
						)
					}
					if _, err = mergesort.SortBlockColumns(
						sortData.Vecs, int(objectData.sortKey), backupPool,
					); err != nil {
						return err
					}
				}
				objectData.data[block] = containers.ToCNBatch(sortData)
				if _, err = writer.WriteBatch(objectData.data[block]); err != nil {
					return err
				}
				writtenBlocks++
			}
			if writtenBlocks == 0 {
				return moerr.NewInternalErrorNoCtxf(
					"backup object %s has no visible rows to write", objectName.String(),
				)
			}
			blocks, extent, err := writer.Sync(ctx)
			if err != nil {
				return err
			}
			if len(blocks) != writtenBlocks || len(blocks) == 0 {
				return moerr.NewInternalErrorNoCtxf(
					"backup object %s wrote %d blocks, expected %d",
					objectName.String(), len(blocks), writtenBlocks,
				)
			}
			files = append(files, name.String())
			blockLocation := objectio.BuildLocation(name, extent, blocks[0].GetRows(), blocks[0].GetID())
			ss := writer.GetObjectStats()
			objectData.stats = &ss
			if err = objectio.SetObjectStatsLocation(objectData.stats, blockLocation); err != nil {
				return err
			}
			insertObjBatch[objectData.tid] = append(insertObjBatch[objectData.tid], objectData)
		}
		return nil
	}

	// tombstonesData2 is used to merge the source of ds
	dsTombstone := tombstonesData2
	for key, objectData := range tombstonesData {
		if dsTombstone[key] == nil {
			dsTombstone[key] = objectData
		}
	}
	err = insertBatchFun(
		objectsData,
		func(oData *objData, writer *ioutil.BlockWriter) (bool, error) {
			ds := NewBackupDeltaLocDataSource(ctx, fs, ts, dsTombstone)
			defer ds.Close()
			oData.sortKey = uint16(math.MaxUint16)
			sortKeySet := false
			blockCount, err := validateObjectStatsBlockCount(ctx, *oData.stats)
			if err != nil {
				return true, err
			}
			oData.data = make([]*batch.Batch, 0, blockCount)
			if err := forEachObjectBlockLocation(ctx, *oData.stats, func(location objectio.Location) error {
				blk := oData.stats.ConstructBlockInfo(location.ID())
				bat, sortKey, err := blockio.BlockDataReadBackup(ctx, &blk, ds, nil, ts, fs)
				if err != nil {
					return err
				}
				if bat == nil || len(bat.Vecs) == 0 || bat.Vecs[0] == nil {
					if bat != nil {
						bat.Clean(common.DebugAllocator)
					}
					return moerr.NewInternalErrorNoCtxf(
						"backup data object %s block %d is malformed",
						oData.stats.ObjectName().String(), location.ID(),
					)
				}
				if bat.RowCount() == 0 {
					bat.Clean(common.DebugAllocator)
					return nil
				}
				layout, err := loadSpecialColumnLayout(ctx, fs, location, false)
				if err != nil {
					bat.Clean(common.DebugAllocator)
					return err
				}
				specialPositions := make(map[uint16]struct{}, 3)
				for _, pos := range []uint16{layout.PhysicalAddr, layout.CommitTS, layout.Abort} {
					if pos == objectio.InvalidSpecialColumnPosition {
						continue
					}
					if int(pos) >= len(bat.Vecs) {
						bat.Clean(common.DebugAllocator)
						return moerr.NewInternalErrorNoCtxf(
							"backup data object %s block %d has invalid special column %d",
							oData.stats.ObjectName().String(), location.ID(), pos,
						)
					}
					specialPositions[pos] = struct{}{}
				}
				resultColumns := len(bat.Vecs) - len(specialPositions)
				if resultColumns <= 0 {
					bat.Clean(common.DebugAllocator)
					return moerr.NewInternalErrorNoCtxf(
						"backup data object %s block %d has invalid projected layout",
						oData.stats.ObjectName().String(), location.ID(),
					)
				}
				projectedSortKey, err := projectBackupSortKey(
					sortKey, specialPositions, len(bat.Vecs),
				)
				if err != nil {
					bat.Clean(common.DebugAllocator)
					return moerr.NewInternalErrorNoCtxf(
						"backup data object %s block %d has invalid sort key: %v",
						oData.stats.ObjectName().String(), location.ID(), err,
					)
				}
				if !sortKeySet {
					oData.sortKey = projectedSortKey
					sortKeySet = true
				} else if oData.sortKey != projectedSortKey {
					bat.Clean(common.DebugAllocator)
					return moerr.NewInternalErrorNoCtxf(
						"backup data object %s changes sort-key position from %d to %d",
						oData.stats.ObjectName().String(), oData.sortKey, projectedSortKey,
					)
				}
				result := batch.NewWithSize(resultColumns)
				resultPos := 0
				for pos, vec := range bat.Vecs {
					if _, special := specialPositions[uint16(pos)]; special {
						vec.Free(common.DebugAllocator)
						continue
					}
					result.Vecs[resultPos] = vec
					resultPos++
				}
				bat.Vecs = nil
				bat.Attrs = nil
				bat.SetRowCount(0)
				oData.data = append(oData.data, formatData(result))
				return nil
			}); err != nil {
				return true, err
			}
			if len(oData.data) == 0 {
				logutil.Info("[Data Empty] ReWrite Checkpoint",
					zap.String("object", oData.stats.ObjectName().String()),
					zap.Uint64("tid", oData.tid))
				return true, nil
			}
			if oData.sortKey != math.MaxUint16 {
				writer.SetPrimaryKey(oData.sortKey)
			}
			return false, nil
		})

	if err != nil {
		return nil, nil, nil, err
	}

	err = insertBatchFun(
		tombstonesData,
		func(oData *objData, writer *ioutil.BlockWriter) (bool, error) {
			hasVisibleRows := false
			for _, bat := range oData.data {
				if bat != nil && bat.RowCount() > 0 {
					hasVisibleRows = true
					break
				}
			}
			if !hasVisibleRows {
				logutil.Info("[Data Empty] ReWrite Checkpoint",
					zap.String("tombstone", oData.stats.ObjectName().String()),
					zap.Uint64("tid", oData.tid))
				return true, nil
			}
			writer.SetTombstone()
			writer.SetPrimaryKeyWithType(
				uint16(objectio.TombstonePrimaryKeyIdx),
				index.HBF,
				index.ObjectPrefixFn,
				index.BlockPrefixFn,
			)
			return false, nil
		})

	if err != nil {
		return nil, nil, nil, err
	}

	phaseNumber = 5

	dataSinker := ckputil.NewDataSinker(
		common.CheckpointAllocator, dstFs, ioutil.WithMemorySizeThreshold(DefaultCheckpointSize))
	defer dataSinker.Close()
	encoder := types.NewPacker()
	defer encoder.Close()
	if len(insertObjBatch) > 0 {
		objectInfoMeta := ckputil.NewObjectListBatch()
		defer objectInfoMeta.Clean(common.CheckpointAllocator)
		tombstoneInfoMeta := ckputil.NewObjectListBatch()
		defer tombstoneInfoMeta.Clean(common.CheckpointAllocator)
		infoInsert := make(map[int]*objData, 0)
		infoInsertTombstone := make(map[int]*objData, 0)
		for tid := range insertObjBatch {
			for i := range insertObjBatch[tid] {
				obj := insertObjBatch[tid][i]
				if obj == nil || obj.stats == nil {
					return nil, nil, nil, moerr.NewInternalErrorNoCtx(
						"backup checkpoint insertion has missing object metadata",
					)
				}
				switch obj.dataType {
				case objectio.SchemaData:
					if infoInsert[obj.ckpRow] != nil {
						return nil, nil, nil, moerr.NewInternalErrorNoCtxf(
							"backup checkpoint data row %d has multiple replacements", obj.ckpRow,
						)
					}
					infoInsert[obj.ckpRow] = obj
				case objectio.SchemaTombstone:
					if infoInsertTombstone[obj.ckpRow] != nil {
						return nil, nil, nil, moerr.NewInternalErrorNoCtxf(
							"backup checkpoint tombstone row %d has multiple replacements", obj.ckpRow,
						)
					}
					infoInsertTombstone[obj.ckpRow] = obj
				default:
					return nil, nil, nil, moerr.NewInternalErrorNoCtxf(
						"backup checkpoint replacement has invalid data type %d", obj.dataType,
					)
				}
			}

		}

		initCkpBatch := func(
			objectType int8,
			newMeta *batch.Batch,
			insertObjData map[int]*objData,
		) error {
			i := 0
			seenInsertions := 0
			err := ckpReader.ForEachRow(
				ctx,
				func(
					account uint32,
					dbid, tid uint64,
					objectType2 int8,
					objectStats objectio.ObjectStats,
					create, delete types.TS,
					rowID types.Rowid,
				) error {
					if objectType2 != objectType {
						return nil
					}
					replacement := insertObjData[i]
					i++
					if replacement == nil {
						return appendValToBatch(
							account, dbid, tid, objectType2, objectStats, create, delete,
							encoder, newMeta, common.CheckpointAllocator,
						)
					}
					seenInsertions++
					if !replacement.appendable {
						return appendValToBatch(
							account, dbid, tid, objectType2, objectStats, create, types.TS{},
							encoder, newMeta, common.CheckpointAllocator,
						)
					}
					if err := appendValToBatch(
						account, dbid, tid, objectType2, objectStats, create, delete,
						encoder, newMeta, common.CheckpointAllocator,
					); err != nil {
						return err
					}
					if backupReplacementIsSorted(replacement) {
						objectio.WithSorted()(replacement.stats)
					}
					return appendValToBatch(
						account, dbid, tid, objectType2, *replacement.stats, create, types.TS{},
						encoder, newMeta, common.CheckpointAllocator,
					)
				},
			)
			if err != nil {
				return err
			}
			if seenInsertions != len(insertObjData) {
				return moerr.NewInternalErrorNoCtxf(
					"backup checkpoint consumed %d of %d object replacements",
					seenInsertions, len(insertObjData),
				)
			}
			return nil
		}

		if err = initCkpBatch(ckputil.ObjectType_Data, objectInfoMeta, infoInsert); err != nil {
			return nil, nil, nil, err
		}
		if err = initCkpBatch(
			ckputil.ObjectType_Tombstone, tombstoneInfoMeta, infoInsertTombstone,
		); err != nil {
			return nil, nil, nil, err
		}
		if err = dataSinker.Write(ctx, objectInfoMeta); err != nil {
			return nil, nil, nil, err
		}
		if err = dataSinker.Write(ctx, tombstoneInfoMeta); err != nil {
			return nil, nil, nil, err
		}

	} else {
		dest := ckputil.NewObjectListBatch()
		defer dest.Clean(common.CheckpointAllocator)
		if err = ckpReader.ForEachRow(
			ctx,
			func(
				account uint32,
				dbid, tid uint64,
				objectType int8,
				objectStats objectio.ObjectStats,
				create, delete types.TS,
				rowID types.Rowid,
			) error {
				return appendValToBatch(
					account, dbid, tid, objectType, objectStats, create, delete, encoder, dest, common.CheckpointAllocator,
				)
			},
		); err != nil {
			return nil, nil, nil, err
		}
		if err = dataSinker.Write(ctx, dest); err != nil {
			return nil, nil, nil, err
		}
	}
	newData := NewCheckpointDataWithSinker(dataSinker, common.CheckpointAllocator)
	location, checkpointFiles, err := newData.Sync(
		ctx, dstFs,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	logutil.Info("[Done]",
		common.AnyField("checkpoint", location.String()),
		common.OperationField("ReWrite Checkpoint"),
		common.AnyField("new object", checkpointFiles))
	files = append(files, checkpointFiles...)
	files = append(files, location.Name().String())
	return location, location, files, nil
}
