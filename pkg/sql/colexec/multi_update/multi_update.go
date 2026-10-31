// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

func (update *MultiUpdate) String(buf *bytes.Buffer) {
	buf.WriteString(": " + opName)
}

func (update *MultiUpdate) OpType() vm.OpType {
	return vm.MultiUpdate
}

func (update *MultiUpdate) Prepare(proc *process.Process) error {
	if update.OpAnalyzer == nil {
		update.OpAnalyzer = process.NewAnalyzer(update.GetIdx(), update.IsFirst, update.IsLast, opName)
	} else {
		update.OpAnalyzer.Reset()
	}
	if update.ctr.sources == nil {
		update.ctr.sources = make(map[uint64]engine.Relation)
	} else {
		for _, source := range update.ctr.sources {
			if err := source.Reset(proc.GetTxnOperator()); err != nil {
				return err
			}
		}
	}

	if update.ctr.updateCtxInfos == nil {
		update.ctr.updateCtxInfos = make(map[string]*updateCtxInfo)
		for _, updateCtx := range update.MultiUpdateCtx {
			isPkMoveLink := isUpdatePkMoveLinkCtx(updateCtx)
			if !isPkMoveLink && !features.IsIndexTable(updateCtx.TableDef.FeatureFlag) {
				update.mainTable = updateCtx.TableDef.TblId
			}

			info := new(updateCtxInfo)
			for _, col := range updateCtx.TableDef.Cols {
				if col.Name != catalog.Row_ID {
					info.insertAttrs = append(info.insertAttrs, col.Name)
				}
			}

			tableType := UpdateMainTable
			if isPkMoveLink {
				tableType = UpdatePkMoveLinkTable
			} else if catalog.IsUniqueIndexTable(updateCtx.TableDef.Name) {
				tableType = UpdateUniqueIndexTable
			} else if catalog.IsSecondaryIndexTable(updateCtx.TableDef.Name) {
				tableType = UpdateSecondaryIndexTable
			}
			info.tableType = tableType
			info.isContiguous = isContiguousMapping(updateCtx.InsertCols)
			update.ctr.updateCtxInfos[updateCtxKey(updateCtx)] = info
		}
	}
	for _, updateCtx := range update.MultiUpdateCtx {
		info := lookupUpdateCtxInfo(update.ctr.updateCtxInfos, updateCtx)
		if update.Action != UpdateWriteS3 {
			// D221: a cached prepared-compile operator survives across
			// executions while MultiUpdate.Reset keeps updateCtxInfos,
			// so the retention relation handle cached by
			// resolvePkMoveRetentionSource must re-bind to the EXECUTING
			// transaction here exactly like info.Source; otherwise
			// staged identity rows ride the first, already-removed
			// transaction's workspace. A re-bind failure aborts the
			// statement — degrading would risk writing into a dead
			// workspace.
			if info.retentionSource != nil {
				if err := info.retentionSource.Reset(proc.GetTxnOperator()); err != nil {
					return err
				}
			}
			if info.Source == nil && !info.pkMoveLinkDisabled {
				rel, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, update.Engine, updateCtx.ObjRef)
				if err != nil {
					if isUpdatePkMoveLinkCtx(updateCtx) && isPkMoveLinkTableMissingErr(err) {
						// Tenants created before v4.0.15 lack the link
						// table; degrade to the legacy delete+insert diff
						// behaviour instead of failing the update. Any
						// OTHER resolution failure is fatal here: the
						// statement aborts, so user rows and provenance
						// links always commit or abort together.
						info.pkMoveLinkDisabled = true
						logutil.Warn("skip pk-move update links: link table unavailable", zap.Error(err))
						continue
					}
					return err
				}
				info.Source = rel
			} else if info.Source != nil {
				if err := info.Source.Reset(proc.GetTxnOperator()); err != nil {
					return err
				}
			}
		}
	}

	if len(update.ctr.insertBuf) == 0 {
		update.ctr.insertBuf = make([]*batch.Batch, len(update.MultiUpdateCtx))
	}
	if len(update.ctr.deleteBuf) == 0 {
		update.ctr.deleteBuf = make([]*batch.Batch, len(update.MultiUpdateCtx))
	}
	if err := update.prepareSeenTargetRows(proc); err != nil {
		return err
	}
	if update.Action == UpdateWriteS3 {
		if err := update.prepareSeenTargetRowsAdmission(proc.GetService()); err != nil {
			return err
		}
	}

	update.ctr.affectedRows = 0
	update.ctr.s3AffectedRows = 0
	update.ctr.flushed = false
	update.getFlushableS3WriterFunc = update.getFlushableS3Writer
	update.getS3WriterFunc = update.getS3Writer
	update.addAffectedRowsFunc = update.doAddAffectedRows
	update.takeS3AffectedRowsFunc = nil
	if update.Action == UpdateWriteS3 && hasODKUAffectedRows(update.MultiUpdateCtx) {
		// Writer operators can live below a merge PreScope, where Scope.affectedRows
		// cannot see their counters. Transfer ODKU's logical count in-band and let
		// the final FlushS3Info operator own the client-visible count.
		update.addAffectedRowsFunc = update.doAddS3AffectedRows
		update.takeS3AffectedRowsFunc = update.takeS3AffectedRows
	}

	switch update.Action {
	case UpdateWriteS3:
		if update.ctr.s3Writer == nil {
			writer, err := newS3Writer(proc.GetService(), update)
			if err != nil {
				return err
			}
			writer.segmentMap = colexec.MustGetServer(proc.GetService()).GetCnSegmentMap()
			update.ctr.s3Writer = writer
		} else {
			update.ctr.s3Writer.refreshSelectorState(update)
		}

	case UpdateFlushS3Info:
		//resort updateCtxs
		writer, err := newS3Writer(proc.GetService(), update)
		if err != nil {
			return err
		}
		writer.segmentMap = colexec.MustGetServer(proc.GetService()).GetCnSegmentMap()
		update.MultiUpdateCtx = writer.updateCtxs

		err = writer.free(proc)
		if err != nil {
			return err
		}
		writer.updateCtxs = nil

	case UpdateWriteTable:
		//do nothing
	}

	mainCtx := update.MultiUpdateCtx[0]
	if len(mainCtx.DeleteCols) > 0 && len(mainCtx.InsertCols) > 0 {
		update.ctr.action = actionUpdate
	} else if len(mainCtx.InsertCols) > 0 {
		update.ctr.action = actionInsert
	} else {
		update.ctr.action = actionDelete
	}

	return nil
}

func (update *MultiUpdate) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := update.OpAnalyzer

	t := time.Now()
	defer func() {
		analyzer.AddInsertTime(t)
	}()

	switch update.Action {
	case UpdateWriteS3:
		return update.update_s3(proc, analyzer)
	case UpdateWriteTable:
		return update.update(proc, analyzer)
	case UpdateFlushS3Info:
		return update.updateFlushS3Info(proc, analyzer)
	default:
	}

	panic(fmt.Sprintf("unexpected multi_update.UpdateAction: %#v", update.Action))
}

func (update *MultiUpdate) update_s3(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementInsertS3DurationHistogram.Observe(time.Since(start).Seconds())
	}()
	ctr := &update.ctr

	if ctr.state == vm.Build {
		input, err := update.getInput(proc, analyzer)
		if err != nil {
			return input, err
		}

		if input.Batch == nil || input.Batch.IsEmpty() {
			if input.Batch == nil {
				update.ctr.state = vm.Eval
			}
			result := vm.NewCallResult()
			result.Batch = batch.EmptyBatch
			return result, nil
		}

		w, err := update.getS3WriterFunc(proc.GetService(), update.mainTable)
		if err != nil {
			return vm.CancelResult, err
		}

		err = w.append(proc, analyzer, input.Batch)
		if err != nil {
			return vm.CancelResult, err
		}

		// This operator only BUFFERS the update to S3; the durable write
		// happens in the downstream UpdateFlushS3Info operator, which
		// stages independent per-table delete/insert streams. The old->new
		// pairing the pk-move link protocol needs exists only in the
		// buffered batch, so s3WriterDelegate.append records the link rows
		// (with NULL replacement identity) from the same target-deduped
		// batch it splits; the commit-timestamp tie binds the fold.

		result := vm.NewCallResult()
		result.Batch = batch.EmptyBatch
		return result, nil
	}

	if ctr.state == vm.Eval {
		result := vm.NewCallResult()
		var out *batch.Batch
		for {
			writer := update.getFlushableS3WriterFunc()
			if writer == nil {
				ctr.state = vm.End
				result.Batch = out
				return result, nil
			}

			err := writer.flushTailAndWriteToOutput(proc, analyzer)
			if err != nil {
				return vm.CancelResult, err
			}
			if writer.outputBat.RowCount() == 0 {
				return vm.CancelResult, err
			}
			if out == nil {
				out = writer.outputBat
			} else {
				out, err = out.Append(proc.Ctx, proc.Mp(), writer.outputBat)
				if err != nil {
					return vm.CancelResult, err
				}
			}
		}
	}

	return vm.CancelResult, nil
}

func (update *MultiUpdate) update(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	if !update.delegated {
		input, err := vm.ChildrenCall(update.GetChildren(0), proc, analyzer)
		if err != nil {
			return input, err
		}

		if input.Batch == nil || input.Batch.IsEmpty() {
			return input, nil
		}

		update.input = input
	}

	input := update.input
	err := update.updateOneBatch(proc, analyzer, input.Batch)
	if err != nil {
		return vm.CancelResult, err
	}

	return input, nil
}

func (update *MultiUpdate) updateFlushS3Info(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	input, err := vm.ChildrenCall(update.GetChildren(0), proc, analyzer)
	if err != nil {
		return input, err
	}
	if input.Batch == nil || input.Batch.IsEmpty() {
		return input, nil
	}

	actions := vector.MustFixedColNoTypeCheck[uint8](input.Batch.Vecs[0])
	tables := vector.MustFixedColNoTypeCheck[uint64](input.Batch.Vecs[1])
	rowCounts := vector.MustFixedColNoTypeCheck[uint64](input.Batch.Vecs[2])
	nameData, nameArea := vector.MustVarlenaRawData(input.Batch.Vecs[3])
	batData, batArea := vector.MustVarlenaRawData(input.Batch.Vecs[4])

	ctx := proc.Ctx
	batBufs := make(map[actionType]*batch.Batch)
	defer func() {
		for _, bat := range batBufs {
			bat.Clean(proc.Mp())
		}
	}()

	for i, action := range actions {
		if actionType(action) == actionAffectedRows {
			update.addAffectedRowsFunc(rowCounts[i])
			continue
		}
		if actionType(action) == actionPkMoveLink {
			// Link provenance stages in this (coordinator) workspace; a
			// resolution failure degrades to legacy behaviour instead of
			// failing the flush, mirroring the producer-side disable.
			if err := update.flushPkMoveLinkBatch(
				proc, analyzer, tables[i], nameData[i].UnsafeGetString(nameArea),
				batData[i].GetByteSlice(batArea)); err != nil {
				return vm.CancelResult, err
			}
			continue
		}
		source, err := update.getSourceByID(tables[i], proc)
		if err != nil {
			return input, err
		}

		tableType := UpdateMainTable
		if catalog.IsUniqueIndexTable(source.GetTableName()) {
			tableType = UpdateUniqueIndexTable
		} else if catalog.IsSecondaryIndexTable(source.GetTableName()) {
			tableType = UpdateSecondaryIndexTable
		}

		switch actionType(action) {
		case actionDelete:
			if batBufs[actionDelete] == nil {
				batBufs[actionDelete] = batch.NewOffHeapEmpty()
			} else {
				batBufs[actionDelete].CleanOnlyData()
			}
			if err := batBufs[actionDelete].UnmarshalBinaryWithAnyMp(batData[i].GetByteSlice(batArea), proc.Mp()); err != nil {
				return input, err
			}
			// For REPLACE INTO, we don't count DELETE rows in affected rows
			// REPLACE INTO should only return the number of INSERT rows
			name := nameData[i].UnsafeGetString(nameArea)

			crs := analyzer.GetOpCounterSet()
			newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
			newCtx = update.writeContext(newCtx, tables[i])
			err = process.MeasureFilesystemWaitErr(analyzer, func() error {
				return source.Delete(newCtx, batBufs[actionDelete], name)
			})
			if err != nil {
				return input, err
			}
			if tableType == UpdateMainTable {
				update.addDeleteAffectRows(tableType, rowCounts[i])
			}
			analyzer.AddDeletedRows(int64(batBufs[actionDelete].RowCount()))

		case actionInsert:
			if batBufs[actionInsert] == nil {
				batBufs[actionInsert] = batch.NewOffHeapEmpty()
			} else {
				batBufs[actionInsert].CleanOnlyData()
			}
			if err := batBufs[actionInsert].UnmarshalBinaryWithAnyMp(batData[i].GetByteSlice(batArea), proc.Mp()); err != nil {
				return input, err
			}

			// For REPLACE INTO, we need to count INSERT rows based on the actual action
			// Always count INSERT rows for main table, regardless of update.ctr.action
			if tableType == UpdateMainTable && !hasChangedRowsCol(update.MultiUpdateCtx) {
				update.addAffectedRowsFunc(rowCounts[i])
			}

			crs := analyzer.GetOpCounterSet()
			newCtx := perfcounter.AttachS3RequestKey(ctx, crs)
			newCtx = update.writeContext(newCtx, tables[i])
			err = process.MeasureFilesystemWaitErr(analyzer, func() error {
				return source.Write(newCtx, batBufs[actionInsert])
			})
			if err != nil {
				return input, err
			}
			analyzer.AddWrittenRows(int64(batBufs[actionInsert].RowCount()))

		case actionUpdate:
			if batBufs[actionUpdate] == nil {
				batBufs[actionUpdate] = batch.NewOffHeapEmpty()
			} else {
				batBufs[actionUpdate].CleanOnlyData()
			}
			if err := batBufs[actionUpdate].UnmarshalBinaryWithAnyMp(batData[i].GetByteSlice(batArea), proc.Mp()); err != nil {
				return input, err
			}

			err = update.updateOneBatch(proc, analyzer, batBufs[actionUpdate])
		default:
			panic("unexpected multi_update.actionType")
		}

		if err != nil {
			return vm.CancelResult, err
		}
	}

	return input, nil
}

// flushPkMoveLinkBatch stages one ferried pk-move link batch into the
// coordinator's committing workspace. Only this workspace's fill hook and
// commit prune complete provenance (identity back-fill, marker
// consumption, lineage heals, dead-replacement retirement), so the S3
// producers send their synthesized link batches here instead of writing
// a producer-local relation.
//
// The ferried record names the OWNER table: the synthetic link ctx's plan
// TableDef is schema-only with TblId 0 (a per-account virtual catalog
// table), so a by-id lookup can never resolve it. The receiver matches the
// owner id to its own link ctx and resolves the relation through the same
// lazy per-account path the workspace route uses; an unresolvable link
// table (a tenant predating v4.0.15) degrades to legacy diff behaviour
// rather than failing the flush.
func (update *MultiUpdate) flushPkMoveLinkBatch(
	proc *process.Process,
	analyzer process.Analyzer,
	ownerTableID uint64,
	name string,
	data []byte,
) (err error) {
	raw := batch.NewOffHeapEmpty()
	defer raw.Clean(proc.Mp())
	if err = raw.UnmarshalBinaryWithAnyMp(data, proc.Mp()); err != nil {
		return err
	}

	var linkCtx *MultiUpdateCtx
	for _, updateCtx := range update.MultiUpdateCtx {
		if !isUpdatePkMoveLinkCtx(updateCtx) {
			continue
		}
		ownerIdx := updateCtx.TargetUpdateCtxIdx
		if ownerIdx < 0 || ownerIdx >= len(update.MultiUpdateCtx) {
			continue
		}
		if targetTableID(update.MultiUpdateCtx[ownerIdx]) == ownerTableID {
			linkCtx = updateCtx
			break
		}
	}
	if linkCtx == nil {
		logutil.Warn("skip ferried pk-move update links: no link context for owner table",
			zap.Uint64("table_id", ownerTableID))
		return nil
	}
	source, err := update.resolvePkMoveLinkSource(proc, linkCtx)
	if err != nil || source == nil {
		return err
	}

	bat := raw
	if name != "" {
		// The producer spilled its buffered links to one object per link
		// ctx (see spillPkMoveLinkBufs) and ferried the block-info.
		// Rebuild the exact row shape an in-memory batch would have
		// staged so every downstream workspace consumer (fill hook,
		// identity completion, lineage heal) sees one uniform layout.
		// The ferry row PUBLISHED the spill records: cleanup ownership
		// transferred here, to the committing consumer, which deletes
		// object and marker after the staging below succeeds (the
		// producer's reset/free now skip them; a consumer that never
		// runs leaves them to the marker's expired GC replay).
		if name != pkMoveLinkSpillName {
			return moerr.NewInternalError(proc.Ctx,
				fmt.Sprintf("unexpected pk-move link ferry transport %q", name))
		}
		if bat, err = update.loadSpilledPkMoveLinkBatch(proc, raw, linkCtx); err != nil {
			return err
		}
		defer bat.Clean(proc.Mp())
	}
	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
	newCtx = update.writeContext(newCtx, ownerTableID)
	// Retention genesis rides the same transaction on the S3 ferry route
	// (D159): stage the identity row for the ferried link batch's owner —
	// a landing failure aborts the statement and the cut removes both.
	// D218: only a ferried batch carrying a real moved-address row admits
	// genesis; continuity markers alone stage nothing.
	ownerIdx := linkCtx.TargetUpdateCtxIdx
	if ownerIdx >= 0 && ownerIdx < len(update.MultiUpdateCtx) &&
		pkMoveLinkBatchHasRealMove(bat) {
		if err = update.stagePkMoveRetentionRows(
			proc, analyzer, linkCtx, update.MultiUpdateCtx[ownerIdx]); err != nil {
			return err
		}
	}
	if err = process.MeasureFilesystemWaitErr(analyzer, func() error {
		return source.Write(newCtx, bat)
	}); err != nil {
		return err
	}
	if name != "" {
		update.consumePkMoveLinkSpillOwnership(proc, raw)
	}
	analyzer.AddWrittenRows(int64(bat.RowCount()))
	return nil
}

func (update *MultiUpdate) updateOneBatch(proc *process.Process, analyzer process.Analyzer, bat *batch.Batch) (err error) {
	targetBatches := make(map[int]*batch.Batch)
	defer func() {
		for _, targetBatch := range targetBatches {
			if targetBatch != bat {
				targetBatch.Clean(proc.Mp())
			}
		}
	}()

	for i, updateCtx := range update.MultiUpdateCtx {
		targetIdx := updateCtx.TargetUpdateCtxIdx
		if targetIdx < 0 || targetIdx >= len(update.MultiUpdateCtx) {
			return moerr.NewInternalError(proc.Ctx, "invalid multi-target update context index")
		}
		contextBatch, ok := targetBatches[targetIdx]
		if !ok {
			var duplicateRows uint64
			contextBatch, _, duplicateRows, err = filterTargetRows(
				proc,
				update.MultiUpdateCtx[targetIdx],
				bat,
				update.ctr.seenTargetRows[targetTableID(update.MultiUpdateCtx[targetIdx])],
			)
			if err != nil {
				return err
			}
			update.addAffectedRowsFunc(duplicateRows)
			targetBatches[targetIdx] = contextBatch
		}
		if contextBatch.RowCount() == 0 {
			continue
		}

		if isUpdatePkMoveLinkCtx(updateCtx) {
			if pkMoveLinkWriteAction(update.Action) {
				if err = update.insertUpdatePkMoveLinkRows(proc, analyzer, updateCtx, update.MultiUpdateCtx[targetIdx], contextBatch); err != nil {
					return err
				}
			}
			continue
		}

		// delete rows
		if len(updateCtx.DeleteCols) > 0 {
			if err = update.delete_table(proc, analyzer, updateCtx, contextBatch, i); err != nil {
				return err
			}
		}

		// insert rows
		if len(updateCtx.InsertCols) == 0 {
			continue
		}
		tableType := lookupUpdateCtxInfo(update.ctr.updateCtxInfos, updateCtx).tableType
		switch tableType {
		case UpdateMainTable:
			err = update.insert_main_table(proc, analyzer, i, contextBatch)
		case UpdateUniqueIndexTable:
			err = update.insert_unique_index_table(proc, analyzer, i, contextBatch)
		case UpdateSecondaryIndexTable:
			err = update.insert_secondary_index_table(proc, analyzer, i, contextBatch)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func filterTargetRows(
	proc *process.Process,
	updateCtx *MultiUpdateCtx,
	input *batch.Batch,
	seen *hashmap.StrHashMap,
) (*batch.Batch, bool, uint64, error) {
	if !updateCtx.DedupByTargetRowID {
		if updateCtx.AffectedRowsWeightCol != nil || updateCtx.PhysicalChangedRowsCol != nil {
			return filterODKUPhysicalRows(proc, updateCtx, input)
		}
		if len(updateCtx.AffectedRowsCols) > 0 {
			affectedRows, err := countAffectedRowsBySelectors(proc, updateCtx, input)
			return input, false, affectedRows, err
		}
		return input, false, 0, nil
	}
	if len(updateCtx.DeleteCols) < 3 ||
		updateCtx.DeleteCols[0] < 0 ||
		updateCtx.DeleteCols[0] >= len(input.Vecs) ||
		updateCtx.DeleteCols[2] < 0 ||
		updateCtx.DeleteCols[2] >= len(input.Vecs) {
		return nil, false, 0, moerr.NewInternalError(proc.Ctx, "invalid multi-target update selector columns")
	}

	rowIDVec := input.Vecs[updateCtx.DeleteCols[0]]
	rowNumberVec := input.Vecs[updateCtx.DeleteCols[2]]
	if rowIDVec.GetType().Oid != types.T_Rowid ||
		rowNumberVec.GetType().Oid != types.T_int64 {
		return nil, false, 0, moerr.NewInternalError(proc.Ctx, "invalid multi-target update selector types")
	}

	rowNumbers := vector.MustFixedColWithTypeCheck[int64](rowNumberVec)
	rowIDNulls := rowIDVec.GetNulls()
	rowNumberNulls := rowNumberVec.GetNulls()
	activeCols := updateCtx.AffectedRowsCols
	activeVecs := make([]*vector.Vector, len(activeCols))
	for i, col := range activeCols {
		if col < 0 || col >= len(input.Vecs) {
			return nil, false, 0, moerr.NewInternalError(proc.Ctx, "invalid multi-target update selector columns")
		}
		activeVecs[i] = input.Vecs[col]
		if activeVecs[i].GetType().Oid != types.T_bool {
			return nil, false, 0, moerr.NewInternalError(proc.Ctx, "invalid multi-target update selector types")
		}
	}
	var physicalActiveVec *vector.Vector
	if len(updateCtx.DeleteCols) >= 4 {
		physicalActiveCol := updateCtx.DeleteCols[3]
		if physicalActiveCol < 0 || physicalActiveCol >= len(input.Vecs) {
			return nil, false, 0, moerr.NewInternalError(
				proc.Ctx, "invalid multi-target update selector columns")
		}
		physicalActiveVec = input.Vecs[physicalActiveCol]
		if physicalActiveVec.GetType().Oid != types.T_bool {
			return nil, false, 0, moerr.NewInternalError(
				proc.Ctx, "invalid multi-target update selector types")
		}
	}
	if updateCtx.ChangedRowsCol != nil {
		changedCol := *updateCtx.ChangedRowsCol
		if changedCol < 0 || changedCol >= len(input.Vecs) ||
			input.Vecs[changedCol].GetType().Oid != types.T_bool {
			return nil, false, 0, moerr.NewInternalError(proc.Ctx, "invalid UPDATE changed-row column")
		}
	}
	selections := make([]int64, 0, input.RowCount())
	var matchedSemanticAffectedRows uint64
	for i := 0; i < input.RowCount(); i++ {
		if rowIDNulls.Contains(uint64(i)) ||
			rowNumberNulls.Contains(uint64(i)) ||
			rowNumbers[i] != 1 {
			continue
		}
		activeCount := 1
		if len(activeVecs) > 0 {
			activeCount = 0
			for _, activeVec := range activeVecs {
				if !activeVec.IsNull(uint64(i)) &&
					vector.GetFixedAtNoTypeCheck[bool](activeVec, i) {
					activeCount++
				}
			}
		}
		physicalActive := true
		if physicalActiveVec != nil {
			physicalActive = !physicalActiveVec.IsNull(uint64(i)) &&
				vector.GetFixedAtNoTypeCheck[bool](physicalActiveVec, i)
		} else if len(activeVecs) > 0 {
			// Compatibility with plans produced before physical write eligibility
			// was separated from semantic affected-row selectors.
			physicalActive = activeCount > 0
		}
		if !physicalActive {
			continue
		}
		selections = append(selections, int64(i))
		matchedSemanticAffectedRows += uint64(activeCount)
	}

	// The input can carry a build-side allocation account whose lifetime ends
	// at the operator boundary. The filtered batch is independently owned by
	// MULTI_UPDATE, so do not inherit that execution-local account.
	filtered, err := input.CloneWithoutAllocationAccount(proc.Mp(), true)
	if err != nil {
		return nil, false, 0, err
	}
	filtered.Shrink(selections, false)
	filtered.SetRowCount(len(selections))
	if seen == nil || filtered.RowCount() == 0 {
		semanticAffectedRows := matchedSemanticAffectedRows
		if updateCtx.ChangedRowsCol != nil {
			semanticAffectedRows = countSemanticAffectedRows(updateCtx, filtered, activeCols)
		}
		return filtered, true, filterReportedAffectedRows(
			updateCtx, semanticAffectedRows, insertAffectedRows(updateCtx, filtered)), nil
	}

	physicalSelections := make([]int64, 0, filtered.RowCount())
	iterator := seen.NewIterator()
	for offset := 0; offset < filtered.RowCount(); offset += hashmap.UnitLimit {
		count := min(hashmap.UnitLimit, filtered.RowCount()-offset)
		oldGroupCount := seen.GroupCount()
		values, zValues, err := iterator.Insert(
			offset,
			count,
			[]*vector.Vector{filtered.Vecs[updateCtx.DeleteCols[0]]},
		)
		if err != nil {
			filtered.Clean(proc.Mp())
			return nil, false, 0, err
		}
		nextGroup := oldGroupCount
		for i, value := range values {
			if zValues[i] != 0 && value > nextGroup {
				nextGroup++
				physicalSelections = append(physicalSelections, int64(offset+i))
			}
		}
	}
	filtered.Shrink(physicalSelections, false)
	filtered.SetRowCount(len(physicalSelections))
	semanticAffectedRows := matchedSemanticAffectedRows
	if updateCtx.ChangedRowsCol != nil {
		semanticAffectedRows = countSemanticAffectedRows(updateCtx, filtered, activeCols)
	}
	return filtered, true, filterReportedAffectedRows(
		updateCtx, semanticAffectedRows, insertAffectedRows(updateCtx, filtered)), nil
}

func filterODKUPhysicalRows(
	proc *process.Process,
	updateCtx *MultiUpdateCtx,
	input *batch.Batch,
) (*batch.Batch, bool, uint64, error) {
	var affectedRows uint64
	if updateCtx.AffectedRowsWeightCol != nil {
		col := *updateCtx.AffectedRowsWeightCol
		if col < 0 || col >= len(input.Vecs) || input.Vecs[col].GetType().Oid != types.T_uint64 {
			return nil, false, 0, moerr.NewInternalError(proc.Ctx, "invalid ODKU affected-row weight column")
		}
		vec := input.Vecs[col]
		if vec.HasNull() {
			return nil, false, 0, moerr.NewInternalError(proc.Ctx, "NULL ODKU affected-row weight")
		}
		for _, n := range vector.MustFixedColWithTypeCheck[uint64](vec)[:input.RowCount()] {
			affectedRows += n
		}
	}
	if updateCtx.PhysicalChangedRowsCol == nil {
		return input, false, affectedRows, nil
	}
	col := *updateCtx.PhysicalChangedRowsCol
	if col < 0 || col >= len(input.Vecs) || input.Vecs[col].GetType().Oid != types.T_bool {
		return nil, false, 0, moerr.NewInternalError(proc.Ctx, "invalid ODKU physical-change column")
	}
	vec := input.Vecs[col]
	if vec.HasNull() {
		return nil, false, 0, moerr.NewInternalError(proc.Ctx, "NULL ODKU physical-change marker")
	}
	changed := vector.MustFixedColWithTypeCheck[bool](vec)
	allChanged := true
	for row := 0; row < input.RowCount(); row++ {
		if !changed[row] {
			allChanged = false
			break
		}
	}
	if allChanged {
		return input, false, affectedRows, nil
	}
	selections := make([]int64, 0, input.RowCount())
	for row := 0; row < input.RowCount(); row++ {
		if changed[row] {
			selections = append(selections, int64(row))
		}
	}
	filtered, err := input.CloneWithoutAllocationAccount(proc.Mp(), true)
	if err != nil {
		return nil, false, 0, err
	}
	filtered.Shrink(selections, false)
	filtered.SetRowCount(len(selections))
	return filtered, true, affectedRows, nil
}

func filterReportedAffectedRows(
	updateCtx *MultiUpdateCtx,
	semanticAffectedRows uint64,
	physicalAffectedRows uint64,
) uint64 {
	if len(updateCtx.AffectedRowsCols) > 0 {
		return semanticAffectedRows
	}
	return semanticAffectedRows - physicalAffectedRows
}

func countSemanticAffectedRows(updateCtx *MultiUpdateCtx, input *batch.Batch, activeCols []int) uint64 {
	activeVecs := make([]*vector.Vector, len(activeCols))
	for i, col := range activeCols {
		activeVecs[i] = input.Vecs[col]
	}
	var changedVec *vector.Vector
	if updateCtx.ChangedRowsCol != nil {
		changedVec = input.Vecs[*updateCtx.ChangedRowsCol]
	}

	var affectedRows uint64
	for row := 0; row < input.RowCount(); row++ {
		activeCount := 1
		if len(activeVecs) > 0 {
			activeCount = 0
			for _, activeVec := range activeVecs {
				if !activeVec.IsNull(uint64(row)) &&
					vector.GetFixedAtNoTypeCheck[bool](activeVec, row) {
					activeCount++
				}
			}
		}
		if changedVec == nil || (!changedVec.IsNull(uint64(row)) &&
			vector.GetFixedAtNoTypeCheck[bool](changedVec, row)) {
			affectedRows += uint64(activeCount)
		}
	}
	return affectedRows
}

func countAffectedRowsBySelectors(
	proc *process.Process,
	updateCtx *MultiUpdateCtx,
	input *batch.Batch,
) (uint64, error) {
	activeVecs := make([]*vector.Vector, len(updateCtx.AffectedRowsCols))
	for i, col := range updateCtx.AffectedRowsCols {
		if col < 0 || col >= len(input.Vecs) {
			return 0, moerr.NewInternalError(proc.Ctx, "invalid multi-target update selector columns")
		}
		activeVecs[i] = input.Vecs[col]
		if activeVecs[i].GetType().Oid != types.T_bool {
			return 0, moerr.NewInternalError(proc.Ctx, "invalid multi-target update selector types")
		}
	}
	var changedVec *vector.Vector
	if updateCtx.ChangedRowsCol != nil {
		changedCol := *updateCtx.ChangedRowsCol
		if changedCol < 0 || changedCol >= len(input.Vecs) ||
			input.Vecs[changedCol].GetType().Oid != types.T_bool {
			return 0, moerr.NewInternalError(proc.Ctx, "invalid UPDATE changed-row column")
		}
		changedVec = input.Vecs[changedCol]
	}

	var affectedRows uint64
	for row := 0; row < input.RowCount(); row++ {
		if changedVec != nil && (changedVec.IsNull(uint64(row)) ||
			!vector.GetFixedAtNoTypeCheck[bool](changedVec, row)) {
			continue
		}
		for _, activeVec := range activeVecs {
			if !activeVec.IsNull(uint64(row)) &&
				vector.GetFixedAtNoTypeCheck[bool](activeVec, row) {
				affectedRows++
			}
		}
	}
	return affectedRows, nil
}

func (update *MultiUpdate) prepareSeenTargetRows(proc *process.Process) error {
	if update.ctr.seenTargetRows != nil {
		return nil
	}
	targetCounts := make(map[uint64]int)
	for _, ctx := range update.MultiUpdateCtx {
		if !features.IsIndexTable(ctx.TableDef.FeatureFlag) {
			targetCounts[targetTableID(ctx)]++
		}
	}
	update.ctr.seenTargetRows = make(map[uint64]*hashmap.StrHashMap)
	for tableID, count := range targetCounts {
		if count < 2 {
			continue
		}
		seen, err := hashmap.NewStrHashMap(false, proc.Mp())
		if err != nil {
			return err
		}
		update.ctr.seenTargetRows[tableID] = seen
	}
	return nil
}

func (update *MultiUpdate) prepareSeenTargetRowsAdmission(sid string) error {
	if len(update.ctr.seenTargetRows) == 0 {
		return nil
	}
	if update.ctr.seenRowsRSC != nil {
		return nil
	}
	value, ok := runtime.ServiceRuntime(sid).GetGlobalVariables(runtime.CNMemoryThrottler)
	if !ok {
		return moerr.NewInternalErrorNoCtxf("can not get global variable %s", runtime.CNMemoryThrottler)
	}
	update.ctr.seenRowsRSC = value.(rscthrottler.RSCThrottler)
	if err := update.admitSeenTargetRowsGrowth(update.seenTargetRowsSize()); err != nil {
		update.ctr.seenRowsRSC = nil
		return err
	}
	return nil
}

func (update *MultiUpdate) admitSeenTargetRowsGrowth(increment int64) error {
	if increment <= 0 || update.ctr.seenRowsRSC == nil {
		return nil
	}
	if _, granted := update.ctr.seenRowsRSC.Acquire(increment); !granted {
		return moerr.NewInternalErrorNoCtx(
			"multi-target update Rowid deduplication exceeded the CN memory admission limit",
		)
	}
	update.ctr.seenRowsGrant += increment
	return nil
}

func (update *MultiUpdate) seenTargetRowsSize() int64 {
	var size int64
	for _, seen := range update.ctr.seenTargetRows {
		size += seen.Size()
	}
	return size
}

func targetTableID(ctx *MultiUpdateCtx) uint64 {
	if ctx.TargetTableID != 0 {
		return ctx.TargetTableID
	}
	return ctx.TableDef.TblId
}

func (update *MultiUpdate) writeContext(ctx context.Context, tableID uint64) context.Context {
	for _, updateCtx := range update.MultiUpdateCtx {
		if updateCtx != nil && updateCtx.TableDef != nil &&
			targetTableID(updateCtx) == tableID && updateCtx.TableDef.Name == catalog.MO_COLUMNS_UPDATE {
			return context.WithValue(ctx, defines.MoColumnsUpdateKey{}, true)
		}
	}
	return ctx
}

func (update *MultiUpdate) resetMultiUpdateCtxs() {
	update.ctr.updateCtxInfos = make(map[string]*updateCtxInfo, len(update.MultiUpdateCtx))

	for _, updateCtx := range update.MultiUpdateCtx {
		info := new(updateCtxInfo)
		for _, col := range updateCtx.TableDef.Cols {
			if col.Name != catalog.Row_ID {
				info.insertAttrs = append(info.insertAttrs, col.Name)
			}
		}

		tableType := UpdateMainTable
		if isUpdatePkMoveLinkCtx(updateCtx) {
			tableType = UpdatePkMoveLinkTable
		} else if catalog.IsUniqueIndexTable(updateCtx.TableDef.Name) {
			tableType = UpdateUniqueIndexTable
		} else if catalog.IsSecondaryIndexTable(updateCtx.TableDef.Name) {
			tableType = UpdateSecondaryIndexTable
		}
		info.tableType = tableType
		update.ctr.updateCtxInfos[updateCtxKey(updateCtx)] = info
	}
}

func (update *MultiUpdate) resetMultiSources(proc *process.Process) error {
	for _, updateCtx := range update.MultiUpdateCtx {
		info := lookupUpdateCtxInfo(update.ctr.updateCtxInfos, updateCtx)
		info.Source = nil
		// D221: the cached retention handle dies with its transaction
		// too; clearing it lets the lazy resolver re-resolve under the
		// executing txn (the sticky pkMoveRetentionDisabled degrade bit
		// is preserved).
		info.retentionSource = nil
		if update.Action != UpdateWriteS3 {
			if isUpdatePkMoveLinkCtx(updateCtx) {
				// Resolved lazily by insertUpdatePkMoveLinkRows so tenants
				// without the link table degrade instead of erroring.
				continue
			}
			rel, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, update.Engine, updateCtx.ObjRef)
			if err != nil {
				return err
			}
			info.Source = rel
		}
	}
	return nil
}

func (update *MultiUpdate) getInput(
	proc *process.Process,
	analyzer process.Analyzer,
) (vm.CallResult, error) {
	if !update.delegated {
		input, err := vm.ChildrenCall(update.GetChildren(0), proc, analyzer)
		if err != nil {
			return input, err
		}

		update.input = input
	}

	return update.input, nil
}

func (update *MultiUpdate) getS3Writer(sid string, id uint64) (*s3WriterDelegate, error) {
	return update.ctr.s3Writer, nil
}

func (update *MultiUpdate) getFlushableS3Writer() *s3WriterDelegate {
	w := update.ctr.s3Writer
	if update.ctr.flushed {
		return nil
	}
	update.ctr.flushed = true
	return w
}

func (update *MultiUpdate) getSourceByID(
	id uint64,
	proc *process.Process,
) (engine.Relation, error) {
	r, ok := update.ctr.sources[id]
	if ok {
		return r, nil
	}

	_, _, r, err := update.Engine.GetRelationById(
		proc.Ctx,
		proc.GetTxnOperator(),
		id,
	)
	if err != nil {
		return nil, err
	}

	r = engine.NewRelationHandle(r)
	update.ctr.sources[id] = r
	return r, nil
}
