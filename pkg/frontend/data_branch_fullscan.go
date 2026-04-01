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

package frontend

// fullTableScanDiff computes the diff between two tables by reading each
// table's full current state.  It avoids the per-row commit-ts filtering
// required by CollectChanges, so it works even when the underlying objects
// lack a commit-ts column (e.g. TN-merged objects on 3.0-dev).
//
// Semantics are equivalent to the regular hashDiff path:
//   - Rows in target but not in base → INSERT (target side)
//   - Rows in base but not in target → INSERT (base side)
//   - Same PK with different content  → UPDATE (target side)

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

func fullTableScanDiff(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	copt compositeOption,
	emit emitFunc,
) error {
	start := time.Now()

	tarTableID := tblStuff.tarRel.GetTableID(ctx)
	baseTableID := tblStuff.baseRel.GetTableID(ctx)
	snapshotTS := types.TimestampToTS(
		ses.GetTxnHandler().GetTxn().SnapshotTS(),
	)

	logutil.Info("DataBranch-FullScanDiff-Start",
		zap.Uint64("target-table-id", tarTableID),
		zap.Uint64("base-table-id", baseTableID),
		zap.String("snapshot-ts", snapshotTS.ToString()),
	)

	tarHashmap, err := scanTableIntoHashmap(
		ctx, ses, tblStuff, tarTableID, snapshotTS, "target",
	)
	if err != nil {
		return err
	}
	defer tarHashmap.Close()

	baseHashmap, err := scanTableIntoHashmap(
		ctx, ses, tblStuff, baseTableID, snapshotTS, "base",
	)
	if err != nil {
		return err
	}
	defer baseHashmap.Close()

	diffStart := time.Now()
	err = diffFullScanHashmaps(ctx, ses, tblStuff, copt, emit, tarHashmap, baseHashmap)
	diffCost := time.Since(diffStart)

	logutil.Info("DataBranch-FullScanDiff-Done",
		zap.Uint64("target-table-id", tarTableID),
		zap.Uint64("base-table-id", baseTableID),
		zap.Int64("target-rows", tarHashmap.ItemCount()),
		zap.Int64("base-rows", baseHashmap.ItemCount()),
		zap.Duration("diff-cost", diffCost),
		zap.Duration("total-cost", time.Since(start)),
		zap.Error(err),
	)
	return err
}

// scanTableIntoHashmap reads all rows of a table and inserts them into a
// BranchHashmap keyed by PK.
func scanTableIntoHashmap(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	tableID uint64,
	snapshotTS types.TS,
	label string,
) (databranchutils.BranchHashmap, error) {
	hm, err := databranchutils.NewBranchHashmap(
		databranchutils.WithBranchHashmapAllocator(tblStuff.hashmapAllocator),
	)
	if err != nil {
		return nil, err
	}

	var (
		mu       sync.Mutex
		rowCount int64
	)

	scanStart := time.Now()
	err = scanSnapshotRelationByID(
		ctx,
		"FullScanDiff-"+label,
		ses,
		tableID,
		snapshotTS,
		tblStuff.def.colNames,
		tblStuff.def.colTypes,
		nil, // no filter
		0,   // default parallelism
		func(readBatch *batch.Batch) error {
			if readBatch.RowCount() == 0 {
				return nil
			}

			// Copy vectors — the reader reuses the batch.
			vecs := make([]*vector.Vector, len(readBatch.Vecs))
			for i, v := range readBatch.Vecs {
				dup, err := v.Dup(ses.proc.Mp())
				if err != nil {
					return err
				}
				vecs[i] = dup
			}

			mu.Lock()
			rowCount += int64(readBatch.RowCount())
			mu.Unlock()

			if err := hm.PutByVectors(vecs, []int{tblStuff.def.pkColIdx}); err != nil {
				for _, v := range vecs {
					v.Free(ses.proc.Mp())
				}
				return err
			}
			for _, v := range vecs {
				v.Free(ses.proc.Mp())
			}
			return nil
		},
	)
	if err != nil {
		hm.Close()
		return nil, err
	}

	logutil.Info("DataBranch-FullScanDiff-ScanDone",
		zap.String("label", label),
		zap.Uint64("table-id", tableID),
		zap.Int64("row-count", rowCount),
		zap.Int64("hashmap-items", hm.ItemCount()),
		zap.Duration("duration", time.Since(scanStart)),
	)
	return hm, nil
}

// diffFullScanHashmaps compares two full-table hashmaps and emits the diff.
//
//   - Target rows with no base match → INSERT (target)
//   - Base rows with no target match → INSERT (base)
//   - Same PK, different values      → UPDATE (target), or expanded DELETE+INSERT
func diffFullScanHashmaps(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	copt compositeOption,
	emit emitFunc,
	tarHashmap databranchutils.BranchHashmap,
	baseHashmap databranchutils.BranchHashmap,
) error {
	// Phase 1: Walk target rows, probe base.
	if err := tarHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		tarBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
		updateBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)

		if err := cursor.ForEach(func(key []byte, row []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			checkRet, err := baseHashmap.PopByEncodedKey(key, false)
			if err != nil {
				return err
			}

			if !checkRet.Exists {
				// Target-only row → INSERT on target side.
				tarTuple, _, err := tarHashmap.DecodeRow(row)
				if err != nil {
					return err
				}
				return appendTupleToBat(ses, tarBat, tarTuple, tblStuff)
			}

			// Both sides have this PK — compare values.
			tarTuple, _, err := tarHashmap.DecodeRow(row)
			if err != nil {
				return err
			}
			baseTuple, _, err := baseHashmap.DecodeRow(checkRet.Rows[0])
			if err != nil {
				return err
			}

			if rowsEqual(tarTuple, baseTuple, tblStuff) {
				return nil // identical — no diff
			}

			// Different values → UPDATE.
			if copt.expandUpdate {
				return emitUpdate(ses, copt, tblStuff, emit, tarTuple, baseTuple)
			}
			return appendTupleToBat(ses, updateBat, tarTuple, tblStuff)
		}); err != nil {
			tblStuff.retPool.releaseRetBatch(tarBat, false)
			tblStuff.retPool.releaseRetBatch(updateBat, false)
			return err
		}

		// Emit accumulated INSERT batch.
		if tarBat.RowCount() > 0 {
			if stop, err := emitBatch(emit, batchWithKind{
				batch: tarBat,
				kind:  diffInsert,
				name:  tblStuff.tarRel.GetTableName(),
				side:  diffSideTarget,
			}, false, tblStuff.retPool); err != nil {
				tblStuff.retPool.releaseRetBatch(updateBat, false)
				return err
			} else if stop {
				tblStuff.retPool.releaseRetBatch(updateBat, false)
				return nil
			}
		} else {
			tblStuff.retPool.releaseRetBatch(tarBat, false)
		}

		// Emit accumulated UPDATE batch.
		if updateBat.RowCount() > 0 {
			if stop, err := emitBatch(emit, batchWithKind{
				batch: updateBat,
				kind:  diffUpdate,
				name:  tblStuff.tarRel.GetTableName(),
				side:  diffSideTarget,
			}, false, tblStuff.retPool); err != nil {
				return err
			} else if stop {
				return nil
			}
		} else {
			tblStuff.retPool.releaseRetBatch(updateBat, false)
		}

		return nil
	}, -1); err != nil {
		return err
	}

	// Phase 2: Remaining base rows (not matched) → INSERT on base side.
	return baseHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		baseBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)

		if err := cursor.ForEach(func(_ []byte, row []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			baseTuple, _, err := baseHashmap.DecodeRow(row)
			if err != nil {
				return err
			}
			return appendTupleToBat(ses, baseBat, baseTuple, tblStuff)
		}); err != nil {
			tblStuff.retPool.releaseRetBatch(baseBat, false)
			return err
		}

		if baseBat.RowCount() > 0 {
			if stop, err := emitBatch(emit, batchWithKind{
				batch: baseBat,
				kind:  diffInsert,
				name:  tblStuff.baseRel.GetTableName(),
				side:  diffSideBase,
			}, false, tblStuff.retPool); err != nil {
				return err
			} else if stop {
				return nil
			}
		} else {
			tblStuff.retPool.releaseRetBatch(baseBat, false)
		}

		return nil
	}, -1)
}

// rowsEqual compares all visible columns of two decoded tuples.
func rowsEqual(a, b types.Tuple, tblStuff tableStuff) bool {
	for _, idx := range tblStuff.def.visibleIdxes {
		if types.CompareValue(a[idx], b[idx]) != 0 {
			return false
		}
	}
	return true
}

// emitUpdate handles a same-PK-different-content pair for the merge path.
// It emits a DELETE of the old base row followed by an INSERT of the new
// target row so the merge consumer can apply the change.
func emitUpdate(
	ses *Session,
	copt compositeOption,
	tblStuff tableStuff,
	emit emitFunc,
	tarTuple, baseTuple types.Tuple,
) error {
	delBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
	if err := appendTupleToBat(ses, delBat, baseTuple, tblStuff); err != nil {
		tblStuff.retPool.releaseRetBatch(delBat, false)
		return err
	}
	if stop, err := emitBatch(emit, batchWithKind{
		batch: delBat,
		kind:  diffDelete,
		name:  tblStuff.tarRel.GetTableName(),
		side:  diffSideTarget,
	}, false, tblStuff.retPool); err != nil {
		return err
	} else if stop {
		return nil
	}

	insBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
	if err := appendTupleToBat(ses, insBat, tarTuple, tblStuff); err != nil {
		tblStuff.retPool.releaseRetBatch(insBat, false)
		return err
	}
	if stop, err := emitBatch(emit, batchWithKind{
		batch: insBat,
		kind:  diffInsert,
		name:  tblStuff.tarRel.GetTableName(),
		side:  diffSideTarget,
	}, false, tblStuff.retPool); err != nil {
		return err
	} else if stop {
		return nil
	}
	return nil
}
