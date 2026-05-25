// Copyright 2025 Matrix Origin
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

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

// should read the LCA table to get all column values.
func handleDelsOnLCA(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tBat *batch.Batch,
	tblStuff tableStuff,
	snapshot timestamp.Timestamp,
) (dBat *batch.Batch, hitIdxes []int64, err error) {

	if snapshot.PhysicalTime == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtxf("invalid branch ts: %s", snapshot.DebugString())
	}

	if tBat == nil || tBat.RowCount() == 0 {
		dBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
		if tBat != nil {
			for i := range tBat.Vecs {
				tBat.Vecs[i].CleanOnlyData()
			}
			tBat.SetRowCount(0)
		}
		return
	}

	var (
		sqlRet executor.Result

		lcaTblDef  = tblStuff.lcaRel.GetTableDef(ctx)
		baseTblDef = tblStuff.baseRel.GetTableDef(ctx)

		colTypes           = tblStuff.def.colTypes
		expandedPKColIdxes = tblStuff.def.pkColIdxes
		snapshotTS         = types.TimestampToTS(snapshot)
	)

	{
		sqlBuf := acquireBuffer(tblStuff.bufPool)
		valsBuf := acquireBuffer(tblStuff.bufPool)
		defer func() {
			releaseBuffer(tblStuff.bufPool, sqlBuf)
			releaseBuffer(tblStuff.bufPool, valsBuf)
		}()

		// Keep SQL as the fast path because it is much cheaper than reader-based
		// probing. However, this path still relies on name-based time travelling
		// like db.table{MO_TS=...}. After GC, if no snapshot or PITR history was
		// created for the corresponding account/db/table, catalog lookup at that
		// timestamp may fail with unknown db/table even though the caller already
		// knows the stable table ID.
		mots := fmt.Sprintf("{MO_TS=%d} ", snapshot.PhysicalTime)
		pkNames := lcaTblDef.Pkey.Names

		// composite pk
		if baseTblDef.Pkey.CompPkeyCol != nil {
			var tuple types.Tuple
			cols, area := vector.MustVarlenaRawData(tBat.Vecs[0])
			for i := range cols {
				b := cols[i].GetByteSlice(area)
				if tuple, err = types.Unpack(b); err != nil {
					return nil, nil, err
				}

				valsBuf.WriteString(fmt.Sprintf("row(%d,", i))
				for j := range tuple {
					if err = formatValIntoString(
						ses, tuple[j], colTypes[expandedPKColIdxes[j]], valsBuf,
					); err != nil {
						return nil, nil, err
					}
					if j != len(tuple)-1 {
						valsBuf.WriteString(", ")
					}
				}
				valsBuf.WriteString(")")
				if i != len(cols)-1 {
					valsBuf.WriteString(", ")
				}
			}
		} else if baseTblDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
			// fake pk
			pks := vector.MustFixedColNoTypeCheck[uint64](tBat.Vecs[0])
			for i := range pks {
				valsBuf.WriteString(fmt.Sprintf("row(%d,%d)", i, pks[i]))
				if i != len(pks)-1 {
					valsBuf.WriteString(", ")
				}
			}
		} else {
			// real pk
			valsBuf.Reset()
			pkType := colTypes[expandedPKColIdxes[0]]
			for i := range tBat.Vecs[0].Length() {
				valsBuf.WriteString(fmt.Sprintf("row(%d,", i))
				b := tBat.Vecs[0].GetRawBytesAt(i)
				val := types.DecodeValue(b, tBat.Vecs[0].GetType().Oid)
				if err = formatValIntoString(ses, val, pkType, valsBuf); err != nil {
					return nil, nil, err
				}
				valsBuf.WriteString(")")
				if i != tBat.Vecs[0].Length()-1 {
					valsBuf.WriteString(",")
				}
			}
		}

		sqlBuf.Reset()
		sqlBuf.WriteString(fmt.Sprintf(
			"select pks.__idx_, lca.* from %s.%s%s as lca ",
			lcaTblDef.DbName, lcaTblDef.Name, mots),
		)
		sqlBuf.WriteString(fmt.Sprintf(
			"right join (values %s) as pks(__idx_,%s) on ",
			valsBuf.String(), strings.Join(pkNames, ",")),
		)

		for i := range pkNames {
			sqlBuf.WriteString(fmt.Sprintf("lca.%s = ", pkNames[i]))
			if castType, ok := lcaProbeJoinCastType(colTypes[expandedPKColIdxes[i]]); ok {
				sqlBuf.WriteString(fmt.Sprintf("cast(pks.%s as %s)", pkNames[i], castType))
			} else {
				sqlBuf.WriteString(fmt.Sprintf("pks.%s", pkNames[i]))
			}
			if i != len(pkNames)-1 {
				sqlBuf.WriteString(" AND ")
			}
		}
		sqlBuf.WriteString(" order by pks.__idx_")

		if sqlRet, err = runSql(ctx, ses, bh, sqlBuf.String(), nil, nil); err != nil {
			logutil.Error(
				"DataBranch-LCA-SQL-Error",
				zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
				zap.String("db-name", lcaTblDef.DbName),
				zap.String("table-name", lcaTblDef.Name),
				zap.Int("input-pk-rows", tBat.RowCount()),
				zap.String("snapshot-ts", snapshotTS.ToString()),
				zap.Error(err),
			)
			return nil, nil, err
		}
	}

	notExist := func(cols []*vector.Vector, r int) bool {
		exist := false
		for _, col := range cols {
			if !col.GetNulls().Contains(uint64(r)) {
				exist = true
				break
			}
		}
		return !exist
	}

	dBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)

	endIdx := dBat.VectorCount() - 1

	sels := make([]int64, 0, 100)
	sqlRet.ReadRows(func(rowCnt int, cols []*vector.Vector) bool {
		for i := range rowCnt {
			idx := vector.GetFixedAtNoTypeCheck[int64](cols[0], i)
			if notExist(cols[1:], i) {
				sels = append(sels, int64(idx))
				continue
			}
			hitIdxes = append(hitIdxes, int64(idx))

			for j := 1; j < len(cols); j++ {
				if err = dBat.Vecs[j-1].UnionOne(cols[j], int64(i), ses.proc.Mp()); err != nil {
					return false
				}
			}

			// For composite/fake PK tables, the hidden PK column (__cpkey__
			// or __mo_fake_pk_col) may not be returned by SQL "select *".
			// Fill it from the tombstone batch when the loop above does not
			// cover endIdx.
			if tblStuff.def.pkKind != normalKind && len(cols)-2 < endIdx {
				if err = dBat.Vecs[endIdx].UnionOne(tBat.Vecs[0], int64(i), ses.proc.Mp()); err != nil {
					return false
				}
			}
		}

		dBat.SetRowCount(dBat.Vecs[0].Length())
		return true
	})

	sqlRet.Close()

	if len(sels) == 0 {
		for i := range tBat.Vecs {
			tBat.Vecs[i].CleanOnlyData()
		}
		tBat.SetRowCount(0)
	} else {
		for i := range tBat.Vecs {
			tBat.Vecs[i].Shrink(sels, false)
		}
		tBat.SetRowCount(tBat.Vecs[0].Length())
	}

	return
}

func lcaProbeJoinCastType(typ types.Type) (string, bool) {
	switch typ.Oid {
	case types.T_bit:
		return typ.DescString(), true
	case types.T_int8, types.T_int16, types.T_int32:
		return "INT", true
	case types.T_int64:
		return "BIGINT", true
	case types.T_uint8, types.T_uint16, types.T_uint32:
		return "INT UNSIGNED", true
	case types.T_uint64:
		return "BIGINT UNSIGNED", true
	case types.T_float32:
		return "FLOAT", true
	case types.T_float64:
		return "DOUBLE", true
	case types.T_char, types.T_varchar, types.T_text:
		return "VARCHAR", true
	case types.T_binary, types.T_varbinary:
		return "VARBINARY", true
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		return typ.DescString(), true
	case types.T_date, types.T_datetime, types.T_time, types.T_timestamp:
		return typ.String(), true
	default:
		return "", false
	}
}

func hashDiff(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	dagInfo branchMetaInfo,
	copt compositeOption,
	emit emitFunc,
	tarHandle []engine.ChangesHandle,
	baseHandle []engine.ChangesHandle,
	pickKeyHashmap databranchutils.BranchHashmap,
) (
	err error,
) {
	start := time.Now()
	var (
		buildBaseCost   time.Duration
		buildTargetCost time.Duration
		diffCost        time.Duration
	)

	var (
		baseDataHashmap      databranchutils.BranchHashmap
		baseTombstoneHashmap databranchutils.BranchHashmap

		tarDataHashmap      databranchutils.BranchHashmap
		tarTombstoneHashmap databranchutils.BranchHashmap
	)

	defer func() {
		if baseDataHashmap != nil {
			baseDataHashmap.Close()
		}
		if baseTombstoneHashmap != nil {
			baseTombstoneHashmap.Close()
		}
		if tarDataHashmap != nil {
			tarDataHashmap.Close()
		}
		if tarTombstoneHashmap != nil {
			tarTombstoneHashmap.Close()
		}

		if err != nil {
			logutil.Warn("DataBranch-HashDiff-Error",
				zap.Int("base-handle-cnt", len(baseHandle)),
				zap.Int("target-handle-cnt", len(tarHandle)),
				zap.Bool("has-lca", dagInfo.hasLCA()),
				zap.Duration("build-base-cost", buildBaseCost),
				zap.Duration("build-target-cost", buildTargetCost),
				zap.Duration("diff-cost", diffCost),
				zap.Duration("total-cost", time.Since(start)),
				zap.Error(err),
			)
		}
	}()

	buildBaseStart := time.Now()
	if baseDataHashmap, baseTombstoneHashmap, err = buildHashmapForTable(
		ctx, ses.proc.Mp(), &tblStuff, baseHandle, "base", pickKeyHashmap,
	); err != nil {
		return
	}
	buildBaseCost = time.Since(buildBaseStart)

	buildTargetStart := time.Now()
	if tarDataHashmap, tarTombstoneHashmap, err = buildHashmapForTable(
		ctx, ses.proc.Mp(), &tblStuff, tarHandle, "target", pickKeyHashmap,
	); err != nil {
		return
	}
	buildTargetCost = time.Since(buildTargetStart)

	diffStart := time.Now()
	if !dagInfo.hasLCA() {
		if err = hashDiffIfNoLCA(
			ctx, ses, tblStuff, copt, emit,
			tarDataHashmap, tarTombstoneHashmap,
			baseDataHashmap, baseTombstoneHashmap,
		); err != nil {
			return
		}
	} else {
		if err = hashDiffIfHasLCA(
			ctx, ses, bh, dagInfo, tblStuff, copt, emit,
			tarDataHashmap, tarTombstoneHashmap,
			baseDataHashmap, baseTombstoneHashmap,
		); err != nil {
			return
		}
	}
	diffCost = time.Since(diffStart)

	return
}

func hashDiffIfHasLCA(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	dagInfo branchMetaInfo,
	tblStuff tableStuff,
	copt compositeOption,
	emit emitFunc,
	tarDataHashmap databranchutils.BranchHashmap,
	tarTombstoneHashmap databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
	baseTombstoneHashmap databranchutils.BranchHashmap,
) (err error) {

	// Resolve per-side LCA snapshots once: each side's tombstone
	// resolution probes the LCA at "the moment THIS side forked off
	// the LCA" (or, when the side IS the LCA, at the OTHER side's
	// snapshot — see branchMetaInfo helper docs).
	tarSP, baseSP := tblStuff.resolvedSnapshots(ses)
	tarLCAProbe := dagInfo.tarLCASnapshot(baseSP)
	baseLCAProbe := dagInfo.baseLCASnapshot(tarSP)
	tarLCAVisible := dagInfo.tarLCAVisibleSnapshot(tarSP)
	baseLCAVisible := dagInfo.baseLCAVisibleSnapshot(baseSP)

	if err = pruneUnchangedDataOnLCA(
		ctx, ses, bh, tblStuff, "target", tarLCAProbe,
		baseLCAVisible, tarDataHashmap, tarTombstoneHashmap,
	); err != nil {
		return
	}
	if err = pruneUnchangedDataOnLCA(
		ctx, ses, bh, tblStuff, "base", baseLCAProbe,
		tarLCAVisible, baseDataHashmap, baseTombstoneHashmap,
	); err != nil {
		return
	}

	var (
		wg        sync.WaitGroup
		atomicErr atomic.Value

		baseDeleteBatches []batchWithKind
		baseUpdateBatches []batchWithKind
	)

	handleBaseDeleteAndUpdates := func(wrapped batchWithKind) error {
		wrapped.side = diffSideBase
		if err2 := mergeutil.SortColumnsByIndex(
			wrapped.batch.Vecs, tblStuff.def.pkColIdx, ses.proc.Mp(),
		); err2 != nil {
			return err2
		}
		if wrapped.kind == diffDelete {
			baseDeleteBatches = append(baseDeleteBatches, wrapped)
		} else {
			baseUpdateBatches = append(baseUpdateBatches, wrapped)
		}

		return nil
	}

	handleTarDeleteAndUpdates := func(wrapped batchWithKind) (err2 error) {
		wrapped.side = diffSideTarget
		var pickConflictBat *batch.Batch
		if len(baseUpdateBatches) == 0 && len(baseDeleteBatches) == 0 {
			// no need to check conflict
			if stop, e := emitBatch(emit, wrapped, false, tblStuff.retPool); e != nil {
				return e
			} else if stop {
				return nil
			}
			return nil
		}

		if err2 = mergeutil.SortColumnsByIndex(
			wrapped.batch.Vecs, tblStuff.def.pkColIdx, ses.proc.Mp(),
		); err2 != nil {
			return err2
		}

		appendPickConflict := func(baseWrapped batchWithKind, rowIdx int) error {
			if !copt.preservePickConflicts || copt.conflictOpt == nil ||
				copt.conflictOpt.Opt != tree.CONFLICT_ACCEPT {
				return nil
			}
			if pickConflictBat == nil {
				pickConflictBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
			}
			return pickConflictBat.UnionOne(baseWrapped.batch, int64(rowIdx), ses.proc.Mp())
		}

		checkConflict := func(tarWrapped, baseWrapped batchWithKind) (sels1, sels2 []int64, err3 error) {
			var (
				cmp     int
				tarVec  = tarWrapped.batch.Vecs[tblStuff.def.pkColIdx]
				baseVec = baseWrapped.batch.Vecs[tblStuff.def.pkColIdx]
			)

			i, j := 0, 0
			for i < tarVec.Length() && j < baseVec.Length() {
				if cmp, err3 = compareSingleValInVector(
					ctx, ses, i, j, tarVec, baseVec,
				); err3 != nil {
					return
				}

				if cmp == 0 {
					// conflict
					// tar and base both deleted on pk1 => empty
					// tar and base both updated on pk1 => we need compare the left columns, consider
					// the conflict option.
					if copt.conflictOpt == nil {
						if tarWrapped.kind == baseWrapped.kind {
							// compare the left column values
							if cmp, err3 = compareRowInWrappedBatches(
								ctx, ses, tblStuff, i, j, true,
								tarWrapped, baseWrapped,
							); err3 != nil {
								return
							} else if cmp == 0 {
								sels1 = append(sels1, int64(i))
								sels2 = append(sels2, int64(j))
							}
						}
						i++
						j++
					} else if copt.conflictOpt.Opt == tree.CONFLICT_ACCEPT {
						if tarWrapped.kind == diffDelete &&
							baseWrapped.kind == diffDelete &&
							!tarWrapped.fromUpdate && !baseWrapped.fromUpdate {
							if cmp, err3 = compareRowInWrappedBatches(
								ctx, ses, tblStuff, i, j, true,
								tarWrapped, baseWrapped,
							); err3 != nil {
								return
							} else if cmp == 0 {
								sels1 = append(sels1, int64(i))
								sels2 = append(sels2, int64(j))
								i++
								j++
								continue
							}
						}
						if err3 = appendPickConflict(baseWrapped, j); err3 != nil {
							return
						}
						// only keep the rows from tar
						sels2 = append(sels2, int64(j))
						i++
						j++
					} else if copt.conflictOpt.Opt == tree.CONFLICT_SKIP {
						sels1 = append(sels1, int64(i))
						sels2 = append(sels2, int64(j))
						i++
						j++
					} else {
						// copt.conflictOpt == tree.CONFLICT_FAIL
						tarRow := make([]any, 1)
						if err3 = extractRowFromVector(
							ctx, ses, tarVec, 0, tarRow, i, false,
						); err3 != nil {
							return
						}

						buf := acquireBuffer(tblStuff.bufPool)
						if err3 = formatValIntoString(
							ses, tarRow[0], tblStuff.def.colTypes[tblStuff.def.pkColIdx], buf,
						); err3 != nil {
							releaseBuffer(tblStuff.bufPool, buf)
							return
						}

						err3 = moerr.NewInternalErrorNoCtxf(
							"conflict: %s %s and %s %s on pk(%v) with different values",
							tarWrapped.name, tarWrapped.kind,
							baseWrapped.name, baseWrapped.kind, buf.String(),
						)
						releaseBuffer(tblStuff.bufPool, buf)
						return
					}
				} else if cmp < 0 {
					// tarVal < baseVal
					i++
				} else {
					// tarVal > baseVal
					j++
				}
			}

			return
		}

		foo := func(baseWrappedList []batchWithKind) (err3 error) {
			for _, baseWrapped := range baseWrappedList {
				var (
					sels1 []int64
					sels2 []int64
				)
				if sels1, sels2, err3 = checkConflict(wrapped, baseWrapped); err3 != nil {
					return
				} else {
					if len(sels1) != 0 {
						wrapped.batch.Shrink(sels1, true)
					}

					if len(sels2) != 0 {
						baseWrapped.batch.Shrink(sels2, true)
					}
				}

				if wrapped.batch.RowCount() == 0 {
					break
				}
			}

			return
		}

		if err2 = foo(baseDeleteBatches); err2 != nil {
			return
		}

		baseDeleteBatches = plan2.RemoveIf(baseDeleteBatches, func(t batchWithKind) bool {
			if t.batch.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(t.batch, false)
				return true
			}
			return false
		})

		if err2 = foo(baseUpdateBatches); err2 != nil {
			return
		}

		baseUpdateBatches = plan2.RemoveIf(baseUpdateBatches, func(t batchWithKind) bool {
			if t.batch.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(t.batch, false)
				return true
			}
			return false
		})

		if wrapped.batch.RowCount() == 0 {
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			return
		}

		if pickConflictBat != nil {
			if stop, e := emitBatch(emit, batchWithKind{
				batch: pickConflictBat,
				kind:  diffDelete,
				name:  tblStuff.baseRel.GetTableName(),
				side:  diffSideBase,
			}, false, tblStuff.retPool); e != nil {
				return e
			} else if stop {
				return nil
			}
		}

		stop, e := emitBatch(emit, wrapped, false, tblStuff.retPool)
		if e != nil {
			return e
		}
		if stop {
			return nil
		}

		return
	}

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	asyncDelsAndUpdatesHandler := func(forBase bool, tmpCh chan batchWithKind) (err2 error) {
		var (
			branchTS = baseLCAProbe
			hashmap1 = baseDataHashmap
			hashmap2 = baseTombstoneHashmap
			name     = tblStuff.baseRel.GetTableName()
		)

		if !forBase {
			branchTS = tarLCAProbe
			hashmap1 = tarDataHashmap
			hashmap2 = tarTombstoneHashmap
			name = tblStuff.tarRel.GetTableName()
		}

		wg.Add(1)
		if err2 = tblStuff.worker.Submit(func() {
			defer func() {
				close(tmpCh)
				tmpCh = nil
				wg.Done()
			}()

			if err3 := findDeleteAndUpdateBat(
				newCtx, ses, bh, tblStuff, name,
				func() int {
					if forBase {
						return diffSideBase
					}
					return diffSideTarget
				}(),
				tmpCh, branchTS, copt.expandUpdate, hashmap1, hashmap2,
			); err3 != nil {
				atomicErr.Store(err3)
			}
		}); err2 != nil {
			wg.Done()
			return
		}
		return nil
	}

	stepHandler := func(forBase bool) (err2 error) {
		var (
			tmpCh = make(chan batchWithKind, 1)
			first error
		)

		if err2 = asyncDelsAndUpdatesHandler(forBase, tmpCh); err2 != nil {
			return err2
		}

		for wrapped := range tmpCh {
			if first != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			select {
			case <-ctx.Done():
				first = ctx.Err()
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			default:
				if forBase {
					err2 = handleBaseDeleteAndUpdates(wrapped)
				} else {
					err2 = handleTarDeleteAndUpdates(wrapped)
				}

				if errors.Is(err2, context.Canceled) {
					err2 = nil
					cancel()
				}
			}

			if err2 != nil {
				first = err2
				cancel()
			}
		}

		wg.Wait()
		if first != nil {
			return first
		}

		if atomicErr.Load() != nil {
			return atomicErr.Load().(error)
		}

		return first
	}

	// phase 1: handle base dels and updates on lca
	if err = stepHandler(true); err != nil {
		return
	}

	// phase2: handle tar dels and updates on lca
	if err = stepHandler(false); err != nil {
		return
	}

	// what can I do with these left base updates/inserts ?
	if copt.conflictOpt == nil {
		stopped := false
		for i, w := range baseUpdateBatches {
			var stop bool
			if stop, err = emitBatch(emit, w, false, tblStuff.retPool); err != nil {
				return err
			}
			if stop {
				stopped = true
				for j := i + 1; j < len(baseUpdateBatches); j++ {
					tblStuff.retPool.releaseRetBatch(baseUpdateBatches[j].batch, false)
				}
				for _, bw := range baseDeleteBatches {
					tblStuff.retPool.releaseRetBatch(bw.batch, false)
				}
				break
			}
		}

		if !stopped {
			for i, w := range baseDeleteBatches {
				var stop bool
				if stop, err = emitBatch(emit, w, false, tblStuff.retPool); err != nil {
					return err
				}
				if stop {
					for j := i + 1; j < len(baseDeleteBatches); j++ {
						tblStuff.retPool.releaseRetBatch(baseDeleteBatches[j].batch, false)
					}
					break
				}
			}
		}
	}

	return diffDataHelper(ctx, ses, copt, tblStuff, emit, tarDataHashmap, baseDataHashmap)
}

// pruneUnchangedDataOnLCA removes rows that were physically collected as data
// because range replay had to select a whole LCA-derived object, for example a
// rewritten object without per-row commit-ts, while their visible values are
// unchanged at both LCA observation snapshots. Tombstones are excluded per key;
// item counts are not a valid substitute for set containment.
func pruneUnchangedDataOnLCA(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	side string,
	branchTS types.TS,
	commonPrefixEnd types.TS,
	dataHashmap databranchutils.BranchHashmap,
	tombstoneHashmap databranchutils.BranchHashmap,
) error {
	if dataHashmap == nil || tombstoneHashmap == nil ||
		dataHashmap.ItemCount() == 0 {
		return nil
	}

	rowsToRemove := make([][]byte, 0)
	var removeMu sync.Mutex
	maxBatchCnt := tblStuff.maxTombstoneBatchCnt
	if maxBatchCnt <= 0 {
		maxBatchCnt = maxSqlBatchCnt
	}

	if err := dataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		probeBat := tblStuff.retPool.acquireRetBatch(tblStuff, true)
		candidateRows := make([][]byte, 0, maxBatchCnt)
		candidateKeys := make([][]byte, 0, maxBatchCnt)
		defer func() {
			if probeBat != nil {
				tblStuff.retPool.releaseRetBatch(probeBat, true)
			}
		}()

		processBatch := func() error {
			if probeBat == nil || probeBat.RowCount() == 0 {
				return nil
			}
			lcaBat, hitIdxes, err2 := handleDelsOnLCA(
				ctx, ses, bh, probeBat, tblStuff, branchTS.ToTimestamp(),
			)
			if err2 != nil {
				if lcaBat != nil {
					tblStuff.retPool.releaseRetBatch(lcaBat, false)
				}
				return err2
			}
			defer func() {
				if lcaBat != nil {
					tblStuff.retPool.releaseRetBatch(lcaBat, false)
				}
			}()

			commonProbeBat := tblStuff.retPool.acquireRetBatch(tblStuff, true)
			commonRows := make([][]byte, 0, len(hitIdxes))
			defer func() {
				if commonProbeBat != nil {
					tblStuff.retPool.releaseRetBatch(commonProbeBat, true)
				}
			}()

			for lcaRowIdx, candidateIdx := range hitIdxes {
				if candidateIdx < 0 || int(candidateIdx) >= len(candidateRows) {
					return moerr.NewInternalErrorNoCtxf(
						"data branch prune: candidate index %d out of range %d",
						candidateIdx, len(candidateRows),
					)
				}
				dataTuple, _, err3 := dataHashmap.DecodeRow(candidateRows[candidateIdx])
				if err3 != nil {
					return err3
				}
				equal, err3 := visibleTupleEqualBatchRow(dataTuple, lcaBat, lcaRowIdx, tblStuff)
				if err3 != nil {
					return err3
				}
				if equal {
					if err3 = appendPruneProbeRow(
						ses, tblStuff, commonProbeBat,
						candidateKeys[candidateIdx], dataTuple,
					); err3 != nil {
						return err3
					}
					commonRows = append(commonRows, candidateRows[candidateIdx])
				}
			}

			if commonProbeBat.RowCount() > 0 {
				commonBat, commonHitIdxes, err3 := handleDelsOnLCA(
					ctx, ses, bh, commonProbeBat, tblStuff, commonPrefixEnd.ToTimestamp(),
				)
				if err3 != nil {
					if commonBat != nil {
						tblStuff.retPool.releaseRetBatch(commonBat, false)
					}
					return err3
				}
				defer func() {
					if commonBat != nil {
						tblStuff.retPool.releaseRetBatch(commonBat, false)
					}
				}()
				for commonRowIdx, commonCandidateIdx := range commonHitIdxes {
					if commonCandidateIdx < 0 || int(commonCandidateIdx) >= len(commonRows) {
						return moerr.NewInternalErrorNoCtxf(
							"data branch prune: common candidate index %d out of range %d",
							commonCandidateIdx, len(commonRows),
						)
					}
					dataTuple, _, err3 := dataHashmap.DecodeRow(commonRows[commonCandidateIdx])
					if err3 != nil {
						return err3
					}
					equal, err3 := visibleTupleEqualBatchRow(dataTuple, commonBat, commonRowIdx, tblStuff)
					if err3 != nil {
						return err3
					}
					if equal {
						removeMu.Lock()
						rowsToRemove = append(rowsToRemove, append([]byte(nil), commonRows[commonCandidateIdx]...))
						removeMu.Unlock()
					}
				}
			}

			tblStuff.retPool.releaseRetBatch(probeBat, true)
			probeBat = tblStuff.retPool.acquireRetBatch(tblStuff, true)
			candidateRows = candidateRows[:0]
			candidateKeys = candidateKeys[:0]
			return nil
		}

		if err := cursor.ForEach(func(key []byte, row []byte) error {
			tombRet, err2 := tombstoneHashmap.GetByEncodedKey(key)
			if err2 != nil {
				return err2
			}
			if tombRet.Exists {
				return nil
			}

			dataTuple, _, err2 := dataHashmap.DecodeRow(row)
			if err2 != nil {
				return err2
			}
			if err2 = appendPruneProbeRow(ses, tblStuff, probeBat, key, dataTuple); err2 != nil {
				return err2
			}
			candidateRows = append(candidateRows, append([]byte(nil), row...))
			candidateKeys = append(candidateKeys, append([]byte(nil), key...))
			if probeBat.RowCount() >= maxBatchCnt {
				return processBatch()
			}
			return nil
		}); err != nil {
			return err
		}
		return processBatch()
	}, -1); err != nil {
		return err
	}

	removed := 0
	for _, row := range rowsToRemove {
		n, err2 := dataHashmap.PopByEncodedFullValueExact(row, false)
		if err2 != nil {
			return err2
		}
		removed += n
	}
	if removed > 0 {
		logutil.Debug(
			"DataBranch-PruneUnchanged-LCA-Done",
			zap.String("side", side),
			zap.Uint64("lca-table-id", tblStuff.lcaRel.GetTableID(ctx)),
			zap.String("snapshot-ts", branchTS.ToString()),
			zap.String("common-prefix-end", commonPrefixEnd.ToString()),
			zap.Int("removed-rows", removed),
			zap.Int64("remaining-data-items", dataHashmap.ItemCount()),
		)
	}
	return nil
}

func visibleTupleEqualBatchRow(tuple types.Tuple, bat *batch.Batch, rowIdx int, tblStuff tableStuff) (bool, error) {
	for _, colIdx := range tblStuff.def.visibleIdxes {
		val, err := getTupleColumnValue(tuple, tblStuff, colIdx)
		if err != nil {
			return false, err
		}
		if !tupleValueEqualVector(val, bat.Vecs[colIdx], rowIdx) {
			return false, nil
		}
	}
	return true, nil
}

func appendPruneProbeRow(
	ses *Session,
	tblStuff tableStuff,
	bat *batch.Batch,
	key []byte,
	dataTuple types.Tuple,
) error {
	pkVal, err := getTupleColumnValue(dataTuple, tblStuff, tblStuff.def.pkColIdx)
	if err != nil {
		return err
	}
	if err = appendTupleValueToVector(bat.Vecs[0], pkVal, ses.proc.Mp()); err != nil {
		return err
	}
	if err = vector.AppendFixed(bat.Vecs[1], types.Rowid{}, false, ses.proc.Mp()); err != nil {
		return err
	}
	if err = vector.AppendBytes(bat.Vecs[2], append([]byte(nil), key...), false, ses.proc.Mp()); err != nil {
		return err
	}
	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
}

func tupleValueEqualVector(val any, vec *vector.Vector, rowIdx int) bool {
	if val == nil || vec.IsNull(uint64(rowIdx)) {
		return val == nil && vec.IsNull(uint64(rowIdx))
	}
	vecVal := vector.GetAny(vec, rowIdx, true)
	valBytes, valOK := val.([]byte)
	vecBytes, vecOK := vecVal.([]byte)
	if valOK || vecOK {
		return valOK && vecOK && bytes.Equal(valBytes, vecBytes)
	}
	return reflect.DeepEqual(val, vecVal)
}

func hashDiffIfNoLCA(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	copt compositeOption,
	emit emitFunc,
	tarDataHashmap databranchutils.BranchHashmap,
	tarTombstoneHashmap databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
	baseTombstoneHashmap databranchutils.BranchHashmap,
) (err error) {

	if err = tarTombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		return cursor.ForEach(func(key []byte, _ []byte) error {
			_, err2 := tarDataHashmap.PopByEncodedKey(key, true)
			return err2
		})

	}, -1); err != nil {
		return
	}

	if err = baseTombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		return cursor.ForEach(func(key []byte, _ []byte) error {
			_, err2 := baseDataHashmap.PopByEncodedKey(key, true)
			return err2
		})

	}, -1); err != nil {
		return
	}

	return diffDataHelper(ctx, ses, copt, tblStuff, emit, tarDataHashmap, baseDataHashmap)
}

func compareRowInWrappedBatches(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	rowIdx1 int,
	rowIdx2 int,
	skipPKCols bool,
	wrapped1 batchWithKind,
	wrapped2 batchWithKind,
) (int, error) {
	if wrapped1.kind == wrapped2.kind && wrapped1.kind == diffDelete && skipPKCols {
		return 0, nil
	}

	for i, colIdx := range tblStuff.def.visibleIdxes {
		if skipPKCols {
			if slices.Index(tblStuff.def.pkColIdxes, colIdx) != -1 {
				continue
			}
		}

		var (
			vec1 = wrapped1.batch.Vecs[i]
			vec2 = wrapped2.batch.Vecs[i]
		)

		if cmp, err := compareSingleValInVector(
			ctx, ses, rowIdx1, rowIdx2, vec1, vec2,
		); err != nil {
			return 0, err
		} else if cmp != 0 {
			return cmp, nil
		}

	}

	return 0, nil
}

func findDeleteAndUpdateBat(
	ctx context.Context, ses *Session, bh BackgroundExec,
	tblStuff tableStuff, tblName string, side int, tmpCh chan batchWithKind, branchTS types.TS,
	expandUpdate bool,
	dataHashmap, tombstoneHashmap databranchutils.BranchHashmap,
) (err error) {

	maxTombstoneBatchCnt := tblStuff.maxTombstoneBatchCnt
	if maxTombstoneBatchCnt <= 0 {
		maxTombstoneBatchCnt = maxSqlBatchCnt
	}

	if err = tombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2  error
			tuple types.Tuple
			tBat1 = tblStuff.retPool.acquireRetBatch(tblStuff, true)
		)

		send := func(bwk batchWithKind) error {
			select {
			case <-ctx.Done():
				tblStuff.retPool.releaseRetBatch(bwk.batch, false)
				return ctx.Err()
			case tmpCh <- bwk:
				return nil
			}
		}

		processBatch := func(tombBat *batch.Batch) error {
			if tombBat.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(tombBat, true)
				return nil
			}

			dBat, _, err2 := handleDelsOnLCA(
				ctx, ses, bh, tombBat, tblStuff, branchTS.ToTimestamp(),
			)
			if err2 != nil {
				tblStuff.retPool.releaseRetBatch(tombBat, true)
				return err2
			}

			// merge inserts and deletes on the tar
			// this deletes is not on the lca
			if tombBat.RowCount() > 0 {
				removedLiveRows := 0
				tombRowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](tombBat.Vecs[1])
				for i := 0; i < tombBat.RowCount(); i++ {
					keyBytes := tombBat.Vecs[2].GetBytesAt(i)
					var dataRet databranchutils.GetResult
					if dataRet, err2 = dataHashmap.GetByEncodedKey(keyBytes); err2 != nil {
						tblStuff.retPool.releaseRetBatch(tombBat, true)
						tblStuff.retPool.releaseRetBatch(dBat, false)
						return err2
					}
					if !dataRet.Exists {
						continue
					}
					var matchedRow []byte
					for _, row := range dataRet.Rows {
						var liveTuple types.Tuple
						if liveTuple, _, err2 = dataHashmap.DecodeRow(row); err2 != nil {
							tblStuff.retPool.releaseRetBatch(tombBat, true)
							tblStuff.retPool.releaseRetBatch(dBat, false)
							return err2
						}
						liveRowIDBytes, ok := liveTuple[0].([]uint8)
						if !ok {
							continue
						}
						liveRowID := types.DecodeFixed[types.Rowid](liveRowIDBytes)
						if !liveRowID.EQ(&tombRowIDs[i]) {
							continue
						}
						matchedRow = row
						break
					}
					if matchedRow == nil {
						continue
					}
					var removed int
					if removed, err2 = dataHashmap.PopByEncodedKeyValue(keyBytes, matchedRow, false); err2 != nil {
						tblStuff.retPool.releaseRetBatch(tombBat, true)
						tblStuff.retPool.releaseRetBatch(dBat, false)
						return err2
					}
					removedLiveRows += removed
				}
				tblStuff.retPool.releaseRetBatch(tombBat, true)
			}

			// find update
			if dBat.RowCount() > 0 {
				pkVecLen := dBat.Vecs[tblStuff.def.pkColIdx].Length()
				if pkVecLen != dBat.RowCount() {
					logutil.Error(
						"DataBranch-findDeleteAndUpdateBat-VecLenMismatch",
						zap.String("table-name", tblName),
						zap.Int("dBat-row-count", dBat.RowCount()),
						zap.Int("pkVec-length", pkVecLen),
						zap.Int("pkColIdx", tblStuff.def.pkColIdx),
						zap.Int("vec-count", dBat.VectorCount()),
					)
				}
				tBat2 := tblStuff.retPool.acquireRetBatch(tblStuff, false)
				seen := make([]bool, dBat.RowCount())
				var updateBat *batch.Batch
				var updateDeleteBat *batch.Batch
				if _, err2 = dataHashmap.PopByVectorsStream(
					[]*vector.Vector{dBat.Vecs[tblStuff.def.pkColIdx]}, false,
					func(idx int, _ []byte, row []byte) error {
						seen[idx] = true
						// delete on lca and insert into tar ==> update
						if updateBat == nil {
							updateBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
						}
						if expandUpdate && updateDeleteBat == nil {
							updateDeleteBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
						}

						if tuple, _, err2 = dataHashmap.DecodeRow(row); err2 != nil {
							return err2
						}

						if expandUpdate {
							if err2 = updateDeleteBat.UnionOne(dBat, int64(idx), ses.proc.Mp()); err2 != nil {
								return err2
							}
						}

						if err2 = appendTupleToBat(ses, updateBat, tuple, tblStuff); err2 != nil {
							return err2
						}
						return nil
					},
				); err2 != nil {
					tblStuff.retPool.releaseRetBatch(dBat, false)
					tblStuff.retPool.releaseRetBatch(tBat2, false)
					if updateBat != nil {
						tblStuff.retPool.releaseRetBatch(updateBat, false)
					}
					if updateDeleteBat != nil {
						tblStuff.retPool.releaseRetBatch(updateDeleteBat, false)
					}
					return err2
				}

				for i := 0; i < dBat.RowCount(); i++ {
					if seen[i] {
						continue
					}
					// delete on lca
					if err2 = tBat2.UnionOne(dBat, int64(i), ses.proc.Mp()); err2 != nil {
						tblStuff.retPool.releaseRetBatch(dBat, false)
						tblStuff.retPool.releaseRetBatch(tBat2, false)
						if updateBat != nil {
							tblStuff.retPool.releaseRetBatch(updateBat, false)
						}
						if updateDeleteBat != nil {
							tblStuff.retPool.releaseRetBatch(updateDeleteBat, false)
						}
						return err2
					}
				}

				tblStuff.retPool.releaseRetBatch(dBat, false)
				tBat2.SetRowCount(tBat2.Vecs[0].Length())

				if updateBat != nil {
					updateBat.SetRowCount(updateBat.Vecs[0].Length())
					if expandUpdate {
						if updateDeleteBat != nil {
							updateDeleteBat.SetRowCount(updateDeleteBat.Vecs[0].Length())
							if err2 = send(batchWithKind{
								name:       tblName,
								side:       side,
								fromUpdate: true,
								batch:      updateDeleteBat,
								kind:       diffDelete,
							}); err2 != nil {
								return err2
							}
						}
						if err2 = send(batchWithKind{
							name:  tblName,
							side:  side,
							batch: updateBat,
							kind:  diffInsert,
						}); err2 != nil {
							return err2
						}
					} else {
						if err2 = send(batchWithKind{
							name:  tblName,
							side:  side,
							batch: updateBat,
							kind:  diffUpdate,
						}); err2 != nil {
							return err2
						}
					}
				}

				if err2 = send(batchWithKind{
					name:  tblName,
					side:  side,
					batch: tBat2,
					kind:  diffDelete,
				}); err2 != nil {
					return err2
				}
				return nil
			}

			tblStuff.retPool.releaseRetBatch(dBat, false)
			return nil
		}

		if err2 = cursor.ForEach(func(key []byte, row []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if tuple, _, err2 = tombstoneHashmap.DecodeRow(key); err2 != nil {
				return err2
			}
			if err2 = vector.AppendAny(tBat1.Vecs[0], tuple[0], false, ses.proc.Mp()); err2 != nil {
				return err2
			}
			var tombTuple types.Tuple
			if tombTuple, _, err2 = tombstoneHashmap.DecodeRow(row); err2 != nil {
				return err2
			}
			rowIDBytes, ok := tombTuple[0].([]uint8)
			if !ok {
				return moerr.NewInternalErrorNoCtx("tombstone row missing rowid in hashmap payload")
			}
			rowID := types.DecodeFixed[types.Rowid](rowIDBytes)
			if err2 = vector.AppendFixed(tBat1.Vecs[1], rowID, false, ses.proc.Mp()); err2 != nil {
				return err2
			}
			if err2 = vector.AppendBytes(tBat1.Vecs[2], append([]byte(nil), key...), false, ses.proc.Mp()); err2 != nil {
				return err2
			}

			tBat1.SetRowCount(tBat1.Vecs[0].Length())
			if tBat1.RowCount() >= maxTombstoneBatchCnt {
				tombBat := tBat1
				tBat1 = nil
				if err2 = processBatch(tombBat); err2 != nil {
					return err2
				}
				tBat1 = tblStuff.retPool.acquireRetBatch(tblStuff, true)
			}
			return nil
		}); err2 != nil {
			if tBat1 != nil {
				tblStuff.retPool.releaseRetBatch(tBat1, true)
			}
			return err2
		}

		if tBat1 != nil {
			return processBatch(tBat1)
		}
		return nil

	}, -1); err != nil {
		return
	}

	return nil
}

func appendTupleToBat(ses *Session, bat *batch.Batch, tuple types.Tuple, tblStuff tableStuff) error {
	if bat.VectorCount() != len(tblStuff.def.colNames) {
		return moerr.NewInternalErrorNoCtxf(
			"unexpected batch width %d for table %s with %d physical columns",
			bat.VectorCount(), tblStuff.tarRel.GetTableName(), len(tblStuff.def.colNames),
		)
	}
	for colIdx := range tblStuff.def.colNames {
		val, err := getTupleColumnValue(tuple, tblStuff, colIdx)
		if err != nil {
			return err
		}
		vec := bat.Vecs[colIdx]
		if err := appendTupleValueToVector(vec, val, ses.proc.Mp()); err != nil {
			return err
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())

	return nil
}

func getTupleColumnValue(tuple types.Tuple, tblStuff tableStuff, colIdx int) (any, error) {
	totalColCnt := len(tblStuff.def.colTypes)
	if colIdx < 0 || colIdx >= totalColCnt {
		return nil, moerr.NewInternalErrorNoCtxf(
			"column index %d out of range for table %s with %d columns",
			colIdx, tblStuff.tarRel.GetTableName(), totalColCnt,
		)
	}
	switch len(tuple) {
	case totalColCnt, totalColCnt + 1:
		return tuple[colIdx], nil
	case totalColCnt + 2:
		return tuple[colIdx+1], nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf(
			"unexpected tuple width %d for table %s with %d visible columns",
			len(tuple), tblStuff.tarRel.GetTableName(), len(tblStuff.def.visibleIdxes),
		)
	}
}

func visibleTupleKeyIdxes(tblStuff tableStuff) []int {
	idxes := make([]int, len(tblStuff.def.visibleIdxes))
	for i, colIdx := range tblStuff.def.visibleIdxes {
		idxes[i] = colIdx + 1
	}
	return idxes
}

func batchSampleRowsForLog(bat *batch.Batch, limit int) []string {
	if bat == nil || bat.RowCount() == 0 || limit <= 0 {
		return nil
	}
	if limit > bat.RowCount() {
		limit = bat.RowCount()
	}
	rows := make([]string, 0, limit)
	for rowIdx := 0; rowIdx < limit; rowIdx++ {
		cols := make([]string, 0, bat.VectorCount())
		for _, vec := range bat.Vecs {
			if vec == nil {
				cols = append(cols, "<nil>")
				continue
			}
			if rowIdx >= vec.Length() {
				cols = append(cols, "<oob>")
				continue
			}
			if vec.GetNulls().Contains(uint64(rowIdx)) {
				cols = append(cols, "NULL")
				continue
			}
			cols = append(cols, fmt.Sprintf("%v", types.DecodeValue(vec.GetRawBytesAt(rowIdx), vec.GetType().Oid)))
		}
		rows = append(rows, strings.Join(cols, ", "))
	}
	return rows
}

func validateLeadingRowID(side, tableName string, isTombstone bool, bat *batch.Batch) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	buildFields := func() []zap.Field {
		return []zap.Field{
			zap.String("side", side),
			zap.String("table-name", tableName),
			zap.Bool("tombstone", isTombstone),
			zap.Int("row-cnt", bat.RowCount()),
			zap.Int("vec-cnt", bat.VectorCount()),
			zap.Strings("attrs", append([]string(nil), bat.Attrs...)),
			zap.Strings("samples", batchSampleRowsForLog(bat, 4)),
		}
	}
	fail := func(msg string, extra ...zap.Field) error {
		logutil.Error(msg, append(buildFields(), extra...)...)
		return moerr.NewInternalErrorNoCtx(msg)
	}
	if bat.VectorCount() == 0 || bat.Vecs[0] == nil {
		return fail("DataBranch-CollectChanges-MissingRowID")
	}
	rowIDVec := bat.Vecs[0]
	if rowIDVec.GetType().Oid != types.T_Rowid {
		return fail("DataBranch-CollectChanges-InvalidRowIDVector",
			zap.String("rowid-vec-type", rowIDVec.GetType().String()),
		)
	}
	if rowIDVec.Length() != bat.RowCount() {
		return fail("DataBranch-CollectChanges-RowIDLenMismatch",
			zap.Int("rowid-vec-len", rowIDVec.Length()),
		)
	}
	rowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](rowIDVec)
	for i := range rowIDs {
		if rowIDVec.GetNulls().Contains(uint64(i)) {
			return fail("DataBranch-CollectChanges-NullRowID", zap.Int("row-idx", i))
		}
		if rowIDs[i].EQ(&types.EmptyRowid) || rowIDs[i].BorrowBlockID().IsEmpty() {
			return fail("DataBranch-CollectChanges-InvalidRowID",
				zap.Int("row-idx", i),
				zap.String("rowid", rowIDs[i].String()),
			)
		}
	}
	return nil
}

func appendTupleValueToVector(vec *vector.Vector, val any, mp *mpool.MPool) error {
	if val == nil {
		return vector.AppendAny(vec, nil, true, mp)
	}
	if raw, ok := val.([]byte); ok {
		if !vec.GetType().IsVarlen() {
			return moerr.NewInternalErrorNoCtxf(
				"unexpected byte slice for fixed-width column type %s",
				vec.GetType().String(),
			)
		}
		return vector.AppendBytes(vec, raw, false, mp)
	}
	return vector.AppendAny(vec, val, false, mp)
}

func checkConflictAndAppendToBat(
	ses *Session, copt compositeOption, tblStuff tableStuff,
	tarBat, baseBat *batch.Batch, tarTuple, baseTuple types.Tuple,
) (err2 error) {
	if copt.conflictOpt != nil {
		switch copt.conflictOpt.Opt {
		case tree.CONFLICT_FAIL:
			buf := acquireBuffer(tblStuff.bufPool)
			for i, idx := range tblStuff.def.pkColIdxes {
				var val any
				if val, err2 = getTupleColumnValue(tarTuple, tblStuff, idx); err2 != nil {
					releaseBuffer(tblStuff.bufPool, buf)
					return err2
				}
				if err2 = formatValIntoString(ses, val, tblStuff.def.colTypes[idx], buf); err2 != nil {
					releaseBuffer(tblStuff.bufPool, buf)
					return err2
				}
				if i < len(tblStuff.def.pkColIdxes)-1 {
					buf.WriteString(",")
				}
			}

			msg := buf.String()
			releaseBuffer(tblStuff.bufPool, buf)
			return moerr.NewInternalErrorNoCtxf(
				"conflict: %s %s and %s %s on pk(%v) with different values",
				tblStuff.tarRel.GetTableName(), diffInsert,
				tblStuff.baseRel.GetTableName(), diffInsert,
				msg,
			)
		case tree.CONFLICT_SKIP:
			return
		case tree.CONFLICT_ACCEPT:
			// accept the tar's insert
			return appendTupleToBat(ses, tarBat, tarTuple, tblStuff)
		}
	} else {
		if err2 = appendTupleToBat(ses, tarBat, tarTuple, tblStuff); err2 != nil {
			return err2
		}
		if err2 = appendTupleToBat(ses, baseBat, baseTuple, tblStuff); err2 != nil {
			return err2
		}
	}
	return
}

func diffDataHelper(
	ctx context.Context,
	ses *Session,
	copt compositeOption,
	tblStuff tableStuff,
	emit emitFunc,
	tarDataHashmap databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
) (err error) {

	// if no pk, we cannot use the fake pk to probe.
	// must probe with full columns

	if tblStuff.def.pkKind == fakeKind {
		var (
			keyIdxes   = visibleTupleKeyIdxes(tblStuff)
			newHashmap databranchutils.BranchHashmap
		)

		if newHashmap, err = baseDataHashmap.Migrate(keyIdxes, -1); err != nil {
			return err
		}
		if err = baseDataHashmap.Close(); err != nil {
			return err
		}
		baseDataHashmap = newHashmap

		if newHashmap, err = tarDataHashmap.Migrate(keyIdxes, -1); err != nil {
			return err
		}
		if err = tarDataHashmap.Close(); err != nil {
			return err
		}
		tarDataHashmap = newHashmap
	}

	if err = tarDataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2          error
			tarBat        *batch.Batch
			baseBat       *batch.Batch
			baseDeleteBat *batch.Batch
			tarTuple      types.Tuple
			baseTuple     types.Tuple
			checkRet      databranchutils.GetResult
		)

		tarBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
		baseBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)

		if err2 = cursor.ForEach(func(key []byte, row []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if tblStuff.def.pkKind == fakeKind {
				if checkRet, err2 = baseDataHashmap.PopByEncodedKey(key, false); err2 != nil {
					return err2
				}
			} else {
				if checkRet, err2 = baseDataHashmap.PopByEncodedKey(key, false); err2 != nil {
					return err2
				}
			}

			if !checkRet.Exists {
				if tarTuple, _, err2 = tarDataHashmap.DecodeRow(row); err2 != nil {
					return err2
				}

				if err2 = appendTupleToBat(ses, tarBat, tarTuple, tblStuff); err2 != nil {
					return err2
				}

			} else {
				// both has the key, we continue compare the left columns,
				// if all columns are equal, exactly the same row, ignore.
				if tblStuff.def.pkKind == fakeKind {
					// all columns already compared.
					// ignore
				} else {
					if tarTuple, _, err2 = tarDataHashmap.DecodeRow(row); err2 != nil {
						return err2
					}

					if baseTuple, _, err2 = baseDataHashmap.DecodeRow(checkRet.Rows[0]); err2 != nil {
						return err2
					}

					notSame := false
					for _, idx := range tblStuff.def.visibleIdxes {
						if slices.Index(tblStuff.def.pkColIdxes, idx) != -1 {
							// pk columns already compared
							continue
						}
						var tarVal, baseVal any
						if tarVal, err2 = getTupleColumnValue(tarTuple, tblStuff, idx); err2 != nil {
							return err2
						}
						if baseVal, err2 = getTupleColumnValue(baseTuple, tblStuff, idx); err2 != nil {
							return err2
						}

						if types.CompareValue(
							tarVal, baseVal,
						) != 0 {
							notSame = true
							break
						}
					}

					if notSame {
						if copt.conflictOpt != nil &&
							copt.conflictOpt.Opt == tree.CONFLICT_ACCEPT &&
							copt.expandUpdate {
							if baseDeleteBat == nil {
								baseDeleteBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
							}
							if err2 = appendTupleToBat(ses, baseDeleteBat, baseTuple, tblStuff); err2 != nil {
								return err2
							}
							if err2 = appendTupleToBat(ses, tarBat, tarTuple, tblStuff); err2 != nil {
								return err2
							}
						} else {
							// conflict happened
							if err2 = checkConflictAndAppendToBat(
								ses, copt, tblStuff, tarBat, baseBat, tarTuple, baseTuple,
							); err2 != nil {
								return err2
							}
						}
					}
				}
			}
			return nil
		}); err2 != nil {
			return err2
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if baseDeleteBat != nil {
			if stop, err3 := emitBatch(emit, batchWithKind{
				batch: baseDeleteBat,
				kind:  diffDelete,
				name:  tblStuff.baseRel.GetTableName(),
				side:  diffSideBase,
			}, false, tblStuff.retPool); err3 != nil {
				return err3
			} else if stop {
				return nil
			}
		}

		if stop, err3 := emitBatch(emit, batchWithKind{
			batch: tarBat,
			kind:  diffInsert,
			name:  tblStuff.tarRel.GetTableName(),
			side:  diffSideTarget,
		}, false, tblStuff.retPool); err3 != nil {
			return err3
		} else if stop {
			return nil
		}

		if stop, err3 := emitBatch(emit, batchWithKind{
			batch: baseBat,
			kind:  diffInsert,
			name:  tblStuff.baseRel.GetTableName(),
			side:  diffSideBase,
		}, false, tblStuff.retPool); err3 != nil {
			return err3
		} else if stop {
			return nil
		}

		return nil
	}, -1); err != nil {
		return
	}

	if copt.conflictOpt != nil {
		if !copt.outputSQL {
			// merge doesnt need the base data
			return nil
		}
	}

	if err = baseDataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2  error
			tuple types.Tuple
			bat   *batch.Batch
		)

		bat = tblStuff.retPool.acquireRetBatch(tblStuff, false)

		if err2 = cursor.ForEach(func(_ []byte, row []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if tuple, _, err2 = baseDataHashmap.DecodeRow(row); err2 != nil {
				return err2
			}

			if err2 = appendTupleToBat(ses, bat, tuple, tblStuff); err2 != nil {
				return err2
			}
			return nil
		}); err2 != nil {
			return err2
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if stop, err3 := emitBatch(emit, batchWithKind{
			batch: bat,
			kind:  diffInsert,
			name:  tblStuff.baseRel.GetTableName(),
			side:  diffSideBase,
		}, false, tblStuff.retPool); err3 != nil {
			return err3
		} else if stop {
			return nil
		}
		return nil
	}, -1); err != nil {
		return
	}

	return nil
}
func buildHashmapForTable(
	ctx context.Context,
	mp *mpool.MPool,
	tblStuff *tableStuff,
	handles []engine.ChangesHandle,
	side string,
	pickKeyHashmap databranchutils.BranchHashmap,
) (
	dataHashmap databranchutils.BranchHashmap,
	tombstoneHashmap databranchutils.BranchHashmap,
	err error,
) {
	var (
		atomicErr          atomic.Value
		dataBat            *batch.Batch
		tombstoneBat       *batch.Batch
		wg                 sync.WaitGroup
		totalRows          int64
		totalBytes         int64
		totalTombstoneRows int64
		start              = time.Now()
	)

	defer func() {
		wg.Wait()

		if dataBat != nil {
			dataBat.Clean(mp)
		}

		if tombstoneBat != nil {
			tombstoneBat.Clean(mp)
		}

		if err != nil {
			logutil.Warn("DataBranch-Hashmap-Build-Error",
				zap.String("side", side),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err),
			)
		}
	}()

	if dataHashmap, err = databranchutils.NewBranchHashmap(
		databranchutils.WithBranchHashmapAllocator(tblStuff.hashmapAllocator),
	); err != nil {
		return
	}

	if tombstoneHashmap, err = databranchutils.NewBranchHashmap(
		databranchutils.WithBranchHashmapAllocator(tblStuff.hashmapAllocator),
	); err != nil {
		return
	}

	putVectors := func(bat *batch.Batch, isTombstone bool) error {
		if bat == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			bat.Clean(mp)
			return ctx.Err()
		default:
		}

		wg.Add(1)

		if err = tblStuff.worker.Submit(func() {
			defer wg.Done()
			defer bat.Clean(mp)

			ll := bat.VectorCount()
			var taskErr error
			select {
			case <-ctx.Done():
				taskErr = ctx.Err()
			default:
				// When a pickKeyHashmap is provided (PICK operation), filter
				// the batch to keep only rows whose PK exists in the hashmap.
				// This moves the precise PK filtering from the consumer
				// (appendPickedBatchRows) into the producer, reducing the
				// data that flows through hashDiff and the channel.
				if pickKeyHashmap != nil {
					pkIdx := tblStuff.def.pkColIdx
					if isTombstone {
						// Data branch keeps rowid in Vec[0] while building the
						// hashmaps, so tombstone PK lives at Vec[1].
						pkIdx = 1
					} else {
						pkIdx++
					}
					var results []databranchutils.GetResult
					results, taskErr = pickKeyHashmap.GetByVectors(
						[]*vector.Vector{bat.Vecs[pkIdx]},
					)
					if taskErr != nil {
						atomicErr.Store(taskErr)
						return
					}
					var sels []int64
					for i, r := range results {
						if r.Exists {
							sels = append(sels, int64(i))
						}
					}
					if len(sels) == 0 {
						return // all rows filtered out
					}
					if len(sels) < bat.RowCount() {
						bat.Shrink(sels, false)
						ll = bat.VectorCount()
					}
				}

				if isTombstone {
					taskErr = tombstoneHashmap.PutByVectors(bat.Vecs[:ll], []int{1})
				} else {
					taskErr = dataHashmap.PutByVectors(bat.Vecs[:ll], []int{tblStuff.def.pkColIdx + 1})
				}
			}
			if taskErr != nil {
				atomicErr.Store(taskErr)
			}
		}); err != nil {
			wg.Done()
			bat.Clean(mp)
		}

		return err
	}

	for _, handle := range handles {
		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			default:
			}
			if dataBat, tombstoneBat, _, err = handle.Next(
				ctx, mp,
			); err != nil {
				return
			} else if dataBat == nil && tombstoneBat == nil {
				// out of data
				break
			}

			tableName := ""
			if side == "base" && tblStuff.baseRel != nil {
				tableName = tblStuff.baseRel.GetTableName()
			} else if side == "target" && tblStuff.tarRel != nil {
				tableName = tblStuff.tarRel.GetTableName()
			}
			if err = validateLeadingRowID(side, tableName, false, dataBat); err != nil {
				return
			}
			if err = validateLeadingRowID(side, tableName, true, tombstoneBat); err != nil {
				return
			}

			if dataBat != nil && dataBat.RowCount() > 0 {
				totalRows += int64(dataBat.RowCount())
				totalBytes += int64(dataBat.Size())
			}
			if tombstoneBat != nil && tombstoneBat.RowCount() > 0 {
				totalTombstoneRows += int64(tombstoneBat.RowCount())
			}

			if atomicErr.Load() != nil {
				err = atomicErr.Load().(error)
				return
			}

			if err = putVectors(dataBat, false); err != nil {
				return
			}

			if err = putVectors(tombstoneBat, true); err != nil {
				return
			}
		}
	}

	wg.Wait()

	if atomicErr.Load() != nil {
		err = atomicErr.Load().(error)
	} else {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
	}

	if err != nil {
		return
	}

	logutil.Debug(
		"DataBranch-Hashmap-Build-Done",
		zap.String("side", side),
		zap.Int64("data-input-rows", totalRows),
		zap.Int64("tombstone-input-rows", totalTombstoneRows),
		zap.Int64("data-items", dataHashmap.ItemCount()),
		zap.Int64("tombstone-items", tombstoneHashmap.ItemCount()),
		zap.Duration("duration", time.Since(start)),
	)

	if tombstoneHashmap.ItemCount() == 0 && dataHashmap.ItemCount() == 0 {
		return
	}

	if tblStuff.maxTombstoneBatchCnt == 0 {
		rowBytes := defaultRowBytes
		if totalRows > 0 {
			rowBytes = totalBytes / totalRows
			if rowBytes <= 0 {
				rowBytes = defaultRowBytes
			}
		}
		rowBytes = rowBytes * tombstoneRowMult / tombstoneRowDiv

		available := tblStuff.hashmapAllocator.Available() / 3

		shardCount := 0
		if tombstoneHashmap != nil {
			shardCount = tombstoneHashmap.ShardCount()
		} else if dataHashmap != nil {
			shardCount = dataHashmap.ShardCount()
		}

		maxByMem := int(available / int64(shardCount) / rowBytes)
		if maxByMem < 1 {
			maxByMem = 1
		} else if maxByMem > maxSqlBatchCnt {
			maxByMem = maxSqlBatchCnt
		}

		tblStuff.maxTombstoneBatchCnt = maxByMem

	}

	// For fake PK tables, each row version has a unique fake PK, so there are
	// no same-key duplicates. However, the dedup phase also pairs tombstones
	// with their corresponding data entries. For example, INSERT(1,1) with
	// FK_old then UPDATE→(1,11) produces tombstone FK_old + stale data FK_old
	// + live data FK_new. Without cleanup, the stale data (1,1) would survive
	// and appear in the diff output with the wrong value.

	/*
		case 1:
			     insert    update    update     update
				   pk1		 pk1       pk1        pk1   ....
				|------|-------------------------------------|
		               |           collect range             |
			1.1. the last one is an update: only keep the last update op
			1.2. the last one is a deletion: only keep the last deletion op

		case 2:
			           insert    update     update
				   		 pk1       pk1        pk1   ....
				|------|-------------------------------------|
		               |           collect range             |
		     2.1. the last one is an update: only keep the last update op
		     2.2. the last one is a deletion: only keep the last deletion op

		case 3:
				 insert    delete    update     update
		           pk1		 pk1       pk1        pk1   ....
				|------|-------------------------------------|
		               |           collect range             |
				3.1. the last one is an update: only keep the last update op
				3.2. the last one is a deletion: only keep the last deletion op
	*/

	if err = tombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2    error
			err3    error
			maxTs   types.TS
			maxIdx  int
			tuple   types.Tuple
			dataRet databranchutils.GetResult
		)
		if err2 = cursor.ForEach(func(key []byte, _ []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			var tombstoneRet databranchutils.GetResult
			if tombstoneRet, err3 = cursor.GetByEncodedKey(key); err3 != nil {
				return err3
			}
			if len(tombstoneRet.Rows) == 0 {
				return nil
			}

			maxIdx = 0
			maxTs = types.MinTs()
			for i := 0; i < len(tombstoneRet.Rows); i++ {
				if tuple, _, err3 = tombstoneHashmap.DecodeRow(tombstoneRet.Rows[i]); err3 != nil {
					return err3
				}
				cur := types.TS(tuple[len(tuple)-1].([]uint8))
				if cur.GT(&maxTs) {
					maxTs = cur
					maxIdx = i
				}
			}

			totalTombstones := len(tombstoneRet.Rows)

			// Remove stale tombstone entries (only needed when multiple exist)
			if len(tombstoneRet.Rows) > 1 {
				for i := range tombstoneRet.Rows {
					if i != maxIdx {
						if _, err3 = cursor.PopByEncodedKeyValue(
							key, tombstoneRet.Rows[i], false,
						); err3 != nil {
							return err3
						}
					}
				}
			}

			// Remove data entries older than the latest tombstone.
			// Count how many stale data entries are removed — each one
			// "consumes" one tombstone from an intermediate operation
			// (e.g., insert→update creates 1 tombstone + 1 stale data entry).
			staleDataCount := 0
			if dataRet, err3 = dataHashmap.GetByEncodedKey(key); err3 != nil {
				return err3
			}

			if dataRet.Exists {
				for i := range dataRet.Rows {
					if tuple, _, err3 = dataHashmap.DecodeRow(dataRet.Rows[i]); err3 != nil {
						return err3
					}
					cur := types.TS(tuple[len(tuple)-1].([]uint8))
					if cur.LT(&maxTs) {
						if _, err3 = dataHashmap.PopByEncodedKeyValue(
							key, dataRet.Rows[i], false,
						); err3 != nil {
							return err3
						}
						staleDataCount++
					}
				}
			}

			// If all tombstones are consumed by stale data entries, the net
			// effect is a pure INSERT (not a delete). Remove the remaining
			// tombstone so that findDeleteAndUpdateBat doesn't incorrectly
			// cancel the surviving data entry.
			//
			// Example: insert(1,1) → update→(1,11) produces 1 tombstone +
			// 1 stale data (1,1) + 1 live data (1,11). staleDataCount=1
			// matches totalTombstones=1, so the tombstone is intermediate.
			//
			// Contrast: update LCA row (0,0)→(0,10) produces 1 tombstone +
			// 0 stale data (the original (0,0) is outside the change range).
			// staleDataCount=0 < totalTombstones=1, so the tombstone stays
			// and findDeleteAndUpdateBat correctly detects UPDATE on the LCA.
			if staleDataCount >= totalTombstones {
				if _, err3 = cursor.PopByEncodedKeyValue(
					key, tombstoneRet.Rows[maxIdx], false,
				); err3 != nil {
					return err3
				}
			}
			return nil
		}); err2 != nil {
			return err2
		}
		return nil
	}, -1); err != nil {
		return nil, nil, err
	}

	return
}
