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
	"context"
	"errors"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
)

// reconcileDataBranchClear consumes matching endpoint rows from data. The
// endpoint image has no tombstones from before the last truncate, so each row
// visible at the comparison baseline is a candidate deletion or update.
func reconcileDataBranchClear(
	ctx context.Context, ses *Session, tables tableStuff, baseline dataBranchSnapshotRelation,
	baselineColNames []string, name string, side int, expandUpdate bool,
	data, pickKeys databranchutils.BranchHashmap, retainMatch func(*batch.Batch) error, deliver func(batchWithKind) error,
) error {
	if baseline.rel == nil {
		return nil
	}
	ctx = identityCtx(ctx, baseline.ident)
	sourceDef, targetDef := baseline.rel.GetTableDef(ctx), tables.tarRel.GetTableDef(ctx)
	if tables.def.pkKind == fakeKind {
		if _, err := dataBranchSourceColToTargetIdx(
			sourceDef, targetDef, tables.def.colNames, tables.def.tarOnlyIdxes, false,
		); err != nil {
			return err
		}
	}
	targetOnly := tables.def.tarOnlyIdxes
	if baseline.columns != nil {
		baselineColNames = make([]string, len(tables.def.colNames))
		targetOnly = slices.Clone(targetOnly)
		for idx, col := range baseline.columns.present {
			if col == nil {
				targetOnly = append(targetOnly, idx)
			} else {
				baselineColNames[idx] = col.Name
			}
		}
	}
	layout, err := buildLCAProbeColumnLayout(sourceDef, targetDef, tables.def.colNames,
		tables.def.colTypes, targetOnly, baselineColNames, false)
	if err != nil {
		return err
	}
	// Missing columns have no value in the scanned snapshot. Only compare
	// them if a later generation supplies a reproducible introduction value.
	compareIdxes := make([]int, 0, len(tables.def.colNames))
	common := tables.def.commonIdxes
	if len(common) == 0 {
		common = tables.def.visibleIdxes
	}
	defaults := make(map[int]*vector.Vector)
	var unknownColumn string
	for _, idx := range common {
		if baseline.upperColumns != nil && baseline.upperColumns[idx] == nil {
			continue
		}
		if slices.Contains(layout.targetIdxes, idx) {
			compareIdxes = append(compareIdxes, idx)
		} else if baseline.columns != nil && baseline.columns.introduced[idx] != nil {
			col := baseline.columns.introduced[idx]
			expr := col.GetDefault().GetExpr()
			if expr == nil && !col.NotNull && col.GeneratedCol == nil && !col.Typ.AutoIncr {
				compareIdxes = append(compareIdxes, idx) // implicit NULL default
			} else if col.GeneratedCol == nil && !col.Typ.AutoIncr && function.IsConstant(expr, false) {
				// Convert the value as COPY ALTER would, before copying its
				// representation into a vector with the endpoint type.
				castTypes := baseline.columns.conversions[idx]
				if !isDataBranchLogicalTypeEquivalent(expr.Typ, col.Typ) {
					castTypes = append([]plan.Type{col.Typ}, castTypes...)
				}
				if len(castTypes) != 0 {
					expr = plan2.DeepCopyExpr(expr)
				}
				for _, typ := range castTypes {
					expr, err = plan2.MakePlan2AssignmentCastExpr(ctx, expr, typ)
					if err != nil {
						return err
					}
				}
				target := dataBranchColumnDefByName(targetDef, tables.def.colNames[idx])
				if target != nil && !isDataBranchLogicalTypeEquivalent(expr.Typ, target.Typ) {
					if len(castTypes) == 0 {
						expr = plan2.DeepCopyExpr(expr)
					}
					expr, err = plan2.MakePlan2AssignmentCastExpr(ctx, expr, target.Typ)
					if err != nil {
						return err
					}
				}
				value, free, err := colexec.GetReadonlyResultFromExpression(ses.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
				if err != nil {
					return err
				}
				defer free()
				defaults[idx] = value
				compareIdxes = append(compareIdxes, idx)
			} else if unknownColumn == "" {
				unknownColumn = tables.def.colNames[idx]
			}
		}
	}
	pkIndex := slices.Index(layout.targetIdxes, tables.def.pkColIdx)
	if pkIndex < 0 {
		return moerr.NewInternalErrorNoCtx("data branch truncate baseline is missing its primary key")
	}
	var fakeKeyIdxes []int
	if tables.def.pkKind == fakeKind {
		fakeKeyIdxes = fakePKVisibleKeyIdxes(tables)
	}
	mp := ses.proc.Mp()
	var pkExpr *plan.Expr
	if pickKeys != nil {
		if pickKeys.ItemCount() == 0 {
			return nil
		}
		pkExpr, err = dataBranchClearKeyFilter(ctx, ses, pickKeys, layout.attrs[pkIndex], layout.types[pkIndex])
		if err != nil {
			return err
		}
	}
	return scanSnapshotRelationWithPreopened(ctx, "data-branch-truncate-baseline", ses,
		baseline.rel.GetTableID(ctx), baseline.at, baseline.rel, layout.attrs, layout.types, pkExpr, 1,
		func(rows *batch.Batch) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var selected []int64
			count := rows.RowCount()
			if pickKeys != nil {
				key := rows.Vecs[pkIndex]
				keyType := tables.def.colTypes[tables.def.pkColIdx]
				if *key.GetType() != keyType {
					// Rebuilt composite keys can carry the same encoding
					// with different hidden-column type attributes.
					projected := vector.NewVec(keyType, mp)
					defer projected.Free(mp)
					if err := projected.UnionBatch(key, 0, count, nil, mp); err != nil {
						return err
					}
					key = projected
				}
				matches, err := pickKeys.GetByVectors([]*vector.Vector{key})
				if err != nil {
					return err
				}
				for i, match := range matches {
					if match.Exists {
						selected = append(selected, int64(i))
					}
				}
				count = len(selected)
			}
			if count == 0 {
				return nil
			}
			bat := tables.retPool.acquireRetBatch(tables, false, ses.proc.Mp())
			defer tables.retPool.releaseRetBatch(bat, false)
			for i, targetIdx := range layout.targetIdxes {
				var err error
				if pickKeys != nil {
					err = bat.Vecs[targetIdx].Union(rows.Vecs[i], selected, mp)
				} else {
					err = bat.Vecs[targetIdx].UnionBatch(rows.Vecs[i], 0, count, nil, mp)
				}
				if err != nil {
					return err
				}
			}
			for idx, v := range bat.Vecs {
				if value := defaults[idx]; value != nil {
					if err := v.UnionMulti(value, 0, count, mp); err != nil {
						return err
					}
				} else if v.Length() == 0 {
					nulls := vector.NewConstNull(*v.GetType(), count, mp)
					err := v.UnionBatch(nulls, 0, count, nil, mp)
					nulls.Free(mp)
					if err != nil {
						return err
					}
				}
			}
			bat.SetRowCount(count)
			return reconcileDataBranchBaselineRows(ctx, ses, tables, bat, name, side,
				expandUpdate, data, fakeKeyIdxes, compareIdxes, unknownColumn, retainMatch, deliver)
		})
}

// Small key sets can use the historical PK directly, without expanding a
// large KEYS subquery into a reader expression. The caller still verifies
// exact membership, even when the reader only prunes blocks.
func dataBranchClearKeyFilter(
	ctx context.Context, ses *Session, keys databranchutils.BranchHashmap, name string, typ types.Type,
) (*plan.Expr, error) {
	const maxClearFilterKeys = 1024
	if keys.ItemCount() > maxClearFilterKeys {
		return nil, nil
	}
	mp := ses.proc.Mp()
	vec := vector.NewVec(typ, mp)
	defer vec.Free(mp)
	err := keys.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		return cursor.ForEach(func(_ []byte, row []byte) error {
			tuple, _, err := keys.DecodeRow(row)
			if err != nil {
				return err
			}
			return appendTupleValueToVector(vec, tuple[0], mp)
		})
	}, 1)
	if err != nil {
		return nil, err
	}
	if err := vec.InplaceSort(mp); err != nil {
		return nil, err
	}
	return readutil.ConstructInExpr(ctx, name, vec)
}

// The final insert pass has already matched destination inserts and updates.
// Consult the actual destination snapshot for inherited live rows and use
// remaining deletion evidence to distinguish a delete from an absent key.
func reconcileBoundedClearPickInserts(
	ctx context.Context, ses *Session, bh BackgroundExec, tables, destination tableStuff,
	deletions []databranchutils.BranchHashmap, wrapped batchWithKind, emit emitFunc,
) (bool, error) {
	compareIdxes := tables.def.commonIdxes
	if len(compareIdxes) == 0 {
		compareIdxes = tables.def.visibleIdxes
	}
	var upper []*plan.ColDef
	if baseline := tables.tarClearBaseline; baseline != nil {
		upper = baseline.upperColumns
	}
	mp := ses.proc.Mp()
	var remove []int64
	for start := 0; start < wrapped.batch.RowCount(); start += maxSqlBatchCnt {
		end := min(start+maxSqlBatchCnt, wrapped.batch.RowCount())
		keys, err := wrapped.batch.Vecs[tables.def.pkColIdx].Window(start, end)
		if err != nil {
			return false, err
		}
		stop, err := func() (bool, error) {
			keyBatch := batch.NewWithSize(1)
			keyBatch.Vecs[0] = keys
			keyBatch.SetRowCount(keys.Length())
			live, misses, err := probeDataBranchRows(ctx, ses, bh, keyBatch, destination, timestamp.Timestamp{})
			if err != nil {
				return false, err
			}
			defer tables.retPool.releaseRetBatch(live, false)
			deleted := make([]bool, keys.Length())
			for _, rows := range deletions {
				matches, err := rows.GetByVectors([]*vector.Vector{keys})
				if err != nil {
					return false, err
				}
				for i, match := range matches {
					deleted[i] = deleted[i] || match.Exists
				}
			}
			var conflicts, updates *batch.Batch
			defer func() {
				for _, b := range []*batch.Batch{conflicts, updates} {
					if b != nil {
						tables.retPool.releaseRetBatch(b, false)
					}
				}
			}()
			add := func(dst **batch.Batch, row int64) error {
				if *dst == nil {
					*dst = tables.retPool.acquireRetBatch(tables, false, mp)
				}
				return (*dst).UnionOne(wrapped.batch, row, mp)
			}
			liveRow := 0
			for i := range keys.Length() {
				if err := ctx.Err(); err != nil {
					return false, err
				}
				row := start + i
				if len(misses) != 0 && int(misses[0]) == i {
					misses = misses[1:]
					if deleted[i] {
						if err := add(&conflicts, int64(row)); err != nil {
							return false, err
						}
					}
					continue // ACCEPT still inserts a missing key.
				}
				remove = append(remove, int64(row)) // Never insert over a live key.
				equal := true
				for _, idx := range compareIdxes {
					if slices.Contains(tables.def.pkColIdxes, idx) || (upper != nil && upper[idx] == nil) {
						continue
					}
					cmp, err := compareSingleValInVector(ctx, ses, row, liveRow, wrapped.batch.Vecs[idx], live.Vecs[idx])
					if err != nil {
						return false, err
					}
					if cmp != 0 {
						equal = false
						break
					}
				}
				liveRow++
				if equal {
					continue
				}
				if err := add(&conflicts, int64(row)); err != nil {
					return false, err
				}
				if err := add(&updates, int64(row)); err != nil {
					return false, err
				}
			}
			if conflicts != nil {
				conflicts.SetRowCount(conflicts.Vecs[0].Length())
				// The marker applies no DELETE: the target is either already
				// missing or will be updated directly on ACCEPT.
				b := batchWithKind{batch: conflicts, side: diffSideBase, kind: diffDelete,
					name: tables.baseRel.GetTableName(), fromUpdate: true}
				conflicts = nil
				if stop, err := emitBatch(emit, b, false, tables.retPool); stop || err != nil {
					return stop, err
				}
			}
			if updates != nil {
				updates.SetRowCount(updates.Vecs[0].Length())
				b := wrapped
				b.batch, b.fromUpdate = updates, true
				b.requiresNativeUpdate = len(tables.def.indexedSpecialUpdateIdxes) != 0
				updates = nil
				return emitBatch(emit, b, false, tables.retPool)
			}
			return false, nil
		}()
		if stop || err != nil {
			return stop, err
		}
	}
	if len(remove) != 0 {
		if err := wrapped.batch.Shrink(remove, true, mp); err != nil {
			return false, err
		}
	}
	// The caller still owns this input batch on stop or error.
	return emit(wrapped)
}

// reconcileDataBranchBaselineRows matches baseline keys against live rows.
// The caller owns baseline; delivery owns each output batch even on failure.
func reconcileDataBranchBaselineRows(
	ctx context.Context, ses *Session, tables tableStuff, baseline *batch.Batch,
	name string, side int, expandUpdate bool, data databranchutils.BranchHashmap,
	fakeKeyIdxes, compareIdxes []int, unknownColumn string, retainMatch func(*batch.Batch) error, deliver func(batchWithKind) error,
) error {
	if baseline.RowCount() == 0 {
		return nil
	}

	var deleted, updateOld, updateNew, nativeUpdate, matched *batch.Batch
	defer func() {
		for _, bat := range []*batch.Batch{deleted, updateOld, updateNew, nativeUpdate, matched} {
			if bat != nil {
				tables.retPool.releaseRetBatch(bat, false)
			}
		}
	}()
	appendBaseline := func(dst **batch.Batch, row int) error {
		if *dst == nil {
			*dst = tables.retPool.acquireRetBatch(tables, false, ses.proc.Mp())
		}
		return (*dst).UnionOne(baseline, int64(row), ses.proc.Mp())
	}

	keys := []*vector.Vector{baseline.Vecs[tables.def.pkColIdx]}
	if len(fakeKeyIdxes) != 0 {
		// Fake PKs are physical row identities. A truncate followed by
		// identical inserts must match the multiset of user-visible rows.
		keys = make([]*vector.Vector, len(fakeKeyIdxes))
		for i, col := range fakeKeyIdxes {
			keys[i] = baseline.Vecs[col]
		}
	}
	seen := make([]bool, baseline.RowCount())
	_, err := data.PopByVectorsStream(keys, false, func(idx int, _ []byte, row []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		seen[idx] = true
		if unknownColumn != "" {
			return moerr.NewNotSupportedNoCtxf("DATA BRANCH cannot establish historical value for column %q across a clear", unknownColumn)
		}
		if len(fakeKeyIdxes) != 0 && retainMatch == nil {
			return nil // the key already compares every visible column
		}
		tuple, _, err := data.DecodeRow(row)
		if err != nil {
			return err
		}
		if len(fakeKeyIdxes) != 0 {
			if matched == nil {
				matched = tables.retPool.acquireRetBatch(tables, false, ses.proc.Mp())
			}
			return appendTupleToBat(ses, matched, tuple, tables)
		}
		cmp, err := compareTupleWithBatchRow(tables, tuple, baseline, idx, tables.def.pkKind != fakeKind, compareIdxes)
		if err != nil || cmp == 0 {
			return err
		}
		out := &updateNew
		if expandUpdate {
			native, err := dataBranchUpdateRequiresNativeUpdateFromBatch(tables, tuple, baseline, idx)
			if err != nil {
				return err
			}
			if native {
				out = &nativeUpdate
			}
			if err := appendBaseline(&updateOld, idx); err != nil {
				return err
			}
		}
		if *out == nil {
			*out = tables.retPool.acquireRetBatch(tables, false, ses.proc.Mp())
		}
		return appendTupleToBat(ses, *out, tuple, tables)
	})
	if err != nil {
		return err
	}

	if matched != nil {
		if err := retainMatch(matched); err != nil {
			return err
		}
	}
	for idx, matched := range seen {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !matched {
			if err := appendBaseline(&deleted, idx); err != nil {
				return err
			}
		}
	}
	emit := func(src **batch.Batch, kind string, fromUpdate, native bool) error {
		bat := *src
		if bat == nil {
			return nil
		}
		*src = nil
		bat.SetRowCount(bat.Vecs[0].Length())
		return deliver(batchWithKind{
			name: name, side: side, kind: kind, batch: bat,
			fromUpdate: fromUpdate, requiresNativeUpdate: native,
		})
	}
	if expandUpdate {
		if err := emit(&updateOld, diffDelete, true, false); err != nil {
			return err
		}
		if err := emit(&updateNew, diffInsert, tables.def.pkKind != fakeKind, false); err != nil {
			return err
		}
		if err := emit(&nativeUpdate, diffInsert, tables.def.pkKind != fakeKind, true); err != nil {
			return err
		}
	} else if err := emit(&updateNew, diffUpdate, false, false); err != nil {
		return err
	}
	return emit(&deleted, diffDelete, false, false)
}

// dataBranchClearColumns describes the endpoint columns in a historical
// schema and the first later definition for a column added since then.
type dataBranchClearColumns struct {
	present, introduced []*plan.ColDef
	conversions         [][]plan.Type
}

func resolveDataBranchClearColumns(
	names []string, defs []*plan.TableDef, lineageOnly []bool,
) *dataBranchClearColumns {
	cols := &dataBranchClearColumns{
		present: make([]*plan.ColDef, len(names)), introduced: make([]*plan.ColDef, len(names)),
		conversions: make([][]plan.Type, len(names)),
	}
	for i, name := range names {
		col := dataBranchColumnDefByName(defs[len(defs)-1], name)
		var laterTypes []plan.Type
		for j := len(defs) - 1; j > 0 && col != nil; j-- {
			previous := dataBranchPreviousLineageColumn(defs[j-1], defs[j], col, lineageOnly[j])
			if previous == nil {
				cols.introduced[i] = col
				slices.Reverse(laterTypes)
				cols.conversions[i] = laterTypes
			} else if !isDataBranchLogicalTypeEquivalent(previous.Typ, col.Typ) {
				laterTypes = append(laterTypes, col.Typ)
			}
			col = previous
		}
		cols.present[i] = col
	}
	return cols
}

func fakePKVisibleKeyIdxes(tables tableStuff) []int {
	if len(tables.def.commonVisibleIdxes) != 0 {
		return tables.def.commonVisibleIdxes
	}
	return tables.def.visibleIdxes
}

// Consume at most one destination copy per source delete. Missing destination
// copies cancel first. Only remaining deletes are assigned live hidden keys.
func matchDataBranchFakeClearDeletes(
	ctx context.Context, ses *Session, tables tableStuff, wrapped batchWithKind,
	rows databranchutils.BranchHashmap, keyIdxes []int, rekey bool, conflict *tree.ConflictOpt,
) error {
	keys := make([]*vector.Vector, len(keyIdxes))
	for i, col := range keyIdxes {
		keys[i] = wrapped.batch.Vecs[col]
	}
	seen := make([]bool, wrapped.batch.RowCount())
	_, err := rows.PopByVectorsStream(keys, false, func(idx int, _ []byte, row []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		seen[idx] = true
		if !rekey {
			if conflict != nil && conflict.Opt == tree.CONFLICT_FAIL {
				key, err := extractPKAsString(ses, tables, wrapped.batch, idx)
				if err != nil {
					return err
				}
				return moerr.NewDataBranchConflictNoCtxf("%s DELETE and %s DELETE on pk(%v)",
					wrapped.name, tables.baseRel.GetTableName(), key)
			}
			return nil
		}
		tuple, _, err := rows.DecodeRow(row)
		if err != nil {
			return err
		}
		pk := tables.def.pkColIdx
		liveKey, err := getTupleColumnValue(tuple, tables, pk)
		if err != nil {
			return err
		}
		return vector.SetFixedAtWithTypeCheck(wrapped.batch.Vecs[pk], idx, liveKey.(uint64))
	})
	if err != nil {
		return err
	}
	var remove []int64
	for idx, matched := range seen {
		if matched != rekey {
			remove = append(remove, int64(idx))
		}
	}
	if len(remove) != 0 {
		return wrapped.batch.Shrink(remove, true, ses.proc.Mp())
	}
	return nil
}

func emitDataBranchClearChanges(
	ctx context.Context, ses *Session, tables tableStuff,
	rows databranchutils.BranchHashmap, meta batchWithKind, emit emitFunc,
) (bool, error) {
	var out *batch.Batch
	defer func() {
		if out != nil {
			tables.retPool.releaseRetBatch(out, false)
		}
	}()
	stop := false
	flush := func() error {
		wrapped := meta
		wrapped.batch = out
		out = nil
		var err error
		stop, err = emitBatch(emit, wrapped, false, tables.retPool)
		if stop && err == nil {
			return context.Canceled
		}
		return err
	}
	err := rows.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		return cursor.ForEach(func(_, row []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			tuple, _, err := rows.DecodeRow(row)
			if err != nil {
				return err
			}
			if out == nil {
				out = tables.retPool.acquireRetBatch(tables, false, ses.proc.Mp())
			}
			if err := appendTupleToBat(ses, out, tuple, tables); err != nil {
				return err
			}
			if out.RowCount() >= maxSqlBatchCnt {
				return flush()
			}
			return nil
		})
	}, 1)
	if err == nil && out != nil {
		err = flush()
	}
	if stop && errors.Is(err, context.Canceled) {
		return true, nil
	}
	return stop, err
}
