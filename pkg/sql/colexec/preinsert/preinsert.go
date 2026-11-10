// Copyright 2022 Matrix Origin
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

package preinsert

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"
)

const opName = "preinsert"

const autoIncrementGeneratedAttr = "__mo_auto_increment_generated"
const odkuInputOrdinalAttr = "__mo_odku_input_ordinal"

func (preInsert *PreInsert) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": pre processing insert")
}

func (preInsert *PreInsert) OpType() vm.OpType {
	return vm.PreInsert
}

func (preInsert *PreInsert) Prepare(proc *process.Process) (err error) {
	if preInsert.OpAnalyzer == nil {
		preInsert.OpAnalyzer = process.NewAnalyzer(preInsert.GetIdx(), preInsert.IsFirst, preInsert.IsLast, "preinsert")
	} else {
		preInsert.OpAnalyzer.Reset()
	}

	if preInsert.ctr.canFreeVecIdx == nil {
		preInsert.ctr.canFreeVecIdx = make(map[int]bool)
	}
	if preInsert.ctr.ownedVecSlots == nil {
		preInsert.ctr.ownedVecSlots = make(map[int]bool)
	}
	if preInsert.CompPkeyExpr != nil && preInsert.ctr.compPkExecutor == nil {
		preInsert.ctr.compPkExecutor, err = colexec.NewExpressionExecutor(proc, preInsert.CompPkeyExpr)
		if err != nil {
			return
		}
	}
	if preInsert.ClusterByExpr != nil && preInsert.ctr.clusterByExecutor == nil {
		preInsert.ctr.clusterByExecutor, err = colexec.NewExpressionExecutor(proc, preInsert.ClusterByExpr)
		if err != nil {
			return
		}
	}
	if preInsert.HasAutoCol {
		preInsert.ctr.tblId = preInsert.TableDef.TblId
		if err = preInsert.refreshAutoIncrementTableID(proc); err != nil {
			return
		}
	}
	if (preInsert.TrackAutoIncrementGenerated || preInsert.TrackODKUResult) && preInsert.AutoIncrementGeneratedColumn < 0 {
		return moerr.NewInvalidInput(proc.Ctx, "invalid auto-increment provenance column")
	}
	if preInsert.TrackODKUResult && preInsert.ODKUOrdinalColumn < 0 {
		return moerr.NewInvalidInput(proc.Ctx, "invalid ODKU input ordinal column")
	}
	if preInsert.TrackODKUResult && preInsert.ODKUAutoIncrementColumn < 0 {
		return moerr.NewInvalidInput(proc.Ctx, "invalid ODKU auto-increment source column")
	}
	if preInsert.TrackODKUResult && !preInsert.HasAutoCol {
		return moerr.NewInvalidInput(proc.Ctx,
			"ODKU result tracking requires an auto-increment column")
	}
	if preInsert.TrackODKUResult {
		colIdx := int(preInsert.ODKUAutoIncrementColumn)
		if colIdx >= len(preInsert.TableDef.Cols) {
			return moerr.NewInvalidInput(proc.Ctx,
				"ODKU auto-increment source column is outside table definition")
		}
		col := preInsert.TableDef.Cols[colIdx]
		if col == nil || col.Hidden || !col.Typ.AutoIncr {
			return moerr.NewInvalidInput(proc.Ctx,
				"ODKU auto-increment source column is not a user auto-increment column")
		}
	}
	return
}

func (preInsert *PreInsert) refreshAutoIncrementTableID(proc *proc) error {
	if preInsert.TableDef == nil || preInsert.TableDef.Name == "" || preInsert.SchemaName == "" {
		return nil
	}
	if preInsert.TableDef.IsTemporary {
		return nil
	}
	if proc.Base.SessionInfo.StorageEngine == nil || proc.Base.TxnOperator == nil {
		return nil
	}

	db, err := proc.Base.SessionInfo.StorageEngine.Database(proc.Ctx, preInsert.SchemaName, proc.Base.TxnOperator)
	if err != nil {
		return err
	}
	rel, err := db.Relation(proc.Ctx, preInsert.TableDef.Name, nil)
	if err != nil {
		return err
	}
	preInsert.ctr.tblId = rel.GetTableID(proc.Ctx)
	return nil
}

func (preInsert *PreInsert) retryWithDefChanged() error {
	return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
}

func (preInsert *PreInsert) constructColBuf(proc *proc, bat *batch.Batch, first bool) (err error) {
	// Direct drives (Prepare-less tests) initialize the container literal
	// themselves; keep the ownership marker usable on every entry path.
	if preInsert.ctr.ownedVecSlots == nil {
		preInsert.ctr.ownedVecSlots = make(map[int]bool)
	}
	if first {
		for idx := range preInsert.Attrs {
			if preInsert.TableDef.Cols[idx].Typ.AutoIncr {
				preInsert.ctr.canFreeVecIdx[int(preInsert.ColOffset)+idx] = true
			}
		}
		if preInsert.IsNewUpdate {
			preInsert.ctr.buf = batch.NewOffHeapWithSize(len(bat.Vecs))
		} else {
			preInsert.ctr.buf = batch.NewOffHeapWithSize(len(preInsert.Attrs))
			preInsert.ctr.buf.Attrs = slices.Clone(preInsert.Attrs)
		}
	} else {
		preInsert.ctr.buf.SetRowCount(0)
	}
	// if col is AutoIncr, genAutoIncrCol function may change the vector of this col, we should copy the vec from children vec, so it in canFreeVecIdx
	// and the other cols of preInsert.Attrs is stable, we just use the vecs of children's vecs
	vecCnt := len(preInsert.Attrs)
	if preInsert.IsNewUpdate {
		vecCnt = len(bat.Vecs)
	}
	if !preInsert.IsNewUpdate {
		// Repeated plain source references reach this operator as one child
		// vector installed into two output columns: a same-type assignment
		// keeps the plan cast-free and the column expression executor then
		// hands out the input vector itself. The IGNORE adjustment and the
		// NULL primary-key drop rewrite output slots in place, so a vector
		// shared by two columns would let the first column's adjustment set
		// its sibling's cell to the implicit default and clear a null bitmap
		// the sibling still needs. Give every repeated vector after its first
		// appearance an owned independent copy, reusing the canFreeVecIdx
		// ownership the auto-increment columns already have: later calls
		// refresh it by CleanOnlyData+UnionBatch and Free releases it.
		seen := make(map[*vector.Vector]struct{}, vecCnt)
		for idx := 0; idx < vecCnt; idx++ {
			v := bat.Vecs[idx]
			if v == nil || v.IsConst() || preInsert.ctr.canFreeVecIdx[idx] {
				continue
			}
			if _, shared := seen[v]; shared {
				preInsert.ctr.canFreeVecIdx[idx] = true
			} else {
				seen[v] = struct{}{}
			}
		}
	}
	for idx := 0; idx < vecCnt; idx++ {
		if preInsert.ctr.canFreeVecIdx[idx] {
			typ := bat.Vecs[idx].GetType()
			if preInsert.ctr.buf.Vecs[idx] != nil && preInsert.ctr.ownedVecSlots[idx] {
				// Storage this operator allocated on an earlier batch:
				// safe to clean and refill in place.
				preInsert.ctr.buf.Vecs[idx].CleanOnlyData()
			} else {
				// canFreeVecIdx marks the slot owned from now on, but the
				// buffer entry may still be a child vector an earlier batch
				// borrowed into this slot (the repeated vector only became
				// shared this batch). Cleaning that borrowed entry would
				// clear the live child vector before its length is read and
				// leave every destination it feeds with zero-length data,
				// and Free would later release storage the child still owns,
				// so install fresh independent storage instead.
				preInsert.ctr.buf.Vecs[idx] = vector.NewOffHeapVecWithType(*typ)
				preInsert.ctr.ownedVecSlots[idx] = true
			}
			cnt := bat.Vecs[idx].Length()
			if bat.Vecs[idx].IsConst() {
				cnt = bat.RowCount()
			}
			if err = preInsert.ctr.buf.Vecs[idx].UnionBatch(bat.Vecs[idx], 0, cnt, nil, proc.Mp()); err != nil {
				return err
			}
		} else {
			if bat.Vecs[idx].IsConst() {
				preInsert.ctr.canFreeVecIdx[idx] = true
				//expland const vector
				typ := bat.Vecs[idx].GetType()
				tmpVec := vector.NewOffHeapVecWithType(*typ)
				if err = tmpVec.UnionBatch(bat.Vecs[idx], 0, bat.RowCount(), nil, proc.Mp()); err != nil {
					tmpVec.Free(proc.Mp())
					return err
				}
				preInsert.ctr.buf.Vecs[idx] = tmpVec
				preInsert.ctr.ownedVecSlots[idx] = true
			} else {
				preInsert.ctr.buf.SetVector(int32(idx), bat.Vecs[idx])
				preInsert.ctr.ownedVecSlots[idx] = false
			}
		}
	}
	return
}

func (preInsert *PreInsert) constructHiddenColBuf(proc *proc, bat *batch.Batch, first bool) (err error) {
	if first {
		if preInsert.ctr.compPkExecutor != nil {
			vec, err := preInsert.ctr.compPkExecutor.Eval(proc, []*batch.Batch{preInsert.ctr.buf}, nil)
			if err != nil {
				return err
			}
			preInsert.ctr.canFreeVecIdx[len(preInsert.ctr.buf.Vecs)] = true
			preInsert.ctr.ownedVecSlots[len(preInsert.ctr.buf.Vecs)] = true
			preInsert.ctr.buf.Vecs = append(preInsert.ctr.buf.Vecs, vec)
			preInsert.ctr.buf.Attrs = append(preInsert.ctr.buf.Attrs, catalog.CPrimaryKeyColName)
		}
		if preInsert.ctr.clusterByExecutor != nil {
			vec, err := preInsert.ctr.clusterByExecutor.Eval(proc, []*batch.Batch{preInsert.ctr.buf}, nil)
			if err != nil {
				return err
			}
			preInsert.ctr.canFreeVecIdx[len(preInsert.ctr.buf.Vecs)] = true
			preInsert.ctr.ownedVecSlots[len(preInsert.ctr.buf.Vecs)] = true
			preInsert.ctr.buf.Vecs = append(preInsert.ctr.buf.Vecs, vec)
			preInsert.ctr.buf.Attrs = append(preInsert.ctr.buf.Attrs, preInsert.TableDef.ClusterBy.Name)
		}
		if preInsert.IsOldUpdate {
			rowIdIdx := len(bat.Vecs) - 1
			preInsert.ctr.buf.Attrs = append(preInsert.ctr.buf.Attrs, catalog.Row_ID)
			rowIdVec := vector.NewOffHeapVecWithType(*bat.GetVector(int32(rowIdIdx)).GetType())
			err = rowIdVec.UnionBatch(bat.Vecs[rowIdIdx], 0, bat.Vecs[rowIdIdx].Length(), nil, proc.Mp())
			if err != nil {
				rowIdVec.Free(proc.Mp())
				return err
			}
			preInsert.ctr.canFreeVecIdx[len(preInsert.ctr.buf.Vecs)] = true
			preInsert.ctr.ownedVecSlots[len(preInsert.ctr.buf.Vecs)] = true
			preInsert.ctr.buf.Vecs = append(preInsert.ctr.buf.Vecs, rowIdVec)
		}
	} else {
		colIdx := len(preInsert.Attrs)
		if preInsert.ctr.compPkExecutor != nil {
			vec, err := preInsert.ctr.compPkExecutor.Eval(proc, []*batch.Batch{preInsert.ctr.buf}, nil)
			if err != nil {
				return err
			}
			preInsert.ctr.buf.Vecs[colIdx] = vec
			preInsert.ctr.ownedVecSlots[colIdx] = true
			colIdx += 1
		}
		if preInsert.ctr.clusterByExecutor != nil {
			vec, err := preInsert.ctr.clusterByExecutor.Eval(proc, []*batch.Batch{preInsert.ctr.buf}, nil)
			if err != nil {
				return err
			}
			preInsert.ctr.buf.Vecs[colIdx] = vec
			preInsert.ctr.ownedVecSlots[colIdx] = true
			colIdx += 1
		}
		if preInsert.IsOldUpdate {
			rowIdIdx := len(bat.Vecs) - 1
			rowIdVec := preInsert.ctr.buf.Vecs[colIdx]
			rowIdVec.CleanOnlyData()
			err = rowIdVec.UnionBatch(bat.Vecs[rowIdIdx], 0, bat.Vecs[rowIdIdx].Length(), nil, proc.Mp())
			if err != nil {
				return err
			}
		}
	}

	return err
}

// ignoreFillColumnAlignment returns the ColDefs aligned one-to-one with
// preInsert.Attrs, plus the attr index of the one column the IGNORE fill must
// leave untouched: the resolvable single-column primary key that forbids NULL
// and is not auto-incremented. That key keeps its supplied NULL through
// PRE_INSERT so the write side keeps dropping the row (publishing its own
// ER_BAD_NULL_ERROR warning) instead of letting several adjusted rows claim
// the same primary key inside one statement before duplicate arbitration sees
// it. Composite or synthesized keys are not affected: their member columns
// follow the regular fill, and an auto-increment key has already allocated a
// value before the fill runs.
func (preInsert *PreInsert) ignoreFillColumnAlignment() (colsByAttr []*plan.ColDef, skipAttr int) {
	skipAttr = -1
	if preInsert.TableDef == nil {
		return nil, -1
	}
	name2Col := make(map[string]*plan.ColDef, len(preInsert.TableDef.Cols))
	for _, col := range preInsert.TableDef.Cols {
		if col != nil {
			name2Col[col.Name] = col
			name2Col[col.GetOriginCaseName()] = col
		}
	}
	colsByAttr = make([]*plan.ColDef, len(preInsert.Attrs))
	for i, attr := range preInsert.Attrs {
		colsByAttr[i] = name2Col[attr]
	}
	pkey := preInsert.TableDef.Pkey
	if pkey == nil || len(pkey.Names) != 1 ||
		pkey.Names[0] == catalog.FakePrimaryKeyColName ||
		pkey.Names[0] == catalog.CPrimaryKeyColName {
		return colsByAttr, -1
	}
	for i, col := range colsByAttr {
		if col == nil || (col.Name != pkey.Names[0] && col.GetOriginCaseName() != pkey.Names[0]) {
			continue
		}
		if col.Typ.AutoIncr || (!col.NotNull && (col.Default == nil || col.Default.NullAbility)) {
			return colsByAttr, -1
		}
		skipAttr = i
	}
	return colsByAttr, skipAttr
}

// dropNullPkRowsForIgnoreFill removes from the operator output every row
// whose resolvable single-column primary-key cell (fillSkipAttr, with
// fillCols carrying the aligned column definitions) still carries its
// supplied NULL, publishing one ER_BAD_NULL_ERROR warning per dropped row.
// The pre-insert fill deliberately left that cell untouched, and dropping
// the row here keeps every downstream consumer (CHECK filter, unique-key
// arbitration, the foreign-key check, the parent-key lock, index encoding,
// the write) from evaluating a row that can never store. UPDATE pipelines
// are excluded before the batch even reaches this point.
func (preInsert *PreInsert) dropNullPkRowsForIgnoreFill(
	proc *proc,
	fillSkipAttr int,
	fillCols []*plan.ColDef,
) error {
	if preInsert.IsNewUpdate {
		return nil
	}
	if proc == nil || proc.GetStmtProfile() == nil || !proc.GetStmtProfile().GetStatementIgnore() {
		return nil
	}
	if fillSkipAttr < 0 || fillSkipAttr >= len(fillCols) || fillCols[fillSkipAttr] == nil {
		return nil
	}
	keyPos := int(preInsert.ColOffset) + fillSkipAttr
	if keyPos < 0 || keyPos >= len(preInsert.ctr.buf.Vecs) {
		return nil
	}
	_, err := colexec.DropNullPkRowsForIgnore(
		proc, preInsert.ctr.buf.Vecs[keyPos], fillCols[fillSkipAttr].Name, preInsert.ctr.buf)
	return err
}

func (preInsert *PreInsert) Call(proc *proc) (vm.CallResult, error) {
	analyzer := preInsert.OpAnalyzer

	result, err := vm.ChildrenCall(preInsert.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}

	if result.Batch == nil || result.Batch.IsEmpty() {
		return result, nil
	}
	bat := result.Batch

	first := preInsert.ctr.buf == nil
	err = preInsert.constructColBuf(proc, bat, first)
	if err != nil {
		return result, err
	}
	workBat := preInsert.ctr.buf
	var selectedRows []int64
	if preInsert.HasTargetSelector {
		selectedRows, err = preInsert.targetSelectedRows(proc, bat)
		if err != nil {
			return result, err
		}
		workBat, err = preInsert.ctr.buf.Clone(proc.Mp(), true)
		if err != nil {
			return result, err
		}
		defer workBat.Clean(proc.Mp())
		if err := workBat.Shrink(selectedRows, false, proc.Mp()); err != nil {
			return result, err
		}
		workBat.SetRowCount(len(selectedRows))
	}
	if err = checkZeroTemporalInStrictMode(preInsert, workBat, proc); err != nil {
		return result, err
	}
	// keep shuffleIDX unchanged
	preInsert.ctr.buf.ShuffleIDX = bat.ShuffleIDX
	// constructColBuf copies the child vectors into the reusable output batch,
	// but deliberately leaves its row count for this point.  Set it before
	// capturing per-row ODKU provenance so the marker and ordinal slices cover
	// the actual rows passed to the allocator.  The output batch is then not
	// counted a second time below.
	preInsert.ctr.buf.AddRowCount(bat.RowCount())

	if preInsert.HasAutoCol && (!preInsert.HasTargetSelector || len(selectedRows) > 0) {
		if shouldConvertZeroToNull(preInsert, proc) {
			convertZeroToNull(workBat, preInsert)
		}
		if err = preInsert.captureAutoIncrementGeneratedRows(workBat); err != nil {
			return result, err
		}
		if err = preInsert.captureODKUInputOrdinals(proc, workBat); err != nil {
			return result, err
		}
		start := time.Now()
		err = genAutoIncrCol(workBat, proc, preInsert)
		if err != nil {
			return result, err
		}
		if preInsert.HasTargetSelector {
			for idx, col := range preInsert.TableDef.Cols {
				if !col.Typ.AutoIncr {
					continue
				}
				vecIdx := int(preInsert.ColOffset) + idx
				for selectedIdx, inputIdx := range selectedRows {
					if err = preInsert.ctr.buf.Vecs[vecIdx].Copy(
						workBat.Vecs[vecIdx], inputIdx, int64(selectedIdx), proc.Mp()); err != nil {
						return result, err
					}
				}
			}
		}
		analyzer.AddIncrementTime(start)
	}
	// An unmatched target contributes no row to this UPDATE branch. Its outer
	// join placeholders must not participate in target-table constraints.
	var fillCols []*plan.ColDef
	fillSkipAttr := -1
	if !preInsert.HasTargetSelector || len(selectedRows) > 0 {
		// check new rows not null
		tempVecs := workBat.Vecs[preInsert.ColOffset : int(preInsert.ColOffset)+len(preInsert.Attrs)]
		if proc != nil && proc.GetStmtProfile() != nil &&
			proc.GetStmtProfile().GetStatementIgnore() && !preInsert.IsNewUpdate {
			// IGNORE keeps the row and adjusts the value instead of failing
			// the statement: set the implicit default of the column type and
			// report one ER_BAD_NULL_ERROR warning per adjusted cell. UPDATE
			// pipelines own their own rejection and never reach this fill.
			fillCols, fillSkipAttr = preInsert.ignoreFillColumnAlignment()
			fillVecs := tempVecs
			fillAttrs := preInsert.Attrs
			fillOpCols := fillCols
			originIdx := []int(nil)
			if fillSkipAttr >= 0 && fillSkipAttr < len(tempVecs) {
				// A resolvable single-column NOT NULL primary key keeps its
				// supplied NULL here and is dropped from the output below, so
				// no insert leg can adjust the key cell and let several rows
				// claim the same primary key inside one statement. The full
				// aligned triplets stay in fillCols for that drop, which
				// names the key from the pre-exclusion definition.
				originIdx = make([]int, 0, len(tempVecs))
				vecs2 := make([]*vector.Vector, 0, len(tempVecs))
				attrs2 := make([]string, 0, len(preInsert.Attrs))
				cols2 := make([]*plan.ColDef, 0, len(fillCols))
				for j := range tempVecs {
					if j == fillSkipAttr {
						continue
					}
					originIdx = append(originIdx, j)
					vecs2 = append(vecs2, tempVecs[j])
					attrs2 = append(attrs2, preInsert.Attrs[j])
					cols2 = append(cols2, fillCols[j])
				}
				fillVecs, fillAttrs, fillOpCols = vecs2, attrs2, cols2
			}
			filledVecIdx, ferr := colexec.BatchDataNotNullFillForIgnore(
				fillVecs, fillAttrs, fillOpCols, proc)
			if ferr != nil {
				return result, ferr
			}
			if originIdx != nil {
				for i, j := range filledVecIdx {
					filledVecIdx[i] = originIdx[j]
				}
			}
			if preInsert.HasTargetSelector && len(filledVecIdx) > 0 {
				// The selector branch validates and fills the shrunk clone.
				// Mirror the adjusted cells back so the full-width output
				// batch carries them, exactly like the auto-increment column.
				for _, j := range filledVecIdx {
					vecIdx := int(preInsert.ColOffset) + j
					for selectedIdx, inputIdx := range selectedRows {
						if cerr := preInsert.ctr.buf.Vecs[vecIdx].Copy(
							workBat.Vecs[vecIdx], inputIdx, int64(selectedIdx), proc.Mp()); cerr != nil {
							return result, cerr
						}
					}
				}
			}
		} else if err = colexec.BatchDataNotNullCheck(tempVecs, preInsert.Attrs, preInsert.TableDef, proc.Ctx); err != nil {
			return result, err
		}
	}

	if err = preInsert.constructHiddenColBuf(proc, bat, first); err != nil {
		return result, err
	}
	provenanceBat := bat
	if preInsert.TrackODKUResult {
		provenanceBat = workBat
	}
	if err = preInsert.constructAutoIncrementGeneratedCol(proc, provenanceBat, first); err != nil {
		return result, err
	}

	// The pinned single-column NULL primary-key drop runs here, after the
	// whole output row image (hidden helpers and provenance columns
	// included) is materialized but before any downstream consumer
	// evaluates the batch: the CHECK filter, unique-key arbitration, the
	// foreign-key check, the parent-key lock, index encoding and the write
	// never see a row whose resolvable key cell still carries its supplied
	// NULL. The pre-insert fill above already published those rows' other
	// per-cell warnings, so the only diagnostics published here are the
	// per-row key warnings. Selector pipelines keep the write-side backstop,
	// and UPDATE pipelines never reach this path.
	if !preInsert.HasTargetSelector && fillSkipAttr >= 0 && fillCols != nil {
		if err = preInsert.dropNullPkRowsForIgnoreFill(proc, fillSkipAttr, fillCols); err != nil {
			return result, err
		}
	}

	result.Batch = preInsert.ctr.buf
	return result, nil
}

func (preInsert *PreInsert) captureODKUInputOrdinals(proc *proc, bat *batch.Batch) error {
	if !preInsert.TrackODKUResult {
		return nil
	}
	if bat == nil {
		return moerr.NewInternalError(proc.Ctx, "ODKU input ordinal capture received nil batch")
	}
	start, err := proc.NextODKUInputOrdinal(bat.RowCount())
	if err != nil {
		return err
	}
	preInsert.ctr.odkuOrdinals = make([]uint64, bat.RowCount())
	for i := range preInsert.ctr.odkuOrdinals {
		preInsert.ctr.odkuOrdinals[i] = start + uint64(i)
	}
	return nil
}

func (preInsert *PreInsert) captureAutoIncrementGeneratedRows(bat *batch.Batch) error {
	if !preInsert.TrackAutoIncrementGenerated && !preInsert.TrackODKUResult {
		return nil
	}
	autoCol := int(preInsert.ODKUAutoIncrementColumn)
	if preInsert.TrackODKUResult {
		// ODKU carries the selected user column explicitly. MatrixOne tables can
		// also contain a hidden fake primary-key allocator, which must never be
		// mistaken for the column whose generated value is published.
		if autoCol < 0 || autoCol >= len(preInsert.TableDef.Cols) {
			return moerr.NewInvalidInputNoCtx("invalid ODKU auto-increment source column")
		}
	} else {
		autoCol = -1
		for i, col := range preInsert.TableDef.Cols {
			if !col.Typ.AutoIncr {
				continue
			}
			if autoCol >= 0 {
				return moerr.NewInvalidInputNoCtx("auto-increment provenance requires one auto-increment column")
			}
			autoCol = i
		}
	}
	if autoCol < 0 {
		return moerr.NewInvalidInputNoCtx("auto-increment provenance has no auto-increment column")
	}
	pos := int(preInsert.ColOffset) + autoCol
	if pos < 0 || pos >= len(bat.Vecs) {
		return moerr.NewInternalErrorNoCtxf(
			"auto-increment provenance input column %d is outside batch width %d",
			pos, len(bat.Vecs))
	}
	vec := bat.Vecs[pos]
	generated := make([]bool, bat.RowCount())
	for row := range generated {
		generated[row] = vec.IsNull(uint64(row))
	}
	preInsert.ctr.autoIncrementGenerated = generated
	return nil
}

func (preInsert *PreInsert) constructAutoIncrementGeneratedCol(
	proc *proc,
	bat *batch.Batch,
	first bool,
) error {
	if !preInsert.TrackAutoIncrementGenerated && !preInsert.TrackODKUResult {
		return nil
	}
	markerPos := int(preInsert.AutoIncrementGeneratedColumn)
	if markerPos < 0 {
		return moerr.NewInvalidInput(proc.Ctx, "invalid auto-increment provenance column")
	}
	if first {
		if markerPos != len(preInsert.ctr.buf.Vecs) {
			return moerr.NewInternalErrorf(proc.Ctx,
				"auto-increment provenance column %d does not follow PRE_INSERT output width %d",
				markerPos, len(preInsert.ctr.buf.Vecs))
		}
		preInsert.ctr.canFreeVecIdx[markerPos] = true
		preInsert.ctr.ownedVecSlots[markerPos] = true
		preInsert.ctr.buf.Vecs = append(preInsert.ctr.buf.Vecs,
			vector.NewOffHeapVecWithType(types.T_bool.ToType()))
		preInsert.ctr.buf.Attrs = append(preInsert.ctr.buf.Attrs, autoIncrementGeneratedAttr)
		if preInsert.TrackODKUResult {
			ordinalPos := int(preInsert.ODKUOrdinalColumn)
			if ordinalPos != len(preInsert.ctr.buf.Vecs) {
				return moerr.NewInternalErrorf(proc.Ctx,
					"ODKU input ordinal column %d does not follow provenance output width %d",
					ordinalPos, len(preInsert.ctr.buf.Vecs))
			}
			preInsert.ctr.canFreeVecIdx[ordinalPos] = true
			preInsert.ctr.ownedVecSlots[ordinalPos] = true
			preInsert.ctr.buf.Vecs = append(preInsert.ctr.buf.Vecs,
				vector.NewOffHeapVecWithType(types.T_uint64.ToType()))
			preInsert.ctr.buf.Attrs = append(preInsert.ctr.buf.Attrs, odkuInputOrdinalAttr)
		}
	} else {
		if markerPos >= len(preInsert.ctr.buf.Vecs) {
			return moerr.NewInternalErrorf(proc.Ctx,
				"auto-increment provenance column %d is outside PRE_INSERT output width %d",
				markerPos, len(preInsert.ctr.buf.Vecs))
		}
		preInsert.ctr.buf.Vecs[markerPos].CleanOnlyData()
		if preInsert.TrackODKUResult {
			ordinalPos := int(preInsert.ODKUOrdinalColumn)
			if ordinalPos < 0 || ordinalPos >= len(preInsert.ctr.buf.Vecs) {
				return moerr.NewInternalErrorf(proc.Ctx,
					"ODKU input ordinal column %d is outside PRE_INSERT output width %d",
					ordinalPos, len(preInsert.ctr.buf.Vecs))
			}
			preInsert.ctr.buf.Vecs[ordinalPos].CleanOnlyData()
		}
	}
	generated := preInsert.ctr.autoIncrementGenerated
	preInsert.ctr.autoIncrementGenerated = nil
	if len(generated) != bat.RowCount() {
		return moerr.NewInternalErrorf(proc.Ctx,
			"auto-increment provenance rows %d do not match batch rows %d",
			len(generated), bat.RowCount())
	}
	for _, value := range generated {
		if err := vector.AppendFixed(
			preInsert.ctr.buf.Vecs[markerPos], value, false, proc.Mp()); err != nil {
			return err
		}
	}
	if preInsert.TrackODKUResult {
		ordinals := preInsert.ctr.odkuOrdinals
		preInsert.ctr.odkuOrdinals = nil
		if len(ordinals) != bat.RowCount() {
			return moerr.NewInternalErrorf(proc.Ctx,
				"ODKU input ordinal rows %d do not match batch rows %d",
				len(ordinals), bat.RowCount())
		}
		ordinalPos := int(preInsert.ODKUOrdinalColumn)
		for _, ordinal := range ordinals {
			if err := vector.AppendFixed(
				preInsert.ctr.buf.Vecs[ordinalPos], ordinal, false, proc.Mp()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (preInsert *PreInsert) targetSelectedRows(proc *proc, bat *batch.Batch) ([]int64, error) {
	rowNumberPos := int(preInsert.TargetRowNumberCol)
	activePos := int(preInsert.TargetActiveCol)
	rowIDPos := int(preInsert.TargetRowIDCol)
	if rowNumberPos < 0 || rowNumberPos >= len(bat.Vecs) || activePos < 0 || activePos >= len(bat.Vecs) ||
		rowIDPos < 0 || rowIDPos >= len(bat.Vecs) {
		return nil, moerr.NewInternalErrorf(
			proc.Ctx,
			"invalid pre-insert target selector columns: row_number=%d active=%d row_id=%d input=%d",
			rowNumberPos, activePos, rowIDPos, len(bat.Vecs),
		)
	}
	rowNumberVec := bat.Vecs[rowNumberPos]
	activeVec := bat.Vecs[activePos]
	rowIDVec := bat.Vecs[rowIDPos]
	if rowNumberVec.GetType().Oid != types.T_int64 || activeVec.GetType().Oid != types.T_bool ||
		rowIDVec.GetType().Oid != types.T_Rowid {
		return nil, moerr.NewInternalError(proc.Ctx, "invalid pre-insert target selector types")
	}
	selected := make([]int64, 0, bat.RowCount())
	for row := 0; row < bat.RowCount(); row++ {
		if rowNumberVec.IsNull(uint64(row)) || activeVec.IsNull(uint64(row)) || rowIDVec.IsNull(uint64(row)) ||
			vector.GetFixedAtNoTypeCheck[int64](rowNumberVec, row) != 1 ||
			!vector.GetFixedAtNoTypeCheck[bool](activeVec, row) {
			continue
		}
		selected = append(selected, int64(row))
	}
	return selected, nil
}

func shouldTreatZeroAsAutoIncr(proc *proc) bool {
	if proc != nil {
		if ses := proc.GetSession(); ses != nil {
			if hasNoAutoValueOnZero, ok := ses.GetSqlModeNoAutoValueOnZero(); ok {
				return !hasNoAutoValueOnZero
			}
		}
	}
	resolve := proc.GetResolveVariableFunc()
	if resolve == nil {
		return false
	}
	value, err := resolve("sql_mode", true, false)
	if err != nil {
		return false
	}
	mode, ok := value.(string)
	if !ok {
		return false
	}
	return !strings.Contains(strings.ToUpper(mode), "NO_AUTO_VALUE_ON_ZERO")
}

func shouldConvertZeroToNull(preInsert *PreInsert, proc *proc) bool {
	if preInsert.IsOldUpdate || preInsert.IsNewUpdate {
		return false
	}
	return shouldTreatZeroAsAutoIncr(proc)
}

// checkZeroTemporalInStrictMode covers expression-produced values that bypass
// literal conversion. It also enforces the unconditional TIMESTAMP lower bound.
func checkZeroTemporalInStrictMode(preInsert *PreInsert, bat *batch.Batch, proc *proc) error {
	if preInsert == nil || bat == nil || proc == nil {
		return nil
	}

	for idx := range preInsert.Attrs {
		vecIdx := int(preInsert.ColOffset) + idx
		if vecIdx >= len(bat.Vecs) || bat.Vecs[vecIdx] == nil {
			continue
		}
		vec := bat.Vecs[vecIdx]
		switch vec.GetType().Oid {
		case types.T_timestamp:
		case types.T_date, types.T_datetime:
			if !preInsert.RejectZeroTemporal {
				continue
			}
		default:
			continue
		}
		for row := 0; row < vec.Length(); row++ {
			if vec.IsNull(uint64(row)) {
				continue
			}
			switch vec.GetType().Oid {
			case types.T_date:
				if preInsert.RejectZeroTemporal && vector.GetFixedAtNoTypeCheck[types.Date](vec, row) == types.ZeroDate {
					return moerr.NewTruncatedValueForField(proc.Ctx, "date", "0000-00-00", "value", row+1)
				}
			case types.T_datetime:
				if preInsert.RejectZeroTemporal && vector.GetFixedAtNoTypeCheck[types.Datetime](vec, row) == types.ZeroDatetime {
					return moerr.NewTruncatedValueForField(proc.Ctx, "datetime", "0000-00-00 00:00:00", "value", row+1)
				}
			case types.T_timestamp:
				timestamp := vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, row)
				if !types.ValidTimestamp(timestamp) {
					return moerr.NewTruncatedValueForField(proc.Ctx, "datetime", fmt.Sprintf("%d", int64(timestamp)), "value", row+1)
				}
				if preInsert.RejectZeroTemporal && timestamp == types.ZeroTimestamp {
					return moerr.NewTruncatedValueForField(proc.Ctx, "datetime", "0000-00-00 00:00:00", "value", row+1)
				}
			}
		}
	}
	return nil
}

func convertZeroToNull(bat *batch.Batch, preInsert *PreInsert) {
	for i, col := range preInsert.TableDef.Cols {
		if !col.Typ.AutoIncr {
			continue
		}
		idx := preInsert.ColOffset + int32(i)
		vec := bat.GetVector(idx)
		if vec == nil || vec.Length() == 0 {
			continue
		}
		switch vec.GetType().Oid {
		case types.T_int8:
			convertZeroToNullFixed(vector.MustFixedColWithTypeCheck[int8](vec), vec.GetNulls())
		case types.T_int16:
			convertZeroToNullFixed(vector.MustFixedColWithTypeCheck[int16](vec), vec.GetNulls())
		case types.T_int32:
			convertZeroToNullFixed(vector.MustFixedColWithTypeCheck[int32](vec), vec.GetNulls())
		case types.T_int64:
			convertZeroToNullFixed(vector.MustFixedColWithTypeCheck[int64](vec), vec.GetNulls())
		case types.T_uint8:
			convertZeroToNullFixed(vector.MustFixedColWithTypeCheck[uint8](vec), vec.GetNulls())
		case types.T_uint16:
			convertZeroToNullFixed(vector.MustFixedColWithTypeCheck[uint16](vec), vec.GetNulls())
		case types.T_uint32:
			convertZeroToNullFixed(vector.MustFixedColWithTypeCheck[uint32](vec), vec.GetNulls())
		case types.T_uint64:
			convertZeroToNullFixed(vector.MustFixedColWithTypeCheck[uint64](vec), vec.GetNulls())
		}
	}
}

func convertZeroToNullFixed[T constraints.Integer](vals []T, nsp *nulls.Nulls) {
	for i, v := range vals {
		if v == 0 {
			nulls.Add(nsp, uint64(i))
		}
	}
}

// Need to checks if the value generated by incrservice has been manually inserted by the user before, if not sure, it needs to be regenerated
// Otherwise, the SQL executed by the user may have an unexplained Duplicate error
// when the unique detection is optimized away, there may even be a correctness problem
// Therefore, checking with PrimaryKeysMayBeModified is normally required, except for the following three scenarios
func checkIfNeedReGenAutoIncrCol(bat *batch.Batch, preInsert *PreInsert) map[string]int {
	needCheck := make(map[string]int)

	var pkSet map[string]bool
	if preInsert.TableDef.IsTemporary || preInsert.TableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		// Temporary tables and internal fake primary keys skip the persistent duplicate probe.
		// Users cannot provide values for __mo_fake_pk_col.
	} else {
		pkSet = make(map[string]bool)
		for _, n := range preInsert.TableDef.Pkey.Names {
			pkSet[n] = true
		}
		for i, col := range preInsert.TableDef.Cols {
			if pkSet[col.Name] && col.Typ.AutoIncr {
				vec := bat.GetVector(preInsert.ColOffset + int32(i))
				if vec.AllNull() {
					needCheck[col.Name] = i
				}
			}
		}
	}
	return needCheck
}

func genAutoIncrCol(bat *batch.Batch, proc *proc, preInsert *PreInsert) error {
	eng := proc.Base.SessionInfo.StorageEngine
	currentTxn := proc.Base.TxnOperator
	retriedWithFreshTableID := false

retryInsertValues:
	tableID := preInsert.ctr.tblId
	policyCtx := incrservice.WithAutoIDCachePolicy(proc.Ctx, preInsert.TableDef.TblId, preInsert.TableDef.AutoIdCache)
	needReCheck := checkIfNeedReGenAutoIncrCol(bat, preInsert)

	// Capture the oldest active range's allocation timestamp before InsertValues.
	// InsertValues may consume that range and advance the cache to a newer one,
	// but conflict detection must still cover every value generated for this batch.
	lastAllocateTSMap := make(map[string]timestamp.Timestamp)
	for col := range needReCheck {
		ts, err := proc.GetIncrService().GetLastAllocateTS(policyCtx, tableID, preInsert.TableDef.AutoIncrEpoch, currentTxn, col)
		if err != nil {
			return err
		}
		lastAllocateTSMap[col] = ts
	}

	options := incrservice.NormalizeAutoIncrementOptions(
		proc.GetSessionInfo().AutoIncrementIncrement,
		proc.GetSessionInfo().AutoIncrementOffset,
	)
	autoIncrementCtx := incrservice.WithAutoIncrementOptions(
		policyCtx, options.Increment, options.Offset)
	lastInsertValue, err := proc.GetIncrService().InsertValues(
		autoIncrementCtx,
		tableID,
		preInsert.TableDef.AutoIncrEpoch,
		currentTxn,
		bat.Vecs[preInsert.ColOffset:int(preInsert.ColOffset)+len(preInsert.Attrs)],
		bat.RowCount(),
		preInsert.EstimatedRowCount,
	)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			if preInsert.TableDef.IsTemporary {
				logutil.Error("insert auto increment column failed", zap.Error(err))
				return moerr.NewNoSuchTableNoCtx(preInsert.SchemaName, preInsert.TableDef.Name)
			}
			if !retriedWithFreshTableID {
				retriedWithFreshTableID = true
				if refreshErr := preInsert.refreshAutoIncrementTableID(proc); refreshErr != nil {
					if moerr.IsMoErrCode(refreshErr, moerr.ErrNoSuchTable) {
						return preInsert.retryWithDefChanged()
					}
					return refreshErr
				}
				if preInsert.ctr.tblId != tableID {
					goto retryInsertValues
				}
			}
			logutil.Error("insert auto increment column failed", zap.Error(err))
			return preInsert.retryWithDefChanged()
		}
		return err
	}

	if len(needReCheck) > 0 {
		if currentTxn.GetWorkspace() == nil {
			// currently, workspace does not support remote access,
			// so a new workspace is needed for remote CN to check if the value generated by incrservice has been manually inserted by the user before
			ws := disttae.NewTxnWorkSpace(eng.(*disttae.Engine), proc)
			currentTxn.AddWorkspace(ws)
			ws.BindTxnOp(currentTxn)
		}
		db, err := eng.Database(proc.Ctx, preInsert.SchemaName, currentTxn)
		if err != nil {
			return err
		}
		rel, err := db.Relation(proc.Ctx, preInsert.TableDef.Name, nil)
		if err != nil {
			return err
		}

		// Use the captured lastAllocateAt timestamps (before InsertValues)
		for col, idx := range needReCheck {
			fromTs := types.TimestampToTS(lastAllocateTSMap[col])
			toTs := types.TimestampToTS(proc.Base.TxnOperator.SnapshotTS())
			if mayChanged, err := rel.PrimaryKeysMayBeUpserted(proc.Ctx, fromTs, toTs, bat, preInsert.ColOffset+int32(idx)); err == nil {
				if mayChanged {
					return moerr.NewTxnNeedRetry(proc.Ctx)
				}
			} else {
				if moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) {
					refreshed, refreshErr := proc.GetIncrService().ReloadForPKRetry(proc.Ctx, tableID, preInsert.TableDef.AutoIncrEpoch, currentTxn)
					if refreshErr != nil {
						return refreshErr
					}
					if !refreshed {
						return moerr.NewInternalError(proc.Ctx, "AUTO_INCREMENT PK history is unavailable for transaction-private state; roll back and retry the transaction")
					}
				}
				return err
			}
		}
	}

	if lastInsertValue != 0 && !preInsert.TrackAutoIncrementGenerated && !preInsert.TrackODKUResult {
		// A parallel INSERT ... SELECT has one PreInsert operator per scope,
		// all sharing the statement-wide process state.  Publish the smallest
		// generated value through the shared coordinator so scheduling cannot make
		// LAST_INSERT_ID depend on which scope happens to finish first.  Subsequent
		// batches in a serial scope naturally keep the first value because
		// auto-increment allocations are monotonic.
		proc.SetStatementLastInsertIDIfEarlier(lastInsertValue)
	}
	return nil
}
