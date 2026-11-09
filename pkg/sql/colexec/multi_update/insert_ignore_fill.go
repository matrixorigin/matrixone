// Copyright 2026 MatrixOne
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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// insertNotNullFillOwned reports whether this operator is executing a
// statement whose pipeline owns INSERT semantics for the IGNORE NOT NULL
// adjustment: INSERT (plain and IGNORE), LOAD with the IGNORE duplicate
// policy, and INSERT-shaped ON DUPLICATE KEY statements whose main target
// only inserts. An UPDATE pipeline (and REPLACE) instead pairs every insert
// leg with a delete leg, so the main context carries DeleteCols; for those
// statements the fill and the row drop are not applied and the strict NOT
// NULL error keeps its historical behavior, leaving UPDATE IGNORE and
// REPLACE outside this INSERT-scoped fix.
func (update *MultiUpdate) insertNotNullFillOwned() bool {
	return len(update.MultiUpdateCtx) > 0 && len(update.MultiUpdateCtx[0].DeleteCols) == 0
}

// fillMainInsertNotNullForIgnore applies the statement IGNORE rule to the
// main insert legs of INSERT-owned statements. Under IGNORE it first
// adjusts every NULL cell of a NOT NULL column to the implicit default of
// the column type with one ER_BAD_NULL_ERROR warning per cell, keeping the
// row instead of failing the statement, like the pre-insert pipeline does.
// It then drops every row whose resolvable single-column primary key still
// carries its supplied NULL, publishing one further ER_BAD_NULL_ERROR
// warning per dropped row: a main-table entry cannot carry a NULL primary
// key, and adjusting it instead would let several adjusted rows claim the
// same primary key inside one statement before any duplicate arbitration
// can see it. Dropping such rows keeps the primary-key invariant and
// matches the pinned skip-rows contract of the report; MySQL instead
// adjusts the key to the implicit default and lets uniqueness filter the
// duplicates, which is documented as a known divergence. The fill runs
// before the drop so a dropped row still publishes the warnings of its
// other supplied-NULL cells, exactly like a row the pre-insert pass drops.
//
// The pre-insert fill normally runs before this leg for INSERT IGNORE
// statements (the planner forces the PRE_INSERT node whenever a supplied
// column enforces NOT NULL), so this fill remains a no-op backstop there
// and the live path for pipelines without that node, such as LOAD legs.
//
// useInsertIdx selects the vector layout. The operator's input batch keeps
// table columns at InsertCols[inputIdx]; the shrunk write batch built by
// check_null_and_insert_main_table keeps them at the InsertCols index
// position. insertOwned is computed by the caller from the statement-owned
// shape (see insertNotNullFillOwned) so UPDATE-style pipelines keep the
// strict check. The function reports false when the statement does not run
// under IGNORE so callers keep the strict check unchanged, and reports
// droppedRows=true only when the batch also lost rows, so derived batches
// sharing the vectors can be re-counted. Cluster-by helper columns are
// skipped exactly like the strict check does.
func fillMainInsertNotNullForIgnore(
	proc *process.Process,
	updateCtx *MultiUpdateCtx,
	bat *batch.Batch,
	useInsertIdx bool,
	insertOwned bool,
) (filled, droppedRows bool, err error) {
	if !insertOwned ||
		proc == nil || proc.GetStmtProfile() == nil || !proc.GetStmtProfile().GetStatementIgnore() {
		return false, false, nil
	}
	// The pinned drop key is resolved from TableDef.Pkey by name: ordinary
	// INSERT plans never populate InsertPkColIdx (it stays a REPLACE-only
	// field), so any position taken from that field resolves the key to
	// column 0 and moves the rule sideways for tables whose primary key is
	// not the first column. The resolved key column is excluded from the
	// fill so its cell keeps the NULL that identifies the row for the
	// drop below.
	pkColIdx := colexec.IgnoreNullPkColumnIdx(updateCtx.TableDef)
	var (
		vecs       = make([]*vector.Vector, 0, len(updateCtx.InsertCols))
		attrs      = make([]string, 0, len(updateCtx.InsertCols))
		cols       = make([]*plan.ColDef, 0, len(updateCtx.InsertCols))
		constSlots [][2]int
	)
	for insertIdx, inputIdx := range updateCtx.InsertCols {
		if insertIdx >= len(updateCtx.TableDef.Cols) {
			continue
		}
		if insertIdx == pkColIdx {
			continue
		}
		col := updateCtx.TableDef.Cols[insertIdx]
		if col == nil || strings.HasPrefix(col.Name, catalog.PrefixCBColName) {
			continue
		}
		if useInsertIdx {
			inputIdx = insertIdx
		}
		if inputIdx < 0 || inputIdx >= len(bat.Vecs) || bat.Vecs[inputIdx] == nil {
			continue
		}
		vecs = append(vecs, bat.Vecs[inputIdx])
		attrs = append(attrs, col.Name)
		cols = append(cols, col)
		// vecs is a fresh list, so a constant NULL the shared fill replaces
		// (it owns no backing storage) must be installed back into the
		// batch the write legs read.
		if bat.Vecs[inputIdx].IsConst() {
			constSlots = append(constSlots, [2]int{len(vecs) - 1, inputIdx})
		}
	}
	if _, err := colexec.BatchDataNotNullFillForIgnore(vecs, attrs, cols, proc); err != nil {
		return true, false, err
	}
	for _, slot := range constSlots {
		if vecs[slot[0]] != bat.Vecs[slot[1]] {
			bat.Vecs[slot[1]] = vecs[slot[0]]
		}
	}
	if pkColIdx < 0 {
		return true, false, nil
	}
	inputIdx := updateCtx.InsertCols[pkColIdx]
	if useInsertIdx {
		inputIdx = pkColIdx
	}
	if inputIdx < 0 || inputIdx >= len(bat.Vecs) || bat.Vecs[inputIdx] == nil {
		return true, false, nil
	}
	droppedRows, err = colexec.DropNullPkRowsForIgnore(
		proc, bat.Vecs[inputIdx], updateCtx.TableDef.Cols[pkColIdx].Name, bat)
	return true, droppedRows, err
}
