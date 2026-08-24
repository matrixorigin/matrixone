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

package table_scan

import (
	"context"
	"slices"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Short varlen values are commonly cheaper to fetch with the predicate
// columns in one I/O. At the standard 8192-row object block, a 256-byte
// declared value can represent 2 MiB of logical payload, large enough to
// justify a second selected-row read. Unbounded payload types are always
// candidates.
const lateMaterializationMinVarlenWidth = 256

func isLateMaterializationCandidate(typ plan.Type) bool {
	switch types.T(typ.Id) {
	case types.T_text,
		types.T_blob,
		types.T_json,
		types.T_datalink,
		types.T_geometry,
		types.T_geometry32,
		types.T_array_float32,
		types.T_array_float64,
		types.T_array_bf16,
		types.T_array_float16,
		types.T_array_int8,
		types.T_array_uint8:
		return true
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary:
		return typ.Width >= lateMaterializationMinVarlenWidth
	default:
		return false
	}
}

func collectFilterColumnPositions(expr *plan.Expr, columns []bool) bool {
	if expr == nil {
		return true
	}
	switch item := expr.Expr.(type) {
	case *plan.Expr_Col:
		if item.Col == nil {
			return false
		}
		pos := int(item.Col.ColPos)
		if pos < 0 || pos >= len(columns) {
			return false
		}
		columns[pos] = true
		return true
	case *plan.Expr_F:
		if item.F == nil {
			return false
		}
		for _, arg := range item.F.Args {
			if !collectFilterColumnPositions(arg, columns) {
				return false
			}
		}
		return true
	case *plan.Expr_List:
		if item.List == nil {
			return false
		}
		for _, arg := range item.List.List {
			if !collectFilterColumnPositions(arg, columns) {
				return false
			}
		}
		return true
	case *plan.Expr_Lit:
		if item.Lit == nil {
			return false
		}
		return collectFilterColumnPositions(item.Lit.Src, columns)
	case *plan.Expr_P:
		return item.P != nil
	case *plan.Expr_V:
		return item.V != nil
	case *plan.Expr_T:
		return item.T != nil
	case *plan.Expr_Max:
		return item.Max != nil
	case *plan.Expr_Vec:
		return item.Vec != nil
	case *plan.Expr_Fold:
		return item.Fold != nil
	case *plan.Expr_Raw, *plan.Expr_W, *plan.Expr_Sub, *plan.Expr_Corr:
		// These forms either refer to a column outside the ordinary ColRef tree
		// or require execution at a different logical boundary. Stay eager.
		return false
	default:
		// Future expression forms stay eager until their column ownership is
		// understood here.
		return false
	}
}

func (tableScan *TableScan) configureLateMaterialization() {
	tableScan.ctr.earlyColumns = tableScan.ctr.earlyColumns[:0]
	tableScan.ctr.lateColumns = tableScan.ctr.lateColumns[:0]

	if len(tableScan.ctr.allFilterExecutors) == 0 ||
		len(tableScan.Attrs) != len(tableScan.Types) ||
		len(tableScan.Types) < 2 {
		return
	}

	filterColumns := make([]bool, len(tableScan.Types))
	for _, expr := range tableScan.RuntimeFilterExprs {
		if !collectFilterColumnPositions(expr, filterColumns) {
			return
		}
	}
	for _, expr := range tableScan.FilterExprs {
		if !collectFilterColumnPositions(expr, filterColumns) {
			return
		}
	}
	if !slices.Contains(filterColumns, true) {
		return
	}

	for pos, typ := range tableScan.Types {
		if !filterColumns[pos] && isLateMaterializationCandidate(typ) {
			tableScan.ctr.lateColumns = append(tableScan.ctr.lateColumns, pos)
		} else {
			tableScan.ctr.earlyColumns = append(tableScan.ctr.earlyColumns, pos)
		}
	}
	if len(tableScan.ctr.earlyColumns) == 0 || len(tableScan.ctr.lateColumns) == 0 {
		tableScan.ctr.earlyColumns = tableScan.ctr.earlyColumns[:0]
		tableScan.ctr.lateColumns = tableScan.ctr.lateColumns[:0]
	}
}

func (tableScan *TableScan) batchColumnView(
	bat *batch.Batch,
	positions []int,
	rowCount int,
) *batch.Batch {
	if positions == nil {
		return bat
	}
	if tableScan.ctr.metricView == nil {
		tableScan.ctr.metricView = batch.NewWithSize(len(tableScan.Types))
	}
	view := tableScan.ctr.metricView
	view.Vecs = view.Vecs[:len(positions)]
	for i, pos := range positions {
		view.Vecs[i] = bat.Vecs[pos]
	}
	view.SetRowCount(rowCount)
	return view
}

func (tableScan *TableScan) recordFilterInput(bat *batch.Batch, loadedColumns []int) {
	view := tableScan.batchColumnView(bat, loadedColumns, bat.RowCount())
	tableScan.OpAnalyzer.InputBlock()
	tableScan.OpAnalyzer.Input(view)
	tableScan.OpAnalyzer.ScanBytes(view)
	tableScan.ctr.maxAllocSize = max(tableScan.ctr.maxAllocSize, bat.Size())
	tableScan.ctr.filterReadMetrics = true
}

func (tableScan *TableScan) recordLateInput(bat *batch.Batch) {
	// The rows were already counted when the early columns were filtered. A
	// zero-row view adds only the bytes materialized for surviving rows.
	view := tableScan.batchColumnView(bat, tableScan.ctr.lateColumns, 0)
	tableScan.OpAnalyzer.Input(view)
	tableScan.OpAnalyzer.ScanBytes(view)
	tableScan.ctr.maxAllocSize = max(tableScan.ctr.maxAllocSize, bat.Size())
}

func (tableScan *TableScan) applyReaderFilter(
	proc *process.Process,
	bat *batch.Batch,
	loadedColumns []int,
) (engine.ReaderFilterResult, error) {
	start := time.Now()
	defer func() {
		tableScan.ctr.filterActiveDuration += time.Since(start)
	}()
	if loadedColumns == nil {
		// Eager fallbacks still have a complete pre-filter batch, so preserve the
		// existing transaction data-trace boundary.
		tableScan.traceRead(proc, bat)
	}
	tableScan.recordFilterInput(bat, loadedColumns)
	tableScan.ctr.filterLateMaterialized = loadedColumns != nil
	return tableScan.evalFilter(proc, bat, loadedColumns)
}

func (tableScan *TableScan) readBatch(
	ctx context.Context,
	proc *process.Process,
) (bool, error) {
	lateReader, ok := tableScan.Reader.(engine.LateMaterializationReader)
	// Data tracing records complete pre-filter rows. Preserve that diagnostic
	// contract by using the eager path while the feature is enabled. Reader
	// summaries have the same pre-filter diagnostic contract.
	traceDataEnabled := trace.GetService(proc.GetService()).Enabled(trace.FeatureTraceData)
	readerSummaryEnabled := ctx.Value(defines.ReaderSummaryKey{}) != nil
	if !ok || tableScan.ctr.readerFilter == nil || traceDataEnabled || readerSummaryEnabled {
		return process.MeasureFilesystemWait(tableScan.OpAnalyzer, func() (bool, error) {
			return tableScan.Reader.Read(
				ctx,
				tableScan.Attrs,
				nil,
				proc.Mp(),
				tableScan.ctr.buf,
			)
		})
	}

	return process.MeasureFilesystemWaitExcluding(
		tableScan.OpAnalyzer,
		&tableScan.ctr.filterActiveDuration,
		func() (bool, error) {
			return lateReader.ReadWithFilter(
				ctx,
				tableScan.Attrs,
				tableScan.ctr.earlyColumns,
				tableScan.ctr.readerFilter,
				proc.Mp(),
				tableScan.ctr.buf,
			)
		},
	)
}
