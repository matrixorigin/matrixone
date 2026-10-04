// Copyright 2021-2023 Matrix Origin
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

package value_scan

import (
	"bytes"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	oneBatchMaxRow = int(options.DefaultBlockMaxRows)
)

func (valueScan *ValueScan) String(buf *bytes.Buffer) {
	buf.WriteString(thisOperator + ": value_scan")
}

func evalRowsetData(proc *process.Process, rowsetExpr []*plan.RowsetExpr, vec *vector.Vector, exprExecs []colexec.ExpressionExecutor, input *batch.Batch,
) error {
	if vec == nil {
		return moerr.NewInternalErrorNoCtx("value scan has no destination vector")
	}
	var inputWindow *batch.Batch
	var inputWindowRow int32 = -1
	defer func() {
		if inputWindow != nil {
			inputWindow.Clean(nil)
		}
	}()

	for i, expr := range exprExecs {
		if expr == nil || i >= len(rowsetExpr) || rowsetExpr[i] == nil {
			return moerr.NewInternalErrorNoCtxf("value scan has an invalid rowset expression at position %d", i)
		}
		rowPos := rowsetExpr[i].RowPos
		if rowPos < 0 || int(rowPos) >= vec.Length() {
			return moerr.NewInternalErrorNoCtxf(
				"value scan expression has invalid destination row position %d", rowPos)
		}
		bats := []*batch.Batch{batch.EmptyForConstFoldBatch}
		if rowsetExprHasLocalColumnRef(rowsetExpr[i].Expr) {
			// A row-local DEFAULT dependency must read the value already
			// materialized in an earlier VALUE_SCAN column. Evaluate against a
			// one-row window rather than the complete VALUES batch: passing the full
			// batch makes every per-row executor produce and retain an N-row result,
			// turning N row-local defaults into quadratic work and memory.
			if input == nil {
				return moerr.NewInternalErrorNoCtx("value scan row-local expression has no input batch")
			}
			if rowPos < 0 || int(rowPos) >= input.RowCount() {
				return moerr.NewInternalErrorNoCtxf(
					"value scan row-local expression has invalid row position %d", rowPos)
			}
			if inputWindow == nil || inputWindowRow != rowPos {
				if inputWindow != nil {
					inputWindow.Clean(nil)
				}
				var err error
				inputWindow, err = input.Window(int(rowPos), int(rowPos)+1)
				if err != nil {
					return err
				}
				inputWindowRow = rowPos
			}
			bats = []*batch.Batch{inputWindow}
		}
		val, err := expr.Eval(proc, bats, nil)
		if err != nil {
			return err
		}
		if val == nil {
			return moerr.NewInternalErrorNoCtxf("value scan expression at row position %d returned no vector", rowsetExpr[i].RowPos)
		}
		if val.Length() == 0 {
			return moerr.NewInternalErrorNoCtxf("value scan expression at row position %d returned an empty vector", rowsetExpr[i].RowPos)
		}
		// Constant/folded executors return a scalar vector; row-local executors
		// receive a one-row window. Both are copied from their first value.
		if err := vec.Copy(val, int64(rowsetExpr[i].RowPos), 0, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func rowsetExprHasLocalColumnRef(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return impl.Col != nil && impl.Col.RelPos == 0
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			if rowsetExprHasLocalColumnRef(arg) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, item := range impl.List.List {
			if rowsetExprHasLocalColumnRef(item) {
				return true
			}
		}
	}
	return false
}

func collectRowsetLocalColumnRefs(expr *plan.Expr, refs map[int32]struct{}) {
	if expr == nil {
		return
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col != nil && impl.Col.RelPos == 0 {
			refs[impl.Col.ColPos] = struct{}{}
		}
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			collectRowsetLocalColumnRefs(arg, refs)
		}
	case *plan.Expr_List:
		for _, item := range impl.List.List {
			collectRowsetLocalColumnRefs(item, refs)
		}
	}
}

// valueScanColumnOrder returns a dependency order for rowset expressions.
// Explicit INSERT columns may be listed in an order different from the table
// definition, so a later vector can be the source of a DEFAULT expression. A
// VALUE_SCAN evaluates all rowset expressions into one batch; topologically
// ordering the columns guarantees that every local reference reads a
// materialized value rather than an uninitialized vector. Schema validation
// rejects cycles, but retaining a runtime check keeps malformed plans from
// silently producing data.
func valueScanColumnOrder(rowsetData *plan.RowsetData) ([]int, error) {
	if rowsetData == nil {
		return nil, nil
	}
	columnCount := len(rowsetData.Cols)
	var deps []map[int]struct{}
	hasDependency := false
	for colIdx, col := range rowsetData.Cols {
		if col == nil {
			continue
		}
		for _, rowExpr := range col.Data {
			if rowExpr == nil {
				return nil, moerr.NewInternalErrorNoCtxf(
					"value scan has a nil rowset expression in column %d", colIdx)
			}
			// Keep the historical fast path allocation-free for ordinary VALUES:
			// only the exceptional row-local DEFAULT protocol needs a dependency
			// map and topological sort.
			if !rowsetExprHasLocalColumnRef(rowExpr.Expr) {
				continue
			}
			if deps == nil {
				deps = make([]map[int]struct{}, columnCount)
			}
			hasDependency = true
			refs := make(map[int32]struct{})
			collectRowsetLocalColumnRefs(rowExpr.Expr, refs)
			for ref := range refs {
				if ref < 0 || int(ref) >= columnCount {
					return nil, moerr.NewInternalErrorNoCtxf(
						"value scan column %d references column %d outside the rowset",
						colIdx, ref)
				}
				if deps[colIdx] == nil {
					deps[colIdx] = make(map[int]struct{})
				}
				deps[colIdx][int(ref)] = struct{}{}
			}
		}
	}
	if !hasDependency {
		order := make([]int, columnCount)
		for colIdx := range order {
			order[colIdx] = colIdx
		}
		return order, nil
	}

	state := make([]uint8, columnCount) // 0=unvisited, 1=visiting, 2=done
	order := make([]int, 0, columnCount)
	var visit func(int) error
	visit = func(colIdx int) error {
		switch state[colIdx] {
		case 1:
			return moerr.NewInternalErrorNoCtxf(
				"value scan has a circular rowset dependency at column %d", colIdx)
		case 2:
			return nil
		}
		state[colIdx] = 1
		orderedDeps := make([]int, 0, len(deps[colIdx]))
		for dep := range deps[colIdx] {
			orderedDeps = append(orderedDeps, dep)
		}
		sort.Ints(orderedDeps)
		for _, dep := range orderedDeps {
			if err := visit(dep); err != nil {
				return err
			}
		}
		state[colIdx] = 2
		order = append(order, colIdx)
		return nil
	}
	for colIdx := 0; colIdx < columnCount; colIdx++ {
		if err := visit(colIdx); err != nil {
			return nil, err
		}
	}
	return order, nil
}

func (valueScan *ValueScan) makeValueScanBatch(proc *process.Process) (err error) {
	var exprList []colexec.ExpressionExecutor

	if valueScan.RowsetData == nil { // select 1,2
		valueScan.Batchs = append(valueScan.Batchs, batch.EmptyForConstFoldBatch)
		valueScan.Batchs = append(valueScan.Batchs, nil)
		return nil
	}

	if valueScan.ExprExecLists == nil {
		if err := valueScan.InitExprExecList(proc); err != nil {
			return err
		}
	}
	if len(valueScan.Batchs) == 0 || valueScan.Batchs[0] == nil {
		return moerr.NewInternalErrorNoCtx("value scan has no input batch")
	}
	if len(valueScan.RowsetData.Cols) != len(valueScan.Batchs[0].Vecs) ||
		len(valueScan.ExprExecLists) != len(valueScan.RowsetData.Cols) {
		return moerr.NewInternalErrorNoCtx("value scan rowset and batch columns are inconsistent")
	}

	// select * from (values row(1,1), row(2,2), row(3,3)) a;
	bat := valueScan.Batchs[0]

	order, err := valueScanColumnOrder(valueScan.RowsetData)
	if err != nil {
		return err
	}
	for _, i := range order {
		exprList = valueScan.ExprExecLists[i]
		if len(exprList) == 0 {
			continue
		}
		vec := bat.Vecs[i]
		if err := evalRowsetData(proc, valueScan.RowsetData.Cols[i].Data, vec, exprList, bat); err != nil {
			return err
		}
	}

	return nil
}

func (valueScan *ValueScan) InitExprExecList(proc *process.Process) error {
	if valueScan.RowsetData == nil {
		return moerr.NewInternalErrorNoCtx("value scan has no rowset data")
	}
	exprExecLists := make([][]colexec.ExpressionExecutor, len(valueScan.RowsetData.Cols))
	for i, col := range valueScan.RowsetData.Cols {
		if col == nil {
			freeExpressionExecLists(exprExecLists)
			valueScan.ExprExecLists = nil
			return moerr.NewInternalErrorNoCtxf("value scan has a nil column definition at position %d", i)
		}
		var exprExecList []colexec.ExpressionExecutor
		for j, data := range col.Data {
			if data == nil || data.Expr == nil {
				freeExpressionExecLists([][]colexec.ExpressionExecutor{exprExecList})
				freeExpressionExecLists(exprExecLists)
				valueScan.ExprExecLists = nil
				return moerr.NewInternalErrorNoCtxf(
					"value scan has a nil expression at column %d row %d", i, j)
			}
			exprExecutor, err := colexec.NewExpressionExecutor(proc, data.Expr)
			if err != nil {
				freeExpressionExecLists(exprExecLists)
				freeExpressionExecLists([][]colexec.ExpressionExecutor{exprExecList})
				valueScan.ExprExecLists = nil
				return err
			}
			exprExecList = append(exprExecList, exprExecutor)
		}
		exprExecLists[i] = exprExecList
	}

	valueScan.ExprExecLists = exprExecLists
	return nil
}

func (valueScan *ValueScan) Prepare(proc *process.Process) error {
	if valueScan.OpAnalyzer == nil {
		valueScan.OpAnalyzer = process.NewAnalyzer(valueScan.GetIdx(), valueScan.IsFirst, valueScan.IsLast, "value_scan")
	} else {
		valueScan.OpAnalyzer.Reset()
	}

	err := valueScan.PrepareProjection(proc)
	if err != nil {
		return err
	}

	if valueScan.NodeType == plan.Node_VALUE_SCAN {
		err = valueScan.makeValueScanBatch(proc)
		if err != nil {
			return err
		}
	}

	return err
}

func (valueScan *ValueScan) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := valueScan.OpAnalyzer
	var err error
	result := vm.NewCallResult()
	if valueScan.runningCtx.nowIdx < len(valueScan.Batchs) {
		result.Batch, err = valueScan.genSubBatchFromOriginBatch(proc)
		if err != nil {
			return result, err
		}
	}

	analyzer.Input(result.Batch)
	return result, err
}

func (valueScan *ValueScan) genSubBatchFromOriginBatch(proc *process.Process) (*batch.Batch, error) {
	currBat := valueScan.Batchs[valueScan.runningCtx.nowIdx]
	if currBat == nil {
		return nil, nil
	}
	if currBat.RowCount() <= oneBatchMaxRow {
		valueScan.runningCtx.nowIdx++
		return currBat, nil
	}

	valueScan.runningCtx.start = valueScan.runningCtx.end
	valueScan.runningCtx.end += oneBatchMaxRow

	if valueScan.runningCtx.end > currBat.RowCount() {
		valueScan.runningCtx.end = currBat.RowCount()
	}

	if valueScan.runningCtx.start == valueScan.runningCtx.end {
		valueScan.runningCtx.start = 0
		valueScan.runningCtx.end = 0
		valueScan.runningCtx.nowIdx++ // set for next Call

		return valueScan.genSubBatchFromOriginBatch(proc)
	}

	subBatch, err := currBat.Window(
		valueScan.runningCtx.start,
		valueScan.runningCtx.end,
	)
	if err != nil {
		return nil, err
	}
	return subBatch, nil
}
