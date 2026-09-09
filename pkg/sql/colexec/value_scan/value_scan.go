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
	for i, expr := range exprExecs {
		bats := []*batch.Batch{batch.EmptyForConstFoldBatch}
		sourceRow := int64(0)
		if rowsetExprHasLocalColumnRef(rowsetExpr[i].Expr) {
			// A row-local DEFAULT dependency must read the value already
			// materialized in an earlier VALUE_SCAN column. Evaluating against the
			// empty constant-fold batch would either fail the column lookup or
			// force the dependent default to replay a volatile expression.
			bats = []*batch.Batch{input}
			sourceRow = int64(rowsetExpr[i].RowPos)
		}
		val, err := expr.Eval(proc, bats, nil)
		if err != nil {
			return err
		}
		// Constant/folded executors return a one-value vector while a
		// row-dependent executor returns the full input cardinality. Keep the
		// historical scalar behavior for the former and select this row for the
		// latter.
		if sourceRow >= int64(val.Length()) {
			sourceRow = 0
		}
		if err := vec.Copy(val, int64(rowsetExpr[i].RowPos), sourceRow, proc.Mp()); err != nil {
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
	deps := make([]map[int]struct{}, columnCount)
	for colIdx, col := range rowsetData.Cols {
		if col == nil {
			continue
		}
		for _, rowExpr := range col.Data {
			if rowExpr == nil {
				return nil, moerr.NewInternalErrorNoCtxf(
					"value scan has a nil rowset expression in column %d", colIdx)
			}
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
	exprExecLists := make([][]colexec.ExpressionExecutor, len(valueScan.RowsetData.Cols))
	for i, col := range valueScan.RowsetData.Cols {
		var exprExecList []colexec.ExpressionExecutor
		for _, data := range col.Data {
			exprExecutor, err := colexec.NewExpressionExecutor(proc, data.Expr)
			if err != nil {
				valueScan.ExprExecLists = exprExecLists
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
