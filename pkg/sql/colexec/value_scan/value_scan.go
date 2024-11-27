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

func evalRowsetData(proc *process.Process, rowsetExpr []*plan.RowsetExpr, vec *vector.Vector, exprExecs []colexec.ExpressionExecutor,
) error {
	bats := []*batch.Batch{batch.EmptyForConstFoldBatch}
	for i, expr := range exprExecs {
		val, err := expr.Eval(proc, bats, nil)
		if err != nil {
			return err
		}
		if err := vec.Copy(val, int64(rowsetExpr[i].RowPos), 0, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
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

	for i := 0; i < valueScan.ColCount; i++ {
		exprList = valueScan.ExprExecLists[i]
		if len(exprList) == 0 {
			continue
		}
		vec := bat.Vecs[i]
		if err := evalRowsetData(proc, valueScan.RowsetData.Cols[i].Data, vec, exprList); err != nil {
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
