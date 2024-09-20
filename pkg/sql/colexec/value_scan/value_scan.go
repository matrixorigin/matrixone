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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "value_scan"

func (valueScan *ValueScan) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": value_scan ")
}

func (valueScan *ValueScan) OpType() vm.OpType {
	return vm.ValueScan
}

func evalRowsetData(proc *process.Process, vec *vector.Vector, exprExecs []colexec.ExpressionExecutor,
) error {
	var bats []*batch.Batch

	vec.ResetArea()
	bats = []*batch.Batch{batch.EmptyForConstFoldBatch}
	for i, expr := range exprExecs {
		if err := vector.AppendBytes(vec, nil, true, proc.Mp()); err != nil {
			return err
		}
		val, err := expr.Eval(proc, bats, nil)
		if err != nil {
			return err
		}
		if err := vec.Copy(val, int64(i), 0, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func (valueScan *ValueScan) makeValueScanBatch(proc *process.Process) (err error) {
	var exprList []colexec.ExpressionExecutor

	if valueScan.RowsetData == nil { // select 1,2
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp())
		bat.SetRowCount(1)
		valueScan.Batchs = append(valueScan.Batchs, bat)
		valueScan.Batchs = append(valueScan.Batchs, nil)
		return nil
	}

	if valueScan.ExprExecList == nil {
		if err := valueScan.InitExprExecList(proc); err != nil {
			return err
		}
	}

	// select * from (values row(1,1), row(2,2), row(3,3)) a;
	bat := valueScan.Batchs[0]

	for i := 0; i < valueScan.ColCount; i++ {
		exprList = valueScan.ExprExecList[i]
		vec := bat.Vecs[i]
		if err := evalRowsetData(proc, vec, exprList); err != nil {
			return err
		}
	}

	return nil
}

func (valueScan *ValueScan) InitExprExecList(proc *process.Process) error {
	exprExecList := make([][]colexec.ExpressionExecutor, len(valueScan.RowsetData.Cols))
	for i, col := range valueScan.RowsetData.Cols {
		vec := vector.NewVec(plan.MakeTypeByPlan2Expr(col.Data[0].Expr))
		valueScan.Batchs[0].Vecs[i] = vec
		exprExecList[i] = make([]colexec.ExpressionExecutor, 0, len(col.Data))
		for _, data := range col.Data {
			exprExecutor, err := colexec.NewExpressionExecutor(proc, data.Expr)
			if err != nil {
				return err
			}
			exprExecList[i] = append(exprExecList[i], exprExecutor)
		}
	}

	valueScan.ExprExecList = exprExecList
	return nil
}

func (valueScan *ValueScan) Prepare(proc *process.Process) (err error) {
	if valueScan.OpAnalyzer == nil {
		valueScan.OpAnalyzer = process.NewAnalyzer(valueScan.GetIdx(), valueScan.IsFirst, valueScan.IsLast, "value_scan")
	} else {
		valueScan.OpAnalyzer.Reset()
	}

	err = valueScan.PrepareProjection(proc)
	if err != nil {
		return err
	}
	if valueScan.NodeType == plan2.Node_VALUE_SCAN {
		err := valueScan.makeValueScanBatch(proc)
		if err != nil {
			return err
		}
	}

	return
}

func (valueScan *ValueScan) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := valueScan.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	result := vm.NewCallResult()

	if valueScan.ctr.idx < len(valueScan.Batchs) {
		result.Batch = valueScan.Batchs[valueScan.ctr.idx]
		valueScan.ctr.idx += 1
	}
	var err error
	result.Batch, err = valueScan.EvalProjection(result.Batch, proc)

	analyzer.Input(result.Batch)
	analyzer.Output(result.Batch)
	return result, err

}
