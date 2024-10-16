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
	"fmt"
	util2 "github.com/matrixorigin/matrixone/pkg/common/util"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (valueScan *ValueScan) String(buf *bytes.Buffer) {
	buf.WriteString(thisOperator + ": value_scan")
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

	if valueScan.dataInProcess && len(valueScan.Batchs) == 0 {
		valueScan.Batchs = append(valueScan.Batchs, nil, nil)
		valueScan.Batchs[0], err = valueScan.getReadOnlyBatchFromProcess(proc)
	}
	return err
}

func (valueScan *ValueScan) Call(proc *process.Process) (vm.CallResult, error) {
	err, isCancel := vm.CancelCheck(proc)
	if isCancel {
		return vm.CancelResult, err
	}
	analyzer := valueScan.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	result := vm.NewCallResult()
	if valueScan.runningCtx.nowIdx < len(valueScan.Batchs) {
		result.Batch = valueScan.Batchs[valueScan.runningCtx.nowIdx]

		if valueScan.runningCtx.nowIdx > 0 {
			if !valueScan.dataInProcess {
				valueScan.Batchs[valueScan.runningCtx.nowIdx-1].Clean(proc.GetMPool())
				valueScan.Batchs[valueScan.runningCtx.nowIdx-1] = nil
			}
		}
		valueScan.runningCtx.nowIdx++
	}

	result.Batch, err = valueScan.EvalProjection(result.Batch, proc)
	analyzer.Input(result.Batch)
	analyzer.Output(result.Batch)

	return result, err
}

func (valueScan *ValueScan) getReadOnlyBatchFromProcess(proc *process.Process) (bat *batch.Batch, err error) {
	// if this is a select without source table.
	// for example, select 1.
	if valueScan.RowsetData == nil {
		return batch.EmptyForConstFoldBatch, nil
	}

	// Do Type Check.
	// this is an execute sql for prepared-stmt: execute s1 and s1 is `insert into t select 1.`
	// this is direct value_scan.
	if bat = proc.GetPrepareBatch(); bat == nil {
		if bat = proc.GetValueScanBatch(uuid.UUID(valueScan.Uuid)); bat == nil {
			return nil, moerr.NewInfo(proc.Ctx, fmt.Sprintf("makeValueScanBatch failed, node id: %s", uuid.UUID(valueScan.Uuid).String()))
		}
	}

	// the following codes were copied from the old makeValueScanBatch.
	if colsData := valueScan.RowsetData.Cols; len(colsData) > 0 {
		var exprExeces []colexec.ExpressionExecutor
		var strParam vector.FunctionParameterWrapper[types.Varlena]

		exprs := proc.GetPrepareExprList()
		if params := proc.GetPrepareParams(); params != nil {
			strParam = vector.GenerateFunctionStrParameter(params)
		}

		for i := 0; i < valueScan.ColCount; i++ {
			if exprs != nil {
				exprExeces = exprs.([][]colexec.ExpressionExecutor)[i]
			}
			if strParam != nil {
				for _, row := range colsData[i].Data {
					if row.Pos >= 0 {
						str, isNull := strParam.GetStrValue(uint64(row.Pos - 1))
						if err = util.SetBytesToAnyVector(
							proc, util2.UnsafeBytesToString(str), int(row.RowPos), isNull, bat.Vecs[i]); err != nil {
							return nil, err
						}
					}
				}
			}

			if err = evalRowsetData(proc, colsData[i].Data, bat.Vecs[i], exprExeces); err != nil {
				return nil, err
			}
		}
	}

	return bat, nil
}

func evalRowsetData(
	proc *process.Process,
	exprs []*plan2.RowsetExpr, vec *vector.Vector, exprExecs []colexec.ExpressionExecutor,
) error {
	vec.ResetArea()
	bats := []*batch.Batch{batch.EmptyForConstFoldBatch}

	if len(exprExecs) > 0 {
		for i, expr := range exprExecs {
			val, err := expr.Eval(proc, bats, nil)
			if err != nil {
				return err
			}
			if err := vec.Copy(val, int64(exprs[i].RowPos), 0, proc.Mp()); err != nil {
				return err
			}
		}
	} else {
		for _, expr := range exprs {
			if expr.Pos >= 0 {
				continue
			}

			executor, err := colexec.NewExpressionExecutor(proc, expr.Expr)
			if err != nil {
				return err
			}
			val, err := executor.Eval(proc, bats, nil)
			if err == nil {
				err = vec.Copy(val, int64(expr.RowPos), 0, proc.Mp())
			}
			executor.Free()

			if err != nil {
				return err
			}
		}
	}
	return nil
}
