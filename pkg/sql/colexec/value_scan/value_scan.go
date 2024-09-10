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

const opName = "value_scan"

func (valueScan *ValueScan) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": value_scan ")
}

func (valueScan *ValueScan) OpType() vm.OpType {
	return vm.ValueScan
}

func evalRowsetData(proc *process.Process,
	exprs []*plan2.RowsetExpr, vec *vector.Vector, exprExecs []colexec.ExpressionExecutor,
) error {
	var bats []*batch.Batch

	vec.ResetArea()
	bats = []*batch.Batch{batch.EmptyForConstFoldBatch}
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
			val, err := colexec.EvalExpressionOnce(proc, expr.Expr, bats)
			if err != nil {
				return err
			}
			if err := vec.Copy(val, int64(expr.RowPos), 0, proc.Mp()); err != nil {
				val.Free(proc.Mp())
				return err
			}
			val.Free(proc.Mp())
		}
	}
	return nil
}

func (valueScan *ValueScan) makeValueScanBatch(proc *process.Process) (bat *batch.Batch, err error) {
	var exprList []colexec.ExpressionExecutor

	if valueScan.RowsetData == nil { // select 1,2
		bat = batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp())
		bat.SetRowCount(1)
		return bat, nil
	}
	// select * from (values row(1,1), row(2,2), row(3,3)) a;
	var nodeId uuid.UUID
	copy(nodeId[:], valueScan.Uuid)
	bat = proc.GetPrepareBatch()
	if bat == nil {
		bat = proc.GetValueScanBatch(nodeId)
		if bat == nil {
			return nil, moerr.NewInfo(proc.Ctx, fmt.Sprintf("makeValueScanBatch failed, node id: %s", nodeId.String()))
		}
	}

	colsData := valueScan.RowsetData.Cols
	params := proc.GetPrepareParams()
	if len(colsData) > 0 {
		exprs := proc.GetPrepareExprList()
		for i := 0; i < valueScan.ColCount; i++ {
			if exprs != nil {
				exprList = exprs.([][]colexec.ExpressionExecutor)[i]
			}
			if params != nil {
				vs := vector.MustFixedColWithTypeCheck[types.Varlena](params)
				for _, row := range colsData[i].Data {
					if row.Pos >= 0 {
						isNull := params.GetNulls().Contains(uint64(row.Pos - 1))
						str := vs[row.Pos-1].UnsafeGetString(params.GetArea())
						if err := util.SetBytesToAnyVector(proc.Ctx, str, int(row.RowPos), isNull, bat.Vecs[i],
							proc); err != nil {
							return nil, err
						}
					}
				}
			}
			if err := evalRowsetData(proc, colsData[i].Data, bat.Vecs[i], exprList); err != nil {
				return nil, err
			}
		}
	}

	return bat, nil
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
	if valueScan.NodeType == plan2.Node_VALUE_SCAN && valueScan.Batchs == nil {
		bat, err := valueScan.makeValueScanBatch(proc)
		if err != nil {
			return err
		}
		valueScan.Batchs = make([]*batch.Batch, 2)
		valueScan.Batchs[0] = bat
		if bat != nil {
			valueScan.Batchs[1] = nil
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
		if valueScan.ctr.idx > 0 {
			valueScan.Batchs[valueScan.ctr.idx-1].Clean(proc.GetMPool())
			valueScan.Batchs[valueScan.ctr.idx-1] = nil
		}
		valueScan.ctr.idx += 1
	}
	var err error
	result.Batch, err = valueScan.EvalProjection(result.Batch, proc)

	analyzer.Input(result.Batch)
	analyzer.Output(result.Batch)
	return result, err

}
