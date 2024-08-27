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

package onduplicatekey

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "on_duplicate_key"

func (onDuplicatekey *OnDuplicatekey) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": processing on duplicate key before insert")
}

func (onDuplicatekey *OnDuplicatekey) OpType() vm.OpType {
	return vm.OnDuplicateKey
}

func (onDuplicatekey *OnDuplicatekey) Prepare(p *process.Process) (err error) {
	onDuplicatekey.OpAnalyzer = process.NewAnalyzer(onDuplicatekey.GetIdx(), onDuplicatekey.IsFirst, onDuplicatekey.IsLast, "on_duplicate_key")
	if len(onDuplicatekey.ctr.uniqueCheckExes) == 0 {
		onDuplicatekey.ctr.uniqueCheckExes, err = colexec.NewExpressionExecutorsFromPlanExpressions(p, onDuplicatekey.UniqueColCheckExpr)
		if err != nil {
			return
		}
	}
	return
}

func (onDuplicatekey *OnDuplicatekey) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	//anal := proc.GetAnalyze(onDuplicatekey.GetIdx(), onDuplicatekey.GetParallelIdx(), onDuplicatekey.GetParallelMajor())
	//anal.Start()
	//defer anal.Stop()
	analyzer := onDuplicatekey.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	result := vm.NewCallResult()
	for {
		switch onDuplicatekey.ctr.state {
		case Build:
			for {
				//result, err := onDuplicatekey.GetChildren(0).Call(proc)
				result, err := vm.ChildrenCall(onDuplicatekey.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}

				if result.Batch == nil {
					break
				}
				bat := result.Batch
				//anal.Input(bat, onDuplicatekey.GetIsFirst())
				err = resetInsertBatchForOnduplicateKey(proc, bat, onDuplicatekey)
				if err != nil {
					return result, err
				}

			}
			onDuplicatekey.ctr.state = Eval

		case Eval:
			//if onDuplicatekey.ctr.rbat != nil {
			//	anal.Output(onDuplicatekey.ctr.rbat, onDuplicatekey.GetIsLast())
			//}
			result.Batch = onDuplicatekey.ctr.rbat
			onDuplicatekey.ctr.state = End
			analyzer.Output(result.Batch)
			return result, nil

		case End:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func resetInsertBatchForOnduplicateKey(proc *process.Process, originBatch *batch.Batch, insertArg *OnDuplicatekey) error {
	//get rowid vec index
	rowIdIdx := int32(-1)
	for _, idx := range insertArg.OnDuplicateIdx {
		if originBatch.Vecs[idx].GetType().Oid == types.T_Rowid {
			rowIdIdx = idx
			break
		}
	}
	if rowIdIdx == -1 {
		return moerr.NewConstraintViolation(proc.Ctx, "can not find rowid when insert with on duplicate key")
	}

	insertColCount := int(insertArg.InsertColCount) //columns without hidden columns
	if insertArg.ctr.rbat == nil {
		insertArg.ctr.rbat = batch.NewWithSize(len(insertArg.Attrs))
		insertArg.ctr.rbat.Attrs = insertArg.Attrs

		insertArg.ctr.checkConflictBat = batch.NewWithSize(len(insertArg.Attrs))
		insertArg.ctr.checkConflictBat.Attrs = append(insertArg.ctr.checkConflictBat.Attrs, insertArg.Attrs...)

		for i, v := range originBatch.Vecs {
			newVec := vector.NewVec(*v.GetType())
			insertArg.ctr.rbat.SetVector(int32(i), newVec)

			ckVec := vector.NewVec(*v.GetType())
			insertArg.ctr.checkConflictBat.SetVector(int32(i), ckVec)
		}
	} else {
		insertArg.ctr.rbat.CleanOnlyData()
		insertArg.ctr.checkConflictBat.CleanOnlyData()
	}

	insertBatch := insertArg.ctr.rbat
	checkConflictBatch := insertArg.ctr.checkConflictBat
	attrs := make([]string, len(insertBatch.Attrs))
	copy(attrs, insertBatch.Attrs)

	updateExpr := insertArg.OnDuplicateExpr
	oldRowIdVec := vector.MustFixedCol[types.Rowid](originBatch.Vecs[rowIdIdx])
	for i := 0; i < originBatch.RowCount(); i++ {
		newBatch, err := fetchOneRowAsBatch(i, originBatch, proc, attrs)
		if err != nil {
			return err
		}

		// check if uniqueness conflict found in checkConflictBatch
		oldConflictIdx, conflictMsg, err := checkConflict(proc, newBatch, checkConflictBatch, insertArg.ctr.uniqueCheckExes, insertArg.UniqueCols, insertColCount)
		if err != nil {
			newBatch.Clean(proc.GetMPool())
			return err
		}
		if oldConflictIdx > -1 {

			if insertArg.IsIgnore {
				continue
			}

			// if conflict with origin row. and row_id is not equal row_id of insertBatch's inflict row. then throw error
			if !newBatch.Vecs[rowIdIdx].GetNulls().Contains(0) {
				oldRowId := vector.MustFixedCol[types.Rowid](insertBatch.Vecs[rowIdIdx])[oldConflictIdx]
				newRowId := vector.MustFixedCol[types.Rowid](newBatch.Vecs[rowIdIdx])[0]
				if !bytes.Equal(oldRowId[:], newRowId[:]) {
					newBatch.Clean(proc.GetMPool())
					return moerr.NewConstraintViolation(proc.Ctx, conflictMsg)
				}
			}

			for j := 0; j < insertColCount; j++ {
				fromVec := insertBatch.Vecs[j]
				toVec := newBatch.Vecs[j+insertColCount]
				err := toVec.Copy(fromVec, 0, int64(oldConflictIdx), proc.Mp())
				if err != nil {
					newBatch.Clean(proc.GetMPool())
					return err
				}
			}
			tmpBatch, err := updateOldBatch(newBatch, updateExpr, proc, insertColCount, attrs)
			if err != nil {
				newBatch.Clean(proc.GetMPool())
				return err
			}
			// update the oldConflictIdx of insertBatch by newBatch
			for j := 0; j < insertColCount; j++ {
				fromVec := tmpBatch.Vecs[j]
				toVec := insertBatch.Vecs[j]
				err := toVec.Copy(fromVec, int64(oldConflictIdx), 0, proc.Mp())
				if err != nil {
					tmpBatch.Clean(proc.GetMPool())
					newBatch.Clean(proc.GetMPool())
					return err
				}

				toVec2 := checkConflictBatch.Vecs[j]
				err = toVec2.Copy(fromVec, int64(oldConflictIdx), 0, proc.Mp())
				if err != nil {
					tmpBatch.Clean(proc.GetMPool())
					newBatch.Clean(proc.GetMPool())
					return err
				}
			}
			tmpBatch.Clean(proc.GetMPool())
		} else {
			// row id is null: means no uniqueness conflict found in origin rows
			if len(oldRowIdVec) == 0 || originBatch.Vecs[rowIdIdx].GetNulls().Contains(uint64(i)) {
				_, err := insertBatch.Append(proc.Ctx, proc.Mp(), newBatch)
				if err != nil {
					newBatch.Clean(proc.GetMPool())
					return err
				}
				_, err = checkConflictBatch.Append(proc.Ctx, proc.Mp(), newBatch)
				if err != nil {
					newBatch.Clean(proc.GetMPool())
					return err
				}
			} else {

				if insertArg.IsIgnore {
					newBatch.Clean(proc.GetMPool())
					continue
				}

				tmpBatch, err := updateOldBatch(newBatch, updateExpr, proc, insertColCount, attrs)
				if err != nil {
					newBatch.Clean(proc.GetMPool())
					return err
				}
				conflictIdx, conflictMsg, err := checkConflict(proc, tmpBatch, checkConflictBatch, insertArg.ctr.uniqueCheckExes, insertArg.UniqueCols, insertColCount)
				if err != nil {
					tmpBatch.Clean(proc.GetMPool())
					newBatch.Clean(proc.GetMPool())
					return err
				}
				if conflictIdx > -1 {
					tmpBatch.Clean(proc.GetMPool())
					newBatch.Clean(proc.GetMPool())
					return moerr.NewConstraintViolation(proc.Ctx, conflictMsg)
				} else {
					// append batch to insertBatch
					_, err = insertBatch.Append(proc.Ctx, proc.Mp(), tmpBatch)
					if err != nil {
						tmpBatch.Clean(proc.GetMPool())
						newBatch.Clean(proc.GetMPool())
						return err
					}
					_, err = checkConflictBatch.Append(proc.Ctx, proc.Mp(), tmpBatch)
					if err != nil {
						tmpBatch.Clean(proc.GetMPool())
						newBatch.Clean(proc.GetMPool())
						return err
					}
				}
				tmpBatch.Clean(proc.GetMPool())
			}
		}
		newBatch.Clean(proc.GetMPool())
	}

	return nil
}

func resetColPos(e *plan.Expr, columnCount int) {
	switch tmpExpr := e.Expr.(type) {
	case *plan.Expr_Col:
		tmpExpr.Col.ColPos = tmpExpr.Col.ColPos + int32(columnCount)
	case *plan.Expr_F:
		if tmpExpr.F.Func.ObjName != "values" {
			for _, arg := range tmpExpr.F.Args {
				resetColPos(arg, columnCount)
			}
		}
	}
}

func fetchOneRowAsBatch(idx int, originBatch *batch.Batch, proc *process.Process, attrs []string) (*batch.Batch, error) {
	newBatch := batch.NewWithSize(len(attrs))
	newBatch.Attrs = attrs
	var uErr error
	for i, v := range originBatch.Vecs {
		newVec := vector.NewVec(*v.GetType())
		uErr = newVec.UnionOne(v, int64(idx), proc.Mp())
		if uErr != nil {
			newBatch.Clean(proc.Mp())
			return nil, uErr
		}
		newBatch.SetVector(int32(i), newVec)
	}
	newBatch.SetRowCount(1)
	return newBatch, nil
}

func updateOldBatch(evalBatch *batch.Batch, updateExpr map[string]*plan.Expr, proc *process.Process, columnCount int, attrs []string) (*batch.Batch, error) {
	var originVec *vector.Vector
	newBatch := batch.NewWithSize(len(attrs))
	newBatch.Attrs = attrs
	for i, attr := range newBatch.Attrs {
		if i < columnCount {
			// update insert cols
			if expr, exists := updateExpr[attr]; exists {
				runExpr := plan2.DeepCopyExpr(expr)
				resetColPos(runExpr, columnCount)
				newVec, err := colexec.EvalExpressionOnce(proc, runExpr, []*batch.Batch{evalBatch})
				if err != nil {
					newBatch.Clean(proc.Mp())
					return nil, err
				}
				newBatch.SetVector(int32(i), newVec)
			} else {
				originVec = evalBatch.Vecs[i+columnCount]
				newVec := vector.NewVec(*originVec.GetType())
				err := newVec.UnionOne(originVec, int64(0), proc.Mp())
				if err != nil {
					newBatch.Clean(proc.Mp())
					return nil, err
				}
				newBatch.SetVector(int32(i), newVec)
			}
		} else {
			// keep old cols
			originVec = evalBatch.Vecs[i]
			newVec := vector.NewVec(*originVec.GetType())
			err := newVec.UnionOne(originVec, int64(0), proc.Mp())
			if err != nil {
				newBatch.Clean(proc.Mp())
				return nil, err
			}
			newBatch.SetVector(int32(i), newVec)
		}
	}

	newBatch.SetRowCount(1)
	return newBatch, nil
}

func checkConflict(proc *process.Process, newBatch *batch.Batch, checkConflictBatch *batch.Batch,
	checkExpressionExecutor []colexec.ExpressionExecutor, uniqueCols []string, colCount int) (int, string, error) {
	if checkConflictBatch.RowCount() == 0 {
		return -1, "", nil
	}
	for j := 0; j < colCount; j++ {
		fromVec := newBatch.Vecs[j]
		toVec := checkConflictBatch.Vecs[j+colCount]
		for i := 0; i < checkConflictBatch.RowCount(); i++ {
			err := toVec.Copy(fromVec, int64(i), 0, proc.Mp())
			if err != nil {
				return 0, "", err
			}
		}
	}

	// build the check expr
	for i, executor := range checkExpressionExecutor {
		result, err := executor.Eval(proc, []*batch.Batch{checkConflictBatch}, nil)
		if err != nil {
			return 0, "", err
		}

		// run expr row by row. if result is true, break
		isConflict := vector.MustFixedCol[bool](result)
		for _, flag := range isConflict {
			if flag {
				conflictMsg := fmt.Sprintf("Duplicate entry for key '%s'", uniqueCols[i])
				return i, conflictMsg, nil
			}
		}
	}

	return -1, "", nil
}
