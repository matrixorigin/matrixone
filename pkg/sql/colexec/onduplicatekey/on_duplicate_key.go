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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("processing on duplicate key before insert")
}

func Prepare(_ *proc, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = &container{
		emptyBat: batch.EmptyBatch,
	}
	return nil
}

func Call(idx int, proc *proc, x any, isFirst, isLast bool) (process.ExecStatus, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	var err error
	arg := x.(*Argument)
	bat := proc.Reg.InputBatch
	anal.Input(bat, isFirst)
	if bat == nil {
		if arg.ctr.insertBat != nil {
			newBat, err := arg.ctr.insertBat.Dup(proc.Mp())
			if err != nil {
				return process.ExecNext, err
			}
			anal.Output(newBat, isLast)
			proc.SetInputBatch(newBat)
			logutil.Infof("Table[`%s`] on duplicate key operator output batch: %s", arg.TableDef.Name, newBat.PrintBatch())
		}
		return process.ExecStop, nil
	}

	if bat.RowCount() == 0 {
		bat.Clean(proc.Mp())
		proc.SetInputBatch(arg.ctr.emptyBat)
		return process.ExecNext, nil
	}

	defer proc.PutBatch(bat)
	err = resetInsertBatchForOnduplicateKey(proc, bat, arg)
	if err != nil {
		return process.ExecNext, err
	}

	anal.Output(arg.ctr.emptyBat, isLast)
	proc.SetInputBatch(arg.ctr.emptyBat)
	return process.ExecNext, nil
}

func resetInsertBatchForOnduplicateKey(proc *process.Process, originBatch *batch.Batch, insertArg *Argument) error {
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

	tableDef := insertArg.TableDef
	insertColCount := 0 //columns without hidden columns
	if insertArg.ctr.insertBat == nil {
		attrs := make([]string, 0, len(originBatch.Vecs))
		for _, col := range tableDef.Cols {
			if col.Hidden && col.Name != catalog.FakePrimaryKeyColName {
				continue
			}
			attrs = append(attrs, col.Name)
			insertColCount++
		}
		for _, col := range tableDef.Cols {
			attrs = append(attrs, col.Name)
		}
		attrs = append(attrs, catalog.Row_ID)
		insertArg.ctr.insertBat = batch.NewWithSize(len(attrs))
		insertArg.ctr.insertBat.Attrs = attrs

		insertArg.ctr.checkConflictBat = batch.NewWithSize(len(attrs))
		insertArg.ctr.checkConflictBat.Attrs = append(insertArg.ctr.checkConflictBat.Attrs, attrs...)

		for i, v := range originBatch.Vecs {
			newVec := vector.NewVec(*v.GetType())
			insertArg.ctr.insertBat.SetVector(int32(i), newVec)

			ckVec := vector.NewVec(*v.GetType())
			insertArg.ctr.checkConflictBat.SetVector(int32(i), ckVec)
		}
	} else {
		for _, col := range tableDef.Cols {
			if col.Hidden && col.Name != catalog.FakePrimaryKeyColName {
				continue
			}
			insertColCount++
		}
	}
	uniqueCols := plan2.GetUniqueColAndIdxFromTableDef(tableDef)
	checkExpr, err := plan2.GenUniqueColCheckExpr(proc.Ctx, tableDef, uniqueCols, insertColCount)
	if err != nil {
		return err
	}

	insertBatch := insertArg.ctr.insertBat
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
		oldConflictIdx, conflictMsg, err := checkConflict(proc, newBatch, checkConflictBatch, checkExpr, uniqueCols, insertColCount)
		if err != nil {
			return err
		}
		if oldConflictIdx > -1 {
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
					return err
				}
			}
			tmpBatch, err := updateOldBatch(newBatch, updateExpr, proc, insertColCount, attrs)
			if err != nil {
				return err
			}
			// update the oldConflictIdx of insertBatch by newBatch
			for j := 0; j < insertColCount; j++ {
				fromVec := tmpBatch.Vecs[j]
				toVec := insertBatch.Vecs[j]
				err := toVec.Copy(fromVec, int64(oldConflictIdx), 0, proc.Mp())
				if err != nil {
					return err
				}

				toVec2 := checkConflictBatch.Vecs[j]
				err = toVec2.Copy(fromVec, int64(oldConflictIdx), 0, proc.Mp())
				if err != nil {
					return err
				}
			}
			tmpBatch.Clean(proc.GetMPool())
		} else {
			// row id is null: means no uniqueness conflict found in origin rows
			if len(oldRowIdVec) == 0 || originBatch.Vecs[rowIdIdx].GetNulls().Contains(uint64(i)) {
				insertBatch.Append(proc.Ctx, proc.Mp(), newBatch)
				checkConflictBatch.Append(proc.Ctx, proc.Mp(), newBatch)

			} else {
				tmpBatch, err := updateOldBatch(newBatch, updateExpr, proc, insertColCount, attrs)
				if err != nil {
					return err
				}
				conflictIdx, conflictMsg, err := checkConflict(proc, tmpBatch, checkConflictBatch, checkExpr, uniqueCols, insertColCount)
				if err != nil {
					return err
				}
				if conflictIdx > -1 {
					return moerr.NewConstraintViolation(proc.Ctx, conflictMsg)
				} else {
					// append batch to insertBatch
					insertBatch.Append(proc.Ctx, proc.Mp(), tmpBatch)
					checkConflictBatch.Append(proc.Ctx, proc.Mp(), tmpBatch)
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
	evalBatch.Clean(proc.Mp())
	return newBatch, nil
}

func checkConflict(proc *process.Process, newBatch *batch.Batch, checkConflictBatch *batch.Batch,
	checkExpr []*plan2.Expr, uniqueCols []map[string]int, colCount int) (int, string, error) {
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
	for i, e := range checkExpr {
		executor, err := colexec.NewExpressionExecutor(proc, e)
		if err != nil {
			return 0, "", err
		}

		result, err := executor.Eval(proc, []*batch.Batch{checkConflictBatch})
		if err != nil {
			executor.Free()
			return 0, "", err
		}

		// run expr row by row. if result is true, break
		isConflict := vector.MustFixedCol[bool](result)
		for _, flag := range isConflict {
			if flag {
				keys := make([]string, 0, len(uniqueCols[i]))
				for k := range uniqueCols[i] {
					keys = append(keys, k)
				}
				conflictMsg := fmt.Sprintf("Duplicate entry for key '%s'", strings.Join(keys, ","))
				executor.Free()
				return i, conflictMsg, nil
			}
		}
		executor.Free()
	}

	return -1, "", nil
}
