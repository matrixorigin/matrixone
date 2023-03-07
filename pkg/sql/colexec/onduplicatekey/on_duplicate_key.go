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

func Prepare(_ *proc, _ any) error {
	return nil
}

func Call(idx int, proc *proc, x any, isFirst, isLast bool) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	arg := x.(*Argument)
	bat := proc.Reg.InputBatch
	anal.Input(bat, isFirst)
	if bat == nil {
		proc.SetInputBatch(nil)
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}

	rbat, err := resetInsertBatchForOnduplicateKey(proc, bat, arg)
	if err != nil {
		return false, err
	}
	bat.Clean(proc.Mp())
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	return false, nil
}

func resetInsertBatchForOnduplicateKey(proc *process.Process, originBatch *batch.Batch, insertArg *Argument) (*batch.Batch, error) {
	if len(insertArg.OnDuplicateIdx) == 0 {
		return originBatch, nil
	}
	//get rowid vec index
	rowIdIdx := int32(-1)
	for _, idx := range insertArg.OnDuplicateIdx {
		if originBatch.Vecs[idx].Typ.Oid == types.T_Rowid {
			rowIdIdx = idx
			break
		}
	}
	if rowIdIdx == -1 {
		return nil, moerr.NewConstraintViolation(proc.Ctx, "can not find rowid when insert with on duplicate key")
	}

	tableDef := insertArg.TableDef
	columnCount := len(tableDef.Cols)
	uniqueCols := plan2.GetUniqueColAndIdxFromTableDef(tableDef)
	checkExpr, err := plan2.GenUniqueColCheckExpr(proc.Ctx, tableDef, uniqueCols)
	if err != nil {
		return nil, err
	}

	attrs := make([]string, 0, len(originBatch.Vecs))
	for i := 0; i < 2; i++ {
		for j := 0; j < columnCount; j++ {
			attrs = append(attrs, tableDef.Cols[j].Name)
		}
	}
	for i := len(attrs); i < len(originBatch.Vecs); i++ {
		attrs = append(attrs, "")
	}
	insertBatch := batch.New(true, attrs)
	for i, v := range originBatch.Vecs {
		newVec := vector.New(v.Typ)
		insertBatch.SetVector(int32(i), newVec)
	}
	updateExpr := insertArg.OnDuplicateExpr
	oldRowIdVec := vector.MustTCols[types.Rowid](originBatch.Vecs[rowIdIdx])
	delRowIdVec := vector.New(types.T_Rowid.ToType())

	for i := 0; i < originBatch.Length(); i++ {
		newBatch, err := fetchOneRowAsBatch(i, originBatch, proc, attrs)
		if err != nil {
			return nil, err
		}

		// check if uniqueness conflict found in insertBatch
		oldConflictIdx, conflictMsg, err := checkConflict(proc, newBatch, insertBatch, checkExpr, uniqueCols, columnCount)
		if err != nil {
			return nil, err
		}
		if oldConflictIdx > -1 {
			// if conflict with origin row. and row_id is not equal row_id of insertBatch's inflict row. then throw error
			if !newBatch.Vecs[rowIdIdx].Nsp.Contains(0) {
				oldRowId := vector.MustTCols[types.Rowid](insertBatch.Vecs[rowIdIdx])[oldConflictIdx]
				newRowId := vector.MustTCols[types.Rowid](newBatch.Vecs[rowIdIdx])[0]
				if !bytes.Equal(oldRowId[:], newRowId[:]) {
					return nil, moerr.NewConstraintViolation(proc.Ctx, conflictMsg)
				}
			}

			for j := 0; j < columnCount; j++ {
				fromVec := insertBatch.Vecs[j]
				toVec := newBatch.Vecs[j+columnCount]
				err := vector.Copy(toVec, fromVec, 0, int64(oldConflictIdx), proc.Mp())
				if err != nil {
					return nil, err
				}
			}
			newBatch, err := updateOldBatch(newBatch, oldConflictIdx, insertBatch, updateExpr, proc, columnCount, attrs)
			if err != nil {
				return nil, err
			}
			// update the oldConflictIdx of insertBatch by newBatch
			for j := 0; j < columnCount; j++ {
				fromVec := newBatch.Vecs[j]
				toVec := insertBatch.Vecs[j]
				err := vector.Copy(toVec, fromVec, int64(oldConflictIdx), 0, proc.Mp())
				if err != nil {
					return nil, err
				}
			}
		} else {
			// row id is null: means no uniqueness conflict found in origin rows
			if originBatch.Vecs[rowIdIdx].Nsp.Contains(uint64(i)) {
				insertBatch.Append(proc.Ctx, proc.Mp(), newBatch)
			} else {
				// append row_id to deleteBatch
				err := delRowIdVec.Append(oldRowIdVec[i], false, proc.GetMPool())
				if err != nil {
					return nil, err
				}

				newBatch, err := updateOldBatch(newBatch, i, originBatch, updateExpr, proc, columnCount, attrs)
				if err != nil {
					return nil, err
				}
				conflictIdx, conflictMsg, err := checkConflict(proc, newBatch, insertBatch, checkExpr, uniqueCols, columnCount)
				if err != nil {
					return nil, err
				}
				if conflictIdx > -1 {
					return nil, moerr.NewConstraintViolation(proc.Ctx, conflictMsg)
				} else {
					// append batch to updateBatch
					insertBatch.Append(proc.Ctx, proc.Mp(), newBatch)
				}
			}
		}
	}

	// delete old data
	if delRowIdVec.Length() > 0 {
		deleteBatch := batch.New(true, []string{catalog.Row_ID})
		deleteBatch.SetZs(delRowIdVec.Length(), proc.Mp())
		deleteBatch.SetVector(0, delRowIdVec)

		err := insertArg.Source.Delete(proc.Ctx, deleteBatch, catalog.Row_ID)
		if err != nil {
			deleteBatch.Clean(proc.Mp())
			return nil, err
		}
	}
	return insertBatch, nil

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
	newBatch := batch.New(true, attrs)
	var uErr error
	for i, v := range originBatch.Vecs {
		newVec := vector.New(v.Typ)
		if v.Nsp.Contains(uint64(idx)) {
			uErr = vector.Union(newVec, v, []int64{int64(idx)}, true, proc.Mp())
		} else {
			uErr = vector.Union(newVec, v, []int64{int64(idx)}, false, proc.Mp())
		}
		if uErr != nil {
			newBatch.Clean(proc.Mp())
			return nil, uErr
		}
		newBatch.SetVector(int32(i), newVec)
	}
	newBatch.SetZs(1, proc.Mp())
	return newBatch, nil
}

func updateOldBatch(evalBatch *batch.Batch, rowIdx int, oldBatch *batch.Batch, updateExpr map[string]*plan.Expr, proc *process.Process, columnCount int, attrs []string) (*batch.Batch, error) {
	// evalBatch, err := fetchOneRowAsBatch(rowIdx, oldBatch, proc, attrs)
	// if err != nil {
	// 	return nil, err
	// }

	newBatch := batch.New(true, attrs)
	for i, attr := range newBatch.Attrs {
		if expr, exists := updateExpr[attr]; exists && i < columnCount {
			runExpr := plan2.DeepCopyExpr(expr)
			resetColPos(runExpr, columnCount)
			newVec, err := colexec.EvalExpr(evalBatch, proc, runExpr)
			if err != nil {
				newBatch.Clean(proc.Mp())
				return nil, err
			}
			newBatch.SetVector(int32(i), newVec)
		} else {
			originVec := oldBatch.Vecs[i]
			newVec := vector.New(originVec.Typ)
			err := vector.Union(newVec, originVec, []int64{int64(rowIdx)}, false, proc.Mp())
			if err != nil {
				newBatch.Clean(proc.Mp())
				return nil, err
			}
			newBatch.SetVector(int32(i), newVec)
		}
	}
	newBatch.SetZs(1, proc.Mp())

	return newBatch, nil
}

func checkConflict(proc *process.Process, newBatch *batch.Batch, insertBatch *batch.Batch,
	checkExpr []*plan2.Expr, uniqueCols []map[string]int, colCount int) (int, string, error) {
	// fill newBatch to insertBatch's old columns[these vector was not used]
	for j := 0; j < colCount; j++ {
		fromVec := newBatch.Vecs[j]
		toVec := insertBatch.Vecs[j+colCount]
		for i := 0; i < insertBatch.Length(); i++ {
			err := vector.Copy(toVec, fromVec, int64(i), 0, proc.Mp())
			if err != nil {
				return 0, "", err
			}
		}
	}

	// build the check expr
	for i, e := range checkExpr {
		result, err := colexec.EvalExpr(insertBatch, proc, e)
		if err != nil {
			return 0, "", err
		}

		// run expr row by row. if result is true, break
		isConflict := vector.MustTCols[bool](result)
		for _, flag := range isConflict {
			if flag {
				keys := make([]string, 0, len(uniqueCols[i]))
				for k := range uniqueCols[i] {
					keys = append(keys, k)
				}
				conflictMsg := fmt.Sprintf("Duplicate entry for key '%s'", strings.Join(keys, ","))
				return i, conflictMsg, nil
			}
		}
	}

	return -1, "", nil
}
