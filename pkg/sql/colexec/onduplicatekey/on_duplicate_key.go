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
		if originBatch.Vecs[idx].GetType().Oid == types.T_Rowid {
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
	insertBatch := batch.NewWithSize(len(attrs))
	insertBatch.Attrs = attrs
	checkConflictBatch := batch.NewWithSize(len(attrs))
	checkConflictBatch.Attrs = attrs

	for i, v := range originBatch.Vecs {
		newVec := vector.NewVec(*v.GetType())
		insertBatch.SetVector(int32(i), newVec)

		ckVec := vector.NewVec(*v.GetType())
		checkConflictBatch.SetVector(int32(i), ckVec)
	}
	updateExpr := insertArg.OnDuplicateExpr
	oldRowIdVec := vector.MustFixedCol[types.Rowid](originBatch.Vecs[rowIdIdx])

	for i := 0; i < originBatch.Length(); i++ {
		newBatch, err := fetchOneRowAsBatch(i, originBatch, proc, attrs)
		if err != nil {
			return nil, err
		}

		// check if uniqueness conflict found in checkConflictBatch
		oldConflictIdx, conflictMsg, err := checkConflict(proc, newBatch, checkConflictBatch, checkExpr, uniqueCols, columnCount)
		if err != nil {
			return nil, err
		}
		if oldConflictIdx > -1 {
			// if conflict with origin row. and row_id is not equal row_id of insertBatch's inflict row. then throw error
			if !newBatch.Vecs[rowIdIdx].GetNulls().Contains(0) {
				oldRowId := vector.MustFixedCol[types.Rowid](insertBatch.Vecs[rowIdIdx])[oldConflictIdx]
				newRowId := vector.MustFixedCol[types.Rowid](newBatch.Vecs[rowIdIdx])[0]
				if !bytes.Equal(oldRowId[:], newRowId[:]) {
					return nil, moerr.NewConstraintViolation(proc.Ctx, conflictMsg)
				}
			}

			for j := 0; j < columnCount; j++ {
				fromVec := insertBatch.Vecs[j]
				toVec := newBatch.Vecs[j+columnCount]
				err := toVec.Copy(fromVec, 0, int64(oldConflictIdx), proc.Mp())
				if err != nil {
					return nil, err
				}
			}
			newBatch, err := updateOldBatch(newBatch, updateExpr, proc, columnCount, attrs)
			if err != nil {
				return nil, err
			}
			// update the oldConflictIdx of insertBatch by newBatch
			for j := 0; j < columnCount; j++ {
				fromVec := newBatch.Vecs[j]
				toVec := insertBatch.Vecs[j]
				err := toVec.Copy(fromVec, int64(oldConflictIdx), 0, proc.Mp())
				if err != nil {
					return nil, err
				}

				toVec2 := checkConflictBatch.Vecs[j]
				err = toVec2.Copy(fromVec, int64(oldConflictIdx), 0, proc.Mp())
				if err != nil {
					return nil, err
				}
			}
		} else {
			// row id is null: means no uniqueness conflict found in origin rows
			if len(oldRowIdVec) == 0 || originBatch.Vecs[rowIdIdx].GetNulls().Contains(uint64(i)) {
				insertBatch.Append(proc.Ctx, proc.Mp(), newBatch)
				checkConflictBatch.Append(proc.Ctx, proc.Mp(), newBatch)
			} else {
				newBatch, err := updateOldBatch(newBatch, updateExpr, proc, columnCount, attrs)
				if err != nil {
					return nil, err
				}
				conflictIdx, conflictMsg, err := checkConflict(proc, newBatch, checkConflictBatch, checkExpr, uniqueCols, columnCount)
				if err != nil {
					return nil, err
				}
				if conflictIdx > -1 {
					return nil, moerr.NewConstraintViolation(proc.Ctx, conflictMsg)
				} else {
					// append batch to insertBatch
					insertBatch.Append(proc.Ctx, proc.Mp(), newBatch)
					checkConflictBatch.Append(proc.Ctx, proc.Mp(), newBatch)
				}
			}
		}
	}

	// make return batch like update statement
	// updateAttrs := make([]string, 0, len(originBatch.Vecs))
	// vecs := make([]*vector.Vector, 0, len(originBatch.Vecs))
	// for i := columnCount; i < len(attrs); i++ {
	// 	updateAttrs = append(updateAttrs, attrs[i])
	// 	fromVec := insertBatch.Vecs[i]
	// 	vec := vector.NewVec(*fromVec.GetType())
	// 	err := vec.UnionMulti(fromVec, 0, fromVec.Length(), proc.GetMPool())
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	vecs = append(vecs, vec)
	// }
	// for i := 0; i < columnCount; i++ {
	// 	updateAttrs = append(updateAttrs, attrs[i])
	// 	fromVec := insertBatch.Vecs[i]
	// 	vec := vector.NewVec(*fromVec.GetType())
	// 	err := vec.UnionMulti(fromVec, 0, fromVec.Length(), proc.GetMPool())
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	vecs = append(vecs, vec)
	// }
	// updateBatch := batch.NewWithSize(len(updateAttrs))
	// updateBatch.Attrs = updateAttrs
	// for i, vec := range vecs {
	// 	updateBatch.SetVector(int32(i), vec)
	// }
	// updateBatch.Zs = append(updateBatch.Zs, insertBatch.Zs...)
	// insertBatch.Clean(proc.GetMPool())

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
	newBatch.SetZs(1, proc.Mp())
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
				newVec, err := colexec.EvalExpr(evalBatch, proc, runExpr)
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
		// if expr, exists := updateExpr[attr]; exists && i < columnCount {
		// 	runExpr := plan2.DeepCopyExpr(expr)
		// 	resetColPos(runExpr, columnCount)
		// 	newVec, err := colexec.EvalExpr(evalBatch, proc, runExpr)
		// 	if err != nil {
		// 		newBatch.Clean(proc.Mp())
		// 		return nil, err
		// 	}
		// 	newBatch.SetVector(int32(i), newVec)
		// } else {
		// 	if i < columnCount {
		// 		originVec = oldBatch.Vecs[i+columnCount]
		// 	} else {
		// 		originVec = oldBatch.Vecs[i]
		// 	}
		// 	newVec := vector.NewVec(*originVec.GetType())
		// 	err := newVec.UnionOne(originVec, int64(rowIdx), proc.Mp())
		// 	if err != nil {
		// 		newBatch.Clean(proc.Mp())
		// 		return nil, err
		// 	}
		// 	newBatch.SetVector(int32(i), newVec)
		// }
	}
	newBatch.SetZs(1, proc.Mp())

	return newBatch, nil
}

func checkConflict(proc *process.Process, newBatch *batch.Batch, checkConflictBatch *batch.Batch,
	checkExpr []*plan2.Expr, uniqueCols []map[string]int, colCount int) (int, string, error) {

	for j := 0; j < colCount; j++ {
		fromVec := newBatch.Vecs[j]
		toVec := checkConflictBatch.Vecs[j+colCount]
		for i := 0; i < checkConflictBatch.Length(); i++ {
			err := toVec.Copy(fromVec, int64(i), 0, proc.Mp())
			if err != nil {
				return 0, "", err
			}
		}
	}

	// build the check expr
	for i, e := range checkExpr {
		result, err := colexec.EvalExpr(checkConflictBatch, proc, e)
		if err != nil {
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
				return i, conflictMsg, nil
			}
		}
	}

	return -1, "", nil
}
