// Copyright 2021 Matrix Origin
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

package insert

import (
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

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
	buf.WriteString("insert select")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	if ap.IsRemote {
		container := colexec.NewWriteS3Container(ap.InsertCtx.TableDef)
		ap.Container = container
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	var err error
	var affectedRows uint64
	t1 := time.Now()
	insertArg := arg.(*Argument)
	bat := proc.Reg.InputBatch
	if bat == nil {
		if insertArg.IsRemote {
			// handle the last Batch that batchSize less than DefaultBlockMaxRows
			// for more info, refer to the comments about reSizeBatch
			err = insertArg.Container.WriteS3CacheBatch(proc)
			if err != nil {
				return false, err
			}
		}
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}

	bat, err = resetInsertBatchForOnduplicateKey(proc, bat, insertArg)
	if err != nil {
		return false, err
	}

	insertCtx := insertArg.InsertCtx
	clusterTable := insertCtx.ClusterTable

	var insertBat *batch.Batch
	defer func() {
		bat.Clean(proc.Mp())
		if insertBat != nil {
			insertBat.Clean(proc.Mp())
		}
		anal := proc.GetAnalyze(idx)
		anal.AddInsertTime(t1)
	}()

	insertRows := func() error {
		var affectedRow uint64

		affectedRow, err = colexec.InsertBatch(insertArg.Container, insertArg.Engine, proc, bat, insertCtx.Source,
			insertCtx.Ref, insertCtx.TableDef, insertCtx.ParentIdx, insertCtx.UniqueSource)
		if err != nil {
			return err
		}

		affectedRows = affectedRows + affectedRow
		return nil
	}

	if clusterTable.GetIsClusterTable() {
		accountIdColumnDef := insertCtx.TableDef.Cols[clusterTable.GetColumnIndexOfAccountId()]
		accountIdExpr := accountIdColumnDef.GetDefault().GetExpr()
		accountIdConst := accountIdExpr.GetC()

		vecLen := vector.Length(bat.Vecs[0])
		tmpBat := batch.NewWithSize(0)
		tmpBat.Zs = []int64{1}
		//save auto_increment column if necessary
		savedAutoIncrVectors := make([]*vector.Vector, 0)
		defer func() {
			for _, vec := range savedAutoIncrVectors {
				vector.Clean(vec, proc.Mp())
			}
		}()
		for i, colDef := range insertCtx.TableDef.Cols {
			if colDef.GetTyp().GetAutoIncr() {
				vec2, err := vector.Dup(bat.Vecs[i], proc.Mp())
				if err != nil {
					return false, err
				}
				savedAutoIncrVectors = append(savedAutoIncrVectors, vec2)
			}
		}
		for idx, accountId := range clusterTable.GetAccountIDs() {
			//update accountId in the accountIdExpr
			accountIdConst.Value = &plan.Const_U32Val{U32Val: accountId}
			accountIdVec := bat.Vecs[clusterTable.GetColumnIndexOfAccountId()]
			//clean vector before fill it
			vector.Clean(accountIdVec, proc.Mp())
			//the i th row
			for i := 0; i < vecLen; i++ {
				err := fillRow(tmpBat, accountIdExpr, accountIdVec, proc)
				if err != nil {
					return false, err
				}
			}
			if idx != 0 { //refill the auto_increment column vector
				j := 0
				for colIdx, colDef := range insertCtx.TableDef.Cols {
					if colDef.GetTyp().GetAutoIncr() {
						targetVec := bat.Vecs[colIdx]
						vector.Clean(targetVec, proc.Mp())
						for k := int64(0); k < int64(vecLen); k++ {
							err := vector.UnionOne(targetVec, savedAutoIncrVectors[j], k, proc.Mp())
							if err != nil {
								return false, err
							}
						}
						j++
					}
				}
			}

			err := insertRows()
			if err != nil {
				return false, err
			}
		}
	} else {
		err := insertRows()
		if err != nil {
			return false, err
		}
	}

	if insertArg.IsRemote {
		insertArg.Container.WriteEnd(proc)
	}
	atomic.AddUint64(&insertArg.Affected, affectedRows)
	return false, nil
}

/*
fillRow evaluates the expression and put the result into the targetVec.
tmpBat: store temporal vector
expr: the expression to be evaluated at the position (colIdx,rowIdx)
targetVec: the destination where the evaluated result of expr saved into
*/
func fillRow(tmpBat *batch.Batch,
	expr *plan.Expr,
	targetVec *vector.Vector,
	proc *process.Process) error {
	vec, err := colexec.EvalExpr(tmpBat, proc, expr)
	if err != nil {
		return err
	}
	if vec.Size() == 0 {
		vec = vec.ConstExpand(false, proc.Mp())
	}
	if err := vector.UnionOne(targetVec, vec, 0, proc.Mp()); err != nil {
		vec.Free(proc.Mp())
		return err
	}
	vec.Free(proc.Mp())
	return err
}

func resetInsertBatchForOnduplicateKey(proc *process.Process, originBatch *batch.Batch, insertArg *Argument) (*batch.Batch, error) {
	if len(insertArg.InsertCtx.OnDuplicateIdx) == 0 {
		return originBatch, nil
	}
	//get rowid vec index
	rowIdIdx := int32(-1)
	for _, idx := range insertArg.InsertCtx.OnDuplicateIdx {
		if originBatch.Vecs[idx].Typ.Oid == types.T_Rowid {
			rowIdIdx = idx
			break
		}
	}
	if rowIdIdx == -1 {
		return nil, moerr.NewConstraintViolation(proc.Ctx, "can not find rowid when insert with on duplicate key")
	}

	columnCount := len(insertArg.InsertCtx.TableDef.Cols)
	attrs := make([]string, columnCount)
	for i, col := range insertArg.InsertCtx.TableDef.Cols {
		attrs[i] = col.Name
	}
	uniqueCols := plan2.GetUniqueColAndIdxFromTableDef(insertArg.InsertCtx.TableDef)
	checkExpr, err := plan2.GenUniqueColCheckExpr(proc.Ctx, insertArg.InsertCtx.TableDef, uniqueCols)
	if err != nil {
		return nil, err
	}

	insertBatch := batch.New(true, attrs)
	for i, v := range originBatch.Vecs {
		newVec := vector.New(v.Typ)
		insertBatch.SetVector(int32(i), newVec)
	}
	updateExpr := insertArg.InsertCtx.OnDuplicateExpr
	oldRowIdVec := vector.MustTCols[types.Rowid](originBatch.Vecs[rowIdIdx])
	delRowIdVec := vector.New(types.T_Rowid.ToType())

	for i := 0; i < originBatch.Length(); i++ {
		// row id is null: means no uniqueness conflict found in origin rows
		if originBatch.Vecs[rowIdIdx].Nsp.Contains(uint64(i)) {
			newBatch, err := fetchOneRowAsBatch(i, originBatch, proc)
			if err != nil {
				return nil, err
			}

			// check if uniqueness conflict found in insertBatch
			conflictIdx, _, err := checkConflict(proc, newBatch, insertBatch, checkExpr, uniqueCols, columnCount)
			if err != nil {
				return nil, err
			}
			if conflictIdx > -1 {
				// update insertBatch
				newBatch, err := updateOldBatch(conflictIdx, insertBatch, updateExpr, proc, columnCount)
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
					// append batch to insertBatch
					insertBatch.Append(proc.Ctx, proc.Mp(), newBatch)
				}
			} else {
				// append batch to insertBatch
				insertBatch.Append(proc.Ctx, proc.Mp(), newBatch)
			}
		} else {
			// append row_id to deleteBatch
			err := delRowIdVec.Append(oldRowIdVec[i], false, proc.GetMPool())
			if err != nil {
				return nil, err
			}

			// update origin batch
			newBatch, err := updateOldBatch(i, originBatch, updateExpr, proc, columnCount)
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

	// delete old data
	if delRowIdVec.Length() > 0 {
		deleteBatch := batch.New(true, []string{catalog.Row_ID})
		deleteBatch.SetZs(delRowIdVec.Length(), proc.Mp())
		deleteBatch.SetVector(0, delRowIdVec)

		err := insertArg.InsertCtx.Source.Delete(proc.Ctx, deleteBatch, catalog.Row_ID)
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

func fetchOneRowAsBatch(idx int, originBatch *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	newBatch := batch.New(true, originBatch.Attrs)
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
	return newBatch, nil
}

func updateOldBatch(rowIdx int, oldBatch *batch.Batch, updateExpr map[string]*plan.Expr, proc *process.Process, columnCount int) (*batch.Batch, error) {
	evalBatch, err := fetchOneRowAsBatch(rowIdx, oldBatch, proc)
	if err != nil {
		return nil, err
	}

	newBatch := batch.New(true, oldBatch.Attrs)
	for i, attr := range newBatch.Attrs {
		originVec := oldBatch.Vecs[i]

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
			err := vector.UnionOne(toVec, fromVec, 1, proc.Mp())
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
				conflictMsg := fmt.Sprintf("Duplicate entry in column '%s'", strings.Join(keys, ","))
				return i, conflictMsg, nil
			}
		}
	}

	return -1, "", nil
}
