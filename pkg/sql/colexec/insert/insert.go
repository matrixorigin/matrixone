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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("insert")
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
