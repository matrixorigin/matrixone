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

package preinsert

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

const opName = "preinsert"

func (preInsert *PreInsert) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": pre processing insert")
}

func (preInsert *PreInsert) OpType() vm.OpType {
	return vm.PreInsert
}

func (preInsert *PreInsert) Prepare(proc *process.Process) (err error) {
	if preInsert.ctr.canFreeVecIdx == nil {
		preInsert.ctr.canFreeVecIdx = make(map[int]bool)
	}
	if preInsert.CompPkeyExpr != nil && preInsert.ctr.compPkExecutor == nil {
		preInsert.ctr.compPkExecutor, err = colexec.NewExpressionExecutor(proc, preInsert.CompPkeyExpr)
		if err != nil {
			return
		}
	}
	if preInsert.ClusterByExpr != nil && preInsert.ctr.clusterByExecutor == nil {
		preInsert.ctr.clusterByExecutor, err = colexec.NewExpressionExecutor(proc, preInsert.ClusterByExpr)
		if err != nil {
			return
		}
	}
	return
}

func (preInsert *PreInsert) constructColBuf(proc *proc, bat *batch.Batch, first bool) (err error) {
	if first {
		for idx := range preInsert.Attrs {
			if preInsert.TableDef.Cols[idx].Typ.AutoIncr {
				preInsert.ctr.canFreeVecIdx[idx] = true
			}
		}
		preInsert.ctr.buf = batch.NewWithSize(len(preInsert.Attrs))
		preInsert.ctr.buf.Attrs = make([]string, 0, len(preInsert.Attrs))
		preInsert.ctr.buf.Attrs = append(preInsert.ctr.buf.Attrs, preInsert.Attrs...)
	} else {
		preInsert.ctr.buf.SetRowCount(0)
	}
	// if col is AutoIncr, genAutoIncrCol function may change the vector of this col, we should copy the vec from children vec, so it in canFreeVecIdx
	// and the other cols of preInsert.Attrs is stable, we just use the vecs of children's vecs
	for idx := range preInsert.Attrs {
		if _, ok := preInsert.ctr.canFreeVecIdx[idx]; ok {
			typ := bat.Vecs[idx].GetType()
			if preInsert.ctr.buf.Vecs[idx] != nil {
				preInsert.ctr.buf.Vecs[idx].CleanOnlyData()
			} else {
				preInsert.ctr.buf.Vecs[idx] = vector.NewVec(*typ)
			}
			if err = vector.GetUnionAllFunction(*typ, proc.Mp())(preInsert.ctr.buf.Vecs[idx], bat.Vecs[idx]); err != nil {
				return err
			}
		} else {
			if bat.Vecs[idx].IsConst() {
				preInsert.ctr.canFreeVecIdx[idx] = true
				//expland const vector
				typ := bat.Vecs[idx].GetType()
				tmpVec := vector.NewVec(*typ)
				if err = vector.GetUnionAllFunction(*typ, proc.Mp())(tmpVec, bat.Vecs[idx]); err != nil {
					return err
				}
				preInsert.ctr.buf.Vecs[idx] = tmpVec
			} else {
				preInsert.ctr.buf.SetVector(int32(idx), bat.Vecs[idx])
			}
		}
	}
	return
}
func (preInsert *PreInsert) constructHiddenColBuf(proc *proc, bat *batch.Batch, first bool) (err error) {
	if first {
		if preInsert.ctr.compPkExecutor != nil {
			vec, err := preInsert.ctr.compPkExecutor.Eval(proc, []*batch.Batch{preInsert.ctr.buf}, nil)
			if err != nil {
				return err
			}
			preInsert.ctr.buf.Vecs = append(preInsert.ctr.buf.Vecs, vec)
			preInsert.ctr.buf.Attrs = append(preInsert.ctr.buf.Attrs, catalog.CPrimaryKeyColName)
		}

		if preInsert.ctr.clusterByExecutor != nil {
			vec, err := preInsert.ctr.clusterByExecutor.Eval(proc, []*batch.Batch{preInsert.ctr.buf}, nil)
			if err != nil {
				return err
			}
			preInsert.ctr.buf.Vecs = append(preInsert.ctr.buf.Vecs, vec)
			preInsert.ctr.buf.Attrs = append(preInsert.ctr.buf.Attrs, preInsert.TableDef.ClusterBy.Name)
		}
		if preInsert.IsUpdate {
			idx := len(bat.Vecs) - 1
			preInsert.ctr.buf.Attrs = append(preInsert.ctr.buf.Attrs, catalog.Row_ID)
			rowIdVec := vector.NewVec(*bat.GetVector(int32(idx)).GetType())
			err = rowIdVec.UnionBatch(bat.Vecs[idx], 0, bat.Vecs[idx].Length(), nil, proc.Mp())
			if err != nil {
				rowIdVec.Free(proc.Mp())
				return err
			}
			preInsert.ctr.buf.Vecs = append(preInsert.ctr.buf.Vecs, rowIdVec)
		}

	} else {
		idx := len(preInsert.Attrs)
		if preInsert.ctr.compPkExecutor != nil {
			vec, err := preInsert.ctr.compPkExecutor.Eval(proc, []*batch.Batch{preInsert.ctr.buf}, nil)
			if err != nil {
				return err
			}
			preInsert.ctr.buf.Vecs[idx] = vec
			idx += 1
		}
		if preInsert.ctr.clusterByExecutor != nil {
			vec, err := preInsert.ctr.clusterByExecutor.Eval(proc, []*batch.Batch{preInsert.ctr.buf}, nil)
			if err != nil {
				return err
			}
			preInsert.ctr.buf.Vecs[idx] = vec
			idx += 1
		}

		if preInsert.IsUpdate {
			i := len(bat.Vecs) - 1
			rowIdVec := preInsert.ctr.buf.Vecs[idx]
			rowIdVec.CleanOnlyData()
			err = rowIdVec.UnionBatch(bat.Vecs[i], 0, bat.Vecs[i].Length(), nil, proc.Mp())
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (preInsert *PreInsert) Call(proc *proc) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(preInsert.GetIdx(), preInsert.GetParallelIdx(), preInsert.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result, err := vm.ChildrenCall(preInsert.GetChildren(0), proc, anal)
	if err != nil {
		return result, err
	}
	anal.Input(result.Batch, preInsert.IsFirst)

	if result.Batch == nil || result.Batch.IsEmpty() {
		return result, nil
	}
	bat := result.Batch

	first := preInsert.ctr.buf == nil
	err = preInsert.constructColBuf(proc, bat, first)
	if err != nil {
		return result, err
	}
	// keep shuffleIDX unchanged
	preInsert.ctr.buf.ShuffleIDX = bat.ShuffleIDX
	preInsert.ctr.buf.AddRowCount(bat.RowCount())

	if preInsert.HasAutoCol {
		err := genAutoIncrCol(preInsert.ctr.buf, proc, preInsert)
		if err != nil {
			return result, err
		}
	}
	// check new rows not null
	tempVecs := preInsert.ctr.buf.Vecs[:len(preInsert.Attrs)]
	err = colexec.BatchDataNotNullCheck(tempVecs, preInsert.Attrs, preInsert.TableDef, proc.Ctx)
	if err != nil {
		return result, err
	}

	if err = preInsert.constructHiddenColBuf(proc, bat, first); err != nil {
		return result, err
	}

	result.Batch = preInsert.ctr.buf
	anal.Output(result.Batch, preInsert.IsLast)
	return result, nil
}

func genAutoIncrCol(bat *batch.Batch, proc *proc, preInsert *PreInsert) error {
	lastInsertValue, err := proc.GetIncrService().InsertValues(
		proc.Ctx,
		preInsert.TableDef.TblId,
		bat,
		preInsert.EstimatedRowCount,
	)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			logutil.Error("insert auto increment column failed", zap.Error(err))
			return moerr.NewNoSuchTableNoCtx(preInsert.SchemaName, preInsert.TableDef.Name)
		}
		return err
	}
	proc.SetLastInsertID(lastInsertValue)
	return nil
}
