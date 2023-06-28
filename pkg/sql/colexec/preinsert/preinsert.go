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
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("pre processing insert")
}

func Prepare(_ *proc, _ any) error {
	return nil
}

func Call(idx int, proc *proc, x any, _, _ bool) (bool, error) {
	analy := proc.GetAnalyze(idx)
	analy.Start()
	defer analy.Stop()

	var err error

	arg := x.(*Argument)
	bat := proc.InputBatch()
	if bat == nil {
		proc.SetInputBatch(nil)
		return true, nil
	}
	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		proc.SetInputBatch(batch.EmptyBatch)
		return false, nil
	}
	defer proc.PutBatch(bat)
	newBat := batch.NewWithSize(len(arg.Attrs))
	newBat.Attrs = make([]string, 0, len(arg.Attrs))
	for idx := range arg.Attrs {
		newBat.Attrs = append(newBat.Attrs, arg.Attrs[idx])
		srcVec := bat.Vecs[idx]
		vec := proc.GetVector(*srcVec.GetType())
		if err := vector.GetUnionAllFunction(*srcVec.GetType(), proc.Mp())(vec, srcVec); err != nil {
			newBat.Clean(proc.Mp())
			return false, err
		}
		newBat.SetVector(int32(idx), vec)
	}
	newBat.Zs = append(newBat.Zs, bat.Zs...)
	if arg.HasAutoCol {
		err := genAutoIncrCol(newBat, proc, arg)
		if err != nil {
			newBat.Clean(proc.GetMPool())
			return false, err
		}
	}
	// check new rows not null
	err = colexec.BatchDataNotNullCheck(newBat, arg.TableDef, proc.Ctx)
	if err != nil {
		newBat.Clean(proc.GetMPool())
		return false, err
	}

	// calculate the composite primary key column and append the result vector to batch
	err = genCompositePrimaryKey(newBat, proc, arg.TableDef)
	if err != nil {
		newBat.Clean(proc.GetMPool())
		return false, err
	}
	err = genClusterBy(newBat, proc, arg.TableDef)
	if err != nil {
		newBat.Clean(proc.GetMPool())
		return false, err
	}
	if arg.IsUpdate {
		idx := len(bat.Vecs) - 1
		newBat.Attrs = append(newBat.Attrs, catalog.Row_ID)
		rowIdVec := proc.GetVector(*bat.GetVector(int32(idx)).GetType())
		err := rowIdVec.UnionBatch(bat.Vecs[idx], 0, bat.Vecs[idx].Length(), nil, proc.Mp())
		if err != nil {
			return false, err
		}
		newBat.Vecs = append(newBat.Vecs, rowIdVec)
	}
	proc.SetInputBatch(newBat)
	return false, nil
}

func genAutoIncrCol(bat *batch.Batch, proc *proc, arg *Argument) error {
	lastInsertValue, err := proc.IncrService.InsertValues(
		proc.Ctx,
		arg.TableDef.TblId,
		bat)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return moerr.NewNoSuchTableNoCtx(arg.SchemaName, arg.TableDef.Name)
		}
		return err
	}
	proc.SetLastInsertID(lastInsertValue)
	return nil
}

func genCompositePrimaryKey(bat *batch.Batch, proc *proc, tableDef *pb.TableDef) error {
	// Check whether the composite primary key column is included
	if tableDef.Pkey == nil || tableDef.Pkey.CompPkeyCol == nil {
		return nil
	}

	return util.FillCompositeKeyBatch(bat, catalog.CPrimaryKeyColName, tableDef.Pkey.Names, proc)
}

func genClusterBy(bat *batch.Batch, proc *proc, tableDef *pb.TableDef) error {
	if tableDef.ClusterBy == nil {
		return nil
	}
	clusterBy := tableDef.ClusterBy.Name
	if clusterBy == "" || !util.JudgeIsCompositeClusterByColumn(clusterBy) {
		return nil
	}
	return util.FillCompositeClusterByBatch(bat, clusterBy, proc)
}
