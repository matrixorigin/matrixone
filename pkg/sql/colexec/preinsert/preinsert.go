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
		return false, nil
	}

	defer proc.PutBatch(bat)
	newBat := batch.NewWithSize(len(arg.Attrs))
	newBat.Attrs = arg.Attrs
	for idx := range arg.Attrs {
		newBat.SetVector(int32(idx), vector.NewVec(*bat.GetVector(int32(idx)).GetType()))
	}
	if _, err := newBat.Append(proc.Ctx, proc.GetMPool(), bat); err != nil {
		newBat.Clean(proc.GetMPool())
		return false, err
	}

	if !arg.IsUpdate {
		if arg.HasAutoCol {
			err := genAutoIncrCol(newBat, proc, arg)
			if err != nil {
				newBat.Clean(proc.GetMPool())
				return false, err
			}
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

	//// calculate the partition expression and append the result vector to batch
	//err = genPartitionExpr(newBat, proc, arg.TableDef)
	//if err != nil {
	//	newBat.Clean(proc.GetMPool())
	//	return false, err
	//}

	proc.SetInputBatch(newBat)

	return false, nil
}

func genAutoIncrCol(bat *batch.Batch, proc *proc, arg *Argument) error {
	return colexec.UpdateInsertBatch(arg.Eg, arg.Ctx, proc,
		arg.TableDef.Cols, bat, arg.TableDef.TblId, arg.SchemaName, arg.TableDef.Name)
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

// calculate the partition expression and append the result vector to batch
//func genPartitionExpr(bat *batch.Batch, proc *proc, tableDef *pb.TableDef) error {
//	// Check whether it is a partition table
//	if tableDef.Partition == nil {
//		return nil
//	} else {
//		partitionVec, err := colexec.EvalExpr(bat, proc, tableDef.Partition.PartitionExpression)
//		if err != nil {
//			return err
//		}
//		bat.Attrs = append(bat.Attrs, "__mo_partition_expr__")
//		bat.Vecs = append(bat.Vecs, partitionVec)
//	}
//	return nil
//}
