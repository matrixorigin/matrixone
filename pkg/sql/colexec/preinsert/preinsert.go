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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
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
	defer analyze(idx, proc)()

	arg := x.(*Argument)
	bat := proc.InputBatch()
	if bat == nil {
		proc.SetInputBatch(nil)
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}

	info := colexec.GetInfoForInsertAndUpdate(arg.TableDef, nil)

	//get insert batch
	insertBatch, err := colexec.GetUpdateBatch(proc, bat, info.IdxList, bat.Length(), info.Attrs, nil, arg.ParentIdx)
	if err != nil {
		return false, err
	}

	if info.HasAutoCol {
		err := genAutoIncrCol(insertBatch, proc, arg)
		if err != nil {
			return false, err
		}
	}

	// check new rows not null
	err = colexec.BatchDataNotNullCheck(insertBatch, arg.TableDef, proc.Ctx)
	if err != nil {
		return false, err
	}

	err = genCompositePrimaryKey(insertBatch, proc, arg.TableDef)
	if err != nil {
		return false, err
	}

	err = genClusterBy(insertBatch, proc, arg.TableDef)
	if err != nil {
		return false, err
	}

	proc.SetInputBatch(insertBatch)
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

func analyze(idx int, proc *proc) func() {
	t := time.Now()
	anal := proc.GetAnalyze(idx)
	anal.Start()
	return func() {
		anal.Stop()
		anal.AddInsertTime(t)
	}
}
