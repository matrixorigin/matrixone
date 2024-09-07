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

package colexec

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func FilterRowIdForDel(proc *process.Process, retBat *batch.Batch, srcBat *batch.Batch,
	idx int, primaryKeyIdx int) error {
	sels := proc.Mp().GetSels()
	defer proc.Mp().PutSels(sels)
	rowidVec := retBat.Vecs[0]
	primaryVec := retBat.Vecs[1]
	rowIdMap := make(map[types.Rowid]bool)
	nulls := srcBat.Vecs[idx].GetNulls()
	for i, r := range vector.MustFixedColWithTypeCheck[types.Rowid](srcBat.Vecs[idx]) {
		if !nulls.Contains(uint64(i)) {
			if rowIdMap[r] {
				continue
			}
			rowIdMap[r] = true
			sels = append(sels, int64(i))
		}
	}
	err := rowidVec.Union(srcBat.Vecs[idx], sels, proc.Mp())
	if err != nil {
		return err
	}
	err = primaryVec.Union(srcBat.Vecs[primaryKeyIdx], sels, proc.Mp())
	if err != nil {
		return err
	}
	retBat.SetRowCount(len(sels))
	return nil
}

// FillPartitionBatchForDelete fills the data into the corresponding batch based on the different partitions to which the current `row_id` data belongs.
func FillPartitionBatchForDelete(proc *process.Process, input *batch.Batch, buffer *batch.Batch, expect int32, rowIdIdx int, partitionIdx int, pkIdx int) error {
	// Fill the data into the corresponding batch based on the different partitions to which the current `row_id` data
	rid2pid := vector.MustFixedColWithTypeCheck[int32](input.Vecs[partitionIdx])
	var err error

	for i, rowid := range vector.MustFixedColWithTypeCheck[types.Rowid](input.Vecs[rowIdIdx]) {
		if !input.Vecs[rowIdIdx].GetNulls().Contains(uint64(i)) {
			patition := rid2pid[i]
			if patition == -1 {
				return moerr.NewInvalidInput(proc.Ctx, "Table has no partition for value from column_list")
			} else if patition == expect {
				err = vector.AppendFixed(buffer.Vecs[0], rowid, false, proc.GetMPool())
				if err != nil {
					return err
				}
				err = buffer.Vecs[1].UnionOne(input.Vecs[pkIdx], int64(i), proc.Mp())
				if err != nil {
					return err
				}
			}
		}
	}

	buffer.SetRowCount(buffer.Vecs[0].Length())
	return nil
}

// FillPartitionBatchForInsert fills the partition batch for insert operation.
func FillPartitionBatchForInsert(proc *process.Process, input *batch.Batch, buffer *batch.Batch, expect int32, partitionIdx int) error {
	rid2pid := vector.MustFixedColWithTypeCheck[int32](input.Vecs[partitionIdx])
	for i, partition := range rid2pid {
		if !input.Vecs[partitionIdx].GetNulls().Contains(uint64(i)) {
			if partition == -1 {
				return moerr.NewInvalidInput(proc.Ctx, "Table has no partition for value from column_list")
			} else if partition == expect {
				for j := range buffer.Attrs {
					if err := buffer.Vecs[j].UnionOne(input.Vecs[j], int64(i), proc.GetMPool()); err != nil {
						return err
					}
				}
			}
		}
	}
	buffer.SetRowCount(buffer.Vecs[0].Length())
	return nil
}

func BatchDataNotNullCheck(tmpBat *batch.Batch, tableDef *plan.TableDef, ctx context.Context) error {
	for j := range tmpBat.Vecs {
		if tmpBat.Vecs[j] == nil {
			continue
		}
		nsp := tmpBat.Vecs[j].GetNulls()
		if tableDef.Cols[j].Default != nil && !tableDef.Cols[j].Default.NullAbility && nulls.Any(nsp) {
			return moerr.NewConstraintViolation(ctx, fmt.Sprintf("Column '%s' cannot be null", tmpBat.Attrs[j]))
		}
	}
	return nil
}

func getRelationByObjRef(ctx context.Context, proc *process.Process, eg engine.Engine, ref *plan.ObjectRef) (engine.Relation, error) {
	dbSource, err := eg.Database(ctx, ref.SchemaName, proc.GetTxnOperator())
	if err != nil {
		return nil, err
	}
	relation, err := dbSource.Relation(ctx, ref.ObjName, proc)
	if err == nil {
		return relation, nil
	}

	// try to get temporary table
	dbSource, err = eg.Database(ctx, defines.TEMPORARY_DBNAME, proc.GetTxnOperator())
	if err != nil {
		return nil, moerr.NewNoSuchTable(ctx, ref.SchemaName, ref.ObjName)
	}
	newObjeName := engine.GetTempTableName(ref.SchemaName, ref.ObjName)
	return dbSource.Relation(ctx, newObjeName, proc)
}

func GetRelAndPartitionRelsByObjRef(
	ctx context.Context,
	proc *process.Process,
	eng engine.Engine,
	ref *plan.ObjectRef,
	partitionTableNames []string,
) (source engine.Relation, partitionSources []engine.Relation, err error) {
	source, err = getRelationByObjRef(proc.Ctx, proc, eng, ref)
	if err != nil {
		return
	}

	if len(partitionTableNames) > 0 {
		var dbSource engine.Database
		var tmpRel engine.Relation
		dbSource, err = eng.Database(proc.Ctx, ref.SchemaName, proc.GetTxnOperator())
		if err != nil {
			return
		}
		// get the relation instances for each partition sub table
		for _, pTableName := range partitionTableNames {
			tmpRel, err = dbSource.Relation(proc.Ctx, pTableName, proc)
			if err != nil {
				return
			}
			partitionSources = append(partitionSources, tmpRel)
		}
	}
	return
}
