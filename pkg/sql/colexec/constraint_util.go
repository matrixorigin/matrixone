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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func FilterRowIdForDel(proc *process.Process, retBat *batch.Batch, srcBat *batch.Batch,
	idx int, primaryKeyIdx int) error {
	sels := vector.GetSels()
	defer vector.PutSels(sels)
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

func BatchDataNotNullCheck(vecs []*vector.Vector, attrs []string, tableDef *plan.TableDef, ctx context.Context) error {
	for j, vec := range vecs {
		if vec == nil {
			continue
		}
		col := findColumnByName(tableDef, attrs[j])
		if col == nil || col.Default == nil || col.Default.NullAbility {
			continue
		}
		if vec.IsConstNull() || nulls.Any(vec.GetNulls()) {
			return moerr.NewConstraintViolation(ctx, fmt.Sprintf("Column '%s' cannot be null", attrs[j]))
		}
	}
	return nil
}

func findColumnByName(tableDef *plan.TableDef, name string) *plan.ColDef {
	if tableDef == nil {
		return nil
	}
	if index, ok := tableDef.Name2ColIndex[strings.ToLower(name)]; ok &&
		index >= 0 && int(index) < len(tableDef.Cols) {
		if col := tableDef.Cols[index]; col != nil &&
			(strings.EqualFold(col.Name, name) || strings.EqualFold(col.OriginName, name)) {
			return col
		}
	}
	for _, col := range tableDef.Cols {
		if col != nil && (strings.EqualFold(col.Name, name) || strings.EqualFold(col.OriginName, name)) {
			return col
		}
	}
	return nil
}

func getRelationByObjRef(ctx context.Context, proc *process.Process, eg engine.Engine, ref *plan.ObjectRef) (engine.Relation, error) {
	objName := ref.ObjName
	if ses := proc.GetSession(); ses != nil {
		if real, ok := ses.GetTempTable(ref.SchemaName, objName); ok {
			objName = real
		}
	}

	dbSource, err := eg.Database(ctx, ref.SchemaName, proc.GetTxnOperator())
	if err != nil {
		return nil, err
	}
	relation, err := dbSource.Relation(ctx, objName, proc)
	if err != nil {
		return nil, err
	}

	if relation == nil {
		return nil, moerr.NewNoSuchTable(ctx, ref.SchemaName, objName)
	}

	return engine.NewRelationHandle(relation), nil
}

func GetRelAndPartitionRelsByObjRef(
	ctx context.Context,
	proc *process.Process,
	eng engine.Engine,
	ref *plan.ObjectRef,
) (source engine.Relation, err error) {
	source, err = getRelationByObjRef(proc.Ctx, proc, eng, ref)
	if err != nil {
		return
	}
	return
}
