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

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func UpdateInsertBatch(e engine.Engine, ctx context.Context, proc *process.Process, ColDefs []*plan.ColDef, bat *batch.Batch, namePre string) error {
	taeEngine, ok := e.(moengine.TxnEngine)
	if !ok {
		return errors.New("", "the engine is not tae")
	}

	dbSource, err := e.Database(ctx, "mo_catalog", proc.TxnOperator)
	if err != nil {
		return err
	}
	rel, err := dbSource.Relation(ctx, "mo_increment_columns")
	if err != nil {
		return err
	}

	txnCtx, err := taeEngine.StartTxn(nil)
	if err != nil {
		return err
	}

	for i, col := range ColDefs {
		if !col.AutoIncrement {
			continue
		}
		if err = UpdateBatImpl(rel, ctx, proc, bat, namePre+col.Name, i); err != nil {
			if err2 := txnCtx.Rollback(); err2 != nil {
				return err2
			}
			return err
		}
	}
	if err = txnCtx.Commit(); err != nil {
		return err
	}
	return nil
}

func UpdateInsertValueBatch(e engine.Engine, ctx context.Context, proc *process.Process, p *plan.InsertValues, bat *batch.Batch) error {
	taeEngine, ok := e.(moengine.TxnEngine)
	if !ok {
		return errors.New("", "the engine is not tae")
	}

	namePre := p.DbName + "_" + p.TblName + "_"
	dbSource, err := e.Database(ctx, "mo_catalog", proc.TxnOperator)
	if err != nil {
		return err
	}
	rel, err := dbSource.Relation(ctx, "mo_increment_columns")
	if err != nil {
		return err
	}

	txnCtx, err := taeEngine.StartTxn(nil)
	if err != nil {
		return err
	}
	for i, attr := range p.OrderAttrs {
		isAutoIncr := false
		for _, col := range p.ExplicitCols {
			if col.Name == attr {
				isAutoIncr = col.AutoIncrement
				break
			}
		}
		if !isAutoIncr {
			continue
		}

		if err = UpdateBatImpl(rel, ctx, proc, bat, namePre+attr, i); err != nil {
			if err2 := txnCtx.Rollback(); err2 != nil {
				return err2
			}
			return err
		}
	}
	if err = txnCtx.Commit(); err != nil {
		return err
	}
	return nil
}

func UpdateBatImpl(rel engine.Relation, ctx context.Context, proc *process.Process, bat *batch.Batch, name string, pos int) error {
	rds, _ := rel.NewReader(ctx, 1, nil, nil)
	attrs := []string{"name", "num"}
	var oriNum, curNum int64
	for {
		bat2, err := rds[0].Read(attrs, nil, proc.Mp)
		if err != nil {
			return err
		}
		if bat2 == nil {
			return errors.New("", "can not find colname is table mo_increment_columns")
		}
		oriNum = GetCurrentIndex(bat2, name)
		if oriNum >= 0 {
			break
		}
	}
	curNum = oriNum
	var rowIndex int64

	vec := bat.Vecs[pos]
	for rowIndex = 0; rowIndex < int64(bat.Length()); rowIndex++ {
		switch vec.Typ.Oid {
		case types.T_int32:
			vs := vec.Col.([]int32)
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
				nulls.Del(vec.Nsp, uint64(rowIndex))
				curNum++
				vs[rowIndex] = int32(curNum)
			} else if vs[rowIndex] >= int32(curNum) {
				curNum = int64(vs[rowIndex])
			}
		case types.T_int64:
			vs := vec.Col.([]int64)
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
				nulls.Del(vec.Nsp, uint64(rowIndex))
				curNum++
				vs[rowIndex] = curNum
			} else if vs[rowIndex] >= curNum {
				curNum = int64(vs[rowIndex])
			}
		default:
			return errors.New("", "the auto_incr col is not int32 or int64 type")
		}
	}
	if curNum > oriNum {
		if err := UpdateAutoIncrTable(rel, ctx, curNum, name); err != nil {
			return err
		}
	}
	return nil
}

func GetCurrentIndex(bat *batch.Batch, colName string) int64 {
	if len(bat.Vecs) < 2 {
		panic(errors.New("", "the mo_increment_columns col num is not two"))
	}
	vs := bat.Vecs[0].Col.(*types.Bytes)
	vs2 := bat.Vecs[1].Col.([]int64)

	var rowIndex int64
	for rowIndex = 0; rowIndex < int64(bat.Length()); rowIndex++ {
		str := string(vs.Get(rowIndex))
		if str == colName {
			break
		}
	}
	if rowIndex >= int64(bat.Length()) {
		return -1
	}
	return vs2[rowIndex]
}

func UpdateAutoIncrTable(rel engine.Relation, ctx context.Context, curNum int64, name string) error {
	bat := makeAutoIncrBatch(name, curNum)
	err := rel.Delete(ctx, bat.GetVector(0), "name")
	if err != nil {
		return err
	}

	if err = rel.Write(ctx, bat); err != nil {
		return err
	}
	return nil
}

func makeAutoIncrBatch(name string, num int64) *batch.Batch {
	vBytes := &types.Bytes{
		Offsets: make([]uint32, 1),
		Lengths: make([]uint32, 1),
		Data:    nil,
	}
	vec := &vector.Vector{
		Typ:  types.T_varchar.ToType(),
		Col:  vBytes,
		Data: make([]byte, 0),
		Nsp:  &nulls.Nulls{},
	}

	vBytes.Offsets[0] = uint32(0)
	vBytes.Data = append(vBytes.Data, name...)
	vBytes.Lengths[0] = uint32(len(name))

	vec2 := &vector.Vector{
		Typ: types.T_int64.ToType(),
		Nsp: &nulls.Nulls{},
	}
	vec2.Data = make([]byte, 8)
	vec2.Col = types.DecodeInt64Slice(vec2.Data)
	vec2.Col.([]int64)[0] = num
	bat := &batch.Batch{
		Attrs: []string{"name", "num"},
		Vecs:  []*vector.Vector{vec, vec2},
	}
	return bat
}

// for create table operation, add col in mo_increment_columns table
func CreateAutoIncrCol(e engine.Engine, ctx context.Context, proc *process.Process, cols []*plan.ColDef, namePre string) error {
	dbSource, err := e.Database(ctx, "mo_catalog", proc.TxnOperator)
	if err != nil {
		return err
	}
	rel, err := dbSource.Relation(ctx, "mo_increment_columns")
	if err != nil {
		return err
	}

	for _, attr := range cols {
		if !attr.AutoIncrement {
			continue
		}
		bat := makeAutoIncrBatch(namePre+attr.Name, 0)
		if err = rel.Write(ctx, bat); err != nil {
			return err
		}
	}
	return nil
}

// for delete table operation, delete col in mo_increment_columns table
func DeleteAutoIncrCol(rel engine.Relation, e engine.Engine, ctx context.Context, proc *process.Process, namePre string) error {
	dbSource, err := e.Database(ctx, "mo_catalog", proc.TxnOperator)
	if err != nil {
		return err
	}
	rel2, err := dbSource.Relation(ctx, "mo_increment_columns")
	if err != nil {
		return err
	}

	defs, err := rel.TableDefs(ctx)
	if err != nil {
		return err
	}

	for _, def := range defs {
		switch d := def.(type) {
		case *engine.AttributeDef:
			if !d.Attr.AutoIncrement {
				continue
			}
			bat := makeAutoIncrBatch(namePre+d.Attr.Name, 0)
			if err = rel2.Delete(ctx, bat.GetVector(0), "name"); err != nil {
				return err
			}
		}
	}
	return nil
}
