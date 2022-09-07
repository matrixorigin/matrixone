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
	"math"

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

var AUTO_INCR_TABLE = "%!%mo_increment_columns"
var AUTO_INCR_TABLE_COLNAME []string = []string{"name", "offset", "step"}

type AutoIncrParam struct {
	eg      engine.Engine
	db      engine.Database
	rel     engine.Relation
	ctx     context.Context
	proc    *process.Process
	colDefs []*plan.ColDef
}

func UpdateInsertBatch(e engine.Engine, db engine.Database, ctx context.Context, proc *process.Process, ColDefs []*plan.ColDef, bat *batch.Batch, tableID string) error {
	incrParam := &AutoIncrParam{
		eg:      e,
		db:      db,
		ctx:     ctx,
		proc:    proc,
		colDefs: ColDefs,
	}

	offset, step, err := getRangeFromAutoIncrTable(incrParam, bat, tableID)
	if err != nil {
		return err
	}

	if err = updateBatchImpl(ColDefs, bat, offset, step); err != nil {
		return err
	}
	return nil
}

func UpdateInsertValueBatch(e engine.Engine, ctx context.Context, proc *process.Process, p *plan.InsertValues, bat *batch.Batch) error {
	ColDefs := p.ExplicitCols
	orderColDefs(p.OrderAttrs, ColDefs)
	db, err := e.Database(ctx, p.DbName, proc.TxnOperator)
	if err != nil {
		return err
	}
	rel, err := db.Relation(ctx, p.TblName)
	if err != nil {
		return err
	}
	return UpdateInsertBatch(e, db, ctx, proc, ColDefs, bat, rel.GetTableID(ctx))
}

func getRangeFromAutoIncrTable(param *AutoIncrParam, bat *batch.Batch, tableID string) ([]int64, []int64, error) {
	offset, step := make([]int64, 0), make([]int64, 0)
	var err error
	for i, col := range param.colDefs {
		if !col.AutoIncrement {
			continue
		}
		var d, s int64
		param.rel, err = param.db.Relation(param.ctx, AUTO_INCR_TABLE)
		if err != nil {
			return nil, nil, err
		}
		if d, s, err = getOneColRangeFromAutoIncrTable(param, bat, tableID+"_"+col.Name, i); err != nil {
			return nil, nil, err
		}
		offset = append(offset, d)
		step = append(step, s)
	}
	return offset, step, nil
}

func getOneColRangeFromAutoIncrTable(param *AutoIncrParam, bat *batch.Batch, name string, pos int) (int64, int64, error) {
	taeEngine, ok := param.eg.(moengine.TxnEngine)
	if !ok {
		return 0, 0, errors.New("", "the engine is not tae")
	}

	txnCtx, err := taeEngine.StartTxn(nil)
	if err != nil {
		return 0, 0, err
	}

	oriNum, step := getCurrentIndex(param, name)
	if oriNum < 0 {
		if err2 := txnCtx.Rollback(); err2 != nil {
			return 0, 0, err2
		}
		return 0, 0, errors.New("", "GetIndex from auto_increment table fail")
	}

	vec := bat.Vecs[pos]
	maxNum := oriNum
	switch vec.Typ.Oid {
	case types.T_int32:
		vs := vec.Col.([]int32)
		for rowIndex := 0; rowIndex < bat.Length(); rowIndex++ {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
				maxNum += step
			} else {
				if int64(vs[rowIndex]) > maxNum {
					maxNum = int64(vs[rowIndex])
				}
			}
		}
	case types.T_int64:
		vs := vec.Col.([]int64)
		for rowIndex := 0; rowIndex < bat.Length(); rowIndex++ {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
				maxNum += step
			} else {
				if vs[rowIndex] > maxNum {
					maxNum = vs[rowIndex]
				}
			}
		}
	}

	if maxNum < 0 {
		return 0, 0, errors.New("", "auto_incrment column constant value overflows bigint")
	}
	if err := updateAutoIncrTable(param, maxNum, name); err != nil {
		if err2 := txnCtx.Rollback(); err2 != nil {
			return 0, 0, err2
		}
		return 0, 0, err
	}
	err = txnCtx.Commit()
	if err != nil {
		return 0, 0, err
	}
	return oriNum, step, nil
}

func updateBatchImpl(ColDefs []*plan.ColDef, bat *batch.Batch, offset, step []int64) error {
	pos := 0
	for i, col := range ColDefs {
		if !col.AutoIncrement {
			continue
		}
		vec := bat.Vecs[i]
		curNum := offset[pos]
		stepNum := step[pos]
		pos++
		switch vec.Typ.Oid {
		case types.T_int32:
			vs := vec.Col.([]int32)
			for rowIndex := 0; rowIndex < bat.Length(); rowIndex++ {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
					nulls.Del(vec.Nsp, uint64(rowIndex))
					curNum += stepNum
					if curNum > math.MaxInt32 {
						return fmt.Errorf("auto_incrment column '%s' constant value %d overflows int", col.Name, curNum)
					}
					vs[rowIndex] = int32(curNum)
				} else if vs[rowIndex] >= int32(curNum) {
					curNum = int64(vs[rowIndex])
				}
			}
		case types.T_int64:
			vs := vec.Col.([]int64)
			for rowIndex := 0; rowIndex < bat.Length(); rowIndex++ {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
					nulls.Del(vec.Nsp, uint64(rowIndex))
					curNum += stepNum
					vs[rowIndex] = curNum
				} else if vs[rowIndex] >= curNum {
					curNum = int64(vs[rowIndex])
				}
			}
		default:
			return errors.New("", "the auto_incr col is not int32 or int64 type")
		}
	}
	return nil
}

func getCurrentIndex(param *AutoIncrParam, colName string) (int64, int64) {
	rds, _ := param.rel.NewReader(param.ctx, 1, nil, nil)
	for {
		bat, err := rds[0].Read(AUTO_INCR_TABLE_COLNAME, nil, param.proc.Mp())
		if err != nil || bat == nil {
			return -1, 0
		}
		if len(bat.Vecs) < 2 {
			panic(errors.New("", "the mo_increment_columns col num is not two"))
		}
		vs2 := vector.MustTCols[int64](bat.Vecs[1])
		vs3 := vector.MustTCols[int64](bat.Vecs[2])
		var rowIndex int64
		for rowIndex = 0; rowIndex < int64(bat.Length()); rowIndex++ {
			str := bat.Vecs[0].GetString(rowIndex)
			if str == colName {
				break
			}
		}
		if rowIndex < int64(bat.Length()) {
			return vs2[rowIndex], vs3[rowIndex]
		}
	}
}

func updateAutoIncrTable(param *AutoIncrParam, curNum int64, name string) error {
	bat := makeAutoIncrBatch(name, curNum, 1)
	err := param.rel.Delete(param.ctx, bat.GetVector(0), AUTO_INCR_TABLE_COLNAME[0])
	if err != nil {
		return err
	}

	if err = param.rel.Write(param.ctx, bat); err != nil {
		return err
	}
	return nil
}

func makeAutoIncrBatch(name string, num, step int64) *batch.Batch {
	vec := vector.NewWithStrings(types.T_varchar.ToType(), []string{name}, nil, nil)
	vec2 := vector.NewWithFixed(types.T_int64.ToType(), []int64{num}, nil, nil)
	vec3 := vector.NewWithFixed(types.T_int64.ToType(), []int64{step}, nil, nil)
	bat := &batch.Batch{
		Attrs: AUTO_INCR_TABLE_COLNAME,
		Vecs:  []*vector.Vector{vec, vec2, vec3},
	}
	return bat
}

// for create database operation, add col in mo_increment_columns table
func CreateAutoIncrTable(e engine.Engine, ctx context.Context, proc *process.Process, dbName string) error {
	dbSource, err := e.Database(ctx, dbName, proc.TxnOperator)
	if err != nil {
		return err
	}
	if err = dbSource.Create(ctx, AUTO_INCR_TABLE, getAutoIncrTableDef()); err != nil {
		return err
	}
	return nil
}

// for create table operation, add col in mo_increment_columns table
func CreateAutoIncrCol(db engine.Database, ctx context.Context, proc *process.Process, cols []*plan.ColDef, tblName string) error {
	rel, err := db.Relation(ctx, tblName)
	if err != nil {
		return err
	}

	name := rel.GetTableID(ctx) + "_"
	for _, attr := range cols {
		if !attr.AutoIncrement {
			continue
		}
		if rel, err = db.Relation(ctx, AUTO_INCR_TABLE); err != nil {
			return err
		}
		bat := makeAutoIncrBatch(name+attr.Name, 0, 1)
		if err = rel.Write(ctx, bat); err != nil {
			return err
		}
	}
	return nil
}

// for delete table operation, delete col in mo_increment_columns table
func DeleteAutoIncrCol(rel engine.Relation, db engine.Database, ctx context.Context, proc *process.Process, tableID string) error {
	rel2, err := db.Relation(ctx, AUTO_INCR_TABLE)
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
			bat := makeAutoIncrBatch(tableID+"_"+d.Attr.Name, 0, 1)
			if err = rel2.Delete(ctx, bat.GetVector(0), AUTO_INCR_TABLE_COLNAME[0]); err != nil {
				return err
			}
		}
	}
	return nil
}

func orderColDefs(attrs []string, ColDefs []*plan.ColDef) {
	for i, name := range attrs {
		for j, def := range ColDefs {
			if name == def.Name {
				ColDefs[i], ColDefs[j] = ColDefs[j], ColDefs[i]
			}
		}
	}
}

func getAutoIncrTableDef() []engine.TableDef {
	/*
		mo_increment_columns schema
		| Attribute |     Type     | Primary Key |             Note         |
		| -------   | ------------ | ----------- | ------------------------ |
		|   name    | varchar(770) |             | Name of the db_table_col |
		|  offset   |    int64     |             |   current index number   |
		|   step    |    int64     |             |   every increase step    |
	*/

	nameAttr := &engine.AttributeDef{Attr: engine.Attribute{
		Name:    AUTO_INCR_TABLE_COLNAME[0],
		Alg:     0,
		Type:    types.T_varchar.ToType(),
		Default: &plan.Default{},
		Primary: true,
	}}

	numAttr := &engine.AttributeDef{Attr: engine.Attribute{
		Name:    AUTO_INCR_TABLE_COLNAME[1],
		Alg:     0,
		Type:    types.T_int64.ToType(),
		Default: &plan.Default{},
		Primary: false,
	}}

	stepAttr := &engine.AttributeDef{Attr: engine.Attribute{
		Name:    AUTO_INCR_TABLE_COLNAME[2],
		Alg:     0,
		Type:    types.T_int64.ToType(),
		Default: &plan.Default{},
		Primary: false,
	}}

	defs := make([]engine.TableDef, 0, 3)
	defs = append(defs, nameAttr)
	defs = append(defs, numAttr)
	defs = append(defs, stepAttr)
	defs = append(defs, &engine.PrimaryIndexDef{
		Names: []string{AUTO_INCR_TABLE_COLNAME[0]},
	})

	return defs
}
