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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
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
	rel.Ranges(ctx, nil) // TODO
	return UpdateInsertBatch(e, db, ctx, proc, ColDefs, bat, rel.GetTableID(ctx))
}

func getRangeFromAutoIncrTable(param *AutoIncrParam, bat *batch.Batch, tableID string) ([]uint64, []uint64, error) {
	offset, step := make([]uint64, 0), make([]uint64, 0)
	var err error
	for i, col := range param.colDefs {
		if !col.AutoIncrement {
			continue
		}
		var d, s uint64
		param.rel, err = param.db.Relation(param.ctx, AUTO_INCR_TABLE)
		if err != nil {
			return nil, nil, err
		}
		param.rel.Ranges(param.ctx, nil) // TODO
		if d, s, err = getOneColRangeFromAutoIncrTable(param, bat, tableID+"_"+col.Name, i); err != nil {
			return nil, nil, err
		}
		offset = append(offset, d)
		step = append(step, s)
	}
	return offset, step, nil
}

func getMaxnum[T constraints.Integer](vec *vector.Vector, length, maxNum, step uint64) uint64 {
	vs := vector.MustTCols[T](vec)
	rowIndex := uint64(0)
	for rowIndex = 0; rowIndex < length; rowIndex++ {
		if nulls.Contains(vec.Nsp, rowIndex) {
			maxNum += step
		} else {
			if vs[rowIndex] < 0 {
				continue
			}
			if uint64(vs[rowIndex]) > maxNum {
				maxNum = uint64(vs[rowIndex])
			}
		}
	}
	return maxNum
}

func updateVector[T constraints.Integer](vec *vector.Vector, length, curNum, stepNum uint64) {
	vs := vector.MustTCols[T](vec)
	rowIndex := uint64(0)
	for rowIndex = 0; rowIndex < length; rowIndex++ {
		if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
			nulls.Del(vec.Nsp, rowIndex)
			curNum += stepNum
			vs[rowIndex] = T(curNum)
		} else {
			if vs[rowIndex] < 0 {
				continue
			}
			if uint64(vs[rowIndex]) > curNum {
				curNum = uint64(vs[rowIndex])
			}
		}
	}
}

func getOneColRangeFromAutoIncrTable(param *AutoIncrParam, bat *batch.Batch, name string, pos int) (uint64, uint64, error) {
	txnOperator, err := param.proc.TxnClient.New()
	if err != nil {
		return 0, 0, err
	}

	oriNum, step, err := getCurrentIndex(param, name)
	if err != nil {
		ctx, cancel := context.WithTimeout(
			param.ctx,
			param.eg.Hints().CommitOrRollbackTimeout,
		)
		defer cancel()
		if err2 := txnOperator.Rollback(ctx); err2 != nil {
			return 0, 0, err2
		}
		return 0, 0, moerr.NewInternalError("GetIndex from auto_increment table fail")
	}

	vec := bat.Vecs[pos]
	maxNum := oriNum
	switch vec.Typ.Oid {
	case types.T_int8:
		maxNum = getMaxnum[int8](vec, uint64(bat.Length()), maxNum, step)
		if maxNum > math.MaxInt8 {
			return 0, 0, moerr.NewOutOfRange("tinyint", "value %v", maxNum)
		}
	case types.T_int16:
		maxNum = getMaxnum[int16](vec, uint64(bat.Length()), maxNum, step)
		if maxNum > math.MaxInt16 {
			return 0, 0, moerr.NewOutOfRange("smallint", "value %v", maxNum)
		}
	case types.T_int32:
		maxNum = getMaxnum[int32](vec, uint64(bat.Length()), maxNum, step)
		if maxNum > math.MaxInt32 {
			return 0, 0, moerr.NewOutOfRange("int", "value %v", maxNum)
		}
	case types.T_int64:
		maxNum = getMaxnum[int64](vec, uint64(bat.Length()), maxNum, step)
		if maxNum > math.MaxInt64 {
			return 0, 0, moerr.NewOutOfRange("bigint", "value %v", maxNum)
		}
	case types.T_uint8:
		maxNum = getMaxnum[uint8](vec, uint64(bat.Length()), maxNum, step)
		if maxNum > math.MaxUint8 {
			return 0, 0, moerr.NewOutOfRange("tinyint unsigned", "value %v", maxNum)
		}
	case types.T_uint16:
		maxNum = getMaxnum[uint16](vec, uint64(bat.Length()), maxNum, step)
		if maxNum > math.MaxUint16 {
			return 0, 0, moerr.NewOutOfRange("smallint unsigned", "value %v", maxNum)
		}
	case types.T_uint32:
		maxNum = getMaxnum[uint32](vec, uint64(bat.Length()), maxNum, step)
		if maxNum > math.MaxUint32 {
			return 0, 0, moerr.NewOutOfRange("int unsigned", "value %v", maxNum)
		}
	case types.T_uint64:
		maxNum = getMaxnum[uint64](vec, uint64(bat.Length()), maxNum, step)
		if maxNum < oriNum {
			return 0, 0, moerr.NewOutOfRange("bigint unsigned", "auto_incrment column constant value overflows bigint unsigned")
		}
	default:
		return 0, 0, moerr.NewInvalidInput("the auto_incr col is not integer type")
	}

	if err := updateAutoIncrTable(param, maxNum, name, param.proc.Mp()); err != nil {
		ctx, cancel := context.WithTimeout(
			param.ctx,
			param.eg.Hints().CommitOrRollbackTimeout,
		)
		defer cancel()
		if err2 := txnOperator.Rollback(ctx); err2 != nil {
			return 0, 0, err2
		}
		return 0, 0, err
	}
	ctx, cancel := context.WithTimeout(
		param.ctx,
		param.eg.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	err = txnOperator.Commit(ctx)
	if err != nil {
		return 0, 0, err
	}
	return oriNum, step, nil
}

func updateBatchImpl(ColDefs []*plan.ColDef, bat *batch.Batch, offset, step []uint64) error {
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
		case types.T_int8:
			updateVector[int8](vec, uint64(bat.Length()), curNum, stepNum)
		case types.T_int16:
			updateVector[int16](vec, uint64(bat.Length()), curNum, stepNum)
		case types.T_int32:
			updateVector[int32](vec, uint64(bat.Length()), curNum, stepNum)
		case types.T_int64:
			updateVector[int64](vec, uint64(bat.Length()), curNum, stepNum)
		case types.T_uint8:
			updateVector[uint8](vec, uint64(bat.Length()), curNum, stepNum)
		case types.T_uint16:
			updateVector[uint16](vec, uint64(bat.Length()), curNum, stepNum)
		case types.T_uint32:
			updateVector[uint32](vec, uint64(bat.Length()), curNum, stepNum)
		case types.T_uint64:
			updateVector[uint64](vec, uint64(bat.Length()), curNum, stepNum)
		default:
			return moerr.NewInvalidInput("invalid auto_increment type '%v'", vec.Typ.Oid)
		}
	}
	return nil
}

func getCurrentIndex(param *AutoIncrParam, colName string) (uint64, uint64, error) {
	rds, _ := param.rel.NewReader(param.ctx, 1, nil, nil)
	for {
		bat, err := rds[0].Read(AUTO_INCR_TABLE_COLNAME, nil, param.proc.Mp())
		if err != nil || bat == nil {
			return 0, 0, moerr.NewInvalidInput("can not find the auto col")
		}
		if len(bat.Vecs) < 2 {
			panic(moerr.NewInternalError("the mo_increment_columns col num is not two"))
		}
		vs2 := vector.MustTCols[uint64](bat.Vecs[1])
		vs3 := vector.MustTCols[uint64](bat.Vecs[2])
		var rowIndex int64
		for rowIndex = 0; rowIndex < int64(bat.Length()); rowIndex++ {
			str := bat.Vecs[0].GetString(rowIndex)
			if str == colName {
				break
			}
		}
		if rowIndex < int64(bat.Length()) {
			return vs2[rowIndex], vs3[rowIndex], nil
		}
	}
}

func updateAutoIncrTable(param *AutoIncrParam, curNum uint64, name string, mp *mpool.MPool) error {
	bat := makeAutoIncrBatch(name, curNum, 1, mp)
	err := param.rel.Delete(param.ctx, bat.GetVector(0), AUTO_INCR_TABLE_COLNAME[0])
	if err != nil {
		return err
	}

	if err = param.rel.Write(param.ctx, bat); err != nil {
		return err
	}
	return nil
}

func makeAutoIncrBatch(name string, num, step uint64, mp *mpool.MPool) *batch.Batch {
	vec := vector.NewWithStrings(types.T_varchar.ToType(), []string{name}, nil, mp)
	vec2 := vector.NewWithFixed(types.T_uint64.ToType(), []uint64{num}, nil, mp)
	vec3 := vector.NewWithFixed(types.T_uint64.ToType(), []uint64{step}, nil, mp)
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
		bat := makeAutoIncrBatch(name+attr.Name, 0, 1, proc.Mp())
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
			bat := makeAutoIncrBatch(tableID+"_"+d.Attr.Name, 0, 1, proc.Mp())
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
		|  offset   |    uint64     |             |   current index number   |
		|   step    |    uint64     |             |   every increase step    |
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
		Type:    types.T_uint64.ToType(),
		Default: &plan.Default{},
		Primary: false,
	}}

	stepAttr := &engine.AttributeDef{Attr: engine.Attribute{
		Name:    AUTO_INCR_TABLE_COLNAME[2],
		Alg:     0,
		Type:    types.T_uint64.ToType(),
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
