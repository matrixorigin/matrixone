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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

var AUTO_INCR_TABLE = "%!%mo_increment_columns"
var AUTO_INCR_TABLE_COLNAME []string = []string{catalog.Row_ID, "name", "offset", "step"}

type AutoIncrParam struct {
	eg      engine.Engine
	rel     engine.Relation
	ctx     context.Context
	proc    *process.Process
	dbName  string
	tblName string
	colDefs []*plan.ColDef
}

func UpdateInsertBatch(e engine.Engine, ctx context.Context, proc *process.Process, ColDefs []*plan.ColDef, bat *batch.Batch, tableID, dbName, tblName string) error {
	incrParam := &AutoIncrParam{
		eg:      e,
		ctx:     ctx,
		proc:    proc,
		colDefs: ColDefs,
		dbName:  dbName,
		tblName: tblName,
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

func UpdateInsertValueBatch(e engine.Engine, ctx context.Context, proc *process.Process, p *plan.InsertValues, bat *batch.Batch, dbName, tblName string) error {
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
	return UpdateInsertBatch(e, ctx, proc, ColDefs, bat, rel.GetTableID(ctx), dbName, tblName)
}

func getRangeFromAutoIncrTable(param *AutoIncrParam, bat *batch.Batch, tableID string) ([]uint64, []uint64, error) {
	txn, err := NewTxn(param.eg, param.proc, param.ctx)
	if err != nil {
		return nil, nil, err
	}

	offset, step := make([]uint64, 0), make([]uint64, 0)
	for i, col := range param.colDefs {
		if !col.Typ.AutoIncr {
			continue
		}
		var d, s uint64
		param.rel, err = GetNewRelation(param.eg, param.dbName, AUTO_INCR_TABLE, txn, param.ctx)
		if err != nil {
			return nil, nil, err
		}
		if d, s, err = getOneColRangeFromAutoIncrTable(param, bat, tableID+"_"+col.Name, i); err != nil {
			if err2 := RolllbackTxn(param.eg, txn, param.ctx); err2 != nil {
				return nil, nil, err2
			}
			return nil, nil, err
		}
		offset = append(offset, d)
		step = append(step, s)
	}
	if err = CommitTxn(param.eg, txn, param.ctx); err != nil {
		return nil, nil, err
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
	oriNum, step, err := getCurrentIndex(param, name, param.proc.Mp())
	if err != nil {
		return 0, 0, err
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
		return 0, 0, err
	}
	return oriNum, step, nil
}

func updateBatchImpl(ColDefs []*plan.ColDef, bat *batch.Batch, offset, step []uint64) error {
	pos := 0
	for i, col := range ColDefs {
		if !col.Typ.AutoIncr {
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

func getCurrentIndex(param *AutoIncrParam, colName string, mp *mpool.MPool) (uint64, uint64, error) {
	var rds []engine.Reader

	ret, err := param.rel.Ranges(param.ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	switch {
	case len(ret) == 0:
		if rds, err = param.rel.NewReader(param.ctx, 1, nil, nil); err != nil {
			return 0, 0, err
		}
	case len(ret) == 1 && len(ret[0]) == 0:
		if rds, err = param.rel.NewReader(param.ctx, 1, nil, nil); err != nil {
			return 0, 0, err
		}
	case len(ret[0]) == 0:
		rds0, err := param.rel.NewReader(param.ctx, 1, nil, nil)
		if err != nil {
			return 0, 0, err
		}
		rds1, err := param.rel.NewReader(param.ctx, 1, nil, ret[1:])
		if err != nil {
			return 0, 0, err
		}
		rds = append(rds, rds0...)
		rds = append(rds, rds1...)
	default:
		rds, _ = param.rel.NewReader(param.ctx, 1, nil, ret)
	}

	for len(rds) > 0 {
		bat, err := rds[0].Read(AUTO_INCR_TABLE_COLNAME, nil, param.proc.Mp())
		if err != nil {
			return 0, 0, moerr.NewInvalidInput("can not find the auto col")
		}
		if bat == nil {
			rds[0].Close()
			rds = rds[1:]
			continue
		}
		if len(bat.Vecs) < 2 {
			return 0, 0, moerr.NewInternalError("the mo_increment_columns col num is not two")
		}
		vs2 := vector.MustTCols[uint64](bat.Vecs[2])
		vs3 := vector.MustTCols[uint64](bat.Vecs[3])
		var rowIndex int64
		for rowIndex = 0; rowIndex < int64(bat.Length()); rowIndex++ {
			str := bat.Vecs[1].GetString(rowIndex)
			if str == colName {
				break
			}
		}
		if rowIndex < int64(bat.Length()) {
			bat.Clean(mp)
			return vs2[rowIndex], vs3[rowIndex], nil
		}
		bat.Clean(mp)
	}
	return 0, 0, nil
}

func updateAutoIncrTable(param *AutoIncrParam, curNum uint64, name string, mp *mpool.MPool) error {
	bat, _ := GetDeleteBatch(param.rel, param.ctx, name, mp)
	if bat == nil {
		return moerr.NewInternalError("the deleted batch is nil")
	}
	bat.SetZs(bat.GetVector(0).Length(), mp)
	err := param.rel.Delete(param.ctx, bat, AUTO_INCR_TABLE_COLNAME[0])
	if err != nil {
		bat.Clean(mp)
		return err
	}
	bat = makeAutoIncrBatch(name, curNum, 1, mp)
	if err = param.rel.Write(param.ctx, bat); err != nil {
		bat.Clean(mp)
		return err
	}
	return nil
}

func makeAutoIncrBatch(name string, num, step uint64, mp *mpool.MPool) *batch.Batch {
	vec := vector.NewWithStrings(types.T_varchar.ToType(), []string{name}, nil, mp)
	vec2 := vector.NewWithFixed(types.T_uint64.ToType(), []uint64{num}, nil, mp)
	vec3 := vector.NewWithFixed(types.T_uint64.ToType(), []uint64{step}, nil, mp)
	bat := &batch.Batch{
		Attrs: AUTO_INCR_TABLE_COLNAME[1:],
		Vecs:  []*vector.Vector{vec, vec2, vec3},
	}
	return bat
}

func GetDeleteBatch(rel engine.Relation, ctx context.Context, colName string, mp *mpool.MPool) (*batch.Batch, uint64) {
	var rds []engine.Reader

	ret, err := rel.Ranges(ctx, nil)
	if err != nil {
		panic(err)
	}
	switch {
	case len(ret) == 0:
		rds, _ = rel.NewReader(ctx, 1, nil, nil)
	case len(ret) == 1 && len(ret[0]) == 0:
		rds, _ = rel.NewReader(ctx, 1, nil, nil)
	case len(ret[0]) == 0:
		rds0, _ := rel.NewReader(ctx, 1, nil, nil)
		rds1, _ := rel.NewReader(ctx, 1, nil, ret[1:])
		rds = append(rds, rds0...)
		rds = append(rds, rds1...)
	default:
		rds, _ = rel.NewReader(ctx, 1, nil, ret)
	}

	retbat := &batch.Batch{
		Vecs: []*vector.Vector{},
	}

	for len(rds) > 0 {
		bat, err := rds[0].Read(AUTO_INCR_TABLE_COLNAME, nil, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, 0
		}
		if bat == nil {
			rds[0].Close()
			rds = rds[1:]
			continue
		}
		if len(bat.Vecs) < 2 {
			panic(moerr.NewInternalError("the mo_increment_columns col num is not two"))
		}
		var rowIndex int64
		for rowIndex = 0; rowIndex < int64(bat.Length()); rowIndex++ {
			str := bat.Vecs[1].GetString(rowIndex)
			if str == colName {
				currentNum := vector.MustTCols[uint64](bat.Vecs[2])[rowIndex : rowIndex+1]
				retbat.Vecs = append(retbat.Vecs, bat.Vecs[0])
				retbat.Vecs[0].Col = retbat.Vecs[0].Col.([]types.Rowid)[rowIndex : rowIndex+1]
				retbat.SetZs(1, mp)
				bat.Clean(mp)
				return retbat, currentNum[0]
			}
		}
		bat.Clean(mp)
	}
	return nil, 0
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
func CreateAutoIncrCol(eg engine.Engine, ctx context.Context, db engine.Database, proc *process.Process, cols []*plan.ColDef, dbName, tblName string) error {
	rel, err := db.Relation(ctx, tblName)
	if err != nil {
		return err
	}
	name := rel.GetTableID(ctx) + "_"

	txn, err := NewTxn(eg, proc, ctx)
	if err != nil {
		return err
	}

	for _, attr := range cols {
		if !attr.Typ.AutoIncr {
			continue
		}
		rel2, err := GetNewRelation(eg, dbName, AUTO_INCR_TABLE, txn, ctx)
		if err != nil {
			return err
		}
		bat := makeAutoIncrBatch(name+attr.Name, 0, 1, proc.Mp())
		if err = rel2.Write(ctx, bat); err != nil {
			if err2 := RolllbackTxn(eg, txn, ctx); err2 != nil {
				return err2
			}
			return err
		}
	}
	if err = CommitTxn(eg, txn, ctx); err != nil {
		return err
	}
	return nil
}

// for delete table operation, delete col in mo_increment_columns table
func DeleteAutoIncrCol(eg engine.Engine, ctx context.Context, rel engine.Relation, proc *process.Process, dbName, tableID string) error {
	txn, err := NewTxn(eg, proc, ctx)
	if err != nil {
		return err
	}
	rel2, err := GetNewRelation(eg, dbName, AUTO_INCR_TABLE, txn, ctx)
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
			bat, _ := GetDeleteBatch(rel2, ctx, tableID+"_"+d.Attr.Name, proc.Mp())
			if bat == nil {
				return moerr.NewInternalError("the deleted batch is nil")
			}
			if err = rel2.Delete(ctx, bat, AUTO_INCR_TABLE_COLNAME[0]); err != nil {
				bat.Clean(proc.Mp())
				if err2 := RolllbackTxn(eg, txn, ctx); err2 != nil {
					return err2
				}
				return err
			}
			bat.Clean(proc.Mp())
		}
	}
	if err = CommitTxn(eg, txn, ctx); err != nil {
		return err
	}
	return nil
}

// for delete table operation, move old col as new col in mo_increment_columns table
func MoveAutoIncrCol(eg engine.Engine, ctx context.Context, tblName string, db engine.Database, proc *process.Process, oldTableID, dbName string) error {
	var err error
	newRel, err := db.Relation(ctx, tblName)
	if err != nil {
		return err
	}
	defs, err := newRel.TableDefs(ctx)
	if err != nil {
		return err
	}

	txn, err := NewTxn(eg, proc, ctx)
	if err != nil {
		return err
	}
	autoRel, err := GetNewRelation(eg, dbName, AUTO_INCR_TABLE, txn, ctx)
	if err != nil {
		return err
	}

	newName := newRel.GetTableID(ctx) + "_"
	for _, def := range defs {
		switch d := def.(type) {
		case *engine.AttributeDef:
			if !d.Attr.AutoIncrement {
				continue
			}

			bat, currentNum := GetDeleteBatch(autoRel, ctx, oldTableID+"_"+d.Attr.Name, proc.Mp())
			if bat == nil {
				return moerr.NewInternalError("the deleted batch is nil")
			}
			if err = autoRel.Delete(ctx, bat, AUTO_INCR_TABLE_COLNAME[0]); err != nil {
				if err2 := RolllbackTxn(eg, txn, ctx); err2 != nil {
					return err2
				}
				return err
			}

			bat2 := makeAutoIncrBatch(newName+d.Attr.Name, currentNum, 1, proc.Mp())
			if err = autoRel.Write(ctx, bat2); err != nil {
				if err2 := RolllbackTxn(eg, txn, ctx); err2 != nil {
					return err2
				}
				return err
			}
		}
	}
	if err = CommitTxn(eg, txn, ctx); err != nil {
		return err
	}
	return nil
}

// for truncate table operation, reset col in mo_increment_columns table
func ResetAutoInsrCol(eg engine.Engine, ctx context.Context, tblName string, db engine.Database, proc *process.Process, tableID, dbName string) error {
	rel, err := db.Relation(ctx, tblName)
	if err != nil {
		return err
	}
	defs, err := rel.TableDefs(ctx)
	if err != nil {
		return err
	}

	txn, err := NewTxn(eg, proc, ctx)
	if err != nil {
		return err
	}
	autoRel, err := GetNewRelation(eg, dbName, AUTO_INCR_TABLE, txn, ctx)
	if err != nil {
		return err
	}

	name := rel.GetTableID(ctx) + "_"
	for _, def := range defs {
		switch d := def.(type) {
		case *engine.AttributeDef:
			if !d.Attr.AutoIncrement {
				continue
			}
			bat, _ := GetDeleteBatch(autoRel, ctx, tableID+"_"+d.Attr.Name, proc.Mp())
			if bat == nil {
				return moerr.NewInternalError("the deleted batch is nil")
			}
			if err = autoRel.Delete(ctx, bat, AUTO_INCR_TABLE_COLNAME[0]); err != nil {
				if err2 := RolllbackTxn(eg, txn, ctx); err2 != nil {
					return err2
				}
				return err
			}

			bat2 := makeAutoIncrBatch(name+d.Attr.Name, 0, 1, proc.Mp())
			if err = autoRel.Write(ctx, bat2); err != nil {
				if err2 := RolllbackTxn(eg, txn, ctx); err2 != nil {
					return err2
				}
				return err
			}
		}
	}
	if err = CommitTxn(eg, txn, ctx); err != nil {
		return err
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

func NewTxn(eg engine.Engine, proc *process.Process, ctx context.Context) (txn client.TxnOperator, err error) {
	if proc.TxnClient == nil {
		return nil, moerr.NewInternalError("must set txn client")
	}
	txn, err = proc.TxnClient.New()
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, moerr.NewInternalError("context should not be nil")
	}
	ctx, cancel := context.WithTimeout(
		ctx,
		eg.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	if err = eg.New(ctx, txn); err != nil {
		return nil, err
	}
	return txn, nil
}

func CommitTxn(eg engine.Engine, txn client.TxnOperator, ctx context.Context) error {
	if txn == nil {
		return nil
	}
	if ctx == nil {
		return moerr.NewInternalError("context should not be nil")
	}
	ctx, cancel := context.WithTimeout(
		ctx,
		eg.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	if err := eg.Commit(ctx, txn); err != nil {
		return err
	}
	err := txn.Commit(ctx)
	txn = nil
	return err
}

func RolllbackTxn(eg engine.Engine, txn client.TxnOperator, ctx context.Context) error {
	if txn == nil {
		return nil
	}
	if ctx == nil {
		return moerr.NewInternalError("context should not be nil")
	}
	ctx, cancel := context.WithTimeout(
		ctx,
		eg.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	if err := eg.Rollback(ctx, txn); err != nil {
		return err
	}
	err := txn.Rollback(ctx)
	txn = nil
	return err
}

func GetNewRelation(eg engine.Engine, dbName, tbleName string, txn client.TxnOperator, ctx context.Context) (engine.Relation, error) {
	dbHandler, err := eg.Database(ctx, dbName, txn)
	if err != nil {
		return nil, err
	}
	tableHandler, err := dbHandler.Relation(ctx, tbleName)
	if err != nil {
		return nil, err
	}
	return tableHandler, nil
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
		Name:    AUTO_INCR_TABLE_COLNAME[1],
		Alg:     0,
		Type:    types.T_varchar.ToType(),
		Default: &plan.Default{},
		Primary: true,
	}}

	numAttr := &engine.AttributeDef{Attr: engine.Attribute{
		Name:    AUTO_INCR_TABLE_COLNAME[2],
		Alg:     0,
		Type:    types.T_uint64.ToType(),
		Default: &plan.Default{},
		Primary: false,
	}}

	stepAttr := &engine.AttributeDef{Attr: engine.Attribute{
		Name:    AUTO_INCR_TABLE_COLNAME[3],
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
		Names: []string{AUTO_INCR_TABLE_COLNAME[1]},
	})

	return defs
}
