// Copyright 2021 - 2022 Matrix Origin
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

package ctl

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// MoTableRows returns an estimated row number of a table.
func MoTableRows(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec := vector.New(types.New(types.T_int64, 0, 0, 0))
	count := vecs[0].Length()
	dbs := vector.MustStrCols(vecs[0])
	tbls := vector.MustStrCols(vecs[1])
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn, err := proc.TxnClient.New()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback(proc.Ctx)
	if err := e.New(proc.Ctx, txn); err != nil {
		return nil, err
	}
	defer e.Rollback(proc.Ctx, txn)
	for i := 0; i < count; i++ {
		db, err := e.Database(proc.Ctx, dbs[i], txn)
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(proc.Ctx, tbls[i])
		if err != nil {
			return nil, err
		}
		rel.Ranges(proc.Ctx, nil)
		rows, err := rel.Rows(proc.Ctx)
		if err != nil {
			return nil, err
		}
		if err := vec.Append(rows, false, proc.Mp()); err != nil {
			vec.Free(proc.Mp())
			return nil, err
		}
	}
	return vec, nil
}

// MoTableSize returns an estimated size of a table.
func MoTableSize(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec := vector.New(types.New(types.T_int64, 0, 0, 0))
	count := vecs[0].Length()
	dbs := vector.MustStrCols(vecs[0])
	tbls := vector.MustStrCols(vecs[1])
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn, err := proc.TxnClient.New()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback(proc.Ctx)
	if err := e.New(proc.Ctx, txn); err != nil {
		return nil, err
	}
	defer e.Rollback(proc.Ctx, txn)
	for i := 0; i < count; i++ {
		db, err := e.Database(proc.Ctx, dbs[i], txn)
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(proc.Ctx, tbls[i])
		if err != nil {
			return nil, err
		}
		rel.Ranges(proc.Ctx, nil)
		rows, err := rel.Rows(proc.Ctx)
		if err != nil {
			return nil, err
		}
		attrs, err := rel.TableColumns(proc.Ctx)
		if err != nil {
			return nil, err
		}
		size := int64(0)
		for _, attr := range attrs {
			size += rows * int64(attr.Type.TypeSize())
		}
		if err := vec.Append(size, false, proc.Mp()); err != nil {
			vec.Free(proc.Mp())
			return nil, err
		}
	}
	return vec, nil

}

// MoTableColMax return the max value of the column
func MoTableColMax(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	count := vecs[0].Length()
	dbs := vector.MustStrCols(vecs[0])
	tbls := vector.MustStrCols(vecs[1])
	cols := vector.MustStrCols(vecs[2])

	returnType := types.T_varchar.ToType()
	var resultVec *vector.Vector = nil
	resultValues := make([]string, count)
	resultNsp := nulls.NewWithSize(count)

	if vecs[0].IsScalarNull() || vecs[1].IsScalarNull() || vecs[2].IsScalarNull() {
		return proc.AllocScalarNullVector(returnType), nil
	}

	// set null row
	nulls.Or(vecs[0].Nsp, vecs[1].Nsp, resultNsp)
	nulls.Or(vecs[2].Nsp, resultNsp, resultNsp)

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn, err := proc.TxnClient.New()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback(proc.Ctx)
	if err := e.New(proc.Ctx, txn); err != nil {
		return nil, err
	}
	defer e.Rollback(proc.Ctx, txn)

	for i := 0; i < count; i++ {
		col := cols[i]
		if col == "__mo_rowid" {
			return nil, moerr.NewInvalidArg(proc.Ctx, "mo_table_col_max has bad input column", col)
		}
		if tbls[i] == "mo_database" || tbls[i] == "mo_tables" || tbls[i] == "mo_columns" || tbls[i] == "sys_async_task" {
			return nil, moerr.NewInvalidArg(proc.Ctx, "mo_table_col_max has bad input table", tbls[i])
		}

		db, err := e.Database(proc.Ctx, dbs[i], txn)
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(proc.Ctx, tbls[i])
		if err != nil {
			return nil, err
		}
		rel.Ranges(proc.Ctx, nil)

		tableColumns, err := rel.TableColumns(proc.Ctx)
		if err != nil {
			return nil, err
		}

		//Get table max and min value from zonemap
		tableVal, _, err := rel.MaxAndMinValues(proc.Ctx)
		if err != nil {
			return nil, err
		}

		for j := 0; j < len(tableColumns); j++ {
			if tableColumns[j].Name == col {
				resultValues[i] = getVlaueInStr(tableVal[j][1])
				break
			}
		}
	}
	resultVec = vector.NewWithStrings(types.T_varchar.ToType(), resultValues, resultNsp, proc.Mp())
	return resultVec, nil
}

// MoTableColMax return the max value of the column
func MoTableColMin(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	count := vecs[0].Length()
	dbs := vector.MustStrCols(vecs[0])
	tbls := vector.MustStrCols(vecs[1])
	cols := vector.MustStrCols(vecs[2])

	returnType := types.T_varchar.ToType()
	var resultVec *vector.Vector = nil
	resultValues := make([]string, count)
	resultNsp := nulls.NewWithSize(count)

	if vecs[0].IsScalarNull() || vecs[1].IsScalarNull() || vecs[2].IsScalarNull() {
		return proc.AllocScalarNullVector(returnType), nil
	}

	// set null row
	nulls.Or(vecs[0].Nsp, vecs[1].Nsp, resultNsp)
	nulls.Or(vecs[2].Nsp, resultNsp, resultNsp)

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn, err := proc.TxnClient.New()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback(proc.Ctx)
	if err := e.New(proc.Ctx, txn); err != nil {
		return nil, err
	}
	defer e.Rollback(proc.Ctx, txn)

	for i := 0; i < count; i++ {
		col := cols[i]
		if col == "__mo_rowid" {
			return nil, moerr.NewInvalidArg(proc.Ctx, "mo_table_col_min has bad input column", col)
		}
		if tbls[i] == "mo_database" || tbls[i] == "mo_tables" || tbls[i] == "mo_columns" || tbls[i] == "sys_async_task" {
			return nil, moerr.NewInvalidArg(proc.Ctx, "mo_table_col_min has bad input table:", tbls[i])
		}

		db, err := e.Database(proc.Ctx, dbs[i], txn)
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(proc.Ctx, tbls[i])
		if err != nil {
			return nil, err
		}
		rel.Ranges(proc.Ctx, nil)

		tableColumns, err := rel.TableColumns(proc.Ctx)
		if err != nil {
			return nil, err
		}

		//Get table max and min value from zonemap
		tableVal, _, err := rel.MaxAndMinValues(proc.Ctx)
		if err != nil {
			return nil, err
		}

		for j := 0; j < len(tableColumns); j++ {
			if tableColumns[j].Name == col {
				resultValues[i] = getVlaueInStr(tableVal[j][0])
				break
			}
		}
	}
	resultVec = vector.NewWithStrings(types.T_varchar.ToType(), resultValues, resultNsp, proc.Mp())
	return resultVec, nil
}

func getVlaueInStr(value any) string {
	switch v := value.(type) {
	case bool:
		if v {
			return "true"
		} else {
			return "false"
		}
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(uint64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(int64(v), 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 32)
	case string:
		return v
	case []byte:
		return string(v)
	case int:
		return strconv.FormatInt(int64(v), 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case types.Date:
		return v.String()
	case types.Time:
		return v.String()
	case types.Datetime:
		return v.String()
	case types.Timestamp:
		return v.String()
	case bytejson.ByteJson:
		return v.String()
	case types.Uuid:
		return v.ToString()
	case types.Decimal64:
		return v.String()
	case types.Decimal128:
		return v.String()
	default:
		return ""
	}
}
