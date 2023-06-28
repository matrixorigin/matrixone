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

package function

import (
	"context"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	AllColumns = "*"
)

// XXX Porting mo functions to function2.
// Mo function unit tests are not ported, because it is too heavy and does not test enough cases.
// Mo functions are better tested with bvt.

// MoTableRows returns an estimated row number of a table.
func MoTableRows(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[int64](result)
	dbs := vector.GenerateFunctionStrParameter(ivecs[0])
	tbls := vector.GenerateFunctionStrParameter(ivecs[1])

	// XXX WTF
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if proc.TxnOperator == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableRows: txn operator is nil")
	}
	txn := proc.TxnOperator

	// XXX old code starts a new transaction.   why?
	for i := uint64(0); i < uint64(length); i++ {
		db, dbnull := dbs.GetStrValue(i)
		tbl, tblnull := tbls.GetStrValue(i)
		if dbnull || tblnull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			var rel engine.Relation
			dbStr := functionUtil.QuickBytesToStr(db)
			tblStr := functionUtil.QuickBytesToStr(tbl)

			ctx := proc.Ctx
			if isClusterTable(dbStr, tblStr) {
				//if it is the cluster table in the general account, switch into the sys account
				ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(sysAccountID))
			}
			dbo, err := e.Database(ctx, dbStr, txn)
			if err != nil {
				return err
			}
			rel, err = dbo.Relation(ctx, tblStr, nil)
			if err != nil {
				return err
			}

			// get the table definition information and check whether the current table is a partition table
			engineDefs, err := rel.TableDefs(ctx)
			if err != nil {
				return err
			}
			var partitionInfo *plan.PartitionByDef
			for _, def := range engineDefs {
				if partitionDef, ok := def.(*engine.PartitionDef); ok {
					if partitionDef.Partitioned > 0 {
						p := &plan.PartitionByDef{}
						err = p.UnMarshalPartitionInfo(([]byte)(partitionDef.Partition))
						if err != nil {
							return err
						}
						partitionInfo = p
					}
				}
			}

			var rows int64

			// check if the current table is partitioned
			if partitionInfo != nil {
				var prel engine.Relation
				var prows int64
				// for partition table,  the table rows is equal to the sum of the partition tables.
				for _, partitionTable := range partitionInfo.PartitionTableNames {
					prel, err = dbo.Relation(ctx, partitionTable, nil)
					if err != nil {
						return err
					}
					prel.Ranges(ctx, nil)
					prows, err = prel.Rows(ctx)
					if err != nil {
						return err
					}
					rows += prows
				}
			} else {
				rel.Ranges(ctx, nil)
				rows, err = rel.Rows(ctx)
				if err != nil {
					return err
				}
			}

			if err = rs.Append(rows, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// MoTableSize returns an estimated size of a table.
func MoTableSize(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[int64](result)
	dbs := vector.GenerateFunctionStrParameter(ivecs[0])
	tbls := vector.GenerateFunctionStrParameter(ivecs[1])

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if proc.TxnOperator == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableRows: txn operator is nil")
	}
	txn := proc.TxnOperator

	// XXX old code starts a new transaction.   why?
	for i := uint64(0); i < uint64(length); i++ {
		db, dbnull := dbs.GetStrValue(i)
		tbl, tblnull := tbls.GetStrValue(i)
		if dbnull || tblnull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			var rel engine.Relation
			ctx := proc.Ctx
			dbStr := functionUtil.QuickBytesToStr(db)
			tblStr := functionUtil.QuickBytesToStr(tbl)

			if isClusterTable(dbStr, tblStr) {
				//if it is the cluster table in the general account, switch into the sys account
				ctx = context.WithValue(proc.Ctx, defines.TenantIDKey{}, uint32(sysAccountID))
			}
			dbo, err := e.Database(ctx, dbStr, txn)
			if err != nil {
				return err
			}
			rel, err = dbo.Relation(ctx, tblStr, nil)
			if err != nil {
				return err
			}

			// get the table definition information and check whether the current table is a partition table
			engineDefs, err := rel.TableDefs(ctx)
			if err != nil {
				return err
			}
			var partitionInfo *plan.PartitionByDef
			for _, def := range engineDefs {
				if partitionDef, ok := def.(*engine.PartitionDef); ok {
					if partitionDef.Partitioned > 0 {
						p := &plan.PartitionByDef{}
						err = p.UnMarshalPartitionInfo(([]byte)(partitionDef.Partition))
						if err != nil {
							return err
						}
						partitionInfo = p
					}
				}
			}

			var size int64

			// check if the current table is partitioned
			if partitionInfo != nil {
				var prel engine.Relation
				var psize int64
				// for partition table, the table size is equal to the sum of the partition tables.
				for _, partitionTable := range partitionInfo.PartitionTableNames {
					prel, err = dbo.Relation(ctx, partitionTable, nil)
					if err != nil {
						return err
					}
					prel.Ranges(ctx, nil)
					psize, err = prel.Size(ctx, AllColumns)
					if err != nil {
						return err
					}
					size += psize
				}
			} else {
				rel.Ranges(ctx, nil)
				size, err = rel.Size(ctx, AllColumns)
				if err != nil {
					return err
				}
			}
			if err = rs.Append(size, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// MoTableColMax return the max value of the column
func MoTableColMax(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return moTableColMaxMinImpl("mo_table_col_max", ivecs, result, proc, length)
}

// MoTableColMax return the max value of the column
func MoTableColMin(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return moTableColMaxMinImpl("mo_table_col_min", ivecs, result, proc, length)
}

func moTableColMaxMinImpl(fnName string, parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	e, ok := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if !ok || proc.TxnOperator == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableRows: txn operator is nil")
	}
	txn := proc.TxnOperator

	dbNames := vector.GenerateFunctionStrParameter(parameters[0])
	tableNames := vector.GenerateFunctionStrParameter(parameters[1])
	columnNames := vector.GenerateFunctionStrParameter(parameters[2])

	minMaxIdx := 0
	if fnName == "mo_table_col_max" {
		minMaxIdx = 1
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		db, null1 := dbNames.GetStrValue(i)
		table, null2 := tableNames.GetStrValue(i)
		column, null3 := columnNames.GetStrValue(i)
		if null1 || null2 || null3 {
			rs.AppendMustNull()
		} else {
			dbStr := functionUtil.QuickBytesToStr(db)
			tableStr := functionUtil.QuickBytesToStr(table)
			columnStr := functionUtil.QuickBytesToStr(column)

			// Magic code. too confused.
			if tableStr == "mo_database" || tableStr == "mo_tables" || tableStr == "mo_columns" || tableStr == "sys_async_task" {
				return moerr.NewInvalidInput(proc.Ctx, "%s has bad input table %s", fnName, tableStr)
			}
			if columnStr == "__mo_rowid" {
				return moerr.NewInvalidInput(proc.Ctx, "%s has bad input column %s", fnName, columnStr)
			}

			db, err := e.Database(proc.Ctx, dbStr, txn)
			if err != nil {
				return err
			}
			rel, err := db.Relation(proc.Ctx, tableStr, nil)
			if err != nil {
				return err
			}
			tableColumns, err := rel.TableColumns(proc.Ctx)
			if err != nil {
				return err
			}

			ranges, err := rel.Ranges(proc.Ctx, nil)
			if err != nil {
				return err
			}

			getValueFailed := true
			if len(ranges) > 1 {
				// BUG： if user delete the max or min value within the same txn, the result will be wrong.
				tValues, _, er := rel.MaxAndMinValues(proc.Ctx)
				if er != nil {
					return er
				}

				// BUG: if user drop the col and add it back with the same name within the same txn, the result will be wrong.
				for j := range tableColumns {
					if tableColumns[j].Name == columnStr {
						strval := getValueInStr(tValues[j][minMaxIdx])
						if err = rs.AppendMustBytesValue(functionUtil.QuickStrToBytes(strval)); err != nil {
							return err
						}
						getValueFailed = false
						break
					}
				}
			}
			if getValueFailed {
				rs.AppendMustNull()
			}
		}
	}
	return nil
}

func getValueInStr(value any) string {
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
		return v.Format(0)
	case types.Decimal128:
		return v.Format(0)
	default:
		return ""
	}
}

func isClusterTable(dbName, name string) bool {
	if dbName == moCatalog {
		//if it is neither among the tables nor the index table,
		//it is the cluster table.
		if _, ok := predefinedTables[name]; !ok && !isIndexTable(name) {
			return true
		}
	}
	return false
}

func isIndexTable(name string) bool {
	return strings.HasPrefix(name, catalog.IndexTableNamePrefix)
}

const (
	moCatalog    = "mo_catalog"
	sysAccountID = 0
)

var (
	predefinedTables = map[string]int8{
		"mo_database":                 0,
		"mo_tables":                   0,
		"mo_columns":                  0,
		"mo_account":                  0,
		"mo_user":                     0,
		"mo_role":                     0,
		"mo_user_grant":               0,
		"mo_role_grant":               0,
		"mo_role_privs":               0,
		"mo_user_defined_function":    0,
		"mo_stored_procedure":         0,
		"mo_mysql_compatibility_mode": 0,
		catalog.MOAutoIncrTable:       0,
		"mo_indexes":                  0,
		"mo_pubs":                     0,
	}
)
