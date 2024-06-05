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
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
func MoTableRows(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[int64](result)
	dbs := vector.GenerateFunctionStrParameter(ivecs[0])
	tbls := vector.GenerateFunctionStrParameter(ivecs[1])

	// XXX WTF
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if proc.TxnOperator == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableRows: txn operator is nil")
	}
	txn := proc.TxnOperator

	var ok bool
	// XXX old code starts a new transaction.   why?
	for i := uint64(0); i < uint64(length); i++ {
		foolCtx := proc.Ctx

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

			if ok, err = specialTableFilterForNonSys(foolCtx, dbStr, tblStr); ok && err == nil {
				if err = rs.Append(int64(0), false); err != nil {
					return err
				}
				continue
			}

			if err != nil {
				return err
			}

			var dbo engine.Database
			dbo, err = e.Database(foolCtx, dbStr, txn)
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
					var buf bytes.Buffer
					for j := uint64(0); j < uint64(length); j++ {
						db2, _ := dbs.GetStrValue(j)
						tbl2, _ := tbls.GetStrValue(j)

						dbStr2 := functionUtil.QuickBytesToStr(db2)
						tblStr2 := functionUtil.QuickBytesToStr(tbl2)

						buf.WriteString(fmt.Sprintf("%s-%s; ", dbStr2, tblStr2))
					}

					logutil.Errorf(fmt.Sprintf("db not found when mo_table_size: %s-%s, extra: %s", dbStr, tblStr, buf.String()))
					return moerr.NewInvalidArgNoCtx("db not found when mo_table_size", fmt.Sprintf("%s-%s", dbStr, tblStr))
				}
				return err
			}
			rel, err = dbo.Relation(foolCtx, tblStr, nil)
			if err != nil {
				return err
			}

			// get the table definition information and check whether the current table is a partition table
			var engineDefs []engine.TableDef
			engineDefs, err = rel.TableDefs(foolCtx)
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

			var rows uint64
			// check if the current table is partitioned
			if partitionInfo != nil {
				var prel engine.Relation
				var prows uint64
				// for partition table,  the table rows is equal to the sum of the partition tables.
				for _, partitionTable := range partitionInfo.PartitionTableNames {
					prel, err = dbo.Relation(foolCtx, partitionTable, nil)
					if err != nil {
						return err
					}
					prows, err = prel.Rows(foolCtx)
					if err != nil {
						return err
					}
					rows += prows
				}
			} else {
				if rows, err = rel.Rows(foolCtx); err != nil {
					return err
				}
			}

			if err = rs.Append(int64(rows), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// MoTableSize returns an estimated size of a table.
func MoTableSize(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[int64](result)
	dbs := vector.GenerateFunctionStrParameter(ivecs[0])
	tbls := vector.GenerateFunctionStrParameter(ivecs[1])

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if proc.TxnOperator == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableSize: txn operator is nil")
	}
	txn := proc.TxnOperator

	var ok bool
	// XXX old code starts a new transaction.   why?
	for i := uint64(0); i < uint64(length); i++ {
		foolCtx := proc.Ctx

		db, dbnull := dbs.GetStrValue(i)
		tbl, tblnull := tbls.GetStrValue(i)
		if dbnull || tblnull {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			var rel engine.Relation
			dbStr := functionUtil.QuickBytesToStr(db)
			tblStr := functionUtil.QuickBytesToStr(tbl)

			if ok, err = specialTableFilterForNonSys(foolCtx, dbStr, tblStr); ok && err == nil {
				if err = rs.Append(int64(0), false); err != nil {
					return err
				}
				continue
			}

			if err != nil {
				return err
			}

			var dbo engine.Database
			dbo, err = e.Database(foolCtx, dbStr, txn)
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
					var buf bytes.Buffer
					for j := uint64(0); j < uint64(length); j++ {
						db2, _ := dbs.GetStrValue(j)
						tbl2, _ := tbls.GetStrValue(j)

						dbStr2 := functionUtil.QuickBytesToStr(db2)
						tblStr2 := functionUtil.QuickBytesToStr(tbl2)

						buf.WriteString(fmt.Sprintf("%s#%s; ", dbStr2, tblStr2))
					}

					originalAccId, _ := defines.GetAccountId(proc.Ctx)
					attachedAccId, _ := defines.GetAccountId(foolCtx)

					logutil.Errorf(
						fmt.Sprintf("db not found when mo_table_size: %s#%s, acc: %d-%d, extra: %s",
							dbStr, tblStr, attachedAccId, originalAccId, buf.String()))
					return moerr.NewInvalidArgNoCtx("db not found when mo_table_size", fmt.Sprintf("%s-%s", dbStr, tblStr))
				}
				return err
			}
			rel, err = dbo.Relation(foolCtx, tblStr, nil)
			if err != nil {
				return err
			}

			var oSize, iSize uint64
			if oSize, err = originalTableSize(foolCtx, dbo, rel); err != nil {
				return err
			}
			if iSize, err = indexesTableSize(foolCtx, dbo, rel); err != nil {
				return err
			}

			if err = rs.Append(int64(oSize+iSize), false); err != nil {
				return err
			}
		}
	}
	return nil
}

var specialRegexp = regexp.MustCompile(fmt.Sprintf("%s|%s|%s",
	catalog.MO_TABLES, catalog.MO_DATABASE, catalog.MO_COLUMNS))

func specialTableFilterForNonSys(ctx context.Context, dbStr, tblStr string) (bool, error) {
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return false, err
	}

	if accountId == sysAccountID || dbStr != catalog.MO_CATALOG {
		return false, nil
	}

	if specialRegexp.MatchString(tblStr) || isClusterTable(dbStr, tblStr) {
		return true, nil
	}

	return false, nil
}

func originalTableSize(ctx context.Context, db engine.Database, rel engine.Relation) (size uint64, err error) {
	return getTableSize(ctx, db, rel)
}

func getTableSize(ctx context.Context, db engine.Database, rel engine.Relation) (size uint64, err error) {
	// get the table definition information and check whether the current table is a partition table
	var engineDefs []engine.TableDef
	engineDefs, err = rel.TableDefs(ctx)
	if err != nil {
		return 0, err
	}
	var partitionInfo *plan.PartitionByDef
	for _, def := range engineDefs {
		if partitionDef, ok := def.(*engine.PartitionDef); ok {
			if partitionDef.Partitioned > 0 {
				p := &plan.PartitionByDef{}
				err = p.UnMarshalPartitionInfo(([]byte)(partitionDef.Partition))
				if err != nil {
					return 0, err
				}
				partitionInfo = p
			}
		}
	}

	// check if the current table is partitioned
	if partitionInfo != nil {
		var prel engine.Relation
		var psize uint64
		// for partition table, the table size is equal to the sum of the partition tables.
		for _, partitionTable := range partitionInfo.PartitionTableNames {
			prel, err = db.Relation(ctx, partitionTable, nil)
			if err != nil {
				return 0, err
			}
			if psize, err = prel.Size(ctx, AllColumns); err != nil {
				return 0, err
			}
			size += psize
		}
	} else {
		if size, err = rel.Size(ctx, AllColumns); err != nil {
			return 0, err
		}
	}

	return size, nil
}

func indexesTableSize(ctx context.Context, db engine.Database, rel engine.Relation) (totalSize uint64, err error) {
	var irel engine.Relation
	var size uint64
	for _, idef := range rel.GetTableDef(ctx).Indexes {
		if irel, err = db.Relation(ctx, idef.IndexTableName, nil); err != nil {
			logutil.Info("indexesTableSize->Relation",
				zap.String("originTable", rel.GetTableName()),
				zap.String("indexTableName", idef.IndexTableName),
				zap.Error(err))
			continue
		}

		if size, err = getTableSize(ctx, db, irel); err != nil {
			logutil.Info("indexesTableSize->getTableSize",
				zap.String("originTable", rel.GetTableName()),
				zap.String("indexTableName", idef.IndexTableName),
				zap.Error(err))
			continue
		}

		totalSize += size
	}

	// this is a quick fix for the issue of the indexTableName is empty.
	// the empty indexTableName causes the `SQL parser err: table "" does not exist` err and
	// then the mo_table_size call will fail.
	// this fix does not fix the issue but only avoids the failure caused by it.
	err = nil
	return totalSize, err
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
		return moerr.NewInternalError(proc.Ctx, "MoTableColMaxMin: txn operator is nil")
	}
	txn := proc.TxnOperator

	dbNames := vector.GenerateFunctionStrParameter(parameters[0])
	tableNames := vector.GenerateFunctionStrParameter(parameters[1])
	columnNames := vector.GenerateFunctionStrParameter(parameters[2])

	minMaxIdx := 0
	if fnName == "mo_table_col_max" {
		minMaxIdx = 1
	}

	var getValueFailed bool
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

			if isClusterTable(dbStr, tableStr) {
				//if it is the cluster table in the general account, switch into the sys account
				accountId, err := defines.GetAccountId(proc.Ctx)
				if err != nil {
					return err
				}
				if accountId != uint32(sysAccountID) {
					proc.Ctx = defines.AttachAccountId(proc.Ctx, uint32(sysAccountID))
				}
			}
			ctx := proc.Ctx

			db, err := e.Database(ctx, dbStr, txn)
			if err != nil {
				return err
			}

			if db.IsSubscription(ctx) {
				// get sub info
				var sub *plan.SubscriptionMeta
				if sub, err = proc.SessionInfo.SqlHelper.GetSubscriptionMeta(dbStr); err != nil {
					return err
				}

				// replace with pub account id
				ctx = defines.AttachAccountId(ctx, uint32(sysAccountID))
				// replace with real dbname(sub.DbName)
				if db, err = e.Database(ctx, sub.DbName, txn); err != nil {
					return err
				}
			}

			rel, err := db.Relation(ctx, tableStr, nil)
			if err != nil {
				return err
			}
			tableColumns, err := rel.TableColumns(ctx)
			if err != nil {
				return err
			}

			//ranges, err := rel.Ranges(ctx, nil)
			ranges, err := rel.Ranges(ctx, nil, 0)
			if err != nil {
				return err
			}

			if ranges.Len() == 0 {
				getValueFailed = true
			} else if ranges.Len() == 1 && engine.IsMemtable(ranges.GetBytes(0)) {
				getValueFailed = true
			} else {
				// BUG： if user delete the max or min value within the same txn, the result will be wrong.
				tValues, _, er := rel.MaxAndMinValues(ctx)
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
	case []float32:
		// Used by zonemap Min,Max
		// Used by MO_TABLE_COL_MAX
		return types.ArrayToString[float32](v)
	case []float64:
		return types.ArrayToString[float64](v)
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
		"mo_stages":                   0,
		"mo_snapshots":                0,
	}
)

// CastIndexToValue returns enum type index according to the value
func CastIndexToValue(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	typeEnums := vector.GenerateFunctionStrParameter(ivecs[0])
	indexs := vector.GenerateFunctionFixedTypeParameter[uint16](ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		typeEnum, typeEnumNull := typeEnums.GetStrValue(i)
		indexVal, indexnull := indexs.GetValue(i)
		if typeEnumNull || indexnull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			typeEnumVal := functionUtil.QuickBytesToStr(typeEnum)
			var enumVlaue string

			enumVlaue, err := types.ParseEnumIndex(typeEnumVal, indexVal)
			if err != nil {
				return err
			}

			if err = rs.AppendBytes([]byte(enumVlaue), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// CastValueToIndex returns enum type index according to the value
func CastValueToIndex(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint16](result)
	typeEnums := vector.GenerateFunctionStrParameter(ivecs[0])
	enumValues := vector.GenerateFunctionStrParameter(ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		typeEnum, typeEnumNull := typeEnums.GetStrValue(i)
		enumValue, enumValNull := enumValues.GetStrValue(i)
		if typeEnumNull || enumValNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			typeEnumVal := functionUtil.QuickBytesToStr(typeEnum)
			enumStr := functionUtil.QuickBytesToStr(enumValue)

			var index uint16
			index, err := types.ParseEnum(typeEnumVal, enumStr)
			if err != nil {
				return err
			}

			if err = rs.Append(index, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// CastIndexValueToIndex returns enum type index according to the index value
func CastIndexValueToIndex(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint16](result)
	typeEnums := vector.GenerateFunctionStrParameter(ivecs[0])
	enumIndexValues := vector.GenerateFunctionFixedTypeParameter[uint16](ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		typeEnum, typeEnumNull := typeEnums.GetStrValue(i)
		enumValueIndex, enumValNull := enumIndexValues.GetValue(i)
		if typeEnumNull || enumValNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			typeEnumVal := functionUtil.QuickBytesToStr(typeEnum)
			var index uint16

			index, err := types.ParseEnumValue(typeEnumVal, enumValueIndex)
			if err != nil {
				return err
			}

			if err = rs.Append(index, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// CastNanoToTimestamp returns timestamp string according to the nano
func CastNanoToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	nanos := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])

	layout := "2006-01-02 15:04:05.999999999"
	for i := uint64(0); i < uint64(length); i++ {
		nano, null := nanos.GetValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			t := time.Unix(0, nano).UTC()
			if err := rs.AppendBytes([]byte(t.Format(layout)), false); err != nil {
				return err
			}
		}
	}
	return nil
}
