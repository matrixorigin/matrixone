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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	AllColumns = "*"
)

// XXX Porting mo functions to function2.
// Mo function unit tests are not ported, because it is too heavy and does not test enough cases.
// Mo functions are better tested with bvt.

// MoTableSize/Rows get the estimated table size/rows in bytes
// an account can only query these tables that belongs to it.
// some special cases:
// 1. cluster table

type GetMoTableSizeRowsFuncType = func() func(context.Context, string, []uint64, []uint64, []uint64) ([]uint64, error)

var GetMoTableSizeFunc atomic.Pointer[GetMoTableSizeRowsFuncType]
var GetMoTableRowsFunc atomic.Pointer[GetMoTableSizeRowsFuncType]

func waitStatsReady() (ok bool) {
	if GetMoTableSizeFunc.Load() != nil {
		return true
	}

	ctx, cel := context.WithTimeout(context.Background(), time.Second*30)
	defer cel()

	ticker := time.NewTicker(time.Millisecond * 100)
	for range ticker.C {
		select {
		case <-ctx.Done():
			return false

		default:
			if GetMoTableSizeFunc.Load() != nil {
				return true
			}
		}
	}

	return false
}

func MoTableSizeRows(
	iVecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	executor *atomic.Pointer[GetMoTableSizeRowsFuncType],
) (err error) {

	if !waitStatsReady() {
		return moerr.NewInternalError(proc.Ctx, "wait table stats done time out")
	}

	var (
		ok  bool
		db  engine.Database
		rel engine.Relation

		dbNull, tblNull   bool
		dbBytes, tblBytes []byte

		dbName, tblName string

		eng engine.Engine
		txn client.TxnOperator

		ret                   []uint64
		accIds, dbIds, tblIds []uint64

		rs   *vector.FunctionResult[int64]
		dbs  vector.FunctionParameterWrapper[types.Varlena]
		tbls vector.FunctionParameterWrapper[types.Varlena]
	)

	rs = vector.MustFunctionResult[int64](result)
	dbs = vector.GenerateFunctionStrParameter(iVecs[0])
	tbls = vector.GenerateFunctionStrParameter(iVecs[1])

	eng = proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if eng == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableSizeRows: mo table engine is nil")
	}

	if proc.GetTxnOperator() == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableSizeRows: txn operator is nil")
	}

	accountId := proc.Ctx.Value(defines.TenantIDKey{}).(uint32)

	decodeNames := func(i uint64) (string, string, bool) {
		dbBytes, dbNull = dbs.GetStrValue(i)
		tblBytes, tblNull = tbls.GetStrValue(i)
		if dbNull || tblNull {
			return "", "", false
		}

		d := functionUtil.QuickBytesToStr(dbBytes)
		t := functionUtil.QuickBytesToStr(tblBytes)

		return d, t, true
	}

	defer func() {
		if err != nil {
			var names []string
			for i := uint64(0); i < uint64(length); i++ {
				d, t, _ := decodeNames(i)
				names = append(names, fmt.Sprintf("%s-%s", d, t))
			}

			logutil.Error("MoTableSizeRows",
				zap.Error(err),
				zap.String("db", dbName),
				zap.String("table", tblName),
				zap.Uint32("account id", accountId),
				zap.String("tbl list", strings.Join(names, ",")))
		}
	}()

	txn = proc.GetTxnOperator()

	for i := uint64(0); i < uint64(length); i++ {
		if dbName, tblName, ok = decodeNames(i); !ok {
			if err = rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		if ok, err = specialTableFilterForNonSys(proc.Ctx, dbName, tblName); ok && err == nil {
			if err = rs.Append(int64(0), false); err != nil {
				return err
			}
			continue
		}

		if err != nil {
			return err
		}

		if db, err = eng.Database(proc.Ctx, dbName, txn); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				return moerr.NewInternalErrorNoCtxf("db not exist: %s", dbName)
			}
			return err
		}

		if rel, err = db.Relation(proc.Ctx, tblName, nil); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				return moerr.NewNoSuchTableNoCtx(dbName, tblName)
			}
			return err
		}

		accIds = append(accIds, uint64(accountId))
		dbIds = append(dbIds, uint64(rel.GetDBID(proc.Ctx)))
		tblIds = append(tblIds, uint64(rel.GetTableID(proc.Ctx)))
	}

	ret, err = (*executor.Load())()(proc.Ctx, proc.GetService(), accIds, dbIds, tblIds)
	if err != nil {
		return err
	}

	for _, val := range ret {
		if err = rs.Append(int64(val), false); err != nil {
			return err
		}
	}

	return nil
}

func MoTableSize(
	iVecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {

	return MoTableSizeRows(iVecs, result, proc, length, selectList, &GetMoTableSizeFunc)
}

func MoTableRows(
	iVecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {

	return MoTableSizeRows(iVecs, result, proc, length, selectList, &GetMoTableRowsFunc)
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

// MoTableColMax return the max value of the column
func MoTableColMax(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return moTableColMaxMinImpl("mo_table_col_max", ivecs, result, proc, length, selectList)
}

// MoTableColMax return the max value of the column
func MoTableColMin(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return moTableColMaxMinImpl("mo_table_col_min", ivecs, result, proc, length, selectList)
}

func moTableColMaxMinImpl(fnName string, parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	e, ok := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if !ok || proc.GetTxnOperator() == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableColMaxMin: txn operator is nil")
	}
	txn := proc.GetTxnOperator()

	dbNames := vector.GenerateFunctionStrParameter(parameters[0])
	tableNames := vector.GenerateFunctionStrParameter(parameters[1])
	columnNames := vector.GenerateFunctionStrParameter(parameters[2])

	minMaxIdx := 0
	if fnName == "mo_table_col_max" {
		minMaxIdx = 1
	}

	var getValueFailed bool
	rs := vector.MustFunctionResult[types.Varlena](result)

	sysAccountCtx := proc.Ctx
	if accountId, err := defines.GetAccountId(proc.Ctx); err != nil {
		return err
	} else if accountId != uint32(sysAccountID) {
		sysAccountCtx = defines.AttachAccountId(proc.Ctx, uint32(sysAccountID))
	}

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
				return moerr.NewInvalidInputf(proc.Ctx, "%s has bad input table %s", fnName, tableStr)
			}
			if columnStr == "__mo_rowid" {
				return moerr.NewInvalidInputf(proc.Ctx, "%s has bad input column %s", fnName, columnStr)
			}

			ctx := proc.Ctx
			if isClusterTable(dbStr, tableStr) {
				//if it is the cluster table in the general account, switch into the sys account
				ctx = sysAccountCtx
			}

			db, err := e.Database(ctx, dbStr, txn)
			if err != nil {
				return err
			}

			if db.IsSubscription(ctx) {
				// get sub info
				var sub *plan.SubscriptionMeta
				if sub, err = proc.GetSessionInfo().SqlHelper.GetSubscriptionMeta(dbStr); err != nil {
					return err
				}
				if sub != nil && !pubsub.InSubMetaTables(sub, tableStr) {
					return moerr.NewInternalErrorf(ctx, "table %s not found in publication %s", tableStr, sub.Name)
				}

				// replace with pub account id
				ctx = defines.AttachAccountId(ctx, uint32(sub.AccountId))
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
			ranges, err := rel.Ranges(ctx, engine.DefaultRangesParam)
			if err != nil {
				return err
			}

			if ranges.DataCnt() == 0 {
				getValueFailed = true
			} else if ranges.DataCnt() == 1 {
				first := ranges.GetBlockInfo(0)
				if first.IsMemBlk() {
					getValueFailed = true
				}
			} else {
				// BUG： if user delete the max or min value within the same txn, the result will be wrong.
				tValues, _, er := rel.MaxAndMinValues(ctx)
				if er != nil {
					return er
				}

				// BUG: if user drop the col and add it back with the same name within the same txn, the result will be wrong.
				for j := range tableColumns {
					if strings.EqualFold(tableColumns[j].Name, columnStr) {
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
		return v.String()
	case types.Decimal64:
		return v.Format(0)
	case types.Decimal128:
		return v.Format(0)
	case types.Enum:
		return v.String()
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
		"mo_pitr":                     0,
		catalog.MO_TABLE_STATS:        0,
	}
)

// enum("a","b","c") -> CastIndexToValue(1) -> "a"
// CastIndexToValue returns enum type index according to the value
func CastIndexToValue(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	typeEnums := vector.GenerateFunctionStrParameter(ivecs[0])
	indexs := vector.GenerateFunctionFixedTypeParameter[types.Enum](ivecs[1])

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

// enum("a","b","c") -> CastValueToIndex("a") -> 1
// CastValueToIndex returns enum type index according to the value
func CastValueToIndex(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Enum](result)
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

			var index types.Enum
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

// enum("a","b","c") -> CastIndexValueToIndex(1) -> 1
// CastIndexValueToIndex returns enum type index according to the index value
func CastIndexValueToIndex(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Enum](result)
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
			var index types.Enum

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
func CastNanoToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
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

// CastRangeValueUnit returns the value in hour unit according to the range value and unit
func CastRangeValueUnit(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryFixedStrToFixedWithErrorCheck[uint8, int64](ivecs, result, proc, length,
		castRangevalueUnitToHourUnit, selectList)
}

func castRangevalueUnitToHourUnit(value uint8, unit string) (int64, error) {
	switch unit {
	case "h":
		return int64(value), nil
	case "d":
		return int64(value) * 24, nil
	case "mo":
		return int64(value) * 24 * 30, nil
	case "y":
		return int64(value) * 24 * 365, nil
	default:
		return -1, moerr.NewInvalidArgNoCtx("invalid pitr time unit %s", unit)
	}
}
