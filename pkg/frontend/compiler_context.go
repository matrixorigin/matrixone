// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ plan2.CompilerContext = &TxnCompilerContext{}

type TxnCompilerContext struct {
	dbName               string
	buildAlterView       bool
	dbOfView, nameOfView string
	sub                  *plan.SubscriptionMeta
	snapshot             *plan2.Snapshot
	views                []string
	//for support explain analyze
	tcw     ComputationWrapper
	execCtx *ExecCtx
	mu      sync.Mutex
}

func (tcc *TxnCompilerContext) Close() {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.execCtx = nil
	tcc.snapshot = nil
	tcc.views = nil
}

func (tcc *TxnCompilerContext) GetLowerCaseTableNames() int64 {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	val, err := tcc.execCtx.ses.GetSessionSysVar("lower_case_table_names")
	if err != nil {
		val = int64(1)
	}
	return val.(int64)
}

func (tcc *TxnCompilerContext) SetExecCtx(execCtx *ExecCtx) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.execCtx = execCtx
}

func (tcc *TxnCompilerContext) GetViews() []string {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.views
}

func (tcc *TxnCompilerContext) SetViews(views []string) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.views = views
}

func (tcc *TxnCompilerContext) GetSnapshot() *plan2.Snapshot {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.snapshot
}

func (tcc *TxnCompilerContext) SetSnapshot(snapshot *plan2.Snapshot) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.snapshot = snapshot
}

func (tcc *TxnCompilerContext) InitExecuteStmtParam(execPlan *plan.Execute) (*plan.Plan, tree.Statement, error) {
	_, p, st, _, err := initExecuteStmtParam(tcc.execCtx.reqCtx, tcc.execCtx.ses.(*Session), tcc.tcw.(*TxnComputationWrapper), execPlan)
	return p, st, err
}

func (tcc *TxnCompilerContext) GetStatsCache() *plan2.StatsCache {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.execCtx.ses.GetStatsCache()
}

func InitTxnCompilerContext(db string) *TxnCompilerContext {
	return &TxnCompilerContext{dbName: db}
}

func (tcc *TxnCompilerContext) SetBuildingAlterView(yesOrNo bool, dbName, viewName string) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.buildAlterView = yesOrNo
	tcc.dbOfView = dbName
	tcc.nameOfView = viewName
}

func (tcc *TxnCompilerContext) GetBuildingAlterView() (bool, string, string) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.buildAlterView, tcc.dbOfView, tcc.nameOfView
}

func (tcc *TxnCompilerContext) GetSession() FeSession {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.execCtx.ses
}

func (tcc *TxnCompilerContext) GetTxnHandler() *TxnHandler {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.execCtx.ses.GetTxnHandler()
}

func (tcc *TxnCompilerContext) GetUserName() string {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.execCtx.ses.GetUserName()
}

func (tcc *TxnCompilerContext) SetDatabase(db string) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.dbName = db
}

func (tcc *TxnCompilerContext) GetDatabase() string {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.dbName
}

func (tcc *TxnCompilerContext) DefaultDatabase() string {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.dbName
}

func (tcc *TxnCompilerContext) GetRootSql() string {
	return tcc.GetSession().GetSql()
}

func (tcc *TxnCompilerContext) GetAccountId() (uint32, error) {
	return tcc.execCtx.ses.GetAccountId(), nil
}

func (tcc *TxnCompilerContext) GetContext() context.Context {
	return tcc.execCtx.reqCtx
}

func (tcc *TxnCompilerContext) SetContext(ctx context.Context) {
	tcc.execCtx.reqCtx = ctx
}

func (tcc *TxnCompilerContext) DatabaseExists(name string, snapshot *plan2.Snapshot) bool {
	var err error
	tempCtx := tcc.execCtx.reqCtx
	txn := tcc.GetTxnHandler().GetTxn()

	// change txn to snapshot txn
	if plan2.IsSnapshotValid(snapshot) && snapshot.TS.Less(txn.Txn().SnapshotTS) {
		txn = txn.CloneSnapshotOp(*snapshot.TS)

		if snapshot.Tenant != nil {
			tempCtx = context.WithValue(tempCtx, defines.TenantIDKey{}, snapshot.Tenant.TenantID)
		}
	}

	//open database
	ses := tcc.GetSession()
	_, err = tcc.GetTxnHandler().GetStorage().Database(tempCtx, name, txn)
	if err != nil {
		ses.Error(tempCtx,
			"Failed to get database",
			zap.String("databaseName", name),
			zap.Error(err))
		return false
	}

	return true
}

func (tcc *TxnCompilerContext) GetDatabaseId(dbName string, snapshot *plan2.Snapshot) (uint64, error) {
	dbName, _, err := tcc.ensureDatabaseIsNotEmpty(dbName, false, snapshot)
	if err != nil {
		return 0, err
	}
	tempCtx := tcc.execCtx.reqCtx
	txn := tcc.GetTxnHandler().GetTxn()
	// change txn to snapshot txn

	if plan2.IsSnapshotValid(snapshot) && snapshot.TS.Less(txn.Txn().SnapshotTS) {
		txn = txn.CloneSnapshotOp(*snapshot.TS)

		if snapshot.Tenant != nil {
			tempCtx = context.WithValue(tempCtx, defines.TenantIDKey{}, snapshot.Tenant.TenantID)
		}
	}

	database, err := tcc.GetTxnHandler().GetStorage().Database(tempCtx, dbName, txn)
	if err != nil {
		return 0, err
	}
	databaseId, err := strconv.ParseUint(database.GetDatabaseId(tempCtx), 10, 64)
	if err != nil {
		return 0, moerr.NewInternalErrorf(tempCtx, "The databaseid of '%s' is not a valid number", dbName)
	}
	return databaseId, nil
}

func (tcc *TxnCompilerContext) GetDbLevelConfig(dbName string, varName string) (string, error) {
	switch varName {
	case "unique_check_on_autoincr":
		// check if the database is a system database
		if _, ok := sysDatabases[dbName]; !ok {
			ses := tcc.GetSession()
			if _, ok := ses.(*backSession); ok {
				return "None", nil
			}
			ret, err := ses.GetConfig(tcc.GetContext(), dbName, varName)
			if err != nil {
				return "", err
			}
			if ret != nil {
				return ret.(string), nil
			}
			return "None", nil
		}
		return "Check", nil
	default:
	}
	return "", moerr.NewInternalErrorf(tcc.GetContext(), "The variable '%s' is not a valid database level variable", varName)
}

// getRelation returns the context (maybe updated) and the relation
func (tcc *TxnCompilerContext) getRelation(dbName string, tableName string, sub *plan.SubscriptionMeta, snapshot *plan2.Snapshot) (context.Context, engine.Relation, error) {
	dbName, _, err := tcc.ensureDatabaseIsNotEmpty(dbName, false, snapshot)
	if err != nil {
		return nil, nil, err
	}

	start := time.Now()
	defer func() {
		v2.GetRelationDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	ses := tcc.GetSession()
	txn := tcc.GetTxnHandler().GetTxn()
	tempCtx := tcc.execCtx.reqCtx

	if plan2.IsSnapshotValid(snapshot) && snapshot.TS.Less(txn.Txn().SnapshotTS) {
		txn = txn.CloneSnapshotOp(*snapshot.TS)

		if snapshot.Tenant != nil {
			tempCtx = context.WithValue(tempCtx, defines.TenantIDKey{}, snapshot.Tenant.TenantID)
		}
	}

	account := ses.GetTenantInfo()
	if isClusterTable(dbName, tableName) {
		//if it is the cluster table in the general account, switch into the sys account
		if account != nil && account.GetTenantID() != sysAccountID {
			tempCtx = defines.AttachAccountId(tempCtx, sysAccountID)
		}
	}
	if sub != nil {
		tempCtx = defines.AttachAccountId(tempCtx, uint32(sub.AccountId))
		dbName = sub.DbName
	}

	//for system_metrics.metric and system.statement_info,
	//it is special under the no sys account, should switch into the sys account first.
	if dbName == catalog.MO_SYSTEM && tableName == catalog.MO_STATEMENT {
		tempCtx = defines.AttachAccountId(tempCtx, uint32(sysAccountID))
	}

	if dbName == catalog.MO_SYSTEM_METRICS && (tableName == catalog.MO_METRIC || tableName == catalog.MO_SQL_STMT_CU) {
		tempCtx = defines.AttachAccountId(tempCtx, uint32(sysAccountID))
	}

	start1 := time.Now()

	//open database
	db, err := tcc.GetTxnHandler().GetStorage().Database(tempCtx, dbName, txn)
	if err != nil {
		ses.Error(tempCtx,
			"Failed to get database",
			zap.String("databaseName", dbName),
			zap.Error(err))
		return nil, nil, err
	}

	v2.OpenDBDurationHistogram.Observe(time.Since(start1).Seconds())

	// tableNames, err := db.Relations(ctx)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// logDebugf(ses.GetDebugString(), "dbName %v tableNames %v", dbName, tableNames)

	start2 := time.Now()

	//open table
	table, err := db.Relation(tempCtx, tableName, nil)
	if err != nil {
		tmpTable, e := tcc.getTmpRelation(tempCtx, engine.GetTempTableName(dbName, tableName))
		if e != nil {
			ses.Error(tempCtx,
				"Failed to get table",
				zap.String("tableName", tableName),
				zap.Error(err))
			return nil, nil, err
		} else {
			table = tmpTable
		}
	}

	v2.OpenTableDurationHistogram.Observe(time.Since(start2).Seconds())

	return tempCtx, table, nil
}

func (tcc *TxnCompilerContext) getTmpRelation(ctx context.Context, tableName string) (engine.Relation, error) {
	start := time.Now()
	defer func() {
		v2.GetTmpTableDurationHistogram.Observe(time.Since(start).Seconds())
	}()
	e := tcc.execCtx.ses.GetTxnHandler().GetStorage()
	txn := tcc.execCtx.ses.GetTxnHandler().GetTxn()
	db, err := e.Database(ctx, defines.TEMPORARY_DBNAME, txn)
	if err != nil {
		tcc.execCtx.ses.Error(ctx,
			"Failed to get temp database",
			zap.Error(err))
		return nil, err
	}
	table, err := db.Relation(ctx, tableName, nil)
	return table, err
}

func (tcc *TxnCompilerContext) ensureDatabaseIsNotEmpty(dbName string, checkSub bool, snapshot *plan2.Snapshot) (string, *plan.SubscriptionMeta, error) {
	start := time.Now()
	defer func() {
		v2.EnsureDatabaseDurationHistogram.Observe(time.Since(start).Seconds())
	}()
	if len(dbName) == 0 {
		dbName = tcc.DefaultDatabase()
	}
	if len(dbName) == 0 {
		return "", nil, moerr.NewNoDB(tcc.GetContext())
	}
	var sub *plan.SubscriptionMeta
	var err error
	if checkSub && !util.DbIsSystemDb(dbName) {
		sub, err = tcc.GetSubscriptionMeta(dbName, snapshot)
		if err != nil {
			return "", nil, err
		}
	}
	return dbName, sub, nil
}

func (tcc *TxnCompilerContext) ResolveById(tableId uint64, snapshot *plan2.Snapshot) (*plan2.ObjectRef, *plan2.TableDef) {
	tempCtx := tcc.execCtx.reqCtx
	txn := tcc.GetTxnHandler().GetTxn()

	if plan2.IsSnapshotValid(snapshot) && snapshot.TS.Less(txn.Txn().SnapshotTS) {
		txn = txn.CloneSnapshotOp(*snapshot.TS)

		if snapshot.Tenant != nil {
			tempCtx = context.WithValue(tempCtx, defines.TenantIDKey{}, snapshot.Tenant.TenantID)
		}
	}

	dbName, tableName, table, err := tcc.GetTxnHandler().GetStorage().GetRelationById(tempCtx, txn, tableId)
	if err != nil {
		return nil, nil
	}

	// convert
	obj := &plan2.ObjectRef{
		SchemaName: dbName,
		ObjName:    tableName,
		Obj:        int64(tableId),
	}
	tableDef := table.CopyTableDef(tempCtx)
	return obj, tableDef
}

func (tcc *TxnCompilerContext) ResolveSubscriptionTableById(tableId uint64, subMeta *plan.SubscriptionMeta) (*plan2.ObjectRef, *plan2.TableDef) {
	txn := tcc.GetTxnHandler().GetTxn()

	pubContext := tcc.execCtx.reqCtx
	if subMeta != nil {
		pubContext = context.WithValue(pubContext, defines.TenantIDKey{}, uint32(subMeta.AccountId))
	}

	dbName, tableName, table, err := tcc.GetTxnHandler().GetStorage().GetRelationById(pubContext, txn, tableId)
	if err != nil {
		return nil, nil
	}

	// convert
	obj := &plan2.ObjectRef{
		SchemaName: dbName,
		ObjName:    tableName,
		Obj:        int64(tableId),
	}
	tableDef := table.CopyTableDef(pubContext)
	return obj, tableDef
}

func (tcc *TxnCompilerContext) checkTableDefChange(dbName string, tableName string, originTblId uint64, originVersion uint32) (bool, error) {
	// In order to be compatible with various GUI clients and BI tools, lower case db and table name if it's a mysql system table
	if slices.Contains(mysql.CaseInsensitiveDbs, strings.ToLower(dbName)) {
		dbName = strings.ToLower(dbName)
		tableName = strings.ToLower(tableName)
	}

	dbName, sub, err := tcc.ensureDatabaseIsNotEmpty(dbName, true, nil)
	if err != nil || sub != nil && !pubsub.InSubMetaTables(sub, tableName) {
		return false, err
	}

	ctx, table, err := tcc.getRelation(dbName, tableName, sub, nil)
	if err != nil {
		return false, moerr.NewNoSuchTableNoCtx(dbName, tableName)
	}

	return table.GetTableDef(ctx).Version != originVersion || table.GetTableID(ctx) != originTblId, nil
}

func (tcc *TxnCompilerContext) Resolve(dbName string, tableName string, snapshot *plan2.Snapshot) (*plan2.ObjectRef, *plan2.TableDef) {
	start := time.Now()
	defer func() {
		end := time.Since(start).Seconds()
		v2.TxnStatementResolveDurationHistogram.Observe(end)
		v2.TotalResolveDurationHistogram.Observe(end)
	}()

	// In order to be compatible with various GUI clients and BI tools, lower case db and table name if it's a mysql system table
	if slices.Contains(mysql.CaseInsensitiveDbs, strings.ToLower(dbName)) {
		dbName = strings.ToLower(dbName)
		tableName = strings.ToLower(tableName)
	}

	dbName, sub, err := tcc.ensureDatabaseIsNotEmpty(dbName, true, snapshot)
	if err != nil || sub != nil && !pubsub.InSubMetaTables(sub, tableName) {
		return nil, nil
	}

	ctx, table, err := tcc.getRelation(dbName, tableName, sub, snapshot)
	if err != nil {
		return nil, nil
	}
	tableDef := table.CopyTableDef(ctx)
	if tableDef.IsTemporary {
		tableDef.Name = tableName
	}

	// convert
	var subscriptionName string
	var pubAccountId int32 = -1
	if sub != nil {
		subscriptionName = sub.SubName
		pubAccountId = sub.AccountId
		dbName = sub.DbName
	}

	obj := &plan2.ObjectRef{
		SchemaName:       dbName,
		ObjName:          tableName,
		Obj:              int64(table.GetTableID(ctx)),
		SubscriptionName: subscriptionName,
	}
	if pubAccountId != -1 {
		obj.PubInfo = &plan.PubInfo{
			TenantId: pubAccountId,
		}
	}
	return obj, tableDef
}

func (tcc *TxnCompilerContext) ResolveUdf(name string, args []*plan.Expr) (udf *function.Udf, err error) {
	var matchNum int
	var argstr string
	var argTypeStr string
	var sql string
	var erArray []ExecResult

	start := time.Now()
	defer func() {
		v2.TxnStatementResolveUdfDurationHistogram.Observe(time.Since(start).Seconds())
	}()
	ses := tcc.GetSession()
	ctx := tcc.execCtx.reqCtx

	err = inputNameIsInvalid(ctx, name)
	if err != nil {
		return nil, err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
		if execResultArrayHasData(erArray) {
			if matchNum < 1 {
				err = errors.Join(err, moerr.NewInvalidInput(ctx, fmt.Sprintf("No matching function for call to %s(%s)", name, argTypeStr)))
			} else if matchNum > 1 {
				err = errors.Join(err, moerr.NewInvalidInput(ctx, fmt.Sprintf("call to %s(%s) is ambiguous", name, argTypeStr)))
			}
		}
	}()
	if err != nil {
		return nil, err
	}

	sql = fmt.Sprintf(`select args, body, language, rettype, db, modified_time from mo_catalog.mo_user_defined_function where name = "%s" and db = "%s";`, name, tcc.DefaultDatabase())
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	if execResultArrayHasData(erArray) {
		fromList := make([]types.Type, len(args))
		for i, arg := range args {
			fromList[i] = types.Type{
				Oid:   types.T(arg.Typ.Id),
				Width: arg.Typ.Width,
				Scale: arg.Typ.Scale,
			}

			argTypeStr += strings.ToLower(fromList[i].String())
			if i+1 != len(args) {
				argTypeStr += ", "
			}
		}

		// find function which has min type cast cost in reload functions
		type MatchUdf struct {
			Udf      *function.Udf
			Cost     int
			TypeList []types.T
		}
		matchedList := make([]*MatchUdf, 0)

		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			argstr, err = erArray[0].GetString(ctx, i, 0)
			if err != nil {
				return nil, err
			}
			udf = &function.Udf{}
			udf.Body, err = erArray[0].GetString(ctx, i, 1)
			if err != nil {
				return nil, err
			}
			udf.Language, err = erArray[0].GetString(ctx, i, 2)
			if err != nil {
				return nil, err
			}
			udf.RetType, err = erArray[0].GetString(ctx, i, 3)
			if err != nil {
				return nil, err
			}
			udf.Db, err = erArray[0].GetString(ctx, i, 4)
			if err != nil {
				return nil, err
			}
			udf.ModifiedTime, err = erArray[0].GetString(ctx, i, 5)
			if err != nil {
				return nil, err
			}
			udf.ModifiedTime = strings.ReplaceAll(udf.ModifiedTime, " ", "_")
			udf.ModifiedTime = strings.ReplaceAll(udf.ModifiedTime, ":", "-")
			// arg type check
			argList := make([]*function.Arg, 0)
			err = json.Unmarshal([]byte(argstr), &argList)
			if err != nil {
				return nil, err
			}
			if len(argList) != len(args) { // mismatch
				continue
			}

			toList := make([]types.T, len(args))
			for j := range argList {
				if fromList[j].IsDecimal() && argList[j].Type == "decimal" {
					toList[j] = fromList[j].Oid
				} else {
					toList[j] = types.Types[argList[j].Type]
				}
			}

			canCast, cost := function.UdfArgTypeMatch(fromList, toList)
			if !canCast { // mismatch
				continue
			}

			udf.Args = argList
			matchedList = append(matchedList, &MatchUdf{
				Udf:      udf,
				Cost:     cost,
				TypeList: toList,
			})
		}

		if len(matchedList) == 0 {
			return nil, err
		}

		sort.Slice(matchedList, func(i, j int) bool {
			return matchedList[i].Cost < matchedList[j].Cost
		})

		minCost := matchedList[0].Cost
		for _, matchUdf := range matchedList {
			if matchUdf.Cost == minCost {
				matchNum++
			}
		}

		if matchNum == 1 {
			matchedList[0].Udf.ArgsType = function.UdfArgTypeCast(fromList, matchedList[0].TypeList)
			return matchedList[0].Udf, err
		}

		return nil, err
	} else {
		return nil, moerr.NewNotSupportedf(ctx, "function or operator '%s'", name)
	}
}

func (tcc *TxnCompilerContext) ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (varValue interface{}, err error) {
	stats := statistic.StatsInfoFromContext(tcc.execCtx.reqCtx)
	start := time.Now()
	defer func() {
		stats.AddBuildPlanResolveVarConsumption(time.Since(start))
	}()

	ctx := tcc.execCtx.reqCtx

	if ctx.Value(defines.InSp{}) != nil && ctx.Value(defines.InSp{}).(bool) {
		tmpScope := ctx.Value(defines.VarScopeKey{}).(*[]map[string]interface{})
		for i := len(*tmpScope) - 1; i >= 0; i-- {
			curScope := (*tmpScope)[i]
			if val, ok := curScope[strings.ToLower(varName)]; ok {
				return val, nil
			}
		}
	}

	if isSystemVar {
		if isGlobalVar {
			if varValue, err = tcc.GetSession().GetGlobalSysVar(varName); err != nil {
				return
			}
		} else {
			if varValue, err = tcc.GetSession().GetSessionSysVar(varName); err != nil {
				return
			}
		}
	} else {
		var udVar *UserDefinedVar
		if udVar, err = tcc.GetSession().GetUserDefinedVar(varName); err != nil || udVar == nil {
			return nil, err
		}

		varValue = udVar.Value
	}

	return
}

func (tcc *TxnCompilerContext) ResolveAccountIds(accountNames []string) (accountIds []uint32, err error) {
	var sql string
	var erArray []ExecResult
	var targetAccountId uint64
	if len(accountNames) == 0 {
		return []uint32{}, nil
	}

	dedup := make(map[string]int8)
	for _, name := range accountNames {
		dedup[name] = 1
	}

	ses := tcc.GetSession()
	ctx := tcc.execCtx.reqCtx
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return nil, err
	}

	for name := range dedup {
		sql, err = getSqlForCheckTenant(ctx, name)
		if err != nil {
			return nil, err
		}
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			return nil, err
		}

		erArray, err = getResultSet(ctx, bh)
		if err != nil {
			return nil, err
		}

		if execResultArrayHasData(erArray) {
			for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
				targetAccountId, err = erArray[0].GetUint64(ctx, i, 0)
				if err != nil {
					return nil, err
				}
			}
			accountIds = append(accountIds, uint32(targetAccountId))
		} else {
			return nil, moerr.NewInternalErrorf(ctx, "there is no account %s", name)
		}
	}
	return accountIds, err
}

func (tcc *TxnCompilerContext) GetPrimaryKeyDef(dbName string, tableName string, snapshot *plan2.Snapshot) []*plan2.ColDef {
	dbName, sub, err := tcc.ensureDatabaseIsNotEmpty(dbName, true, snapshot)
	if err != nil || sub != nil && !pubsub.InSubMetaTables(sub, tableName) {
		return nil
	}
	ctx, relation, err := tcc.getRelation(dbName, tableName, sub, snapshot)
	if err != nil {
		return nil
	}

	priKeys, err := relation.GetPrimaryKeys(ctx)
	if err != nil {
		return nil
	}
	if len(priKeys) == 0 {
		return nil
	}

	priDefs := make([]*plan2.ColDef, 0, len(priKeys))
	for _, key := range priKeys {
		priDefs = append(priDefs, &plan2.ColDef{
			Name:       strings.ToLower(key.Name),
			OriginName: key.Name,
			Typ: plan2.Type{
				Id:    int32(key.Type.Oid),
				Width: key.Type.Width,
				Scale: key.Type.Scale,
			},
			Primary: key.Primary,
		})
	}
	return priDefs
}

func (tcc *TxnCompilerContext) Stats(obj *plan2.ObjectRef, snapshot *plan2.Snapshot) (*pb.StatsInfo, error) {
	stats := statistic.StatsInfoFromContext(tcc.execCtx.reqCtx)
	start := time.Now()
	defer func() {
		v2.TxnStatementStatsDurationHistogram.Observe(time.Since(start).Seconds())
		stats.AddBuildPlanStatsConsumption(time.Since(start))
	}()

	dbName := obj.GetSchemaName()
	tableName := obj.GetObjName()
	checkSub := true
	if obj.PubInfo != nil {
		checkSub = false
	}
	dbName, sub, err := tcc.ensureDatabaseIsNotEmpty(dbName, checkSub, snapshot)
	if err != nil {
		return nil, err
	}
	if sub != nil && !pubsub.InSubMetaTables(sub, tableName) {
		return nil, moerr.NewInternalErrorNoCtxf("table %s not found in publication %s", tableName, sub.Name)
	}
	if !checkSub {
		sub = &plan.SubscriptionMeta{
			AccountId: obj.PubInfo.TenantId,
			DbName:    dbName,
		}
	}
	ctx, table, err := tcc.getRelation(dbName, tableName, sub, snapshot)
	if err != nil {
		return nil, err
	}
	cached, needUpdate := tcc.statsInCache(ctx, dbName, table, snapshot)
	if cached == nil {
		return nil, nil
	}
	if !needUpdate {
		return cached, nil
	}
	tableDefs, err := table.TableDefs(ctx)
	if err != nil {
		return nil, err
	}
	var partitionInfo *plan.PartitionByDef
	for _, def := range tableDefs {
		if partitionDef, ok := def.(*engine.PartitionDef); ok {
			if partitionDef.Partitioned > 0 {
				p := &plan.PartitionByDef{}
				err = p.UnMarshalPartitionInfo(([]byte)(partitionDef.Partition))
				if err != nil {
					return nil, err
				}
				partitionInfo = p
			}
			break
		}
	}
	var statsInfo *pb.StatsInfo
	// This is a partition table.
	if partitionInfo != nil {
		statsInfo = plan2.NewStatsInfo()
		for _, partitionTable := range partitionInfo.PartitionTableNames {
			parCtx, parTable, err := tcc.getRelation(dbName, partitionTable, sub, snapshot)
			if err != nil {
				return cached, err
			}
			parStats, err := parTable.Stats(parCtx, true)
			if err != nil {
				return cached, err
			}
			statsInfo.Merge(parStats)
		}
	} else {
		statsInfo, err = table.Stats(ctx, true)
		if err != nil {
			return cached, err
		}
	}
	if statsInfo != nil {
		tcc.UpdateStatsInCache(table.GetTableID(ctx), statsInfo)
		return statsInfo, nil
	}
	return cached, nil
}

func (tcc *TxnCompilerContext) UpdateStatsInCache(tid uint64, s *pb.StatsInfo) {
	tcc.GetStatsCache().SetStatsInfo(tid, s)
}

// statsInCache get the *pb.StatsInfo from session cache. If the info is nil, just return nil and false,
// else, check if the info needs to be updated.
func (tcc *TxnCompilerContext) statsInCache(ctx context.Context, dbName string, table engine.Relation, snapshot *plan2.Snapshot) (*pb.StatsInfo, bool) {
	s := tcc.GetStatsCache().GetStatsInfo(table.GetTableID(ctx), true)
	if s == nil {
		return nil, false
	}

	var partitionInfo *plan2.PartitionByDef
	engineDefs, err := table.TableDefs(ctx)
	if err != nil {
		return nil, false
	}
	for _, def := range engineDefs {
		if partitionDef, ok := def.(*engine.PartitionDef); ok {
			if partitionDef.Partitioned > 0 {
				p := &plan2.PartitionByDef{}
				err = p.UnMarshalPartitionInfo(([]byte)(partitionDef.Partition))
				if err != nil {
					return nil, false
				}
				partitionInfo = p
			}
		}
	}
	approxNumObjects := 0
	if partitionInfo != nil {
		if rand.Float32() < 0.999 {
			// for partition table,  do not update stats for 99.9% probability
			approxNumObjects = int(s.ApproxObjectNumber)
		} else {
			for _, PartitionTableName := range partitionInfo.PartitionTableNames {
				_, ptable, err := tcc.getRelation(dbName, PartitionTableName, nil, snapshot)
				if err != nil {
					return nil, false
				}
				approxNumObjects += ptable.ApproxObjectsNum(ctx)
			}
		}
	} else {
		approxNumObjects = table.ApproxObjectsNum(ctx)
	}
	if approxNumObjects == 0 {
		return nil, false
	}
	if s.NeedUpdate(int64(approxNumObjects)) {
		return s, true
	}
	return s, false
}

func (tcc *TxnCompilerContext) GetProcess() *process.Process {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.execCtx.proc
}

func (tcc *TxnCompilerContext) GetQueryResultMeta(uuid string) ([]*plan.ColDef, string, error) {
	proc := tcc.execCtx.proc
	// get file size
	path := catalog.BuildQueryResultMetaPath(proc.GetSessionInfo().Account, uuid)
	// read meta's meta
	reader, err := blockio.NewFileReader(proc.GetService(), proc.Base.FileService, path)
	if err != nil {
		return nil, "", err
	}
	idxs := make([]uint16, 2)
	idxs[0] = catalog.COLUMNS_IDX
	idxs[1] = catalog.RESULT_PATH_IDX
	// read meta's data
	bats, release, err := reader.LoadAllColumns(tcc.execCtx.reqCtx, idxs, common.DefaultAllocator)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil, "", moerr.NewResultFileNotFound(tcc.execCtx.reqCtx, makeResultMetaPath(proc.Base.SessionInfo.Account, uuid))
		}
		return nil, "", err
	}
	defer func() {
		if release != nil {
			release()
		}
	}()
	// cols
	vec := bats[0].Vecs[0]
	def := vec.UnsafeGetStringAt(0)
	r := &plan.ResultColDef{}
	if err = r.Unmarshal([]byte(def)); err != nil {
		return nil, "", err
	}
	// paths
	vec = bats[0].Vecs[1]
	str := vec.GetStringAt(0)
	return r.ResultCols, str, nil
}

func (tcc *TxnCompilerContext) GetSubscriptionMeta(dbName string, snapshot *plan2.Snapshot) (*plan.SubscriptionMeta, error) {
	start := time.Now()
	defer func() {
		v2.GetSubMetaDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	tempCtx := tcc.execCtx.reqCtx
	txn := tcc.GetTxnHandler().GetTxn()
	if plan2.IsSnapshotValid(snapshot) && snapshot.TS.Less(txn.Txn().SnapshotTS) {
		txn = txn.CloneSnapshotOp(*snapshot.TS)

		if snapshot.Tenant != nil {
			tempCtx = context.WithValue(tempCtx, defines.TenantIDKey{}, snapshot.Tenant.TenantID)
		}
	}

	return getSubscriptionMeta(tempCtx, dbName, tcc.GetSession(), txn)
}

func (tcc *TxnCompilerContext) CheckSubscriptionValid(subName, accName, pubName string) error {
	_, err := checkSubscriptionValidCommon(tcc.GetContext(), tcc.GetSession(), subName, accName, pubName)
	return err
}

func (tcc *TxnCompilerContext) SetQueryingSubscription(meta *plan.SubscriptionMeta) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.sub = meta
}

func (tcc *TxnCompilerContext) GetQueryingSubscription() *plan.SubscriptionMeta {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.sub
}

func (tcc *TxnCompilerContext) IsPublishing(dbName string) (bool, error) {
	return isDbPublishing(tcc.GetContext(), dbName, tcc.GetSession())
}

func (tcc *TxnCompilerContext) BuildTableDefByMoColumns(dbName, table string) (*plan.TableDef, error) {
	return buildTableDefFromMoColumns(tcc.GetContext(), uint64(tcc.GetSession().GetAccountId()), dbName, table, tcc.GetSession())
}

// makeResultMetaPath gets query result meta path
func makeResultMetaPath(accountName string, statementId string) string {
	return fmt.Sprintf("query_result_meta/%s_%s.blk", accountName, statementId)
}

func (tcc *TxnCompilerContext) ResolveSnapshotWithSnapshotName(snapshotName string) (*plan2.Snapshot, error) {
	tenantCtx := tcc.GetContext()
	if snapshot := tcc.GetSnapshot(); snapshot != nil && snapshot.GetTenant() != nil {
		tenantCtx = defines.AttachAccount(tenantCtx, snapshot.Tenant.TenantID, GetAdminUserId(), GetAccountAdminRoleId())
	}
	return doResolveSnapshotWithSnapshotName(tenantCtx, tcc.GetSession(), snapshotName)
}

func (tcc *TxnCompilerContext) CheckTimeStampValid(ts int64) (bool, error) {
	return checkTimeStampValid(tcc.GetContext(), tcc.GetSession(), ts)
}
