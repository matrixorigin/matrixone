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
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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
	// cached backExec for subscription meta queries, reused within the same transaction
	cachedBackExec BackgroundExec
	mu             sync.Mutex
}

func (tcc *TxnCompilerContext) Close() {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	if tcc.cachedBackExec != nil {
		tcc.cachedBackExec.Close()
		tcc.cachedBackExec = nil
	}
	tcc.execCtx = nil
	tcc.snapshot = nil
	tcc.views = nil
}

func (tcc *TxnCompilerContext) GetLowerCaseTableNames() int64 {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	val, err := tcc.execCtx.ses.GetSessionSysVar("lower_case_table_names")
	if err != nil || val == nil {
		return 1
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
	_, p, st, _, err := initExecuteStmtParam(tcc.execCtx, tcc.execCtx.ses.(*Session), tcc.tcw.(*TxnComputationWrapper), execPlan, "")
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

func (tcc *TxnCompilerContext) GetAccountName() string {
	return tcc.execCtx.ses.GetTenantName()
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

func (tcc *TxnCompilerContext) GetConfig(varName string, dbName string, tblName string) (string, error) {
	switch varName {
	case "unique_check_on_autoincr":
		// check if the database is a system database
		if _, ok := sysDatabases[dbName]; !ok {
			ses := tcc.GetSession()
			if _, ok := ses.(*backSession); ok {
				return "None", nil
			}
			ret, err := ses.GetConfig(tcc.GetContext(), varName, dbName, tblName)
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

// for system_metrics.metric and system.statement_info,
// it is special under the no sys account, should switch into the sys account first.
func ShouldSwitchToSysAccount(dbName string, tableName string) bool {
	if dbName == catalog.MO_SYSTEM && tableName == catalog.MO_STATEMENT {
		return true
	}

	if dbName == catalog.MO_SYSTEM_METRICS && (tableName == catalog.MO_METRIC || tableName == catalog.MO_SQL_STMT_CU) {
		return true
	}
	return false
}

// getRelation returns the context (maybe updated) and the relation
func (tcc *TxnCompilerContext) getRelation(
	dbName string,
	tableName string,
	sub *plan.SubscriptionMeta,
	snapshot *plan2.Snapshot,
) (context.Context, engine.Relation, error) {
	dbName, _, err := tcc.ensureDatabaseIsNotEmpty(dbName, false, snapshot)
	if err != nil {
		return nil, nil, err
	}

	var (
		start   = time.Now()
		ses     = tcc.GetSession()
		txn     = tcc.GetTxnHandler().GetTxn()
		tempCtx = tcc.execCtx.reqCtx
	)

	defer func() {
		v2.GetRelationDurationHistogram.Observe(time.Since(start).Seconds())
	}()

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

	if ShouldSwitchToSysAccount(dbName, tableName) {
		tempCtx = defines.AttachAccountId(tempCtx, uint32(sysAccountID))
	}

	start = time.Now()

	var (
		db    engine.Database
		table engine.Relation
	)

	//open database
	if db, err = tcc.GetTxnHandler().GetStorage().Database(
		tempCtx, dbName, txn,
	); err != nil {
		ses.Error(
			tempCtx,
			"fe-get-database-failed",
			zap.String("db-name", dbName),
			zap.Error(err),
		)
		return nil, nil, err
	} else {
		v2.OpenDBDurationHistogram.Observe(time.Since(start).Seconds())
	}

	start = time.Now()

	if table, err = db.Relation(tempCtx, tableName, nil); err != nil {
		// maybe have (if exists)
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	v2.OpenTableDurationHistogram.Observe(time.Since(start).Seconds())

	return tempCtx, table, nil
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

func (tcc *TxnCompilerContext) ResolveById(tableId uint64, snapshot *plan2.Snapshot) (*plan2.ObjectRef, *plan2.TableDef, error) {
	tempCtx := tcc.execCtx.reqCtx
	txn := tcc.GetTxnHandler().GetTxn()

	if plan2.IsSnapshotValid(snapshot) && snapshot.TS.Less(txn.Txn().SnapshotTS) {
		txn = txn.CloneSnapshotOp(*snapshot.TS)

		if snapshot.Tenant != nil {
			tempCtx = context.WithValue(tempCtx, defines.TenantIDKey{}, snapshot.Tenant.TenantID)
		}
	}

	eng := tcc.GetTxnHandler().GetStorage()
	dbName, tableName, table, err := eng.GetRelationById(tempCtx, txn, tableId)
	if err != nil {
		return nil, nil, err
	}

	// convert
	returnTableID := int64(tableId)
	obj := &plan2.ObjectRef{
		SchemaName: dbName,
		ObjName:    tableName,
		Obj:        returnTableID,
	}
	tableDef := table.CopyTableDef(tempCtx)
	return obj, tableDef, nil
}

func (tcc *TxnCompilerContext) ResolveSubscriptionTableById(tableId uint64, subMeta *plan.SubscriptionMeta) (*plan2.ObjectRef, *plan2.TableDef, error) {
	txn := tcc.GetTxnHandler().GetTxn()

	pubContext := tcc.execCtx.reqCtx
	if subMeta != nil {
		pubContext = context.WithValue(pubContext, defines.TenantIDKey{}, uint32(subMeta.AccountId))
	}

	eng := tcc.GetTxnHandler().GetStorage()
	dbName, tableName, table, err := eng.GetRelationById(pubContext, txn, tableId)
	if err != nil {
		return nil, nil, err
	}

	// convert
	returnTableID := int64(tableId)
	obj := &plan2.ObjectRef{
		SchemaName: dbName,
		ObjName:    tableName,
		Obj:        returnTableID,
	}
	tableDef := table.CopyTableDef(pubContext)
	return obj, tableDef, nil
}

func (tcc *TxnCompilerContext) Resolve(dbName string, tableName string, snapshot *plan2.Snapshot) (*plan2.ObjectRef, *plan2.TableDef, error) {
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
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoDB) {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	if sub != nil {
		isSubMetaTable := pubsub.InSubMetaTables(sub, tableName)
		if !isSubMetaTable {
			return nil, nil, nil
		}
	}

	// Check if it is a temporary table in the current session
	realName, isTmpTable := tcc.GetSession().GetTempTable(dbName, tableName)
	if isTmpTable {
		tableName = realName
	}

	ctx, table, err := tcc.getRelation(dbName, tableName, sub, snapshot)
	if err != nil {
		return nil, nil, err
	}
	if table == nil {
		return nil, nil, nil
	}
	tableDef := table.CopyTableDef(ctx)
	tableDef.IsTemporary = isTmpTable

	// convert
	var subscriptionName string
	var pubAccountId int32 = -1
	if sub != nil {
		subscriptionName = sub.SubName
		pubAccountId = sub.AccountId
		dbName = sub.DbName
	}

	tableID := int64(table.GetTableID(ctx))
	obj := &plan2.ObjectRef{
		SchemaName:       dbName,
		ObjName:          tableName,
		Obj:              tableID,
		SubscriptionName: subscriptionName,
	}
	if pubAccountId != -1 {
		obj.PubInfo = &plan.PubInfo{
			TenantId: pubAccountId,
		}
	}
	return obj, tableDef, nil
}

func (tcc *TxnCompilerContext) ResolveIndexTableByRef(
	ref *plan.ObjectRef,
	tblName string,
	snapshot *plan2.Snapshot,
) (*plan2.ObjectRef, *plan2.TableDef, error) {
	start := time.Now()
	defer func() {
		end := time.Since(start).Seconds()
		v2.TxnStatementResolveDurationHistogram.Observe(end)
		v2.TotalResolveDurationHistogram.Observe(end)
	}()

	// no need to ensureDatabaseIsNotEmpty

	var subMeta *plan.SubscriptionMeta
	if ref.PubInfo != nil {
		subMeta = &plan.SubscriptionMeta{
			AccountId: ref.PubInfo.TenantId,
		}
	}

	// Check if it is a temporary table in the current session
	realName, isTmpTable := tcc.GetSession().GetTempTable(ref.SchemaName, tblName)
	if isTmpTable {
		tblName = realName
	}

	ctx, table, err := tcc.getRelation(ref.SchemaName, tblName, subMeta, snapshot)
	if err != nil {
		return nil, nil, err
	}
	if table == nil {
		return nil, nil, moerr.NewNoSuchTable(ctx, ref.SchemaName, tblName)
	}

	tableID := int64(table.GetTableID(ctx))
	obj := &plan2.ObjectRef{
		SchemaName:       ref.SchemaName,
		ObjName:          tblName,
		Obj:              tableID,
		SubscriptionName: ref.SubscriptionName,
		PubInfo:          ref.PubInfo,
	}

	tableDef := table.CopyTableDef(ctx)
	if tableDef.IsTemporary {
		tableDef.Name = tblName
	}

	return obj, tableDef, nil
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
		if udVar, err = tcc.GetSession().GetUserDefinedVar(varName); err != nil {
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

func (tcc *TxnCompilerContext) Stats(obj *plan2.ObjectRef, snapshot *plan2.Snapshot) (*pb.StatsInfo, error) {
	statser := statistic.StatsInfoFromContext(tcc.execCtx.reqCtx)
	start := time.Now()
	defer func() {
		v2.TxnStatementStatsDurationHistogram.Observe(time.Since(start).Seconds())
		statser.AddBuildPlanStatsConsumption(time.Since(start))
	}()

	tableID := uint64(obj.Obj)

	// Fast path: return cached result if visited within 3 seconds AND stats is valid
	// Stats is valid if AccurateObjectNumber > 0 (meaning we have real data)
	if w := tcc.GetStatsCache().Get(tableID); w.Exists() {
		if time.Now().Unix()-w.GetLastVisit() < 3 {
			s := w.GetStats()
			if s != nil && s.AccurateObjectNumber > 0 {
				return s, nil
			}
			// Stats is nil or empty, need to re-check
		}
	}

	// Slow path: do heavy work
	result, err := tcc.doStatsHeavyWork(obj, snapshot, tableID)
	if err != nil {
		return nil, err
	}

	// Cache the result
	tcc.GetStatsCache().Set(tableID, result)

	return result, nil
}

func (tcc *TxnCompilerContext) doStatsHeavyWork(obj *plan2.ObjectRef, snapshot *plan2.Snapshot, tableID uint64) (*pb.StatsInfo, error) {
	dbName := obj.GetSchemaName()
	tableName := obj.GetObjName()

	// 1. Check database and subscription
	checkSub := obj.PubInfo == nil
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

	// 2. Get table relation
	ctx, table, err := tcc.getRelation(dbName, tableName, sub, snapshot)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, moerr.NewNoSuchTable(ctx, dbName, tableName)
	}

	// 4. Call table.Stats() to get new data
	newCtx := perfcounter.AttachCalcTableStatsKey(ctx)
	stats, err := table.Stats(newCtx, true)
	if err != nil {
		return nil, err
	}
	if stats != nil && stats.AccurateObjectNumber > 0 {
		return stats, nil
	}
	// Return nil for empty table, calcScanStats will use DefaultStats()
	return nil, nil
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
	reader, err := ioutil.NewFileReader(proc.Base.FileService, path)
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

	// lower col name
	for _, col := range r.ResultCols {
		col.Name = strings.ToLower(col.Name)
	}
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

	bh := tcc.getOrCreateBackExec(tempCtx)
	bh.ClearExecResultSet()
	return getSubscriptionMeta(tempCtx, dbName, tcc.GetSession(), txn, bh)
}

func (tcc *TxnCompilerContext) CheckSubscriptionValid(subName, accName, pubName string) error {
	bh := tcc.getOrCreateBackExec(tcc.GetContext())
	bh.ClearExecResultSet()
	_, err := checkSubscriptionValidCommon(tcc.GetContext(), tcc.GetSession(), subName, accName, pubName, bh)
	return err
}

// getOrCreateBackExec returns a cached BackgroundExec or creates a new one.
// The cached backExec shares the same txn with the session, so snapshot updates
// (e.g., in pessimistic transactions after lock acquisition) are automatically visible.
// If the session's txn has changed (e.g., after commit/rollback in autocommit mode),
// the cached backExec's txn reference is updated instead of recreating the entire object.
func (tcc *TxnCompilerContext) getOrCreateBackExec(ctx context.Context) BackgroundExec {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()

	currentTxn := tcc.execCtx.ses.GetTxnHandler().GetTxn()

	if tcc.cachedBackExec != nil {
		cachedTxn := tcc.cachedBackExec.(*backExec).backSes.GetTxnHandler().GetTxn()
		// If txn changed (e.g., after commit/rollback), update the txn reference
		if cachedTxn != currentTxn {
			tcc.cachedBackExec.(*backExec).UpdateTxn(currentTxn)
		}
	} else {
		tcc.cachedBackExec = tcc.execCtx.ses.GetShareTxnBackgroundExec(ctx, false)
	}
	return tcc.cachedBackExec
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
