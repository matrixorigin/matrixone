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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type TxnCompilerContext struct {
	dbName               string
	buildAlterView       bool
	dbOfView, nameOfView string
	sub                  *plan.SubscriptionMeta
	snapshot             *plan2.Snapshot
	views                []string
	//for support explain analyze
	tcw     *TxnComputationWrapper
	execCtx *ExecCtx
	mu      sync.Mutex
}

var _ plan2.CompilerContext = &TxnCompilerContext{}

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

func (tcc *TxnCompilerContext) ReplacePlan(execPlan *plan.Execute) (*plan.Plan, tree.Statement, error) {
	p, st, _, err := replacePlan(tcc.execCtx.reqCtx, tcc.execCtx.ses.(*Session), tcc.tcw, execPlan)
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

func (tcc *TxnCompilerContext) DatabaseExists(name string, snapshot plan2.Snapshot) bool {
	var err error
	tempCtx := tcc.execCtx.reqCtx
	txn := tcc.GetTxnHandler().GetTxn()

	// change txn to snapshot txn
	if plan2.IsSnapshotValid(&snapshot) && snapshot.TS.Less(txn.Txn().SnapshotTS) {
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

func (tcc *TxnCompilerContext) GetDatabaseId(dbName string, snapshot plan2.Snapshot) (uint64, error) {
	dbName, _, err := tcc.ensureDatabaseIsNotEmpty(dbName, false, snapshot)
	if err != nil {
		return 0, err
	}
	tempCtx := tcc.execCtx.reqCtx
	txn := tcc.GetTxnHandler().GetTxn()
	// change txn to snapshot txn

	if plan2.IsSnapshotValid(&snapshot) && snapshot.TS.Less(txn.Txn().SnapshotTS) {
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
		return 0, moerr.NewInternalError(tempCtx, "The databaseid of '%s' is not a valid number", dbName)
	}
	return databaseId, nil
}

// getRelation returns the context (maybe updated) and the relation
func (tcc *TxnCompilerContext) getRelation(dbName string, tableName string, sub *plan.SubscriptionMeta, snapshot plan2.Snapshot) (context.Context, engine.Relation, error) {
	dbName, _, err := tcc.ensureDatabaseIsNotEmpty(dbName, false, snapshot)
	if err != nil {
		return nil, nil, err
	}

	ses := tcc.GetSession()
	txn := tcc.GetTxnHandler().GetTxn()
	tempCtx := tcc.execCtx.reqCtx

	if plan2.IsSnapshotValid(&snapshot) && snapshot.TS.Less(txn.Txn().SnapshotTS) {
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

	//open database
	db, err := tcc.GetTxnHandler().GetStorage().Database(tempCtx, dbName, txn)
	if err != nil {
		ses.Error(tempCtx,
			"Failed to get database",
			zap.String("databaseName", dbName),
			zap.Error(err))
		return nil, nil, err
	}

	// tableNames, err := db.Relations(ctx)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// logDebugf(ses.GetDebugString(), "dbName %v tableNames %v", dbName, tableNames)

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
	return tempCtx, table, nil
}

func (tcc *TxnCompilerContext) getTmpRelation(ctx context.Context, tableName string) (engine.Relation, error) {
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

func (tcc *TxnCompilerContext) ensureDatabaseIsNotEmpty(dbName string, checkSub bool, snapshot plan2.Snapshot) (string, *plan.SubscriptionMeta, error) {
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

func (tcc *TxnCompilerContext) ResolveById(tableId uint64, snapshot plan2.Snapshot) (*plan2.ObjectRef, *plan2.TableDef) {
	tempCtx := tcc.execCtx.reqCtx
	txn := tcc.GetTxnHandler().GetTxn()

	if plan2.IsSnapshotValid(&snapshot) && snapshot.TS.Less(txn.Txn().SnapshotTS) {
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

func (tcc *TxnCompilerContext) ResolveSubscriptionTableById(tableId uint64, pubmeta *plan.SubscriptionMeta) (*plan2.ObjectRef, *plan2.TableDef) {
	txn := tcc.GetTxnHandler().GetTxn()

	pubContext := tcc.execCtx.reqCtx
	if pubmeta != nil {
		pubContext = context.WithValue(pubContext, defines.TenantIDKey{}, uint32(pubmeta.AccountId))
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

func (tcc *TxnCompilerContext) Resolve(dbName string, tableName string, snapshot plan2.Snapshot) (*plan2.ObjectRef, *plan2.TableDef) {
	start := time.Now()
	defer func() {
		v2.TxnStatementResolveDurationHistogram.Observe(time.Since(start).Seconds())
	}()
	dbName, sub, err := tcc.ensureDatabaseIsNotEmpty(dbName, true, snapshot)
	if err != nil {
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
	tableDef.DbName = dbName

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
		return nil, moerr.NewNotSupported(ctx, "function or operator '%s'", name)
	}
}

func (tcc *TxnCompilerContext) ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
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
			return tcc.GetSession().GetGlobalSystemVariableValue(ctx, varName)
		} else {
			return tcc.GetSession().GetSessionVar(ctx, varName)
		}
	} else {
		_, val, err := tcc.GetSession().GetUserDefinedVar(varName)
		if val == nil {
			return nil, err
		}
		return val.Value, err
	}
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
			return nil, moerr.NewInternalError(ctx, "there is no account %s", name)
		}
	}
	return accountIds, err
}

func (tcc *TxnCompilerContext) GetPrimaryKeyDef(dbName string, tableName string, snapshot plan2.Snapshot) []*plan2.ColDef {
	dbName, sub, err := tcc.ensureDatabaseIsNotEmpty(dbName, true, snapshot)
	if err != nil {
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
			Name: key.Name,
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

func (tcc *TxnCompilerContext) Stats(obj *plan2.ObjectRef, snapshot plan2.Snapshot) (*pb.StatsInfo, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementStatsDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	dbName := obj.GetSchemaName()
	checkSub := true
	if obj.PubInfo != nil {
		checkSub = false
	}
	dbName, sub, err := tcc.ensureDatabaseIsNotEmpty(dbName, checkSub, snapshot)
	if err != nil {
		return nil, err
	}
	if !checkSub {
		sub = &plan.SubscriptionMeta{
			AccountId: obj.PubInfo.TenantId,
			DbName:    dbName,
		}
	}
	tableName := obj.GetObjName()
	ctx, table, err := tcc.getRelation(dbName, tableName, sub, snapshot)
	if err != nil {
		return nil, err
	}
	s, needUpdate := tcc.statsInCache(ctx, dbName, table, snapshot)
	if s == nil {
		return nil, nil
	}
	if needUpdate {
		s, err = table.Stats(ctx, true)
		if err != nil {
			return s, err
		}
		if s != nil {
			tcc.UpdateStatsInCache(table.GetTableID(ctx), s)
		}
	}
	return s, nil
}

func (tcc *TxnCompilerContext) UpdateStatsInCache(tid uint64, s *pb.StatsInfo) {
	tcc.GetStatsCache().SetStatsInfo(tid, s)
}

// statsInCache get the *pb.StatsInfo from session cache. If the info is nil, just return nil and false,
// else, check if the info needs to be updated.
func (tcc *TxnCompilerContext) statsInCache(ctx context.Context, dbName string, table engine.Relation, snapshot plan2.Snapshot) (*pb.StatsInfo, bool) {
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
		for _, PartitionTableName := range partitionInfo.PartitionTableNames {
			_, ptable, _ := tcc.getRelation(dbName, PartitionTableName, nil, snapshot)
			approxNumObjects += ptable.ApproxObjectsNum(ctx)
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
	path := catalog.BuildQueryResultMetaPath(proc.SessionInfo.Account, uuid)
	// read meta's meta
	reader, err := blockio.NewFileReader(proc.FileService, path)
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
			return nil, "", moerr.NewResultFileNotFound(tcc.execCtx.reqCtx, makeResultMetaPath(proc.SessionInfo.Account, uuid))
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
	def := vec.GetStringAt(0)
	r := &plan.ResultColDef{}
	if err = r.Unmarshal([]byte(def)); err != nil {
		return nil, "", err
	}
	// paths
	vec = bats[0].Vecs[1]
	str := vec.GetStringAt(0)
	return r.ResultCols, str, nil
}

func (tcc *TxnCompilerContext) GetSubscriptionMeta(dbName string, snapshot plan2.Snapshot) (*plan.SubscriptionMeta, error) {
	tempCtx := tcc.execCtx.reqCtx
	txn := tcc.GetTxnHandler().GetTxn()

	if plan2.IsSnapshotValid(&snapshot) && snapshot.TS.Less(txn.Txn().SnapshotTS) {
		txn = txn.CloneSnapshotOp(*snapshot.TS)

		if snapshot.Tenant != nil {
			tempCtx = context.WithValue(tempCtx, defines.TenantIDKey{}, snapshot.Tenant.TenantID)
		}
	}

	sub, err := getSubscriptionMeta(tempCtx, dbName, tcc.GetSession(), txn)
	if err != nil {
		return nil, err
	}
	return sub, nil
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
