// Copyright 2023 Matrix Origin
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

package compile

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ plan.CompilerContext = new(compilerContext)

type compilerContext struct {
	ctx        context.Context
	defaultDB  string
	engine     engine.Engine
	proc       *process.Process
	statsCache *plan.StatsCache

	buildAlterView       bool
	dbOfView, nameOfView string
	sql                  string
	mu                   sync.Mutex

	lower int64
}

func (c *compilerContext) GetLowerCaseTableNames() int64 {
	return c.lower
}

func (c *compilerContext) GetViews() []string {
	return nil
}

func (c *compilerContext) SetViews(views []string) {}

func (c *compilerContext) GetSnapshot() *plan.Snapshot {
	return nil
}

func (c *compilerContext) SetSnapshot(snapshot *plan.Snapshot) {
}

func (c *compilerContext) InitExecuteStmtParam(execPlan *planpb.Execute) (*planpb.Plan, tree.Statement, error) {
	//TODO implement me
	panic("implement me")
}

func (c *compilerContext) CheckSubscriptionValid(subName, accName string, pubName string) error {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) ResolveSubscriptionTableById(tableId uint64, pubmeta *plan.SubscriptionMeta) (*plan.ObjectRef, *plan.TableDef, error) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) IsPublishing(dbName string) (bool, error) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) BuildTableDefByMoColumns(dbName, table string) (*plan.TableDef, error) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) ResolveSnapshotWithSnapshotName(snapshotName string) (*plan.Snapshot, error) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) CheckTimeStampValid(ts int64) (bool, error) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) SetQueryingSubscription(meta *plan.SubscriptionMeta) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) GetQueryingSubscription() *plan.SubscriptionMeta {
	return nil
}

func (c *compilerContext) ResolveUdf(name string, ast []*plan.Expr) (*function.Udf, error) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) ResolveAccountIds(accountNames []string) ([]uint32, error) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) Stats(obj *plan.ObjectRef, snapshot *plan.Snapshot) (*pb.StatsInfo, error) {
	stats := statistic.StatsInfoFromContext(c.GetContext())
	start := time.Now()
	defer func() {
		stats.AddBuildPlanStatsConsumption(time.Since(start))
	}()

	tableID := uint64(obj.Obj)

	// Fast path: return cached result if visited within 3 seconds AND stats is valid
	// Stats is valid if AccurateObjectNumber > 0 (meaning we have real data)
	if w := c.GetStatsCache().Get(tableID); w.Exists() {
		if time.Now().Unix()-w.GetLastVisit() < 3 {
			s := w.GetStats()
			if s != nil && s.AccurateObjectNumber > 0 {
				return s, nil
			}
			// Stats is nil or empty, need to re-check
		}
	}

	// Slow path: do heavy work
	result, err := c.doStatsHeavyWork(obj, snapshot, tableID)
	if err != nil {
		return nil, err
	}

	// Cache the result
	c.GetStatsCache().Set(tableID, result)

	return result, nil
}

func (c *compilerContext) doStatsHeavyWork(obj *plan.ObjectRef, snapshot *plan.Snapshot, tableID uint64) (*pb.StatsInfo, error) {
	dbName := obj.GetSchemaName()
	tableName := obj.GetObjName()

	dbName, err := c.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil, err
	}
	ctx, table, err := c.getRelation(dbName, tableName, snapshot)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, moerr.NewNoSuchTable(ctx, dbName, tableName)
	}

	// Call table.Stats() to get new data
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

func (c *compilerContext) GetStatsCache() *plan.StatsCache {
	if c.statsCache == nil {
		c.statsCache = plan.NewStatsCache()
	}
	return c.statsCache
}

func (c *compilerContext) GetSubscriptionMeta(dbName string, snapshot *plan.Snapshot) (*plan.SubscriptionMeta, error) {
	return nil, nil
}

func (c *compilerContext) GetProcess() *process.Process {
	return c.proc
}

func (c *compilerContext) GetQueryResultMeta(uuid string) ([]*plan.ColDef, string, error) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) DatabaseExists(name string, snapshot *plan.Snapshot) bool {
	ctx := c.GetContext()
	txnOpt := c.proc.GetTxnOperator()

	if plan.IsSnapshotValid(snapshot) && snapshot.TS.Less(c.proc.GetTxnOperator().Txn().SnapshotTS) {
		txnOpt = c.proc.GetTxnOperator().CloneSnapshotOp(*snapshot.TS)

		if snapshot.Tenant != nil {
			ctx = context.WithValue(ctx, defines.TenantIDKey{}, snapshot.Tenant.TenantID)
		}
	}

	_, err := c.engine.Database(
		ctx,
		name,
		txnOpt,
	)
	return err == nil
}

func (c *compilerContext) GetDatabaseId(dbName string, snapshot *plan.Snapshot) (uint64, error) {
	ctx := c.GetContext()
	txnOpt := c.proc.GetTxnOperator()

	if plan.IsSnapshotValid(snapshot) && snapshot.TS.Less(c.proc.GetTxnOperator().Txn().SnapshotTS) {
		txnOpt = c.proc.GetTxnOperator().CloneSnapshotOp(*snapshot.TS)

		if snapshot.Tenant != nil {
			ctx = context.WithValue(ctx, defines.TenantIDKey{}, snapshot.Tenant.TenantID)
		}
	}

	database, err := c.engine.Database(ctx, dbName, txnOpt)

	if err != nil {
		return 0, err
	}
	databaseId, err := strconv.ParseUint(database.GetDatabaseId(ctx), 10, 64)
	if err != nil {
		return 0, moerr.NewInternalErrorf(ctx, "The databaseid of '%s' is not a valid number", dbName)
	}
	return databaseId, nil
}

func (c *compilerContext) GetConfig(varName, dbName, tblName string) (string, error) {
	switch varName {
	// For scenarios that are background SQL, use the default configuration to avoid triggering background SQL again.
	case "unique_check_on_autoincr":
		return "Check", nil
	default:
		return "", moerr.NewInternalErrorf(c.GetContext(), "The variable '%s' is not a valid database level variable", varName)
	}
}

func (c *compilerContext) DefaultDatabase() string {
	if c.lower == 0 {
		return c.defaultDB
	}
	return strings.ToLower(c.defaultDB)
}

func (c *compilerContext) GetRootSql() string {
	return c.sql
}

func (c *compilerContext) SetRootSql(sql string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sql = sql
}

func (c *compilerContext) GetUserName() string {
	return "root"
}

func (c *compilerContext) GetAccountId() (uint32, error) {
	return defines.GetAccountId(c.proc.GetTopContext())
}

func (c *compilerContext) GetAccountName() string {
	return "sys"
}
func (c *compilerContext) GetContext() context.Context {
	return c.proc.GetTopContext()
}

func (c *compilerContext) SetContext(ctx context.Context) {
	c.proc.ReplaceTopCtx(ctx)
}

func (c *compilerContext) ResolveById(tableId uint64, snapshot *plan.Snapshot) (objRef *plan.ObjectRef, tableDef *plan.TableDef, err error) {
	ctx := c.GetContext()
	txnOpt := c.proc.GetTxnOperator()

	if plan.IsSnapshotValid(snapshot) && snapshot.TS.Less(c.proc.GetTxnOperator().Txn().SnapshotTS) {
		txnOpt = c.proc.GetTxnOperator().CloneSnapshotOp(*snapshot.TS)

		if snapshot.Tenant != nil {
			ctx = context.WithValue(ctx, defines.TenantIDKey{}, snapshot.Tenant.TenantID)
		}
	}

	dbName, tableName, e := c.engine.GetNameById(ctx, txnOpt, tableId)
	if e != nil {
		return nil, nil, e
	}
	if dbName == "" || tableName == "" {
		return nil, nil, nil
	}
	return c.Resolve(dbName, tableName, snapshot)
}

func (c *compilerContext) ResolveIndexTableByRef(ref *plan.ObjectRef, tblName string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	return c.Resolve(plan.DbNameOfObjRef(ref), tblName, snapshot)
}

func (c *compilerContext) Resolve(dbName string, tableName string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	// In order to be compatible with various GUI clients and BI tools, lower case db and table name if it's a mysql system table
	if slices.Contains(mysql.CaseInsensitiveDbs, strings.ToLower(dbName)) {
		dbName = strings.ToLower(dbName)
		tableName = strings.ToLower(tableName)
	}

	dbName, err := c.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoDB) {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	ctx, table, err := c.getRelation(dbName, tableName, snapshot)
	if err != nil {
		return nil, nil, err
	}
	if table == nil {
		return nil, nil, nil
	}

	tableDef := table.CopyTableDef(ctx)
	if tableDef.IsTemporary {
		tableDef.Name = tableName
	}
	tableID := int64(table.GetTableID(ctx))
	obj := &plan.ObjectRef{
		SchemaName: dbName,
		ObjName:    tableName,
		Obj:        tableID,
	}
	return obj, tableDef, nil
}

func (c *compilerContext) ResolveVariable(varName string, isSystemVar bool, isGlobalVar bool) (interface{}, error) {
	return nil, nil
}

func (c *compilerContext) SetBuildingAlterView(yesOrNo bool, dbName, viewName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buildAlterView = yesOrNo
	c.dbOfView = dbName
	c.nameOfView = viewName
}

func (c *compilerContext) GetBuildingAlterView() (bool, string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buildAlterView, c.dbOfView, c.nameOfView
}

func (c *compilerContext) ensureDatabaseIsNotEmpty(dbName string) (string, error) {
	if len(dbName) == 0 {
		dbName = c.DefaultDatabase()
	}
	if len(dbName) == 0 {
		return "", moerr.NewNoDB(c.GetContext())
	}
	return dbName, nil
}

func (c *compilerContext) getRelation(
	dbName string,
	tableName string,
	snapshot *plan.Snapshot,
) (context.Context, engine.Relation, error) {
	dbName, err := c.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil, nil, err
	}

	ctx := c.GetContext()
	txnOpt := c.proc.GetTxnOperator()

	if plan.IsSnapshotValid(snapshot) && snapshot.TS.Less(c.proc.GetTxnOperator().Txn().SnapshotTS) {
		txnOpt = c.proc.GetTxnOperator().CloneSnapshotOp(*snapshot.TS)

		if snapshot.Tenant != nil {
			ctx = context.WithValue(ctx, defines.TenantIDKey{}, snapshot.Tenant.TenantID)
		}
	}

	db, err := c.engine.Database(ctx, dbName, txnOpt)
	if err != nil {
		return nil, nil, err
	}

	table, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	return ctx, table, nil
}
