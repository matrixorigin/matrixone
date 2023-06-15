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
	"strconv"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type TxnCompilerContext struct {
	dbName               string
	txnHandler           *TxnHandler
	ses                  *Session
	proc                 *process.Process
	buildAlterView       bool
	dbOfView, nameOfView string
	sub                  *plan.SubscriptionMeta
	mu                   sync.Mutex
}

var _ plan2.CompilerContext = &TxnCompilerContext{}

func (tcc *TxnCompilerContext) GetStatsCache() *plan2.StatsCache {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.ses.statsCache
}

func InitTxnCompilerContext(txn *TxnHandler, db string) *TxnCompilerContext {
	return &TxnCompilerContext{txnHandler: txn, dbName: db}
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

func (tcc *TxnCompilerContext) SetSession(ses *Session) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.ses = ses
}

func (tcc *TxnCompilerContext) GetSession() *Session {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.ses
}

func (tcc *TxnCompilerContext) GetTxnHandler() *TxnHandler {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.txnHandler
}

func (tcc *TxnCompilerContext) GetUserName() string {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.ses.GetUserName()
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

func (tcc *TxnCompilerContext) GetAccountId() uint32 {
	return tcc.ses.accountId
}

func (tcc *TxnCompilerContext) GetContext() context.Context {
	return tcc.ses.requestCtx
}

func (tcc *TxnCompilerContext) DatabaseExists(name string) bool {
	var err error
	var txnCtx context.Context
	var txn TxnOperator
	txnCtx, txn, err = tcc.GetTxnHandler().GetTxn()
	if err != nil {
		return false
	}
	//open database
	ses := tcc.GetSession()
	_, err = tcc.GetTxnHandler().GetStorage().Database(txnCtx, name, txn)
	if err != nil {
		logErrorf(ses.GetDebugString(), "get database %v failed. error %v", name, err)
		return false
	}

	return true
}

func (tcc *TxnCompilerContext) GetDatabaseId(dbName string) (uint64, error) {
	dbName, _, err := tcc.ensureDatabaseIsNotEmpty(dbName, false)
	if err != nil {
		return 0, err
	}
	txnCtx, txn, err := tcc.GetTxnHandler().GetTxn()
	if err != nil {
		return 0, err
	}
	database, err := tcc.GetTxnHandler().GetStorage().Database(txnCtx, dbName, txn)
	if err != nil {
		return 0, err
	}
	databaseId, err := strconv.ParseUint(database.GetDatabaseId(txnCtx), 10, 64)
	if err != nil {
		return 0, moerr.NewInternalError(txnCtx, "The databaseid of '%s' is not a valid number", dbName)
	}
	return databaseId, nil
}

// getRelation returns the context (maybe updated) and the relation
func (tcc *TxnCompilerContext) getRelation(dbName string, tableName string, sub *plan.SubscriptionMeta) (context.Context, engine.Relation, error) {
	dbName, _, err := tcc.ensureDatabaseIsNotEmpty(dbName, false)
	if err != nil {
		return nil, nil, err
	}

	ses := tcc.GetSession()
	txnCtx, txn, err := tcc.GetTxnHandler().GetTxn()
	if err != nil {
		return nil, nil, err
	}
	account := ses.GetTenantInfo()
	if isClusterTable(dbName, tableName) {
		//if it is the cluster table in the general account, switch into the sys account
		if account != nil && account.GetTenantID() != sysAccountID {
			txnCtx = context.WithValue(txnCtx, defines.TenantIDKey{}, uint32(sysAccountID))
		}
	}
	if sub != nil {
		txnCtx = context.WithValue(txnCtx, defines.TenantIDKey{}, uint32(sub.AccountId))
		dbName = sub.DbName
	}

	//open database
	db, err := tcc.GetTxnHandler().GetStorage().Database(txnCtx, dbName, txn)
	if err != nil {
		logErrorf(ses.GetDebugString(), "get database %v error %v", dbName, err)
		return nil, nil, err
	}

	// tableNames, err := db.Relations(ctx)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// logDebugf(ses.GetDebugString(), "dbName %v tableNames %v", dbName, tableNames)

	//open table
	table, err := db.Relation(txnCtx, tableName)
	if err != nil {
		tmpTable, e := tcc.getTmpRelation(txnCtx, engine.GetTempTableName(dbName, tableName))
		if e != nil {
			logErrorf(ses.GetDebugString(), "get table %v error %v", tableName, err)
			return nil, nil, err
		} else {
			table = tmpTable
		}
	}
	return txnCtx, table, nil
}

func (tcc *TxnCompilerContext) getTmpRelation(_ context.Context, tableName string) (engine.Relation, error) {
	e := tcc.ses.storage
	txnCtx, txn, err := tcc.txnHandler.GetTxn()
	if err != nil {
		return nil, err
	}
	db, err := e.Database(txnCtx, defines.TEMPORARY_DBNAME, txn)
	if err != nil {
		logErrorf(tcc.GetSession().GetDebugString(), "get temp database error %v", err)
		return nil, err
	}
	table, err := db.Relation(txnCtx, tableName)
	return table, err
}

func (tcc *TxnCompilerContext) ensureDatabaseIsNotEmpty(dbName string, checkSub bool) (string, *plan.SubscriptionMeta, error) {
	if len(dbName) == 0 {
		dbName = tcc.DefaultDatabase()
	}
	if len(dbName) == 0 {
		return "", nil, moerr.NewNoDB(tcc.GetContext())
	}
	var sub *plan.SubscriptionMeta
	var err error
	if checkSub && !util.DbIsSystemDb(dbName) {
		sub, err = tcc.GetSubscriptionMeta(dbName)
		if err != nil {
			return "", nil, err
		}
	}
	return dbName, sub, nil
}

func (tcc *TxnCompilerContext) ResolveById(tableId uint64) (*plan2.ObjectRef, *plan2.TableDef) {
	txnCtx, txn, err := tcc.GetTxnHandler().GetTxn()
	if err != nil {
		return nil, nil
	}
	dbName, tableName, table, err := tcc.GetTxnHandler().GetStorage().GetRelationById(txnCtx, txn, tableId)
	if err != nil {
		return nil, nil
	}
	return tcc.getTableDef(txnCtx, table, dbName, tableName, nil)
}

func (tcc *TxnCompilerContext) Resolve(dbName string, tableName string) (*plan2.ObjectRef, *plan2.TableDef) {
	dbName, sub, err := tcc.ensureDatabaseIsNotEmpty(dbName, true)
	if err != nil {
		return nil, nil
	}
	ctx, table, err := tcc.getRelation(dbName, tableName, sub)
	if err != nil {
		return nil, nil
	}
	return tcc.getTableDef(ctx, table, dbName, tableName, sub)
}

func (tcc *TxnCompilerContext) ResolveUdf(name string, args []*plan.Expr) (body string, err error) {
	var expectInvalidArgErr bool
	var expectedInvalidArgLengthErr bool
	var badValue string
	var argstr string
	var sql string
	var erArray []ExecResult

	ses := tcc.GetSession()
	ctx := ses.GetRequestContext()

	err = inputNameIsInvalid(ctx, name)
	if err != nil {
		return "", err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
		if expectedInvalidArgLengthErr {
			err = errors.Join(err, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args)))
		} else if expectInvalidArgErr {
			err = errors.Join(err, moerr.NewInvalidArg(ctx, name+" function have invalid input args", badValue))
		}
	}()
	if err != nil {
		return "", err
	}

	sql = fmt.Sprintf(`select args, body from mo_catalog.mo_user_defined_function where name = "%s" and db = "%s";`, name, tcc.DefaultDatabase())
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return "", err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return "", err
	}

	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			// reset flag
			expectedInvalidArgLengthErr = false
			expectInvalidArgErr = false
			argstr, err = erArray[0].GetString(ctx, i, 0)
			if err != nil {
				return "", err
			}
			body, err = erArray[0].GetString(ctx, i, 1)
			if err != nil {
				return "", err
			}
			// arg type check
			argMap := make(map[string]string)
			json.Unmarshal([]byte(argstr), &argMap)
			if len(argMap) != len(args) {
				expectedInvalidArgLengthErr = true
				continue
			}
			i := 0
			for _, v := range argMap {
				switch t := int32(types.Types[v]); {
				case (t >= 20 && t <= 29): // int family
					if args[i].Typ.Id < 20 || args[i].Typ.Id > 29 {
						expectInvalidArgErr = true
						badValue = v
					}
				case t == 10: // bool family
					if args[i].Typ.Id != 10 {
						expectInvalidArgErr = true
						badValue = v
					}
				case (t >= 30 && t <= 33): // float family
					if args[i].Typ.Id < 30 || args[i].Typ.Id > 33 {
						expectInvalidArgErr = true
						badValue = v
					}
				}
				i++
			}
			if (!expectInvalidArgErr) && (!expectedInvalidArgLengthErr) {
				return body, err
			}
		}
		return "", err
	} else {
		return "", moerr.NewNotSupported(ctx, "function or operator '%s'", name)
	}
}

func (tcc *TxnCompilerContext) getTableDef(ctx context.Context, table engine.Relation, dbName, tableName string, sub *plan.SubscriptionMeta) (*plan2.ObjectRef, *plan2.TableDef) {
	tableId := table.GetTableID(ctx)
	engineDefs, err := table.TableDefs(ctx)
	if err != nil {
		return nil, nil
	}

	var clusterByDef *plan2.ClusterByDef
	var cols []*plan2.ColDef
	var schemaVersion uint32
	var defs []*plan2.TableDefType
	var properties []*plan2.Property
	var TableType, Createsql string
	var partitionInfo *plan2.PartitionByDef
	var viewSql *plan2.ViewDef
	var foreignKeys []*plan2.ForeignKeyDef
	var primarykey *plan2.PrimaryKeyDef
	var indexes []*plan2.IndexDef
	var refChildTbls []uint64

	for _, def := range engineDefs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			col := &plan2.ColDef{
				ColId: attr.Attr.ID,
				Name:  attr.Attr.Name,
				Typ: &plan2.Type{
					Id:          int32(attr.Attr.Type.Oid),
					Width:       attr.Attr.Type.Width,
					Scale:       attr.Attr.Type.Scale,
					AutoIncr:    attr.Attr.AutoIncrement,
					Table:       tableName,
					NotNullable: attr.Attr.Default != nil && !attr.Attr.Default.NullAbility,
				},
				Primary:   attr.Attr.Primary,
				Default:   attr.Attr.Default,
				OnUpdate:  attr.Attr.OnUpdate,
				Comment:   attr.Attr.Comment,
				ClusterBy: attr.Attr.ClusterBy,
				Hidden:    attr.Attr.IsHidden,
				Seqnum:    uint32(attr.Attr.Seqnum),
			}
			// Is it a composite primary key
			if attr.Attr.Name == catalog.CPrimaryKeyColName {
				continue
			}
			if attr.Attr.ClusterBy {
				clusterByDef = &plan.ClusterByDef{
					Name: attr.Attr.Name,
				}
				if util.JudgeIsCompositeClusterByColumn(attr.Attr.Name) {
					continue
				}
			}
			cols = append(cols, col)
		} else if pro, ok := def.(*engine.PropertiesDef); ok {
			for _, p := range pro.Properties {
				switch p.Key {
				case catalog.SystemRelAttr_Kind:
					TableType = p.Value
				case catalog.SystemRelAttr_CreateSQL:
					Createsql = p.Value
				default:
				}
				properties = append(properties, &plan2.Property{
					Key:   p.Key,
					Value: p.Value,
				})
			}
		} else if viewDef, ok := def.(*engine.ViewDef); ok {
			viewSql = &plan2.ViewDef{
				View: viewDef.View,
			}
		} else if c, ok := def.(*engine.ConstraintDef); ok {
			for _, ct := range c.Cts {
				switch k := ct.(type) {
				case *engine.IndexDef:
					indexes = k.Indexes
				case *engine.ForeignKeyDef:
					foreignKeys = k.Fkeys
				case *engine.RefChildTableDef:
					refChildTbls = k.Tables
				case *engine.PrimaryKeyDef:
					primarykey = k.Pkey
				}
			}
		} else if commnetDef, ok := def.(*engine.CommentDef); ok {
			properties = append(properties, &plan2.Property{
				Key:   catalog.SystemRelAttr_Comment,
				Value: commnetDef.Comment,
			})
		} else if partitionDef, ok := def.(*engine.PartitionDef); ok {
			if partitionDef.Partitioned > 0 {
				p := &plan2.PartitionByDef{}
				err = p.UnMarshalPartitionInfo(([]byte)(partitionDef.Partition))
				if err != nil {
					return nil, nil
				}
				partitionInfo = p
			}
		} else if v, ok := def.(*engine.VersionDef); ok {
			schemaVersion = v.Version
		}
	}
	if len(properties) > 0 {
		defs = append(defs, &plan2.TableDefType{
			Def: &plan2.TableDef_DefType_Properties{
				Properties: &plan2.PropertiesDef{
					Properties: properties,
				},
			},
		})
	}

	if primarykey != nil && primarykey.PkeyColName == catalog.CPrimaryKeyColName {
		cols = append(cols, plan2.MakeHiddenColDefByName(catalog.CPrimaryKeyColName))
	}
	if clusterByDef != nil && util.JudgeIsCompositeClusterByColumn(clusterByDef.Name) {
		cols = append(cols, plan2.MakeHiddenColDefByName(clusterByDef.Name))
	}
	rowIdCol := plan2.MakeRowIdColDef()
	cols = append(cols, rowIdCol)

	//convert
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
		SubscriptionName: subscriptionName,
	}
	if pubAccountId != -1 {
		obj.PubInfo = &plan.PubInfo{
			TenantId: pubAccountId,
		}
	}

	tableDef := &plan2.TableDef{
		TblId:     tableId,
		Name:      tableName,
		Cols:      cols,
		Defs:      defs,
		TableType: TableType,
		Createsql: Createsql,
		Pkey:      primarykey,
		//CompositePkey: CompositePkey,
		ViewSql:      viewSql,
		Partition:    partitionInfo,
		Fkeys:        foreignKeys,
		RefChildTbls: refChildTbls,
		ClusterBy:    clusterByDef,
		Indexes:      indexes,
		Version:      schemaVersion,
		IsTemporary:  table.GetEngineType() == engine.Memory,
	}
	return obj, tableDef
}

func (tcc *TxnCompilerContext) ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	ses := tcc.GetSession()
	ctx := ses.GetRequestContext()

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
			return tcc.GetSession().getGlobalSystemVariableValue(varName)
		} else {
			return tcc.GetSession().GetSessionVar(varName)
		}
	} else {
		_, val, err := tcc.GetSession().GetUserDefinedVar(varName)
		return val, err
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
	ctx := ses.GetRequestContext()
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

func (tcc *TxnCompilerContext) GetPrimaryKeyDef(dbName string, tableName string) []*plan2.ColDef {
	dbName, sub, err := tcc.ensureDatabaseIsNotEmpty(dbName, true)
	if err != nil {
		return nil
	}
	ctx, relation, err := tcc.getRelation(dbName, tableName, sub)
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
			Typ: &plan2.Type{
				Id:    int32(key.Type.Oid),
				Width: key.Type.Width,
				Scale: key.Type.Scale,
			},
			Primary: key.Primary,
		})
	}
	return priDefs
}

func (tcc *TxnCompilerContext) Stats(obj *plan2.ObjectRef) bool {

	dbName := obj.GetSchemaName()
	checkSub := true
	if obj.PubInfo != nil {
		checkSub = false
	}
	dbName, sub, err := tcc.ensureDatabaseIsNotEmpty(dbName, checkSub)
	if err != nil {
		return false
	}
	if !checkSub {
		sub = &plan.SubscriptionMeta{
			AccountId: obj.PubInfo.TenantId,
			DbName:    dbName,
		}
	}
	tableName := obj.GetObjName()
	ctx, table, _ := tcc.getRelation(dbName, tableName, sub)

	var partitionInfo *plan2.PartitionByDef
	engineDefs, err := table.TableDefs(ctx)
	if err != nil {
		return false
	}
	for _, def := range engineDefs {
		if partitionDef, ok := def.(*engine.PartitionDef); ok {
			if partitionDef.Partitioned > 0 {
				p := &plan2.PartitionByDef{}
				err = p.UnMarshalPartitionInfo(([]byte)(partitionDef.Partition))
				if err != nil {
					return false
				}
				partitionInfo = p
			}
		}
	}
	var ptables []any
	if partitionInfo != nil {
		ptables = make([]any, len(partitionInfo.PartitionTableNames))
		for i, PartitionTableName := range partitionInfo.PartitionTableNames {
			_, ptable, _ := tcc.getRelation(dbName, PartitionTableName, nil)
			ptables[i] = ptable
		}
	}
	return table.Stats(ctx, ptables, tcc.GetSession().statsCache.GetStatsInfoMap(table.GetTableID(ctx)))
}

func (tcc *TxnCompilerContext) GetProcess() *process.Process {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.proc
}

func (tcc *TxnCompilerContext) GetQueryResultMeta(uuid string) ([]*plan.ColDef, string, error) {
	proc := tcc.proc
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
	bats, err := reader.LoadAllColumns(proc.Ctx, idxs, common.DefaultAllocator)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil, "", moerr.NewResultFileNotFound(proc.Ctx, makeResultMetaPath(proc.SessionInfo.Account, uuid))
		}
		return nil, "", err
	}
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

func (tcc *TxnCompilerContext) SetProcess(proc *process.Process) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.proc = proc
}

func (tcc *TxnCompilerContext) GetSubscriptionMeta(dbName string) (*plan.SubscriptionMeta, error) {
	txnCtx, txn, err := tcc.GetTxnHandler().GetTxn()
	if err != nil {
		return nil, err
	}
	sub, err := getSubscriptionMeta(txnCtx, dbName, tcc.GetSession(), txn)
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
