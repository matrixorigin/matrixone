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
	"strconv"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
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
	mu                   sync.Mutex
}

func (c *compilerContext) ReplacePlan(execPlan *planpb.Execute) (*planpb.Plan, tree.Statement, error) {
	//TODO implement me
	panic("implement me")
}

func (c *compilerContext) CheckSubscriptionValid(subName, accName string, pubName string) error {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) IsPublishing(dbName string) (bool, error) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) SetQueryingSubscription(meta *plan.SubscriptionMeta) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) GetQueryingSubscription() *plan.SubscriptionMeta {
	return nil
}

func newCompilerContext(
	ctx context.Context,
	defaultDB string,
	eng engine.Engine,
	proc *process.Process) *compilerContext {
	return &compilerContext{
		ctx:       ctx,
		defaultDB: defaultDB,
		engine:    eng,
		proc:      proc,
	}
}

func (c *compilerContext) ResolveUdf(name string, ast []*plan.Expr) (*function.Udf, error) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) ResolveAccountIds(accountNames []string) ([]uint32, error) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) Stats(obj *plan.ObjectRef) (*pb.StatsInfo, error) {
	t, err := c.getRelation(obj.GetSchemaName(), obj.GetObjName())
	if err != nil {
		return nil, err
	}
	return t.Stats(c.ctx, true), nil
}

func (c *compilerContext) GetStatsCache() *plan.StatsCache {
	if c.statsCache == nil {
		c.statsCache = plan.NewStatsCache()
	}
	return c.statsCache
}

func (c *compilerContext) GetSubscriptionMeta(dbName string) (*plan.SubscriptionMeta, error) {
	return nil, nil
}

func (c *compilerContext) GetProcess() *process.Process {
	return c.proc
}

func (c *compilerContext) GetQueryResultMeta(uuid string) ([]*plan.ColDef, string, error) {
	panic("not supported in internal sql executor")
}

func (c *compilerContext) DatabaseExists(name string) bool {
	_, err := c.engine.Database(
		c.ctx,
		name,
		c.proc.TxnOperator,
	)
	return err == nil
}

func (c *compilerContext) GetDatabaseId(dbName string) (uint64, error) {
	database, err := c.engine.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		return 0, err
	}
	databaseId, err := strconv.ParseUint(database.GetDatabaseId(c.ctx), 10, 64)
	if err != nil {
		return 0, moerr.NewInternalError(c.ctx, "The databaseid of '%s' is not a valid number", dbName)
	}
	return databaseId, nil
}

func (c *compilerContext) DefaultDatabase() string {
	return c.defaultDB
}

func (c *compilerContext) GetPrimaryKeyDef(
	dbName string,
	tableName string) []*plan.ColDef {
	dbName, err := c.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil
	}
	relation, err := c.getRelation(dbName, tableName)
	if err != nil {
		return nil
	}

	priKeys, err := relation.GetPrimaryKeys(c.ctx)
	if err != nil {
		return nil
	}
	if len(priKeys) == 0 {
		return nil
	}

	priDefs := make([]*plan.ColDef, 0, len(priKeys))
	for _, key := range priKeys {
		priDefs = append(priDefs, &plan.ColDef{
			Name: key.Name,
			Typ: plan.Type{
				Id:    int32(key.Type.Oid),
				Width: key.Type.Width,
				Scale: key.Type.Scale,
			},
			Primary: key.Primary,
		})
	}
	return priDefs
}

func (c *compilerContext) GetRootSql() string {
	return ""
}

func (c *compilerContext) GetUserName() string {
	return "root"
}

func (c *compilerContext) GetAccountId() (uint32, error) {
	return defines.GetAccountId(c.ctx)
}

func (c *compilerContext) GetContext() context.Context {
	return c.ctx
}

func (c *compilerContext) ResolveById(tableId uint64) (objRef *plan.ObjectRef, tableDef *plan.TableDef) {
	dbName, tableName, _ := c.engine.GetNameById(c.ctx, c.proc.TxnOperator, tableId)
	if dbName == "" || tableName == "" {
		return nil, nil
	}
	return c.Resolve(dbName, tableName)
}

func (c *compilerContext) Resolve(dbName string, tableName string) (*plan.ObjectRef, *plan.TableDef) {
	dbName, err := c.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil, nil
	}
	table, err := c.getRelation(dbName, tableName)
	if err != nil {
		return nil, nil
	}
	return c.getTableDef(table, dbName, tableName)
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
	tableName string) (engine.Relation, error) {
	dbName, err := c.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil, err
	}

	db, err := c.engine.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		return nil, err
	}

	table, err := db.Relation(c.ctx, tableName, nil)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (c *compilerContext) getTableDef(
	table engine.Relation,
	dbName, tableName string) (*plan.ObjectRef, *plan.TableDef) {
	tableId := table.GetTableID(c.ctx)
	engineDefs, err := table.TableDefs(c.ctx)
	if err != nil {
		return nil, nil
	}

	var clusterByDef *plan.ClusterByDef
	var cols []*plan.ColDef
	var schemaVersion uint32
	var defs []*plan.TableDefType
	var properties []*plan.Property
	var TableType, Createsql string
	var partitionInfo *plan.PartitionByDef
	var viewSql *plan.ViewDef
	var foreignKeys []*plan.ForeignKeyDef
	var primarykey *plan.PrimaryKeyDef
	var indexes []*plan.IndexDef
	var refChildTbls []uint64
	var subscriptionName string

	for _, def := range engineDefs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			col := &plan.ColDef{
				ColId: attr.Attr.ID,
				Name:  attr.Attr.Name,
				Typ: plan.Type{
					Id:          int32(attr.Attr.Type.Oid),
					Width:       attr.Attr.Type.Width,
					Scale:       attr.Attr.Type.Scale,
					AutoIncr:    attr.Attr.AutoIncrement,
					Table:       tableName,
					NotNullable: attr.Attr.Default != nil && !attr.Attr.Default.NullAbility,
					Enumvalues:  attr.Attr.EnumVlaues,
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
			//if attr.Attr.Name == catalog.CPrimaryKeyColName {
			//	continue
			//}
			if attr.Attr.ClusterBy {
				clusterByDef = &plan.ClusterByDef{
					Name: attr.Attr.Name,
				}
				//if util.JudgeIsCompositeClusterByColumn(attr.Attr.Name) {
				//	continue
				//}
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
				properties = append(properties, &plan.Property{
					Key:   p.Key,
					Value: p.Value,
				})
			}
		} else if viewDef, ok := def.(*engine.ViewDef); ok {
			viewSql = &plan.ViewDef{
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
			properties = append(properties, &plan.Property{
				Key:   catalog.SystemRelAttr_Comment,
				Value: commnetDef.Comment,
			})
		} else if partitionDef, ok := def.(*engine.PartitionDef); ok {
			if partitionDef.Partitioned > 0 {
				p := &plan.PartitionByDef{}
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
		defs = append(defs, &plan.TableDefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			},
		})
	}

	if primarykey != nil && primarykey.PkeyColName == catalog.CPrimaryKeyColName {
		//cols = append(cols, plan.MakeHiddenColDefByName(catalog.CPrimaryKeyColName))
		primarykey.CompPkeyCol = plan.GetColDefFromTable(cols, catalog.CPrimaryKeyColName)
	}
	if clusterByDef != nil && util.JudgeIsCompositeClusterByColumn(clusterByDef.Name) {
		//cols = append(cols, plan.MakeHiddenColDefByName(clusterByDef.Name))
		clusterByDef.CompCbkeyCol = plan.GetColDefFromTable(cols, clusterByDef.Name)
	}
	rowIdCol := plan.MakeRowIdColDef()
	cols = append(cols, rowIdCol)

	//convert
	obj := &plan.ObjectRef{
		SchemaName:       dbName,
		ObjName:          tableName,
		SubscriptionName: subscriptionName,
	}

	tableDef := &plan.TableDef{
		TblId:        tableId,
		Name:         tableName,
		Cols:         cols,
		Defs:         defs,
		TableType:    TableType,
		Createsql:    Createsql,
		Pkey:         primarykey,
		ViewSql:      viewSql,
		Partition:    partitionInfo,
		Fkeys:        foreignKeys,
		RefChildTbls: refChildTbls,
		ClusterBy:    clusterByDef,
		Indexes:      indexes,
		Version:      schemaVersion,
	}
	return obj, tableDef
}
