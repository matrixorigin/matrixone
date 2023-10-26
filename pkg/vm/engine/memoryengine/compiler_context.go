// Copyright 2022 Matrix Origin
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

package memoryengine

import (
	"context"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type CompilerContext struct {
	ctx       context.Context
	defaultDB string
	engine    *Engine
	txnOp     client.TxnOperator
}

func (c *CompilerContext) CheckSubscriptionValid(subName, accName string, pubName string) error {
	//TODO implement me
	panic("implement me")
}

func (c *CompilerContext) IsPublishing(dbName string) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (c *CompilerContext) SetQueryingSubscription(meta *plan.SubscriptionMeta) {
	//TODO implement me
	panic("implement me")
}

func (c *CompilerContext) GetQueryingSubscription() *plan.SubscriptionMeta {
	return nil
}

func (e *Engine) NewCompilerContext(
	ctx context.Context,
	defaultDB string,
	txnOp client.TxnOperator,
) *CompilerContext {
	return &CompilerContext{
		ctx:       ctx,
		defaultDB: defaultDB,
		engine:    e,
		txnOp:     txnOp,
	}
}

var _ plan.CompilerContext = new(CompilerContext)

func (c *CompilerContext) ResolveUdf(name string, ast []*plan.Expr) (*function.Udf, error) {
	return nil, nil
}

func (c *CompilerContext) ResolveAccountIds(accountNames []string) ([]uint32, error) {
	return []uint32{catalog.System_Account}, nil
}

func (*CompilerContext) Stats(obj *plan.ObjectRef) bool {
	return false
}

func (*CompilerContext) GetStatsCache() *plan.StatsCache {
	return nil
}

func (c *CompilerContext) GetSubscriptionMeta(dbName string) (*plan.SubscriptionMeta, error) {
	return nil, nil
}

func (c *CompilerContext) GetProcess() *process.Process {
	proc := testutil.NewProcess()
	proc.Ctx = context.Background()
	return proc
}

func (c *CompilerContext) GetQueryResultMeta(uuid string) ([]*plan.ColDef, string, error) {
	return nil, "", nil
}

func (c *CompilerContext) DatabaseExists(name string) bool {
	_, err := c.engine.Database(
		c.ctx,
		name,
		c.txnOp,
	)
	return err == nil
}

func (c *CompilerContext) GetDatabaseId(dbName string) (uint64, error) {
	database, err := c.engine.Database(c.ctx, dbName, c.txnOp)
	if err != nil {
		return 0, err
	}
	databaseId, err := strconv.ParseUint(database.GetDatabaseId(c.ctx), 10, 64)
	if err != nil {
		return 0, moerr.NewInternalError(c.ctx, "The databaseid of '%s' is not a valid number", dbName)
	}
	return databaseId, nil
}

func (c *CompilerContext) DefaultDatabase() string {
	return c.defaultDB
}

func (c *CompilerContext) GetPrimaryKeyDef(dbName string, tableName string) (defs []*plan.ColDef) {
	attrs, err := c.getTableAttrs(dbName, tableName)
	if err != nil {
		panic(err)
	}
	for i, attr := range attrs {
		if !attr.Primary {
			continue
		}
		defs = append(defs, engineAttrToPlanColDef(i, attr))
	}
	return
}

func (*CompilerContext) GetRootSql() string {
	return ""
}

func (*CompilerContext) GetUserName() string {
	return "root"
}

func (c *CompilerContext) GetAccountId() uint32 {
	if v := c.ctx.Value(defines.TenantIDKey{}); v != nil {
		return v.(uint32)
	}
	return 0
}

func (c *CompilerContext) GetContext() context.Context {
	return c.ctx
}

func (c *CompilerContext) ResolveById(tableId uint64) (objRef *plan.ObjectRef, tableDef *plan.TableDef) {
	dbName, tableName, _ := c.engine.GetNameById(c.ctx, c.txnOp, tableId)
	if dbName == "" || tableName == "" {
		return nil, nil
	}
	return c.Resolve(dbName, tableName)
}

func (c *CompilerContext) Resolve(schemaName string, tableName string) (objRef *plan.ObjectRef, tableDef *plan.TableDef) {
	if schemaName == "" {
		schemaName = c.defaultDB
	}

	objRef = &plan.ObjectRef{
		SchemaName: schemaName,
		ObjName:    tableName,
	}

	tableDef = &plan.TableDef{
		Name: tableName,
	}

	attrs, err := c.getTableAttrs(schemaName, tableName)
	if err != nil {
		return nil, nil
	}

	for i, attr := range attrs {
		// return hidden columns for update or detete statement
		//if attr.IsHidden {
		//	switch e.stmt.(type) {
		//	case *tree.Update, *tree.Delete:
		//	default:
		//		continue
		//	}
		//}
		if attr.Primary {
			tableDef.Pkey = &plan.PrimaryKeyDef{
				Cols:        []uint64{uint64(i)},
				PkeyColId:   uint64(i),
				PkeyColName: attr.Name,
				Names:       []string{attr.Name},
			}
		}
		tableDef.Cols = append(tableDef.Cols, engineAttrToPlanColDef(i, attr))
	}

	//TODO properties
	//TODO view

	return
}

func (*CompilerContext) ResolveVariable(varName string, isSystemVar bool, isGlobalVar bool) (interface{}, error) {
	return "", nil
}

func (c *CompilerContext) getTableAttrs(dbName string, tableName string) (attrs []*engine.Attribute, err error) {
	db, err := c.engine.Database(
		c.ctx,
		dbName,
		c.txnOp,
	)
	if err != nil {
		return nil, err
	}
	table, err := db.Relation(
		c.ctx,
		tableName,
		nil,
	)
	if err != nil {
		return nil, err
	}
	defs, err := table.TableDefs(c.ctx)
	if err != nil {
		return nil, err
	}
	for _, def := range defs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		attrs = append(attrs, &attr.Attr)
	}
	return
}

func (c *CompilerContext) SetBuildingAlterView(yesOrNo bool, dbName, viewName string) {}
func (c *CompilerContext) GetBuildingAlterView() (bool, string, string) {
	return false, "", ""
}

func engineAttrToPlanColDef(idx int, attr *engine.Attribute) *plan.ColDef {
	return &plan.ColDef{
		ColId: uint64(attr.ID),
		Name:  attr.Name,
		Typ: &plan.Type{
			Id:          int32(attr.Type.Oid),
			NotNullable: attr.Default != nil && !(attr.Default.NullAbility),
			Width:       attr.Type.Width,
			Scale:       attr.Type.Scale,
			Enumvalues:  attr.EnumVlaues,
		},
		Default:   attr.Default,
		Primary:   attr.Primary,
		Pkidx:     int32(idx),
		Comment:   attr.Comment,
		ClusterBy: attr.ClusterBy,
		Seqnum:    uint32(attr.Seqnum),
	}
}
