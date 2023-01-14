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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type CompilerContext struct {
	ctx       context.Context
	defaultDB string
	engine    *Engine
	txnOp     client.TxnOperator
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

func (c *CompilerContext) GetEngine() engine.Engine {
	return nil
}

func (c *CompilerContext) ResolveAccountIds(accountNames []string) ([]uint32, error) {
	return []uint32{catalog.System_Account}, nil
}

func (*CompilerContext) Stats(obj *plan.ObjectRef, e *plan.Expr) *plan.Stats {
	return &plan.Stats{}
}

func (c *CompilerContext) GetProcess() *process.Process {
	return nil
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

func (c *CompilerContext) DefaultDatabase() string {
	return c.defaultDB
}

func (c *CompilerContext) GetHideKeyDef(dbName string, tableName string) *plan.ColDef {
	attrs, err := c.getTableAttrs(dbName, tableName)
	if err != nil {
		panic(err)
	}
	for i, attr := range attrs {
		if attr.IsHidden {
			return engineAttrToPlanColDef(i, attr)
		}
	}
	return nil
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

		tableDef.Cols = append(tableDef.Cols, engineAttrToPlanColDef(i, attr))
	}

	//TODO properties
	//TODO view

	return
}

func (*CompilerContext) ResolveVariable(varName string, isSystemVar bool, isGlobalVar bool) (interface{}, error) {
	panic("unimplemented")
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

func engineAttrToPlanColDef(idx int, attr *engine.Attribute) *plan.ColDef {
	return &plan.ColDef{
		ColId: uint64(attr.ID),
		Name:  attr.Name,
		Typ: &plan.Type{
			Id:          int32(attr.Type.Oid),
			NotNullable: attr.Default != nil && !(attr.Default.NullAbility),
			Width:       attr.Type.Width,
			Precision:   attr.Type.Precision,
			Size:        attr.Type.Size,
			Scale:       attr.Type.Scale,
		},
		Default:   attr.Default,
		Primary:   attr.Primary,
		Pkidx:     int32(idx),
		Comment:   attr.Comment,
		ClusterBy: attr.ClusterBy,
	}
}
