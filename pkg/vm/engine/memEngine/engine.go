// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memEngine

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/kv"
)

func New(db *kv.KV, n engine.Node) *MemEngine {
	return &MemEngine{
		n:  n,
		db: db,
	}
}

func (e *MemEngine) Delete(_ context.Context, _ string, _ client.TxnOperator) error {
	return nil
}

func (e *MemEngine) Create(_ context.Context, _ string, _ client.TxnOperator) error {
	return nil
}

func (e *MemEngine) Databases(_ context.Context, _ client.TxnOperator) ([]string, error) {
	return []string{"test"}, nil
}

func (e *MemEngine) Database(_ context.Context, name string, _ client.TxnOperator) (engine.Database, error) {
	if name != "test" {
		return nil, fmt.Errorf("database '%s' not exist", name)
	}
	return &database{db: e.db, n: e.n}, nil
}

func (e *MemEngine) Nodes() engine.Nodes {
	return nil
}

func (e *MemEngine) DefaultDatabase() string {
	return "test"
}

func (e *MemEngine) DatabaseExists(name string) bool {
	return name == "test"
}

func (e *MemEngine) Resolve(_ string, tableName string) (*plan.ObjectRef, *plan.TableDef) {
	schemaName := "test"
	ctx := context.TODO()
	db, err := e.Database(ctx, schemaName, nil)
	if err != nil {
		panic(err)
	}
	rel, err := db.Relation(ctx, tableName)
	if err != nil {
		panic(err)
	}
	defs, _ := rel.TableDefs(ctx)
	cols := make([]*plan.ColDef, 0, len(defs))
	for _, def := range defs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		cols = append(cols, &plan.ColDef{
			Name: attr.Attr.Name,
			Typ: &plan.Type{
				Id:    plan.Type_TypeId(attr.Attr.Type.Oid),
				Width: attr.Attr.Type.Width,
				Size:  attr.Attr.Type.Size,
			},
		})
	}
	return &plan.ObjectRef{SchemaName: schemaName, ObjName: schemaName}, &plan.TableDef{Name: tableName, Cols: cols}
}

func (e *MemEngine) ResolveVariable(_ string, _, _ bool) (interface{}, error) {
	return nil, nil
}

func (e *MemEngine) GetPrimaryKeyDef(_ string, _ string) []*plan.ColDef {
	return nil
}

func (e *MemEngine) GetHideKeyDef(_ string, _ string) *plan.ColDef {
	return nil
}

func (e *MemEngine) Cost(_ *plan.ObjectRef, _ *plan.Expr) *plan.Cost {
	return &plan.Cost{}
}
