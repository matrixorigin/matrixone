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

package plan2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type MockCompilerContext struct {
	objects map[string]*plan.ObjectRef
	tables  map[string]*plan.TableDef
}

type col struct {
	Name      string
	Id        uint32
	Nullable  bool
	Width     int32
	Precision int32
}

func NewMockCompilerContext() *MockCompilerContext {
	objects := make(map[string]*plan.ObjectRef)
	tables := make(map[string]*plan.TableDef)

	tpchSchema := make(map[string][]col)
	tpchSchema["NATION"] = []col{
		{"N_NATIONKEY", 1, false, 0, 0},
		{"N_NAME", 2, false, 0, 0},
		{"N_REGIONKEY", 3, false, 0, 0},
		{"N_COMMENT", 4, false, 0, 0},
	}
	tpchSchema["REGION"] = []col{
		{"R_REGIONKEY", 1, false, 0, 0},
		{"R_NAME", 2, false, 0, 0},
		{"R_COMMENT", 3, false, 0, 0},
	}

	defaultDbName := "tpch"

	//build tpch context data(schema)
	for tableName, cols := range tpchSchema {
		var colDefs []*plan.ColDef

		for idx, col := range cols {
			objName := tableName + "." + col.Name

			objects[objName] = &plan.ObjectRef{
				Server:     0,
				Db:         0,
				Schema:     0,
				Obj:        int64(idx),
				ServerName: "",
				DbName:     defaultDbName,
				SchemaName: "",
				ObjName:    col.Name,
			}

			colDefs = append(colDefs, &plan.ColDef{
				Typ: &plan.Type{
					Id:        col.Id,
					Nullable:  col.Nullable,
					Width:     col.Width,
					Precision: col.Precision,
				},
				Name:  col.Name,
				Pkidx: 1,
			})
		}

		tables[tableName] = &plan.TableDef{
			Name: tableName,
			Cols: colDefs,
		}
	}

	return &MockCompilerContext{
		objects: objects,
		tables:  tables,
	}
}

func (m *MockCompilerContext) Resolve(name string) (*plan.ObjectRef, *plan.TableDef) {
	return m.objects[name], m.tables[name]
}

func (m *MockCompilerContext) Cost(obj *ObjectRef, e *Expr) Cost {
	var c Cost
	div := 1.0
	if e != nil {
		div = 10.0
	}

	c.Card = 1000000 / div
	c.Rowsize = 100
	c.Ndv = 900000 / div
	c.Start = 0
	c.Total = 1000
	return c
}

type MockOptimizer struct {
	ctxt MockCompilerContext
}

func newMockOptimizer() *MockOptimizer {
	return &MockOptimizer{
		ctxt: *NewMockCompilerContext(),
	}
}

func (moc *MockOptimizer) Optimize(stmt tree.Statement) (*Query, error) {
	ctx := moc.CurrentContext()
	query, err := buildPlan(ctx, stmt)
	if err != nil {
		fmt.Printf("Optimize statement error: '%v'", tree.String(stmt, dialect.MYSQL))
		return nil, err
	}
	return query, nil
}

func (moc *MockOptimizer) CurrentContext() CompilerContext {
	return &moc.ctxt
}
