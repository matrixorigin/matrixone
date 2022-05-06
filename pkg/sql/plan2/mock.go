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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	tpchSchema["NATION2"] = []col{ //not exist in tpch, create for test NaturalJoin And UsingJoin
		{"N_NATIONKEY", 1, false, 0, 0},
		{"N_NAME", 2, false, 0, 0},
		{"R_REGIONKEY", 3, false, 0, 0}, //change N_REGIONKEY to R_REGIONKEY for test NaturalJoin And UsingJoin
		{"N_COMMENT", 4, false, 0, 0},
	}
	tpchSchema["REGION"] = []col{
		{"R_REGIONKEY", 1, false, 0, 0},
		{"R_NAME", 2, false, 0, 0},
		{"R_COMMENT", 3, false, 0, 0},
	}
	tpchSchema["PART"] = []col{
		{"P_PARTKEY", 1, false, 0, 0},
		{"P_NAME", 2, false, 0, 0},
		{"P_BRAND", 3, false, 0, 0},
		{"P_TYPE", 4, false, 0, 0},
		{"P_SIZE", 5, false, 0, 0},
		{"P_CONTAINER", 6, false, 0, 0},
		{"P_RETAILPRICE", 7, false, 0, 0},
		{"P_COMMENT", 8, false, 0, 0},
	}
	tpchSchema["SUPPLIER"] = []col{
		{"S_SUPPKEY", 1, false, 0, 0},
		{"S_NAME", 2, false, 0, 0},
		{"S_ADDRESS", 3, false, 0, 0},
		{"S_NATIONKEY", 4, false, 0, 0},
		{"S_PHONE", 5, false, 0, 0},
		{"S_ACCTBAL", 6, false, 0, 0},
		{"S_COMMENT", 7, false, 0, 0},
	}
	tpchSchema["PARTSUPP"] = []col{
		{"PS_PARTKEY", 1, false, 0, 0},
		{"PS_SUPPKEY", 2, false, 0, 0},
		{"PS_AVAILQTY", 3, false, 0, 0},
		{"PS_SUPPLYCOST", 4, false, 0, 0},
		{"PS_COMMENT", 5, false, 0, 0},
	}
	tpchSchema["CUSTOMER"] = []col{
		{"C_CUSTKEY", 1, false, 0, 0},
		{"C_NAME", 2, false, 0, 0},
		{"C_ADDRESS", 3, false, 0, 0},
		{"C_NATIONKEY", 4, false, 0, 0},
		{"C_PHONE", 5, false, 0, 0},
		{"C_ACCTBAL", 6, false, 0, 0},
		{"C_MKTSEGMENT", 7, false, 0, 0},
		{"C_COMMENT", 8, false, 0, 0},
	}
	tpchSchema["ORDERS"] = []col{
		{"O_ORDERKEY", 1, false, 0, 0},
		{"O_CUSTKEY", 2, false, 0, 0},
		{"O_TOTALPRICE", 3, false, 0, 0},
		{"O_ORDERDATE", 4, false, 0, 0},
		{"O_ORDERPRIORITY", 5, false, 0, 0},
		{"O_CLERK", 6, false, 0, 0},
		{"O_SHIPPRIORITY", 7, false, 0, 0},
		{"O_COMMENT", 8, false, 0, 0},
	}
	tpchSchema["LINEITEM"] = []col{
		{"L_ORDERKEY", 1, false, 0, 0},
		{"L_PARTKEY", 2, false, 0, 0},
		{"L_SUPPKEY", 3, false, 0, 0},
		{"L_LINENUMBER", 4, false, 0, 0},
		{"L_QUANTITY", 5, false, 0, 0},
		{"L_EXTENDEDPRICE", 6, false, 0, 0},
		{"L_DISCOUNT", 7, false, 0, 0},
		{"L_TAX", 8, false, 0, 0},
		{"L_RETURNFLAG", 9, false, 0, 0},
		{"L_LINESTATUS", 10, false, 0, 0},
		{"L_SHIPDATE", 11, false, 0, 0},
		{"L_COMMITDATE", 12, false, 0, 0},
		{"L_RECEIPTDATE", 13, false, 0, 0},
		{"L_SHIPINSTRUCT", 14, false, 0, 0},
		{"L_SHIPMODE", 15, false, 0, 0},
		{"L_COMMENT", 15, false, 0, 0},
	}

	defaultDbName := "tpch"

	//build tpch context data(schema)
	tableIdx := 0
	for tableName, cols := range tpchSchema {
		var colDefs []*plan.ColDef

		for _, col := range cols {
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

		objects[tableName] = &plan.ObjectRef{
			Server:     0,
			Db:         0,
			Schema:     0,
			Obj:        int64(tableIdx),
			ServerName: "",
			DbName:     defaultDbName,
			SchemaName: "",
			ObjName:    tableName,
		}

		tables[tableName] = &plan.TableDef{
			Name: tableName,
			Cols: colDefs,
		}
		tableIdx++
	}

	return &MockCompilerContext{
		objects: objects,
		tables:  tables,
	}
}

func (m *MockCompilerContext) Resolve(name string) (*plan.ObjectRef, *plan.TableDef) {
	return m.objects[name], m.tables[name]
}

func (m *MockCompilerContext) Cost(obj *ObjectRef, e *Expr) *Cost {
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
	return &c
}

type MockOptimizer struct {
	ctxt MockCompilerContext
}

func newMockOptimizer() *MockOptimizer {
	return &MockOptimizer{
		ctxt: *NewMockCompilerContext(),
	}
}

func NewMockOptimizer2() *MockOptimizer {
	return &MockOptimizer{
		ctxt: *NewMockCompilerContext(),
	}
}

func (moc *MockOptimizer) Optimize(stmt tree.Statement) (*Query, error) {
	ctx := moc.CurrentContext()
	query, err := buildPlan(ctx, stmt)
	if err != nil {
		// fmt.Printf("Optimize statement error: '%v'", tree.String(stmt, dialect.MYSQL))
		return nil, err
	}
	return query, nil
}

func (moc *MockOptimizer) CurrentContext() CompilerContext {
	return &moc.ctxt
}
