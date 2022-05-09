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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type MockCompilerContext struct {
	objects map[string]*plan.ObjectRef
	tables  map[string]*plan.TableDef
}

type col struct {
	Name      string
	Id        plan.Type_TypeId
	Nullable  bool
	Width     int32
	Precision int32
}

func NewMockCompilerContext() *MockCompilerContext {
	objects := make(map[string]*plan.ObjectRef)
	tables := make(map[string]*plan.TableDef)

	tpchSchema := make(map[string][]col)
	tpchSchema["NATION"] = []col{
		{"N_NATIONKEY", plan.Type_INT32, false, 0, 0},
		{"N_NAME", plan.Type_VARCHAR, false, 25, 0},
		{"N_REGIONKEY", plan.Type_INT32, false, 0, 0},
		{"N_COMMENT", plan.Type_VARCHAR, true, 152, 0},
	}
	tpchSchema["NATION2"] = []col{ //not exist in tpch, create for test NaturalJoin And UsingJoin
		{"N_NATIONKEY", plan.Type_INT32, false, 0, 0},
		{"N_NAME", plan.Type_VARCHAR, false, 25, 0},
		{"R_REGIONKEY", plan.Type_INT32, false, 0, 0}, //change N_REGIONKEY to R_REGIONKEY for test NaturalJoin And UsingJoin
		{"N_COMMENT", plan.Type_VARCHAR, true, 152, 0},
	}
	tpchSchema["REGION"] = []col{
		{"R_REGIONKEY", plan.Type_INT32, false, 0, 0},
		{"R_NAME", plan.Type_VARCHAR, false, 25, 0},
		{"R_COMMENT", plan.Type_VARCHAR, true, 152, 0},
	}
	tpchSchema["PART"] = []col{
		{"P_PARTKEY", plan.Type_INT32, false, 0, 0},
		{"P_NAME", plan.Type_VARCHAR, false, 55, 0},
		{"P_BRAND", plan.Type_VARCHAR, false, 10, 0},
		{"P_TYPE", plan.Type_VARCHAR, false, 25, 0},
		{"P_SIZE", plan.Type_INT32, false, 0, 0},
		{"P_CONTAINER", plan.Type_VARCHAR, false, 10, 0},
		{"P_RETAILPRICE", plan.Type_DECIMAL, false, 15, 2},
		{"P_COMMENT", plan.Type_VARCHAR, false, 23, 0},
	}
	tpchSchema["SUPPLIER"] = []col{
		{"S_SUPPKEY", plan.Type_INT32, false, 0, 0},
		{"S_NAME", plan.Type_VARCHAR, false, 25, 0},
		{"S_ADDRESS", plan.Type_VARCHAR, false, 40, 0},
		{"S_NATIONKEY", plan.Type_INT32, false, 0, 0},
		{"S_PHONE", plan.Type_VARCHAR, false, 15, 0},
		{"S_ACCTBAL", plan.Type_DECIMAL, false, 15, 2},
		{"S_COMMENT", plan.Type_VARCHAR, false, 101, 0},
	}
	tpchSchema["PARTSUPP"] = []col{
		{"PS_PARTKEY", plan.Type_INT32, false, 0, 0},
		{"PS_SUPPKEY", plan.Type_INT32, false, 0, 0},
		{"PS_AVAILQTY", plan.Type_INT32, false, 0, 0},
		{"PS_SUPPLYCOST", plan.Type_DECIMAL, false, 15, 2},
		{"PS_COMMENT", plan.Type_VARCHAR, false, 199, 0},
	}
	tpchSchema["CUSTOMER"] = []col{
		{"C_CUSTKEY", plan.Type_INT32, false, 0, 0},
		{"C_NAME", plan.Type_VARCHAR, false, 25, 0},
		{"C_ADDRESS", plan.Type_VARCHAR, false, 40, 0},
		{"C_NATIONKEY", plan.Type_INT32, false, 0, 0},
		{"C_PHONE", plan.Type_VARCHAR, false, 15, 0},
		{"C_ACCTBAL", plan.Type_DECIMAL, false, 15, 2},
		{"C_MKTSEGMENT", plan.Type_VARCHAR, false, 10, 0},
		{"C_COMMENT", plan.Type_VARCHAR, false, 117, 0},
	}
	tpchSchema["ORDERS"] = []col{
		{"O_ORDERKEY", plan.Type_INT64, false, 0, 0},
		{"O_CUSTKEY", plan.Type_INT32, false, 0, 0},
		{"O_ORDERSTATUS", plan.Type_VARCHAR, false, 1, 0},
		{"O_TOTALPRICE", plan.Type_DECIMAL, false, 15, 2},
		{"O_ORDERDATE", plan.Type_DATE, false, 0, 0},
		{"O_ORDERPRIORITY", plan.Type_VARCHAR, false, 15, 0},
		{"O_CLERK", plan.Type_VARCHAR, false, 15, 0},
		{"O_SHIPPRIORITY", plan.Type_INT32, false, 0, 0},
		{"O_COMMENT", plan.Type_VARCHAR, false, 79, 0},
	}
	tpchSchema["LINEITEM"] = []col{
		{"L_ORDERKEY", plan.Type_INT64, false, 0, 0},
		{"L_PARTKEY", plan.Type_INT32, false, 0, 0},
		{"L_SUPPKEY", plan.Type_INT32, false, 0, 0},
		{"L_LINENUMBER", plan.Type_INT32, false, 0, 0},
		{"L_QUANTITY", plan.Type_INT32, false, 0, 0},
		{"L_EXTENDEDPRICE", plan.Type_DECIMAL, false, 15, 2},
		{"L_DISCOUNT", plan.Type_DECIMAL, false, 15, 2},
		{"L_TAX", plan.Type_DECIMAL, false, 15, 2},
		{"L_RETURNFLAG", plan.Type_VARCHAR, false, 1, 0},
		{"L_LINESTATUS", plan.Type_VARCHAR, false, 1, 0},
		{"L_SHIPDATE", plan.Type_DATE, false, 0, 0},
		{"L_COMMITDATE", plan.Type_DATE, false, 0, 0},
		{"L_RECEIPTDATE", plan.Type_DATE, false, 0, 0},
		{"L_SHIPINSTRUCT", plan.Type_VARCHAR, false, 25, 0},
		{"L_SHIPMODE", plan.Type_VARCHAR, false, 10, 0},
		{"L_COMMENT", plan.Type_VARCHAR, false, 44, 0},
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
	name = strings.ToUpper(name)
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
