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
	objects map[string]*ObjectRef
	tables  map[string]*TableDef
}

func (m *MockCompilerContext) ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	return nil, nil
}

type col struct {
	Name      string
	Id        plan.Type_TypeId
	Nullable  bool
	Width     int32
	Precision int32
}

//for test create/drop statement
func NewEmptyCompilerContext() *MockCompilerContext {
	return &MockCompilerContext{
		objects: make(map[string]*ObjectRef),
		tables:  make(map[string]*TableDef),
	}
}

func NewMockCompilerContext() *MockCompilerContext {
	tpchSchema := make(map[string][]col)
	moSchema := make(map[string][]col)

	schemas := map[string]map[string][]col{
		"tpch":       tpchSchema,
		"mo_catalog": moSchema,
	}

	tpchSchema["nation"] = []col{
		{"n_nationkey", plan.Type_INT32, false, 0, 0},
		{"n_name", plan.Type_VARCHAR, false, 25, 0},
		{"n_regionkey", plan.Type_INT32, false, 0, 0},
		{"n_comment", plan.Type_VARCHAR, true, 152, 0},
	}
	tpchSchema["nation2"] = []col{ //not exist in tpch, create for test NaturalJoin And UsingJoin
		{"n_nationkey", plan.Type_INT32, false, 0, 0},
		{"n_name", plan.Type_VARCHAR, false, 25, 0},
		{"r_regionkey", plan.Type_INT32, false, 0, 0}, //change N_REGIONKEY to R_REGIONKEY for test NaturalJoin And UsingJoin
		{"n_comment", plan.Type_VARCHAR, true, 152, 0},
	}
	tpchSchema["region"] = []col{
		{"r_regionkey", plan.Type_INT32, false, 0, 0},
		{"r_name", plan.Type_VARCHAR, false, 25, 0},
		{"r_comment", plan.Type_VARCHAR, true, 152, 0},
	}
	tpchSchema["part"] = []col{
		{"p_partkey", plan.Type_INT32, false, 0, 0},
		{"p_name", plan.Type_VARCHAR, false, 55, 0},
		{"p_mfgr", plan.Type_VARCHAR, false, 25, 0},
		{"p_brand", plan.Type_VARCHAR, false, 10, 0},
		{"p_type", plan.Type_VARCHAR, false, 25, 0},
		{"p_size", plan.Type_INT32, false, 0, 0},
		{"p_container", plan.Type_VARCHAR, false, 10, 0},
		{"p_retailprice", plan.Type_FLOAT64, false, 15, 2},
		{"p_comment", plan.Type_VARCHAR, false, 23, 0},
	}
	tpchSchema["supplier"] = []col{
		{"s_suppkey", plan.Type_INT32, false, 0, 0},
		{"s_name", plan.Type_VARCHAR, false, 25, 0},
		{"s_address", plan.Type_VARCHAR, false, 40, 0},
		{"s_nationkey", plan.Type_INT32, false, 0, 0},
		{"s_phone", plan.Type_VARCHAR, false, 15, 0},
		{"s_acctbal", plan.Type_FLOAT64, false, 15, 2},
		{"s_comment", plan.Type_VARCHAR, false, 101, 0},
	}
	tpchSchema["partsupp"] = []col{
		{"ps_partkey", plan.Type_INT32, false, 0, 0},
		{"ps_suppkey", plan.Type_INT32, false, 0, 0},
		{"ps_availqty", plan.Type_INT32, false, 0, 0},
		{"ps_supplycost", plan.Type_FLOAT64, false, 15, 2},
		{"ps_comment", plan.Type_VARCHAR, false, 199, 0},
	}
	tpchSchema["customer"] = []col{
		{"c_custkey", plan.Type_INT32, false, 0, 0},
		{"c_name", plan.Type_VARCHAR, false, 25, 0},
		{"c_address", plan.Type_VARCHAR, false, 40, 0},
		{"c_nationkey", plan.Type_INT32, false, 0, 0},
		{"c_phone", plan.Type_VARCHAR, false, 15, 0},
		{"c_acctbal", plan.Type_FLOAT64, false, 15, 2},
		{"c_mktsegment", plan.Type_VARCHAR, false, 10, 0},
		{"c_comment", plan.Type_VARCHAR, false, 117, 0},
	}
	tpchSchema["orders"] = []col{
		{"o_orderkey", plan.Type_INT64, false, 0, 0},
		{"o_custkey", plan.Type_INT32, false, 0, 0},
		{"o_orderstatus", plan.Type_VARCHAR, false, 1, 0},
		{"o_totalprice", plan.Type_FLOAT64, false, 15, 2},
		{"o_orderdate", plan.Type_DATE, false, 0, 0},
		{"o_orderpriority", plan.Type_VARCHAR, false, 15, 0},
		{"o_clerk", plan.Type_VARCHAR, false, 15, 0},
		{"o_shippriority", plan.Type_INT32, false, 0, 0},
		{"o_comment", plan.Type_VARCHAR, false, 79, 0},
	}
	tpchSchema["lineitem"] = []col{
		{"l_orderkey", plan.Type_INT64, false, 0, 0},
		{"l_partkey", plan.Type_INT32, false, 0, 0},
		{"l_suppkey", plan.Type_INT32, false, 0, 0},
		{"l_linenumber", plan.Type_INT32, false, 0, 0},
		{"l_quantity", plan.Type_INT32, false, 0, 0},
		{"l_extendedprice", plan.Type_FLOAT64, false, 15, 2},
		{"l_discount", plan.Type_FLOAT64, false, 15, 2},
		{"l_tax", plan.Type_FLOAT64, false, 15, 2},
		{"l_returnflag", plan.Type_VARCHAR, false, 1, 0},
		{"l_linestatus", plan.Type_VARCHAR, false, 1, 0},
		{"l_shipdate", plan.Type_DATE, false, 0, 0},
		{"l_commitdate", plan.Type_DATE, false, 0, 0},
		{"l_receiptdate", plan.Type_DATE, false, 0, 0},
		{"l_shipinstruct", plan.Type_VARCHAR, false, 25, 0},
		{"l_shipmode", plan.Type_VARCHAR, false, 10, 0},
		{"l_comment", plan.Type_VARCHAR, false, 44, 0},
	}

	moSchema["mo_database"] = []col{
		{"datname", plan.Type_VARCHAR, false, 50, 0},
	}
	moSchema["mo_tables"] = []col{
		{"reldatabase", plan.Type_VARCHAR, false, 50, 0},
		{"relname", plan.Type_VARCHAR, false, 50, 0},
	}
	moSchema["mo_columns"] = []col{
		{"att_database", plan.Type_VARCHAR, false, 50, 0},
		{"att_relname", plan.Type_VARCHAR, false, 50, 0},
		{"attname", plan.Type_VARCHAR, false, 50, 0},
		{"atttyp", plan.Type_INT32, false, 0, 0},
		{"attnum", plan.Type_INT32, false, 0, 0},
		{"att_length", plan.Type_INT32, false, 0, 0},
		{"attnotnull", plan.Type_INT8, false, 0, 0},
		{"att_constraint_type", plan.Type_CHAR, false, 1, 0},
		{"att_default", plan.Type_VARCHAR, false, 1024, 0},
		{"att_comment", plan.Type_VARCHAR, false, 1024, 0},
	}

	objects := make(map[string]*ObjectRef)
	tables := make(map[string]*TableDef)
	// build tpch/mo context data(schema)
	for db, schema := range schemas {
		tableIdx := 0
		for tableName, cols := range schema {
			colDefs := make([]*ColDef, 0, len(cols))

			for _, col := range cols {
				colDefs = append(colDefs, &ColDef{
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

			objects[tableName] = &ObjectRef{
				Server:     0,
				Db:         0,
				Schema:     0,
				Obj:        int64(tableIdx),
				ServerName: "",
				DbName:     "",
				SchemaName: db,
				ObjName:    tableName,
			}

			tables[tableName] = &TableDef{
				Name: tableName,
				Cols: colDefs,
			}
			tableIdx++
		}
	}

	return &MockCompilerContext{
		objects: objects,
		tables:  tables,
	}
}

func (m *MockCompilerContext) DatabaseExists(name string) bool {
	return strings.ToLower(name) == "tpch" || strings.ToLower(name) == "mo"
}

func (m *MockCompilerContext) DefaultDatabase() string {
	return "tpch"
}

func (m *MockCompilerContext) Resolve(dbName string, tableName string) (*ObjectRef, *TableDef) {
	name := strings.ToLower(tableName)
	return m.objects[name], m.tables[name]
}

func (m *MockCompilerContext) GetPrimaryKeyDef(dbName string, tableName string) []*ColDef {
	return nil
}

func (m *MockCompilerContext) GetHideKeyDef(dbName string, tableName string) *ColDef {
	return nil
}

func (m *MockCompilerContext) Cost(obj *ObjectRef, e *Expr) *Cost {
	c := &Cost{}
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

func NewEmptyMockOptimizer() *MockOptimizer {
	return &MockOptimizer{
		ctxt: *NewEmptyCompilerContext(),
	}
}

func NewMockOptimizer() *MockOptimizer {
	return &MockOptimizer{
		ctxt: *NewMockCompilerContext(),
	}
}

func (moc *MockOptimizer) Optimize(stmt tree.Statement) (*Query, error) {
	ctx := moc.CurrentContext()
	query, err := BuildPlan(ctx, stmt)
	if err != nil {
		// fmt.Printf("Optimize statement error: '%v'", tree.String(stmt, dialect.MYSQL))
		return nil, err
	}
	return query.GetQuery(), nil
}

func (moc *MockOptimizer) CurrentContext() CompilerContext {
	return &moc.ctxt
}
