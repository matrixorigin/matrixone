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

package plan

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type MockCompilerContext struct {
	objects map[string]*ObjectRef
	tables  map[string]*TableDef
	stats   map[string]*Stats
	pks     map[string][]int

	mysqlCompatible bool

	// ctx default: nil
	ctx context.Context
}

func (m *MockCompilerContext) ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	vars := make(map[string]interface{})
	vars["str_var"] = "str"
	vars["int_var"] = 20
	vars["bool_var"] = false
	vars["float_var"] = 20.20
	dec, _ := types.ParseStringToDecimal128("200.001", 2, 2, false)
	vars["decimal_var"] = dec
	vars["null_var"] = nil

	if m.mysqlCompatible {
		vars["sql_mode"] = ""
	} else {
		vars["sql_mode"] = "ONLY_FULL_GROUP_BY"
	}

	if result, ok := vars[varName]; ok {
		return result, nil
	}

	return nil, moerr.NewInternalError(m.ctx, "var not found")
}

type col struct {
	Name      string
	Id        types.T
	Nullable  bool
	Width     int32
	Precision int32
}

// NewEmptyCompilerContext for test create/drop statement
func NewEmptyCompilerContext() *MockCompilerContext {
	return &MockCompilerContext{
		objects: make(map[string]*ObjectRef),
		tables:  make(map[string]*TableDef),
		ctx:     context.Background(),
	}
}

type Schema struct {
	cols   []col
	pks    []int
	outcnt float64
}

const SF float64 = 1

func NewMockCompilerContext() *MockCompilerContext {
	tpchSchema := make(map[string]*Schema)
	moSchema := make(map[string]*Schema)

	schemas := map[string]map[string]*Schema{
		"tpch":       tpchSchema,
		"mo_catalog": moSchema,
	}

	tpchSchema["nation"] = &Schema{
		cols: []col{
			{"n_nationkey", types.T_int32, false, 0, 0},
			{"n_name", types.T_varchar, false, 25, 0},
			{"n_regionkey", types.T_int32, false, 0, 0},
			{"n_comment", types.T_varchar, true, 152, 0},
		},
		pks:    []int{0},
		outcnt: 25,
	}
	tpchSchema["nation2"] = &Schema{
		cols: []col{ //not exist in tpch, create for test NaturalJoin And UsingJoin
			{"n_nationkey", types.T_int32, false, 0, 0},
			{"n_name", types.T_varchar, false, 25, 0},
			{"r_regionkey", types.T_int32, false, 0, 0}, //change N_REGIONKEY to R_REGIONKEY for test NaturalJoin And UsingJoin
			{"n_comment", types.T_varchar, true, 152, 0},
		},
		pks:    []int{0},
		outcnt: 25,
	}
	tpchSchema["region"] = &Schema{
		cols: []col{
			{"r_regionkey", types.T_int32, false, 0, 0},
			{"r_name", types.T_varchar, false, 25, 0},
			{"r_comment", types.T_varchar, true, 152, 0},
		},
		pks:    []int{0},
		outcnt: 5,
	}
	tpchSchema["part"] = &Schema{
		cols: []col{
			{"p_partkey", types.T_int32, false, 0, 0},
			{"p_name", types.T_varchar, false, 55, 0},
			{"p_mfgr", types.T_varchar, false, 25, 0},
			{"p_brand", types.T_varchar, false, 10, 0},
			{"p_type", types.T_varchar, false, 25, 0},
			{"p_size", types.T_int32, false, 0, 0},
			{"p_container", types.T_varchar, false, 10, 0},
			{"p_retailprice", types.T_float64, false, 15, 2},
			{"p_comment", types.T_varchar, false, 23, 0},
		},
		pks:    []int{0},
		outcnt: SF * 2e5,
	}
	tpchSchema["supplier"] = &Schema{
		cols: []col{
			{"s_suppkey", types.T_int32, false, 0, 0},
			{"s_name", types.T_varchar, false, 25, 0},
			{"s_address", types.T_varchar, false, 40, 0},
			{"s_nationkey", types.T_int32, false, 0, 0},
			{"s_phone", types.T_varchar, false, 15, 0},
			{"s_acctbal", types.T_float64, false, 15, 2},
			{"s_comment", types.T_varchar, false, 101, 0},
		},
		pks:    []int{0},
		outcnt: SF * 1e4,
	}
	tpchSchema["partsupp"] = &Schema{
		cols: []col{
			{"ps_partkey", types.T_int32, false, 0, 0},
			{"ps_suppkey", types.T_int32, false, 0, 0},
			{"ps_availqty", types.T_int32, false, 0, 0},
			{"ps_supplycost", types.T_float64, false, 15, 2},
			{"ps_comment", types.T_varchar, false, 199, 0},
		},
		pks:    []int{0, 1},
		outcnt: SF * 8e5,
	}
	tpchSchema["customer"] = &Schema{
		cols: []col{
			{"c_custkey", types.T_int32, false, 0, 0},
			{"c_name", types.T_varchar, false, 25, 0},
			{"c_address", types.T_varchar, false, 40, 0},
			{"c_nationkey", types.T_int32, false, 0, 0},
			{"c_phone", types.T_varchar, false, 15, 0},
			{"c_acctbal", types.T_float64, false, 15, 2},
			{"c_mktsegment", types.T_varchar, false, 10, 0},
			{"c_comment", types.T_varchar, false, 117, 0},
		},
		pks:    []int{0},
		outcnt: SF * 15e4,
	}
	tpchSchema["orders"] = &Schema{
		cols: []col{
			{"o_orderkey", types.T_int64, false, 0, 0},
			{"o_custkey", types.T_int32, false, 0, 0},
			{"o_orderstatus", types.T_varchar, false, 1, 0},
			{"o_totalprice", types.T_float64, false, 15, 2},
			{"o_orderdate", types.T_date, false, 0, 0},
			{"o_orderpriority", types.T_varchar, false, 15, 0},
			{"o_clerk", types.T_varchar, false, 15, 0},
			{"o_shippriority", types.T_int32, false, 0, 0},
			{"o_comment", types.T_varchar, false, 79, 0},
		},
		pks:    []int{0},
		outcnt: SF * 15e5,
	}
	tpchSchema["lineitem"] = &Schema{
		cols: []col{
			{"l_orderkey", types.T_int64, false, 0, 0},
			{"l_partkey", types.T_int32, false, 0, 0},
			{"l_suppkey", types.T_int32, false, 0, 0},
			{"l_linenumber", types.T_int32, false, 0, 0},
			{"l_quantity", types.T_int32, false, 0, 0},
			{"l_extendedprice", types.T_float64, false, 15, 2},
			{"l_discount", types.T_float64, false, 15, 2},
			{"l_tax", types.T_float64, false, 15, 2},
			{"l_returnflag", types.T_varchar, false, 1, 0},
			{"l_linestatus", types.T_varchar, false, 1, 0},
			{"l_shipdate", types.T_date, false, 0, 0},
			{"l_commitdate", types.T_date, false, 0, 0},
			{"l_receiptdate", types.T_date, false, 0, 0},
			{"l_shipinstruct", types.T_varchar, false, 25, 0},
			{"l_shipmode", types.T_varchar, false, 10, 0},
			{"l_comment", types.T_varchar, false, 44, 0},
		},
		pks:    []int{0, 3},
		outcnt: SF * 6e6,
	}
	// it's a view
	tpchSchema["v1"] = &Schema{
		cols: []col{
			{"n_name", types.T_varchar, false, 50, 0},
		},
	}

	moSchema["mo_database"] = &Schema{
		cols: []col{
			{"datname", types.T_varchar, false, 50, 0},
			{"account_id", types.T_uint32, false, 0, 0},
		},
	}
	moSchema["mo_tables"] = &Schema{
		cols: []col{
			{"reldatabase", types.T_varchar, false, 50, 0},
			{"relname", types.T_varchar, false, 50, 0},
			{"relkind", types.T_varchar, false, 50, 0},
			{"account_id", types.T_uint32, false, 0, 0},
		},
	}
	moSchema["mo_columns"] = &Schema{
		cols: []col{
			{"att_database", types.T_varchar, false, 50, 0},
			{"att_relname", types.T_varchar, false, 50, 0},
			{"attname", types.T_varchar, false, 50, 0},
			{"atttyp", types.T_int32, false, 0, 0},
			{"attnum", types.T_int32, false, 0, 0},
			{"att_length", types.T_int32, false, 0, 0},
			{"attnotnull", types.T_int8, false, 0, 0},
			{"att_constraint_type", types.T_char, false, 1, 0},
			{"att_default", types.T_varchar, false, 1024, 0},
			{"att_comment", types.T_varchar, false, 1024, 0},
			{"account_id", types.T_uint32, false, 0, 0},
		},
	}

	objects := make(map[string]*ObjectRef)
	tables := make(map[string]*TableDef)
	stats := make(map[string]*Stats)
	pks := make(map[string][]int)
	// build tpch/mo context data(schema)
	for db, schema := range schemas {
		tableIdx := 0
		for tableName, table := range schema {
			colDefs := make([]*ColDef, 0, len(table.cols))

			for _, col := range table.cols {
				colDefs = append(colDefs, &ColDef{
					Typ: &plan.Type{
						Id:          int32(col.Id),
						NotNullable: !col.Nullable,
						Width:       col.Width,
						Precision:   col.Precision,
					},
					Name:    col.Name,
					Pkidx:   1,
					Default: &plan.Default{},
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

			tableDef := &TableDef{
				Name: tableName,
				Cols: colDefs,
			}
			if tableName == "v1" {
				tableDef.TableType = catalog.SystemViewRel
				viewData, _ := json.Marshal(ViewData{
					Stmt:            "select n_name from nation where n_nationkey > ?",
					DefaultDatabase: "tpch",
				})
				tableDef.ViewSql = &plan.ViewDef{
					View: string(viewData),
				}
				properties := []*plan.Property{
					{
						Key:   catalog.SystemRelAttr_Kind,
						Value: catalog.SystemViewRel,
					},
				}
				tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
					Def: &plan.TableDef_DefType_Properties{
						Properties: &plan.PropertiesDef{
							Properties: properties,
						},
					},
				})
			}
			tables[tableName] = tableDef
			tableIdx++

			if table.outcnt == 0 {
				table.outcnt = 1
			}
			stats[tableName] = &plan.Stats{
				Outcnt: table.outcnt,
			}

			pks[tableName] = table.pks
		}
	}

	return &MockCompilerContext{
		objects: objects,
		tables:  tables,
		stats:   stats,
		pks:     pks,
	}
}

func (m *MockCompilerContext) DatabaseExists(name string) bool {
	return strings.ToLower(name) == "tpch" || strings.ToLower(name) == "mo"
}

func (m *MockCompilerContext) DefaultDatabase() string {
	return "tpch"
}

func (m *MockCompilerContext) GetRootSql() string {
	return ""
}

func (m *MockCompilerContext) GetUserName() string {
	return "root"
}

func (m *MockCompilerContext) Resolve(dbName string, tableName string) (*ObjectRef, *TableDef) {
	name := strings.ToLower(tableName)
	return m.objects[name], m.tables[name]
}

func (m *MockCompilerContext) GetPrimaryKeyDef(dbName string, tableName string) []*ColDef {
	defs := make([]*ColDef, 0, 2)
	for _, pk := range m.pks[tableName] {
		defs = append(defs, m.tables[tableName].Cols[pk])
	}
	return defs
}

func (m *MockCompilerContext) GetHideKeyDef(dbName string, tableName string) *ColDef {
	return m.tables[tableName].Cols[0]
}

func (m *MockCompilerContext) Stats(obj *ObjectRef, e *Expr) *Stats {
	return m.stats[obj.ObjName]
}

func (m *MockCompilerContext) GetAccountId() uint32 {
	return 0
}

func (m *MockCompilerContext) GetContext() context.Context {
	return m.ctx
}

func (m *MockCompilerContext) GetProcess() *process.Process {
	return testutil.NewProc()
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
		// logutil.Infof("Optimize statement error: '%v'", tree.String(stmt, dialect.MYSQL))
		return nil, err
	}
	return query.GetQuery(), nil
}

func (moc *MockOptimizer) CurrentContext() CompilerContext {
	return &moc.ctxt
}
