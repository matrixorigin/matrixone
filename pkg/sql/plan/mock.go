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
	id2name map[uint64]string

	mysqlCompatible bool

	// ctx default: nil
	ctx context.Context
}

func (m *MockCompilerContext) ResolveAccountIds(accountNames []string) ([]uint32, error) {
	return []uint32{catalog.System_Account}, nil
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

type index struct {
	indexName  string
	tableName  string
	parts      []string
	cols       []col
	tableExist bool
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
	idxs   []index
	outcnt float64
}

const SF float64 = 1

func NewMockCompilerContext() *MockCompilerContext {
	tpchSchema := make(map[string]*Schema)
	moSchema := make(map[string]*Schema)
	indexTestSchema := make(map[string]*Schema)

	schemas := map[string]map[string]*Schema{
		"tpch":       tpchSchema,
		"mo_catalog": moSchema,
		"index_test": indexTestSchema,
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
	tpchSchema["test_idx"] = &Schema{
		cols: []col{
			{"n_nationkey", types.T_int32, false, 0, 0},
			{"n_name", types.T_varchar, false, 25, 0},
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
			{"p_retailprice", types.T_decimal64, false, 15, 2},
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
			{"s_acctbal", types.T_decimal64, false, 15, 2},
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
			{"ps_supplycost", types.T_decimal64, false, 15, 2},
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
			{"c_acctbal", types.T_decimal64, false, 15, 2},
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
			{"o_totalprice", types.T_decimal64, false, 15, 2},
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
			{"l_extendedprice", types.T_decimal64, false, 15, 2},
			{"l_discount", types.T_decimal64, false, 15, 2},
			{"l_tax", types.T_decimal64, false, 15, 2},
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
			{"att_is_hidden", types.T_bool, false, 0, 0},
		},
	}
	moSchema["mo_user"] = &Schema{
		cols: []col{
			{"user_id", types.T_int32, false, 50, 0},
			{"user_host", types.T_varchar, false, 100, 0},
			{"user_name", types.T_varchar, false, 300, 0},
			{"authentication_string", types.T_varchar, false, 100, 0},
			{"status", types.T_varchar, false, 100, 0},
			{"created_time", types.T_timestamp, false, 0, 0},
			{"expired_time", types.T_timestamp, false, 0, 0},
			{"login_type", types.T_varchar, false, 100, 0},
			{"creator", types.T_int32, false, 50, 0},
			{"owner", types.T_int32, false, 50, 0},
			{"default_role", types.T_int32, false, 50, 0},
		},
	}

	moSchema["mo_role_privs"] = &Schema{
		cols: []col{
			{"privilege_level", types.T_varchar, false, 100, 0},
			{"obj_id", types.T_uint64, false, 100, 0},
			{"obj_type", types.T_varchar, false, 16, 0},
			{"role_id", types.T_int32, false, 50, 0},
			{"role_name", types.T_varchar, false, 100, 0},
			{"granted_time", types.T_timestamp, false, 0, 0},
			{"operation_user_id", types.T_uint32, false, 50, 0},
			{"privilege_name", types.T_varchar, false, 100, 0},
			{"with_grant_option", types.T_bool, false, 0, 0},
			{"privilege_id", types.T_int32, false, 50, 0},
		},
	}

	moSchema["mo_user_defined_function"] = &Schema{
		cols: []col{
			{"function_id", types.T_int32, false, 50, 0},
			{"name", types.T_varchar, false, 100, 0},
			{"args", types.T_text, false, 1000, 0},
			{"retType", types.T_varchar, false, 20, 0},
			{"body", types.T_text, false, 1000, 0},
			{"language", types.T_varchar, false, 20, 0},
			{"db", types.T_varchar, false, 100, 0},
			{"definer", types.T_varchar, false, 50, 0},
			{"modified_time", types.T_timestamp, false, 0, 0},
			{"created_time", types.T_timestamp, false, 0, 0},
			{"type", types.T_varchar, false, 10, 0},
			{"security_type", types.T_varchar, false, 10, 0},
			{"comment", types.T_varchar, false, 5000, 0},
			{"character_set_client", types.T_varchar, false, 64, 0},
			{"collation_connection", types.T_varchar, false, 64, 0},
			{"database_collation", types.T_varchar, false, 64, 0},
		},
	}

	//---------------------------------------------index test schema---------------------------------------------------------

	//+----------+--------------+------+------+---------+-------+--------------------------------+
	//| Field    | Type         | Null | Key  | Default | Extra | Comment                        |
	//+----------+--------------+------+------+---------+-------+--------------------------------+
	//| empno    | INT UNSIGNED | YES  | UNI  | NULL    |       |                                |
	//| ename    | VARCHAR(15)  | YES  | UNI  | NULL    |       |                                |
	//| job      | VARCHAR(10)  | YES  |      | NULL    |       |                                |
	//| mgr      | INT UNSIGNED | YES  |      | NULL    |       |                                |
	//| hiredate | DATE         | YES  |      | NULL    |       |                                |
	//| sal      | DECIMAL(7,2) | YES  |      | NULL    |       |                                |
	//| comm     | DECIMAL(7,2) | YES  |      | NULL    |       |                                |
	//| deptno   | INT UNSIGNED | YES  |      | NULL    |       |                                |
	//+----------+--------------+------+------+---------+-------+--------------------------------+

	indexTestSchema["emp"] = &Schema{
		cols: []col{
			{"empno", types.T_uint32, true, 32, 0},
			{"ename", types.T_varchar, true, 15, 0},
			{"job", types.T_varchar, true, 10, 0},
			{"mgr", types.T_uint32, true, 32, 0},
			{"hiredate", types.T_date, true, 0, 0},
			{"sal", types.T_decimal64, true, 7, 0},
			{"comm", types.T_decimal64, true, 7, 0},
			{"deptno", types.T_uint32, true, 32, 0},
			{"__mo_rowid", types.T_Rowid, true, 0, 0},
		},
		idxs: []index{
			{
				indexName: "",
				tableName: "__mo_index_unique__412f4fad-77ba-11ed-b347-000c29847904",
				parts:     []string{"empno"},
				cols: []col{
					{"__mo_index_idx_col", types.T_uint32, true, 0, 0},
				},
				tableExist: true,
			},
			{
				indexName: "",
				tableName: "__mo_index_unique__412f5063-77ba-11ed-b347-000c29847904",
				parts:     []string{"ename"},
				cols: []col{
					{"__mo_index_idx_col", types.T_varchar, true, 0, 0},
				},
				tableExist: true,
			},
		},
		outcnt: 14,
	}

	indexTestSchema["__mo_index_unique__412f4fad-77ba-11ed-b347-000c29847904"] = &Schema{
		cols: []col{
			{"__mo_index_idx_col", types.T_uint32, true, 32, 0},
			{"__mo_rowid", types.T_Rowid, true, 0, 0},
		},
		pks:    []int{0},
		outcnt: 13,
	}

	indexTestSchema["__mo_index_unique__412f5063-77ba-11ed-b347-000c29847904"] = &Schema{
		cols: []col{
			{"__mo_index_idx_col", types.T_varchar, true, 15, 0},
			{"__mo_rowid", types.T_Rowid, true, 0, 0},
		},
		pks:    []int{0},
		outcnt: 13,
	}

	//+--------+--------------+------+------+---------+-------+--------------------+
	//| Field  | Type         | Null | Key  | Default | Extra | Comment            |
	//+--------+--------------+------+------+---------+-------+--------------------+
	//| deptno | INT UNSIGNED | YES  | UNI  | NULL    |       |                    |
	//| dname  | VARCHAR(15)  | YES  |      | NULL    |       |                    |
	//| loc    | VARCHAR(50)  | YES  |      | NULL    |       |                    |
	//+--------+--------------+------+------+---------+-------+--------------------+
	indexTestSchema["dept"] = &Schema{
		cols: []col{
			{"deptno", types.T_uint32, true, 32, 0},
			{"dname", types.T_varchar, true, 15, 0},
			{"loc", types.T_varchar, true, 50, 0},
			{"__mo_rowid", types.T_Rowid, true, 0, 0},
		},
		idxs: []index{
			{
				indexName: "",
				tableName: "__mo_index_unique__8e3246dd-7a19-11ed-ba7d-000c29847904",
				parts:     []string{"deptno"},
				cols: []col{
					{"__mo_index_idx_col", types.T_uint32, true, 0, 0},
				},
				tableExist: true,
			},
		},
		outcnt: 4,
	}

	indexTestSchema["__mo_index_unique__8e3246dd-7a19-11ed-ba7d-000c29847904"] = &Schema{
		cols: []col{
			{"__mo_index_idx_col", types.T_uint32, true, 0, 0},
			{"__mo_rowid", types.T_Rowid, true, 0, 0},
		},
		pks:    []int{0},
		outcnt: 4,
	}

	//+----------+--------------+------+-----+---------+-------+
	//| Field    | Type         | Null | Key | Default | Extra |
	//+----------+--------------+------+-----+---------+-------+
	//| empno    | int unsigned | YES  | MUL | NULL    |       |
	//| ename    | varchar(15)  | YES  |     | NULL    |       |
	//| job      | varchar(10)  | YES  |     | NULL    |       |
	//| mgr      | int unsigned | YES  |     | NULL    |       |
	//| hiredate | date         | YES  |     | NULL    |       |
	//| sal      | decimal(7,2) | YES  |     | NULL    |       |
	//| comm     | decimal(7,2) | YES  |     | NULL    |       |
	//| deptno   | int unsigned | YES  |     | NULL    |       |
	//+----------+--------------+------+-----+---------+-------+
	indexTestSchema["employees"] = &Schema{
		cols: []col{
			{"empno", types.T_uint32, true, 32, 0},
			{"ename", types.T_varchar, true, 15, 0},
			{"job", types.T_varchar, true, 10, 0},
			{"mgr", types.T_uint32, true, 32, 0},
			{"hiredate", types.T_date, true, 0, 0},
			{"sal", types.T_decimal64, true, 7, 0},
			{"comm", types.T_decimal64, true, 7, 0},
			{"deptno", types.T_uint32, true, 32, 0},
			{"__mo_rowid", types.T_Rowid, true, 0, 0},
		},
		idxs: []index{
			{
				indexName: "",
				tableName: "__mo_index_unique__6380d30e-79f8-11ed-9c02-000c29847904",
				parts:     []string{"empno", "ename"},
				cols: []col{
					{"__mo_index_idx_col", types.T_varchar, true, 65535, 0},
				},
				tableExist: true,
			},
		},
		outcnt: 14,
	}

	indexTestSchema["__mo_index_unique__6380d30e-79f8-11ed-9c02-000c29847904"] = &Schema{
		cols: []col{
			{"__mo_index_idx_col", types.T_varchar, true, 65535, 0},
			{"__mo_rowid", types.T_Rowid, true, 0, 0},
		},
		pks:    []int{0},
		outcnt: 12,
	}

	objects := make(map[string]*ObjectRef)
	tables := make(map[string]*TableDef)
	stats := make(map[string]*Stats)
	pks := make(map[string][]int)
	id2name := make(map[uint64]string)
	// build tpch/mo context data(schema)
	for db, schema := range schemas {
		tableIdx := 0
		for tableName, table := range schema {
			colDefs := make([]*ColDef, 0, len(table.cols))

			for idx, col := range table.cols {
				colDefs = append(colDefs, &ColDef{
					ColId: uint64(idx),
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
				TableType: catalog.SystemOrdinaryRel,
				TblId:     uint64(tableIdx),
				Name:      tableName,
				Cols:      colDefs,
			}

			if table.idxs != nil {
				unidef := &plan.UniqueIndexDef{
					IndexNames:  make([]string, 0),
					TableNames:  make([]string, 0),
					Fields:      make([]*plan.Field, 0),
					TableExists: make([]bool, 0),
				}

				for _, idx := range table.idxs {
					field := &plan.Field{
						Parts: idx.parts,
						Cols:  make([]*ColDef, 0),
					}

					for _, col := range idx.cols {
						field.Cols = append(field.Cols, &ColDef{
							Alg: plan.CompressType_Lz4,
							Typ: &plan.Type{
								Id:          int32(col.Id),
								NotNullable: !col.Nullable,
								Width:       col.Width,
								Precision:   col.Precision,
							},
							Name:  col.Name,
							Pkidx: 1,
							Default: &plan.Default{
								NullAbility:  false,
								Expr:         nil,
								OriginString: "",
							},
						})
					}
					unidef.IndexNames = append(unidef.IndexNames, idx.indexName)
					unidef.TableNames = append(unidef.TableNames, idx.tableName)
					unidef.Fields = append(unidef.Fields, field)
					unidef.TableExists = append(unidef.TableExists, true)
				}

				tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
					Def: &plan.TableDef_DefType_UIdx{
						UIdx: unidef,
					},
				})
			}

			if tableName != "v1" {
				properties := []*plan.Property{
					{
						Key:   catalog.SystemRelAttr_Kind,
						Value: catalog.SystemOrdinaryRel,
					},
					{
						Key:   catalog.SystemRelAttr_Comment,
						Value: tableName,
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

			if tableName == "test_idx" {
				testField := &plan.Field{
					Parts: []string{"n_nationkey"},
				}
				tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
					Def: &plan.TableDef_DefType_UIdx{
						UIdx: &plan.UniqueIndexDef{
							IndexNames:  []string{"idx1"},
							TableNames:  []string{"nation"},
							Fields:      []*plan.Field{testField},
							TableExists: []bool{false},
						},
					},
				})
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
			id2name[tableDef.TblId] = tableName
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
		id2name: id2name,
		stats:   stats,
		pks:     pks,
		ctx:     context.TODO(),
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

func (m *MockCompilerContext) ResolveById(tableId uint64) (*ObjectRef, *TableDef) {
	name := m.id2name[tableId]
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
	return m.tables[tableName].Cols[len(m.tables[tableName].Cols)-1]
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

func (m *MockCompilerContext) GetQueryResultMeta(uuid string) ([]*ColDef, string, error) {
	return nil, "", nil
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
