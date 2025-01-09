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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ CompilerContext = &MockCompilerContext{}

type MockCompilerContext struct {
	dbs             map[string]bool
	objects         map[string]*ObjectRef
	tables          map[string]*TableDef
	pks             map[string][]int
	id2name         map[uint64]string
	isDml           bool
	mysqlCompatible bool

	// ctx default: nil
	ctx context.Context
}

func (m *MockCompilerContext) GetLowerCaseTableNames() int64 {
	return 1
}

func (m *MockCompilerContext) GetViews() []string {
	return nil
}

func (m *MockCompilerContext) SetViews(views []string) {
}

func (m *MockCompilerContext) GetSnapshot() *Snapshot {
	return nil
}

func (m *MockCompilerContext) SetSnapshot(snapshot *Snapshot) {}

func (m *MockCompilerContext) InitExecuteStmtParam(execPlan *plan.Execute) (*plan.Plan, tree.Statement, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockCompilerContext) CheckSubscriptionValid(subName, accName string, pubName string) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockCompilerContext) ResolveIndexTableByRef(ref *ObjectRef, tblName string, snapshot *Snapshot) (*ObjectRef, *TableDef) {
	return m.Resolve(DbNameOfObjRef(ref), tblName, snapshot)
}

func (m *MockCompilerContext) ResolveSubscriptionTableById(tableId uint64, pubmeta *SubscriptionMeta) (*ObjectRef, *TableDef) {
	return nil, nil
}

func (m *MockCompilerContext) ResolveUdf(name string, ast []*plan.Expr) (*function.Udf, error) {
	return nil, nil
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
	dec, _ := types.ParseDecimal128("200.001", 38, 3)
	vars["decimal_var"] = dec
	vars["null_var"] = nil
	vars["delete_opt_to_truncate"] = int64(1)

	if m.mysqlCompatible {
		vars["sql_mode"] = ""
	} else {
		vars["sql_mode"] = "ONLY_FULL_GROUP_BY"
	}

	vars["foreign_key_checks"] = int64(1)

	if result, ok := vars[varName]; ok {
		return result, nil
	}

	return nil, moerr.NewInternalError(m.ctx, "var not found")
}

type col struct {
	Name     string
	Id       types.T
	Nullable bool
	Width    int32
	Scale    int32
}

type index struct {
	indexName  string
	tableName  string
	unique     bool
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
	cols      []col
	pks       []int
	idxs      []index
	fks       []*ForeignKeyDef
	clusterby *ClusterByDef
	outcnt    float64
	tblId     int64
	isView    bool
	viewCfg   ViewCfg
}

type ViewCfg struct {
	sql string
	db  string
}

const SF float64 = 1

func NewMockCompilerContext(isDml bool) *MockCompilerContext {
	tpchSchema := make(map[string]*Schema)
	moSchema := make(map[string]*Schema)
	constraintTestSchema := make(map[string]*Schema)
	cteTestSchema := make(map[string]*Schema)
	bvtTest1Schema := make(map[string]*Schema)
	bvtTest2Schema := make(map[string]*Schema)
	bvtTest3Schema := make(map[string]*Schema)
	informationSchemaSchema := make(map[string]*Schema)
	cteTest2Schema := make(map[string]*Schema)

	schemas := map[string]map[string]*Schema{
		"tpch":               tpchSchema,
		"mo_catalog":         moSchema,
		"constraint_test":    constraintTestSchema,
		"cte_test":           cteTestSchema,
		"bvt_test1":          bvtTest1Schema,
		"bvt_test2":          bvtTest2Schema,
		"bvt_test3":          bvtTest3Schema,
		"information_schema": informationSchemaSchema,
		"cte_test2":          cteTest2Schema,
	}

	dbs := make(map[string]bool)
	for s := range schemas {
		dbs[s] = true
	}

	tpchSchema["nation"] = &Schema{
		cols: []col{
			{"n_nationkey", types.T_int32, false, 0, 0},
			{"n_name", types.T_varchar, false, 25, 0},
			{"n_regionkey", types.T_int32, false, 0, 0},
			{"n_comment", types.T_varchar, true, 152, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
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
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks:    []int{0},
		outcnt: 25,
	}
	tpchSchema["test_idx"] = &Schema{
		cols: []col{
			{"n_nationkey", types.T_int32, false, 0, 0},
			{"n_name", types.T_varchar, false, 25, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks:    []int{0},
		outcnt: 25,
	}
	tpchSchema["region"] = &Schema{
		cols: []col{
			{"r_regionkey", types.T_int32, false, 0, 0},
			{"r_name", types.T_varchar, false, 25, 0},
			{"r_comment", types.T_varchar, true, 152, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
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
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
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
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
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
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
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
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
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
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
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
			{"l_quantity", types.T_decimal64, false, 15, 2},
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
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks:    []int{0, 3},
		outcnt: SF * 6e6,
	}
	// it's a view
	tpchSchema["v1"] = &Schema{
		cols: []col{
			{"n_name", types.T_varchar, false, 50, 0},
		},
		isView: true,
		viewCfg: ViewCfg{
			sql: "select n_name from nation where n_nationkey > ?",
			db:  "tpch",
		},
	}

	moSchema["mo_database"] = &Schema{
		cols: []col{
			{"datname", types.T_varchar, false, 50, 0},
			{"account_id", types.T_uint32, false, 0, 0},
			{"dat_createsql", types.T_varchar, false, 1024, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
	}
	moSchema["mo_tables"] = &Schema{
		cols: []col{
			{"rel_id", types.T_uint64, false, 64, 0},
			{"relname", types.T_varchar, false, 5000, 0},
			{"reldatabase", types.T_varchar, false, 5000, 0},
			{"reldatabase_id", types.T_uint64, false, 64, 0},
			{"relpersistence", types.T_varchar, false, 5000, 0},
			{"relkind", types.T_varchar, false, 5000, 0},
			{"rel_comment", types.T_varchar, false, 5000, 0},
			{"rel_createsql", types.T_text, false, 0, 0},
			{"created_time", types.T_timestamp, false, 0, 0},
			{"creator", types.T_uint32, false, 0, 0},
			{"owner", types.T_uint32, false, 0, 0},
			{"account_id", types.T_uint32, false, 0, 0},
			{"partitioned", types.T_int8, false, 0, 0},
			{"partition_info", types.T_blob, false, 0, 0},
			{"viewdef", types.T_varchar, false, 5000, 0},
			{"constraint", types.T_varchar, false, 5000, 0},
			{"rel_version", types.T_uint32, false, 32, 0},
			{"catalog_version", types.T_uint32, false, 32, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0, 1},
	}
	moSchema["mo_columns"] = &Schema{
		cols: []col{
			{"att_uniq_name", types.T_varchar, false, 256, 0},
			{"account_id", types.T_uint32, false, 0, 0},
			{"att_database_id", types.T_uint32, false, 0, 0},
			{"att_database", types.T_varchar, false, 50, 0},
			{"att_relname_id", types.T_uint32, false, 0, 0},
			{"att_relname", types.T_varchar, false, 50, 0},
			{"attname", types.T_varchar, false, 50, 0},
			{"atttyp", types.T_int32, false, 0, 0},
			{"attnum", types.T_int32, false, 0, 0},
			{"att_length", types.T_int32, false, 0, 0},
			{"attnotnull", types.T_int8, false, 0, 0},
			{"atthasdef", types.T_int8, false, 0, 0},
			{"att_default", types.T_varchar, false, 2048, 0},
			{"attisdropped", types.T_int8, false, 0, 0},
			{"att_constraint_type", types.T_varchar, false, 1, 0},
			{"att_is_unsigned", types.T_int8, false, 0, 0},
			{"att_is_auto_increment", types.T_int8, false, 0, 0},
			{"att_comment", types.T_varchar, false, 1024, 0},
			{"att_is_hidden", types.T_bool, false, 0, 0},
			{"attr_has_update", types.T_int8, false, 0, 0},
			{"attr_update", types.T_varchar, false, 2048, 0},
			{"att_attr_is_clusterby", types.T_int8, false, 0, 0},
			{"attr_seqnum", types.T_int8, false, 0, 0},
			{"attr_enum", types.T_varchar, false, 2048, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
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
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
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
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
	}

	moSchema["mo_user_defined_function"] = &Schema{
		cols: []col{
			{"function_id", types.T_int32, false, 50, 0},
			{"name", types.T_varchar, false, 100, 0},
			{"creator", types.T_uint64, false, 50, 0},
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
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
	}

	moSchema["mo_indexes"] = &Schema{
		cols: []col{
			{"id", types.T_uint64, false, 100, 0},
			{"table_id", types.T_uint64, false, 100, 0},
			{"database_id", types.T_uint64, false, 100, 0},
			{"name", types.T_varchar, false, 64, 0},
			{"type", types.T_varchar, false, 11, 0},
			{"algo", types.T_varchar, false, 11, 0},
			{"algo_table_type", types.T_varchar, false, 11, 0},
			{"algo_params", types.T_varchar, false, 2048, 0},
			{"is_visible", types.T_int8, false, 50, 0},
			{"hidden", types.T_int8, false, 50, 0},
			{"comment", types.T_varchar, false, 2048, 0},
			{"column_name", types.T_varchar, false, 256, 0},
			{"ordinal_position", types.T_uint32, false, 50, 0},
			{"options", types.T_text, true, 50, 0},
			{"index_table_name", types.T_varchar, true, 50, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
	}

	moSchema["mo_role"] = &Schema{
		cols: []col{
			{"role_id", types.T_uint64, false, 100, 0},
			{"role_name", types.T_varchar, false, 64, 0},
			{"creator", types.T_int64, false, 50, 0},
			{"owner", types.T_int64, false, 50, 0},
			{"created_time", types.T_timestamp, false, 0, 0},
			{"comments", types.T_varchar, false, 2048, 0},
		},
		pks: []int{0},
	}

	moSchema["mo_stages"] = &Schema{
		cols: []col{
			{"stage_id", types.T_uint64, false, 100, 0},
			{"stage_name", types.T_varchar, false, 64, 0},
			{"url", types.T_varchar, false, 50, 0},
			{"stage_credentials", types.T_varchar, false, 50, 0},
			{"stage_status", types.T_varchar, false, 50, 0},
			{"created_time", types.T_timestamp, false, 0, 0},
			{"comment", types.T_varchar, false, 2048, 0},
		},
	}

	moSchema["mo_snapshots"] = &Schema{
		cols: []col{
			{"snapshot_id", types.T_uuid, false, 100, 0},
			{"sname", types.T_varchar, false, 64, 0},
			{"ts", types.T_int64, false, 50, 0},
			{"level", types.T_enum, false, 50, 0},
			{"account_name", types.T_varchar, false, 50, 0},
			{"database_name", types.T_varchar, false, 50, 0},
			{"table_name", types.T_varchar, false, 50, 0},
			{"obj_id", types.T_uint64, false, 100, 0},
		},
		pks: []int{0},
	}

	moSchema["mo_pitr"] = &Schema{
		cols: []col{
			{"pitr_id", types.T_uuid, false, 100, 0},
			{"pitr_name", types.T_varchar, false, 64, 0},
			{"create_account", types.T_uint64, false, 50, 0},
			{"create_time", types.T_timestamp, false, 50, 0},
			{"modified_time", types.T_timestamp, false, 50, 0},
			{"level", types.T_varchar, false, 50, 0},
			{"account_id", types.T_uint64, false, 50, 0},
			{"account_name", types.T_varchar, false, 50, 0},
			{"database_name", types.T_varchar, false, 50, 0},
			{"table_name", types.T_varchar, false, 50, 0},
			{"obj_id", types.T_uint64, false, 100, 0},
			{"pitr_length", types.T_int64, false, 50, 0},
			{"pitr_unit", types.T_varchar, false, 50, 0},
		},
		pks: []int{0},
	}

	//---------------------------------------------constraint test schema---------------------------------------------------------
	/*
		create table emp(
			empno int unsigned primary key,
			ename varchar(15),
			job varchar(10),
			mgr int unsigned,
			hiredate date,
			sal decimal(7,2),
			comm decimal(7,2),
			deptno int unsigned,
			unique key(ename, job),
			key (ename, job),
			foreign key (deptno) references dept(deptno)
		);
	*/
	constraintTestSchema["emp"] = &Schema{
		cols: []col{
			{"empno", types.T_uint32, true, 32, 0},
			{"ename", types.T_varchar, true, 15, 0},
			{"job", types.T_varchar, true, 10, 0},
			{"mgr", types.T_uint32, true, 32, 0},
			{"hiredate", types.T_date, true, 0, 0},
			{"sal", types.T_decimal64, true, 7, 0},
			{"comm", types.T_decimal64, true, 7, 0},
			{"deptno", types.T_uint32, true, 32, 0},
			{catalog.Row_ID, types.T_Rowid, true, 0, 0},
		},
		pks: []int{0}, // primary key "empno"
		fks: []*plan.ForeignKeyDef{
			{
				Name:        "fk1",                       // string
				Cols:        []uint64{7},                 // []uint64
				ForeignTbl:  88888,                       // uint64
				ForeignCols: []uint64{1},                 // []uint64
				OnDelete:    plan.ForeignKeyDef_RESTRICT, // ForeignKeyDef_RefAction
				OnUpdate:    plan.ForeignKeyDef_RESTRICT, // ForeignKeyDef_RefAction
			},
		},
		idxs: []index{
			{
				indexName: "",
				tableName: catalog.UniqueIndexTableNamePrefix + "412f4fad-77ba-11ed-b347-000c29847904",
				parts:     []string{"ename", "job"},
				cols: []col{
					{catalog.IndexTableIndexColName, types.T_varchar, true, 65535, 0},
				},
				tableExist: true,
				unique:     true,
			},
			{
				indexName: "",
				tableName: catalog.SecondaryIndexTableNamePrefix + "512f4fad-77ba-11ed-b347-000c29847904",
				parts:     []string{"ename", "job"},
				cols: []col{
					{catalog.IndexTableIndexColName, types.T_varchar, true, 65535, 0},
				},
				tableExist: true,
				unique:     false,
			},
		},
		outcnt: 14,
	}

	// index table
	constraintTestSchema[catalog.UniqueIndexTableNamePrefix+"412f4fad-77ba-11ed-b347-000c29847904"] = &Schema{
		cols: []col{
			{catalog.IndexTableIndexColName, types.T_varchar, true, 65535, 0},
			{catalog.IndexTablePrimaryColName, types.T_uint32, true, 32, 0},
			{catalog.Row_ID, types.T_Rowid, true, 0, 0},
		},
		pks:    []int{0},
		outcnt: 13,
	}
	constraintTestSchema[catalog.SecondaryIndexTableNamePrefix+"512f4fad-77ba-11ed-b347-000c29847904"] = &Schema{
		cols: []col{
			{catalog.IndexTableIndexColName, types.T_varchar, true, 65535, 0},
			{catalog.IndexTablePrimaryColName, types.T_uint32, true, 32, 0},
			{catalog.Row_ID, types.T_Rowid, true, 0, 0},
		},
		pks:    []int{0},
		outcnt: 13,
	}

	/*
		create table dept(
			deptno int unsigned auto_increment,
			dname varchar(15),
			loc varchar(50),
			primary key(deptno),
			unique index(dname)
		);
	*/
	constraintTestSchema["dept"] = &Schema{
		tblId: 88888,
		cols: []col{
			{"deptno", types.T_uint32, true, 32, 0},
			{"dname", types.T_varchar, true, 15, 0},
			{"loc", types.T_varchar, true, 50, 0},
			{catalog.Row_ID, types.T_Rowid, true, 0, 0},
		},
		pks: []int{0}, // primary key "deptno"
		idxs: []index{
			{
				indexName: "",
				tableName: catalog.UniqueIndexTableNamePrefix + "8e3246dd-7a19-11ed-ba7d-000c29847904",
				parts:     []string{"dname"},
				cols: []col{
					{catalog.IndexTableIndexColName, types.T_varchar, true, 15, 0},
				},
				tableExist: true,
				unique:     true,
			},
			{
				indexName: "",
				tableName: catalog.UniqueIndexTableNamePrefix + "35fd5c5f-ab54-4873-85e4-3d5ab0ae20a2",
				parts:     []string{"loc", "dname"},
				cols: []col{
					{catalog.IndexTableIndexColName, types.T_varchar, true, 255, 0},
				},
				tableExist: true,
				unique:     false,
			},
		},
		outcnt: 4,
	}

	// index table
	constraintTestSchema[catalog.UniqueIndexTableNamePrefix+"8e3246dd-7a19-11ed-ba7d-000c29847904"] = &Schema{
		cols: []col{
			{catalog.IndexTableIndexColName, types.T_varchar, true, 15, 0},
			{catalog.IndexTablePrimaryColName, types.T_uint32, true, 32, 0},
			{catalog.Row_ID, types.T_Rowid, true, 0, 0},
		},
		pks:    []int{0},
		outcnt: 4,
	}
	constraintTestSchema[catalog.UniqueIndexTableNamePrefix+"35fd5c5f-ab54-4873-85e4-3d5ab0ae20a2"] = &Schema{
		cols: []col{
			{catalog.IndexTableIndexColName, types.T_varchar, false, 255, 0},
			{catalog.IndexTablePrimaryColName, types.T_uint32, false, 32, 0},
			{catalog.Row_ID, types.T_Rowid, true, 0, 0},
		},
		pks:    []int{0},
		outcnt: 4,
	}
	/*
		create table products (
			pid int not null,
			pname varchar(50) not null,
			description varchar(20) not null,
			price decimal(9,2) not null
		) cluster by(pid,pname);
	*/
	constraintTestSchema["products"] = &Schema{
		cols: []col{
			{"pid", types.T_int32, true, 32, 0},
			{"pname", types.T_varchar, true, 50, 0},
			{"description", types.T_varchar, true, 20, 0},
			{"price", types.T_uint32, true, 9, 0},
			{"__mo_cbkey_003pid005pname", types.T_varchar, true, 65535, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		clusterby: &ClusterByDef{
			Name: "__mo_cbkey_003pid005pname",
		},
		outcnt: 14,
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
	constraintTestSchema["employees"] = &Schema{
		cols: []col{
			{"empno", types.T_uint32, true, 32, 0},
			{"ename", types.T_varchar, true, 15, 0},
			{"job", types.T_varchar, true, 10, 0},
			{"mgr", types.T_uint32, true, 32, 0},
			{"hiredate", types.T_date, true, 0, 0},
			{"sal", types.T_decimal64, true, 7, 0},
			{"comm", types.T_decimal64, true, 7, 0},
			{"deptno", types.T_uint32, true, 32, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0}, // primary key "deptno"
		idxs: []index{
			{
				indexName: "",
				tableName: catalog.UniqueIndexTableNamePrefix + "6380d30e-79f8-11ed-9c02-000c29847904",
				parts:     []string{"empno", "ename"},
				cols: []col{
					{catalog.IndexTableIndexColName, types.T_varchar, true, 65535, 0},
				},
				tableExist: true,
				unique:     true,
			},
		},
		outcnt: 14,
	}

	constraintTestSchema[catalog.UniqueIndexTableNamePrefix+"6380d30e-79f8-11ed-9c02-000c29847904"] = &Schema{
		cols: []col{
			{catalog.IndexTableIndexColName, types.T_varchar, true, 65535, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks:    []int{0},
		outcnt: 12,
	}

	constraintTestSchema["t1"] = &Schema{
		cols: []col{
			{"a", types.T_int64, false, 0, 0},
			{"b", types.T_varchar, false, 1, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks:    []int{0},
		outcnt: 4,
	}

	cteTestSchema["t1"] = &Schema{
		cols: []col{
			{"a", types.T_int64, false, 0, 0},
			{"b", types.T_varchar, false, 1, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
	}

	cteTestSchema["c"] = &Schema{
		cols: []col{
			{"a", types.T_int32, false, 50, 0},
		},
		isView: true,
		viewCfg: ViewCfg{
			sql: "create view c as\nwith \n\tc as (\n\t\tselect a from t1 \n\t), \n\td as (\n\t\tselect a from c \n\t\tunion all \n\t\tselect a+1 from c where a < 2\n\t) \nselect distinct \n\ttt.* \nfrom ( \n\tSELECT * FROM c \n\tUNION ALL \n\tSELECT * FROM d\n) tt \norder by tt.a",
			db:  "cte_test",
		},
	}

	cteTestSchema["v2"] = &Schema{
		cols: []col{
			{"a", types.T_int32, false, 50, 0},
		},
		isView: true,
		viewCfg: ViewCfg{
			sql: "create view v2 as\nwith \n\tv2 as (\n\t\tselect a from t1 \n\t)\nselect distinct \n\t* \nfrom \n\t(\n\t\tselect * from v2\n\t)\n",
			db:  "cte_test",
		},
	}

	bvtTest1Schema["t1"] = &Schema{
		cols: []col{
			{"a", types.T_int64, false, 64, 0},
			{"b", types.T_int64, false, 64, 0},
			{"c", types.T_int64, false, 64, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
	}

	bvtTest2Schema["t2"] = &Schema{
		cols: []col{
			{"a", types.T_int64, false, 64, 0},
			{"b", types.T_int64, false, 64, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
	}

	bvtTest2Schema["t3"] = &Schema{
		cols: []col{
			{"a", types.T_int64, false, 64, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
	}

	bvtTest3Schema["myemployees"] = &Schema{
		cols: []col{
			{"EmployeeID", types.T_int16, false, 16, 0},
			{"FirstName", types.T_varchar, false, 255, 0},
			{"LastName", types.T_varchar, false, 255, 0},
			{"Title", types.T_varchar, false, 255, 0},
			{"DeptID", types.T_int16, false, 16, 0},
			{"ManagerID", types.T_int16, false, 16, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
	}

	bvtTest3Schema["cte_view"] = &Schema{
		cols: []col{
			{"a", types.T_int32, false, 50, 0},
		},
		isView: true,
		viewCfg: ViewCfg{
			sql: "create view cte_view as(\nWITH  RECURSIVE DirectReports(Name, Title, EmployeeID, EmployeeLevel)\nAS (SELECT concat(e.FirstName,\" \",e.LastName) as name,\n        e.Title,\n        e.EmployeeID,\n        1 as EmployeeLevel\n    FROM MyEmployees AS e\n    WHERE e.ManagerID IS NULL\n    UNION ALL\n    SELECT concat(e.FirstName,\" \",e.LastName) as name,\n        e.Title,\n        e.EmployeeID,\n        EmployeeLevel + 1\n    FROM MyEmployees AS e\n    JOIN DirectReports AS d ON e.ManagerID = d.EmployeeID\n    )\nSELECT EmployeeID, Name, Title, EmployeeLevel\nFROM DirectReports order by EmployeeID)",
			db:  "bvt_test3",
		},
	}

	informationSchemaSchema["key_column_usage"] = &Schema{
		cols: []col{
			{"CONSTRAINT_CATALOG", types.T_varchar, false, 64, 0},
			{"CONSTRAINT_SCHEMA", types.T_varchar, false, 64, 0},
			{"CONSTRAINT_NAME", types.T_varchar, false, 64, 0},
			{"TABLE_CATALOG", types.T_varchar, false, 64, 0},
			{"TABLE_SCHEMA", types.T_varchar, false, 64, 0},
			{"TABLE_NAME", types.T_varchar, false, 64, 0},
			{"COLUMN_NAME", types.T_varchar, false, 64, 0},
			{"ORDINAL_POSITION", types.T_uint32, false, 32, 0},
			{"POSITION_IN_UNIQUE_CONSTRAINT", types.T_uint32, false, 32, 0},
			{"REFERENCED_TABLE_SCHEMA", types.T_varchar, false, 64, 0},
			{"REFERENCED_TABLE_NAME", types.T_varchar, false, 64, 0},
			{"REFERENCED_COLUMN_NAME", types.T_varchar, false, 64, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
	}

	informationSchemaSchema["test_referential_constraints"] = &Schema{
		cols: []col{
			{"CONSTRAINT_CATALOG", types.T_varchar, false, 3, 0},
			{"CONSTRAINT_SCHEMA", types.T_varchar, false, 5000, 0},
			{"CONSTRAINT_NAME", types.T_varchar, false, 5000, 0},
			{"unique_constraint_catalog", types.T_varchar, false, 3, 0},
			{"unique_constraint_schema", types.T_varchar, false, 5000, 0},
			{"unique_constraint_name", types.T_varchar, false, 11, 0},
			{"match_option", types.T_varchar, false, 4, 0},
			{"update_rule", types.T_varchar, false, 128, 0},
			{"delete_rule", types.T_varchar, false, 128, 0},
			{"table_name", types.T_varchar, false, 5000, 0},
			{"referenced_table_name", types.T_varchar, false, 5000, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
	}

	informationSchemaSchema["referential_constraints"] = &Schema{
		cols: []col{
			{"CONSTRAINT_CATALOG", types.T_varchar, false, 3, 0},
			{"CONSTRAINT_SCHEMA", types.T_varchar, false, 5000, 0},
			{"CONSTRAINT_NAME", types.T_varchar, false, 5000, 0},
			{"unique_constraint_catalog", types.T_varchar, false, 3, 0},
			{"unique_constraint_schema", types.T_varchar, false, 5000, 0},
			{"unique_constraint_name", types.T_varchar, false, 11, 0},
			{"match_option", types.T_varchar, false, 4, 0},
			{"update_rule", types.T_varchar, false, 128, 0},
			{"delete_rule", types.T_varchar, false, 128, 0},
			{"table_name", types.T_varchar, false, 5000, 0},
			{"referenced_table_name", types.T_varchar, false, 5000, 0},
		},
		isView: true,
		viewCfg: ViewCfg{
			sql: "create view referential_constraints as select * from test_referential_constraints",
			db:  "information_schema",
		},
	}

	informationSchemaSchema["tables"] = &Schema{
		cols: []col{
			{"table_catalog", types.T_varchar, false, 3, 0},
			{"table_schema", types.T_varchar, false, 5000, 0},
			{"table_name", types.T_varchar, false, 5000, 0},
			{"engine", types.T_varchar, false, 3, 0},
			{"version", types.T_int64, false, 64, 0},
			{"row_format", types.T_varchar, false, 10, 0},
			{"table_rows", types.T_int64, false, 64, 0},
			{"avg_row_length", types.T_int64, false, 64, 0},
			{"data_length", types.T_int64, false, 64, 0},
			{"max_data_length", types.T_int64, false, 64, 0},
			{"index_length", types.T_int64, false, 64, 0},
			{"data_free", types.T_int64, false, 64, 0},
			{"auto_increment", types.T_uint64, false, 64, 0},
			{"create_time", types.T_timestamp, false, 0, 0},
			{"update_time", types.T_timestamp, false, 0, 0},
			{"check_time", types.T_timestamp, false, 0, 0},
			{"table_collation", types.T_varchar, false, 18, 0},
			{"checksum", types.T_uint64, false, 64, 0},
			{"create_options", types.T_varchar, false, 256, 0},
			{"table_comment", types.T_text, false, 0, 0},
		},
		isView: true,
		viewCfg: ViewCfg{
			sql: "CREATE VIEW information_schema.TABLES AS SELECT 'def' AS TABLE_CATALOG,reldatabase AS TABLE_SCHEMA,relname AS TABLE_NAME,(case when relkind = 'v' and (reldatabase='mo_catalog' or reldatabase='information_schema') then 'SYSTEM VIEW' when relkind = 'v'  then 'VIEW' when relkind = 'e' then 'EXTERNAL TABLE' when relkind = 'r' then 'BASE TABLE' else 'INTERNAL TABLE' end) AS TABLE_TYPE,if(relkind = 'r','Tae',NULL) AS ENGINE,if(relkind = 'v',NULL,10) AS VERSION,'Compressed' AS ROW_FORMAT,if(relkind = 'v', NULL, 0) AS TABLE_ROWS,if(relkind = 'v', NULL, 0) AS AVG_ROW_LENGTH,if(relkind = 'v', NULL, 0) AS DATA_LENGTH,if(relkind = 'v', NULL, 0) AS MAX_DATA_LENGTH,if(relkind = 'v', NULL, 0) AS INDEX_LENGTH,if(relkind = 'v', NULL, 0) AS DATA_FREE,if(relkind = 'v', NULL, internal_auto_increment(reldatabase, relname)) AS `AUTO_INCREMENT`,created_time AS CREATE_TIME,if(relkind = 'v', NULL, created_time) AS UPDATE_TIME,if(relkind = 'v', NULL, created_time) AS CHECK_TIME,'utf8mb4_0900_ai_ci' AS TABLE_COLLATION,if(relkind = 'v', NULL, 0) AS CHECKSUM,if(relkind = 'v', NULL, if(partitioned = 0, '', cast('partitioned' as varchar(256)))) AS CREATE_OPTIONS,cast(rel_comment as text) AS TABLE_COMMENT FROM mo_catalog.mo_tables tbl WHERE tbl.account_id = current_account_id() and tbl.relname not like '__mo_index_%' and tbl.relkind != 'partition'",
			db:  "information_schema",
		},
	}

	cteTest2Schema["vt1"] = &Schema{
		cols: []col{
			{"a", types.T_int64, false, 0, 0},
			{catalog.Row_ID, types.T_Rowid, false, 16, 0},
		},
		pks: []int{0},
	}

	cteTest2Schema["vv1"] = &Schema{
		cols: []col{
			{"a", types.T_int32, false, 50, 0},
		},
		isView: true,
		viewCfg: ViewCfg{
			sql: "create view vv1 as\nwith \n\tc as (\n\t\tselect * from  vt1\n\t)\nselect \n\t*\nfrom\n\tc;",
			db:  "cte_test2",
		},
	}

	cteTest2Schema["vv2"] = &Schema{
		cols: []col{
			{"a", types.T_int32, false, 50, 0},
		},
		isView: true,
		viewCfg: ViewCfg{
			sql: "create view vv2 as\nwith \n\tvv2 as (\n\t\tselect a from vt1 \n\t)\nselect distinct \n\t* \nfrom \n\t(\n\t\tselect * from vv2\n\t)",
			db:  "cte_test2",
		},
	}

	cteTest2Schema["vv3"] = &Schema{
		cols: []col{
			{"a", types.T_int32, false, 50, 0},
		},
		isView: true,
		viewCfg: ViewCfg{
			sql: "create view vv3 as\nwith \n\tvv3 as (\n\t\tselect a from vt1 \n\t)\nselect distinct \n\t* \nfrom \n\tvv3\n",
			db:  "cte_test2",
		},
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
			tblId := table.tblId
			if tblId == 0 {
				tblId = int64(tableIdx)
			}
			colDefs := make([]*ColDef, 0, len(table.cols))

			for idx, col := range table.cols {
				colDefs = append(colDefs, &ColDef{
					ColId: uint64(idx),
					Typ: plan.Type{
						Id:          int32(col.Id),
						NotNullable: !col.Nullable,
						Width:       col.Width,
						Scale:       col.Scale,
					},
					Name:       strings.ToLower(col.Name),
					OriginName: col.Name,
					Primary:    idx == 0,
					Hidden:     col.Name == catalog.Row_ID || col.Name == catalog.CPrimaryKeyColName,
					Pkidx:      1,
					Default: &plan.Default{
						NullAbility: col.Nullable,
					},
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
				TblId:     uint64(tblId),
				Name:      tableName,
				Cols:      colDefs,
				Indexes:   make([]*IndexDef, len(table.idxs)),
			}
			if len(table.pks) == 1 {
				tableDef.Pkey = &plan.PrimaryKeyDef{
					PkeyColName: colDefs[table.pks[0]].Name,
					Cols:        []uint64{uint64(table.pks[0])},
					Names:       []string{colDefs[table.pks[0]].Name},
					CompPkeyCol: colDefs[table.pks[0]],
				}
			} else if len(table.pks) > 1 {
				names := make([]string, len(table.pks))
				cols := make([]uint64, len(table.pks))
				for pkidx := range table.pks {
					names = append(names, colDefs[table.pks[pkidx]].Name)
					cols = append(cols, uint64(pkidx))
				}
				pkName := catalog.PrefixCBColName + "_" + tableName
				tableDef.Pkey = &plan.PrimaryKeyDef{
					PkeyColName: pkName,
					Names:       names,
					Cols:        cols,
					CompPkeyCol: MakeHiddenColDefByName(pkName),
				}
			}

			if table.idxs != nil {
				for i, idx := range table.idxs {
					indexdef := &plan.IndexDef{
						IndexName:      idx.indexName,
						Parts:          idx.parts,
						Unique:         idx.unique,
						IndexTableName: idx.tableName,
						TableExist:     true,
					}
					tableDef.Indexes[i] = indexdef
				}
			}

			if table.fks != nil {
				tableDef.Fkeys = table.fks
			}

			if table.clusterby != nil {
				tableDef.ClusterBy = &plan.ClusterByDef{
					Name: "__mo_cbkey_003pid005pname",
				}
			}

			{
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
				indexParts := []string{"n_nationkey"}

				p := &plan.IndexDef{
					IndexName:      "idx1",
					Parts:          indexParts,
					Unique:         true,
					IndexTableName: "nation",
					TableExist:     true,
				}
				tableDef.Indexes = []*plan.IndexDef{p}
			}

			if table.isView {
				tableDef.TableType = catalog.SystemViewRel
				viewData, _ := json.Marshal(ViewData{
					Stmt:            table.viewCfg.sql,
					DefaultDatabase: table.viewCfg.db,
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
		dbs:     dbs,
		isDml:   isDml,
		objects: objects,
		tables:  tables,
		id2name: id2name,
		pks:     pks,
		ctx:     context.TODO(),
	}
}

func (m *MockCompilerContext) DatabaseExists(name string, snapshot *Snapshot) bool {
	if _, ok := m.dbs[strings.ToLower(name)]; ok {
		return true
	}
	return false
}

func (m *MockCompilerContext) GetDatabaseId(dbName string, snapshot *Snapshot) (uint64, error) {
	return 0, nil
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

func (m *MockCompilerContext) Resolve(dbName string, tableName string, snapshot *Snapshot) (*ObjectRef, *TableDef) {
	name := strings.ToLower(tableName)
	tableDef := DeepCopyTableDef(m.tables[name], true)
	if tableDef != nil && !m.isDml {
		for i, col := range tableDef.Cols {
			if col.Typ.Id == int32(types.T_Rowid) {
				tableDef.Cols = append(tableDef.Cols[:i], tableDef.Cols[i+1:]...)
				break
			}
		}

		for i, col := range tableDef.Cols {
			// judege whether it is a composite primary key
			if col.Name == catalog.CPrimaryKeyColName {
				tableDef.Cols = append(tableDef.Cols[:i], tableDef.Cols[i+1:]...)
				break
			}
		}
	}
	if tableDef != nil {
		tableDef.DbName = dbName
	}
	return m.objects[name], tableDef
}

func (m *MockCompilerContext) ResolveById(tableId uint64, snapshot *Snapshot) (*ObjectRef, *TableDef) {
	name := m.id2name[tableId]
	tableDef := DeepCopyTableDef(m.tables[name], true)
	if tableDef != nil && !m.isDml {
		for i, col := range tableDef.Cols {
			if col.Typ.Id == int32(types.T_Rowid) {
				tableDef.Cols = append(tableDef.Cols[:i], tableDef.Cols[i+1:]...)
				break
			}
		}
	}
	return m.objects[name], tableDef
}

func (m *MockCompilerContext) GetPrimaryKeyDef(dbName string, tableName string, snapshot *Snapshot) []*ColDef {
	defs := make([]*ColDef, 0, 2)
	for _, pk := range m.pks[tableName] {
		defs = append(defs, m.tables[tableName].Cols[pk])
	}
	return defs
}

func (m *MockCompilerContext) Stats(obj *ObjectRef, snapshot *Snapshot) (*pb.StatsInfo, error) {
	return nil, nil
}

func (m *MockCompilerContext) GetStatsCache() *StatsCache {
	return nil
}

func (m *MockCompilerContext) GetAccountId() (uint32, error) {
	return 0, nil
}

func (m *MockCompilerContext) GetContext() context.Context {
	return m.ctx
}

func (m *MockCompilerContext) SetContext(ctx context.Context) {
	m.ctx = ctx
}

func (m *MockCompilerContext) GetProcess() *process.Process {
	return testutil.NewProc()
}

func (m *MockCompilerContext) GetQueryResultMeta(uuid string) ([]*ColDef, string, error) {
	return nil, "", nil
}

func (m *MockCompilerContext) SetBuildingAlterView(yesOrNo bool, dbName, viewName string) {
}

func (m *MockCompilerContext) GetBuildingAlterView() (bool, string, string) {
	return false, "", ""
}

func (m *MockCompilerContext) GetSubscriptionMeta(dbName string, snapshot *Snapshot) (*SubscriptionMeta, error) {
	return nil, nil
}
func (m *MockCompilerContext) SetQueryingSubscription(*SubscriptionMeta) {

}
func (m *MockCompilerContext) GetQueryingSubscription() *SubscriptionMeta {
	return nil
}
func (m *MockCompilerContext) IsPublishing(dbName string) (bool, error) {
	return false, nil
}
func (m *MockCompilerContext) BuildTableDefByMoColumns(dbName, table string) (*TableDef, error) {
	return nil, nil
}

func (m *MockCompilerContext) ResolveSnapshotWithSnapshotName(snapshotName string) (*Snapshot, error) {
	return nil, nil
}

func (m *MockCompilerContext) CheckTimeStampValid(ts int64) (bool, error) {
	return false, nil
}

type MockOptimizer struct {
	ctxt MockCompilerContext
}

func NewEmptyMockOptimizer() *MockOptimizer {
	return &MockOptimizer{
		ctxt: *NewEmptyCompilerContext(),
	}
}

func NewMockOptimizer(_ bool) *MockOptimizer {
	return &MockOptimizer{
		ctxt: *NewMockCompilerContext(true),
	}
}

func (moc *MockOptimizer) Optimize(stmt tree.Statement) (*Query, error) {
	ctx := moc.CurrentContext()
	query, err := BuildPlan(ctx, stmt, false)
	if err != nil {
		// logutil.Infof("Optimize statement error: '%v'", tree.String(stmt, dialect.MYSQL))
		return nil, err
	}
	return query.GetQuery(), nil
}

func (moc *MockOptimizer) CurrentContext() CompilerContext {
	return &moc.ctxt
}
