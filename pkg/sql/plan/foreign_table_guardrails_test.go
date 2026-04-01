// Copyright 2021-2024 Matrix Origin
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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	mysqlparser "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestCheckTableTypeRejectsForeignTable(t *testing.T) {
	err := checkTableType(context.Background(), &TableDef{TableType: catalog.SystemForeignRel})
	if err == nil {
		t.Fatalf("expected foreign table DML to be rejected")
	}
	if !strings.Contains(err.Error(), "foreign table") {
		t.Fatalf("expected foreign-table error, got %v", err)
	}
}

func TestCheckInsertTargetTableTypeRejectsForeignTable(t *testing.T) {
	err := checkInsertTargetTableType(context.Background(), catalog.SystemForeignRel, "orders_ext")
	if err == nil {
		t.Fatalf("expected foreign table insert to be rejected")
	}
	if !strings.Contains(err.Error(), "foreign table") {
		t.Fatalf("expected foreign-table error, got %v", err)
	}
}

func TestConstructCreateTableSQLUsesForeignTablePrefix(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef := &TableDef{
		Name:      "orders_ext",
		TableType: catalog.SystemForeignRel,
		Cols: []*ColDef{
			{
				Name:       "id",
				OriginName: "id",
				Typ: Type{
					Id:    int32(types.T_int64),
					Width: types.T_int64.ToType().Width,
					Scale: types.T_int64.ToType().Scale,
				},
				Default: &pbplan.Default{NullAbility: true},
			},
		},
	}

	sql, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, nil, false, nil)
	if err != nil {
		t.Fatalf("ConstructCreateTableSQL failed: %v", err)
	}
	if !strings.HasPrefix(sql, "CREATE FOREIGN TABLE `orders_ext`") {
		t.Fatalf("expected foreign table prefix, got %q", sql)
	}
}

func TestConstructCreateTableSQLRoundTripsForeignTable(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef := &TableDef{
		Name:      "orders_ext",
		TableType: catalog.SystemForeignRel,
		Cols: []*ColDef{
			{
				Name:       "id",
				OriginName: "id",
				Typ: Type{
					Id:    int32(types.T_int64),
					Width: types.T_int64.ToType().Width,
					Scale: types.T_int64.ToType().Scale,
				},
				Default: &pbplan.Default{NullAbility: true},
			},
		},
	}

	sql, stmt, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, nil, false, nil)
	if err != nil {
		t.Fatalf("ConstructCreateTableSQL failed: %v", err)
	}
	if !strings.HasPrefix(sql, "CREATE FOREIGN TABLE `orders_ext`") {
		t.Fatalf("expected foreign table prefix, got %q", sql)
	}
	createStmt, ok := stmt.(*tree.CreateTable)
	if !ok {
		t.Fatalf("expected create table stmt, got %T", stmt)
	}
	if !createStmt.IsForeignTable {
		t.Fatalf("expected parsed statement to keep foreign-table marker")
	}
}

func TestBuildCreateForeignTablePlanSetsForeignRelkind(t *testing.T) {
	setupForeignTablePlanTestRuntime()

	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t, "create foreign table orders_ext (id int not null default 1 comment 'id') comment 'remote orders'")
	if err != nil {
		t.Fatalf("build create foreign table failed: %v", err)
	}

	ddl, ok := logicPlan.Plan.(*pbplan.Plan_Ddl)
	if !ok {
		t.Fatalf("expected ddl plan, got %T", logicPlan.Plan)
	}
	createTable := ddl.Ddl.GetCreateTable()
	if createTable == nil {
		t.Fatalf("expected create table definition")
	}
	if createTable.TableDef.TableType != catalog.SystemForeignRel {
		t.Fatalf("expected foreign table type, got %q", createTable.TableDef.TableType)
	}
	if createTable.TableDef.Pkey != nil {
		t.Fatalf("expected no synthetic primary key for foreign table")
	}
	if hasFakePrimaryKey(createTable.TableDef.Cols) {
		t.Fatalf("expected no fake primary key column for foreign table")
	}
	if !hasProperty(createTable.TableDef.Defs, catalog.SystemRelAttr_Kind, catalog.SystemForeignRel) {
		t.Fatalf("expected foreign relkind property")
	}
	if !hasPropertyKey(createTable.TableDef.Defs, catalog.SystemRelAttr_CreateSQL) {
		t.Fatalf("expected create sql property")
	}
}

func TestBuildCreateForeignTableRejectsIndexDefinitions(t *testing.T) {
	setupForeignTablePlanTestRuntime()

	mock := NewMockOptimizer(false)
	_, err := buildSingleStmt(mock, t, "create foreign table orders_ext (id int primary key)")
	if err == nil {
		t.Fatalf("expected create foreign table with primary key to fail")
	}
	if !strings.Contains(err.Error(), "create foreign table does not support column attribute") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildCreateForeignTableRejectsUnsupportedTableOptions(t *testing.T) {
	setupForeignTablePlanTestRuntime()

	mock := NewMockOptimizer(false)
	_, err := buildSingleStmt(mock, t, "create foreign table orders_ext (id int) connection = 'remote_conn'")
	if err == nil {
		t.Fatalf("expected create foreign table with unsupported table option to fail")
	}
	if !strings.Contains(err.Error(), "create foreign table does not support table option") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildAlterForeignTableRejected(t *testing.T) {
	setupForeignTablePlanTestRuntime()

	mock := NewMockOptimizer(false)
	mock.ctxt.tables["orders_ext"] = &TableDef{
		Name:      "orders_ext",
		TableType: catalog.SystemForeignRel,
		Cols: []*ColDef{{
			Name: "id",
			Typ:  pbplan.Type{Id: int32(types.T_int64)},
			Default: &pbplan.Default{
				NullAbility: true,
			},
		}},
	}
	mock.ctxt.objects["orders_ext"] = &ObjectRef{
		SchemaName: "tpch",
		ObjName:    "orders_ext",
	}

	statements, err := mysqlparser.Parse(mock.CurrentContext().GetContext(), "alter table tpch.orders_ext add column c int", 1)
	if err != nil {
		t.Fatalf("parse alter statement failed: %v", err)
	}
	_, err = BuildPlan(mock.CurrentContext(), statements[0], false)
	if err == nil {
		t.Fatalf("expected alter foreign table to fail")
	}
	if !strings.Contains(err.Error(), "alter foreign table") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func setupForeignTablePlanTestRuntime() {
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))
}

func hasFakePrimaryKey(cols []*ColDef) bool {
	for _, col := range cols {
		if col.Name == catalog.FakePrimaryKeyColName {
			return true
		}
	}
	return false
}

func hasProperty(defs []*pbplan.TableDef_DefType, key, value string) bool {
	for _, def := range defs {
		properties := def.GetProperties()
		if properties == nil {
			continue
		}
		for _, property := range properties.Properties {
			if property.Key == key && property.Value == value {
				return true
			}
		}
	}
	return false
}

func hasPropertyKey(defs []*pbplan.TableDef_DefType, key string) bool {
	for _, def := range defs {
		properties := def.GetProperties()
		if properties == nil {
			continue
		}
		for _, property := range properties.Properties {
			if property.Key == key {
				return true
			}
		}
	}
	return false
}
