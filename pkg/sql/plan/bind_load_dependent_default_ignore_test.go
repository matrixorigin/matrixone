// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// loadDependentDrive plans the DDL, registers the table the way the modern
// binder resolves production targets (durable relkind plus the trailing
// hidden rowid the engine catalog appends), and runs the whole LOAD
// statement through the mock planner. The statement text itself carries the
// ignore keyword (or its absence), so the dependent-default read follows the
// same flag the production planner hands to appendLoadDefaultProjections.
//
// bindLoad only reaches appendLoadDefaultProjections through the legacy
// buildLoad fallback, so the legacy shapes attach a self-referencing foreign
// key (ForeignTbl 0, the metadata the modern binder rejects with
// ErrUnsupportedDML) to route the statement through the legacy builder.
func loadDependentDrive(t *testing.T, ddl string, loadSQL string, selfFk bool) (*planpb.Query, *planpb.TableDef) {
	t.Helper()
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(mock.CurrentContext().GetContext(), dialect.MYSQL, ddl, 1)
	require.NoError(t, err)
	createPlan, err := BuildPlan(mock.CurrentContext(), stmt, false, nil)
	require.NoError(t, err)
	stmt.Free()
	tableDef := createPlan.GetDdl().GetCreateTable().GetTableDef()
	tableDef.TableType = catalog.SystemOrdinaryRel
	tableDef.Cols = append(tableDef.Cols, &planpb.ColDef{
		Name:   catalog.Row_ID,
		Hidden: true,
		Typ:    planpb.Type{Id: int32(types.T_Rowid)},
	})
	if selfFk {
		pidColId := uint64(0)
		pkColId := uint64(0)
		for _, col := range tableDef.Cols {
			if col.Name == "pid" {
				pidColId = col.ColId
			}
			if col.Hidden && col.Typ.AutoIncr {
				pkColId = col.ColId
			}
		}
		require.NotZero(t, pidColId, "the legacy shape needs an observable pid column")
		require.NotZero(t, pkColId, "the hidden fake primary key backs the self reference")
		tableDef.Fkeys = append(tableDef.Fkeys, &planpb.ForeignKeyDef{
			Name:        "r10_sfk",
			Cols:        []uint64{pidColId},
			ForeignTbl:  0,
			ForeignCols: []uint64{pkColId},
		})
	}
	tableName := tableDef.Name
	tableID := uint64(29301 + len(mock.ctxt.tables))
	tableDef.TblId = tableID
	qualifiedName := mockQualifiedTableName("tpch", tableName)
	objRef := &planpb.ObjectRef{SchemaName: "tpch", ObjName: tableName, Obj: int64(tableID)}
	mock.ctxt.tables[tableName] = tableDef
	mock.ctxt.objects[tableName] = objRef
	mock.ctxt.tablesByQualifiedName[qualifiedName] = tableDef
	mock.ctxt.objectsByQualifiedName[qualifiedName] = objRef
	mock.ctxt.legacyTableOwners[tableName] = qualifiedName
	mock.ctxt.legacyObjectOwners[tableName] = qualifiedName
	mock.ctxt.id2name[tableID] = qualifiedName

	logicPlan, err := runOneStmt(mock, t, loadSQL)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query, "a LOAD statement must plan into a query")
	return query, mock.ctxt.tables[tableName]
}

// loadDependentColumnIdx resolves one table column's projection position.
func loadDependentColumnIdx(t *testing.T, tableDef *planpb.TableDef, name string) int {
	t.Helper()
	for i, col := range tableDef.Cols {
		if col.Name == name {
			return i
		}
	}
	t.Fatalf("table %s has no column %s", tableDef.Name, name)
	return -1
}

// loadDependentFillCoalesceCount reports how often an expression tree holds
// the IGNORE fill rewrite: a two-argument coalesce whose first argument is a
// column reference and whose second argument is the int64 implicit default
// zero.
func loadDependentFillCoalesceCount(expr *planpb.Expr) int {
	if expr == nil {
		return 0
	}
	count := 0
	switch impl := expr.Expr.(type) {
	case *planpb.Expr_F:
		if impl.F != nil && impl.F.Func != nil && impl.F.Func.ObjName == "coalesce" &&
			len(impl.F.Args) == 2 {
			second := impl.F.Args[1]
			if impl.F.Args[0].GetCol() != nil && second.GetLit() != nil &&
				!second.GetLit().Isnull && second.GetLit().GetI64Val() == 0 {
				count++
			}
		}
		for _, arg := range impl.F.Args {
			count += loadDependentFillCoalesceCount(arg)
		}
	case *planpb.Expr_List:
		for _, item := range impl.List.List {
			count += loadDependentFillCoalesceCount(item)
		}
	}
	return count
}

// loadDependentProjectionCoalesceCount walks every PROJECT node of the
// planned LOAD and sums the fill rewrites staged at one table column's
// projection position.
func loadDependentProjectionCoalesceCount(q *planpb.Query, colIdx int) int {
	total := 0
	for _, node := range q.Nodes {
		if node == nil || node.NodeType != planpb.Node_PROJECT {
			continue
		}
		if colIdx < len(node.ProjectList) {
			total += loadDependentFillCoalesceCount(node.ProjectList[colIdx])
		}
	}
	return total
}

func loadDependentRequireFill(t *testing.T, q *planpb.Query, tableDef *planpb.TableDef, name string, want int) {
	t.Helper()
	idx := loadDependentColumnIdx(t, tableDef, name)
	require.Equal(t, want, loadDependentProjectionCoalesceCount(q, idx),
		"column %s must read its NOT NULL source through the fill coalesce", name)
}

func loadDependentRequireRaw(t *testing.T, q *planpb.Query, tableDef *planpb.TableDef, name string) {
	t.Helper()
	idx := loadDependentColumnIdx(t, tableDef, name)
	require.Zero(t, loadDependentProjectionCoalesceCount(q, idx),
		"column %s must keep its raw staged read", name)
}

// TestLoadIgnoreNormalizesDependentDefaultReads pins the LOAD leg of the
// IGNORE dependent-read contract. The legacy LOAD builder materializes
// omitted column defaults in appendLoadDefaultProjections before
// buildInsertPlans appends the PRE_INSERT node whose fill adjusts a NULL
// cell of a NOT NULL column, so a default expression that references such a
// column must read the adjusted cell - its reference is rebuilt as
// coalesce(reference, implicit default) - exactly like the modern LOAD bind
// and the INSERT pre-insert projection already do. The modern control keeps
// the established wrap, and without the keyword every dependent read stays
// raw.
func TestLoadIgnoreNormalizesDependentDefaultReads(t *testing.T) {
	t.Run("legacy load wraps the nullable dependent default", func(t *testing.T) {
		query, table := loadDependentDrive(t,
			"create table r10_ld_sfk(a int not null, b int default (a+1), pid int)",
			"load data inline format='csv', data='1' ignore into table r10_ld_sfk fields terminated by ',' (a)",
			true)
		loadDependentRequireFill(t, query, table, "b", 1)
		loadDependentRequireRaw(t, query, table, "a")

		strictQuery, strictTable := loadDependentDrive(t,
			"create table r10_ld_sfk(a int not null, b int default (a+1), pid int)",
			"load data inline format='csv', data='1' into table r10_ld_sfk fields terminated by ',' (a)",
			true)
		loadDependentRequireRaw(t, strictQuery, strictTable, "b")
		loadDependentRequireRaw(t, strictQuery, strictTable, "a")
	})

	t.Run("legacy load wraps the not null dependent default", func(t *testing.T) {
		query, table := loadDependentDrive(t,
			"create table r10_ldn_sfk(a int not null, b int not null default (a+1), pid int)",
			"load data inline format='csv', data='1' ignore into table r10_ldn_sfk fields terminated by ',' (a)",
			true)
		loadDependentRequireFill(t, query, table, "b", 1)
		loadDependentRequireRaw(t, query, table, "a")

		strictQuery, strictTable := loadDependentDrive(t,
			"create table r10_ldn_sfk(a int not null, b int not null default (a+1), pid int)",
			"load data inline format='csv', data='1' into table r10_ldn_sfk fields terminated by ',' (a)",
			true)
		loadDependentRequireRaw(t, strictQuery, strictTable, "b")
	})

	t.Run("legacy load stages the adjusted chain intermediate", func(t *testing.T) {
		query, table := loadDependentDrive(t,
			"create table r10_lcl_sfk(a int not null, c int not null default (a*2), b int default (c+1), pid int)",
			"load data inline format='csv', data='1' ignore into table r10_lcl_sfk fields terminated by ',' (a)",
			true)
		loadDependentRequireFill(t, query, table, "c", 1)
		loadDependentRequireFill(t, query, table, "b", 1)
		loadDependentRequireRaw(t, query, table, "a")

		strictQuery, strictTable := loadDependentDrive(t,
			"create table r10_lcl_sfk(a int not null, c int not null default (a*2), b int default (c+1), pid int)",
			"load data inline format='csv', data='1' into table r10_lcl_sfk fields terminated by ',' (a)",
			true)
		loadDependentRequireRaw(t, strictQuery, strictTable, "c")
		loadDependentRequireRaw(t, strictQuery, strictTable, "b")
	})

	t.Run("modern load keeps the established wrap", func(t *testing.T) {
		query, table := loadDependentDrive(t,
			"create table r10_ld_mod(a int not null, b int default (a+1), pid int)",
			"load data inline format='csv', data='1' ignore into table r10_ld_mod fields terminated by ',' (a)",
			false)
		bIdx := loadDependentColumnIdx(t, table, "b")
		require.NotZero(t, loadDependentProjectionCoalesceCount(query, bIdx),
			"the modern bind must keep wrapping the dependent default read")
		loadDependentRequireRaw(t, query, table, "a")
	})
}
