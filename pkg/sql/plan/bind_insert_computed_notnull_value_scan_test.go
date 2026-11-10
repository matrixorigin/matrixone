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

// computedHasPreInsert reports whether the built node set carries a PRE_INSERT
// node, the only place the INSERT IGNORE NULL adjustment runs before the CHECK
// filter, unique arbitration and the write.
func computedHasPreInsert(nodes []*planpb.Node) bool {
	for _, node := range nodes {
		if node != nil && node.NodeType == planpb.Node_PRE_INSERT {
			return true
		}
	}
	return false
}

// computedValueScanExpr finds the VALUE_SCAN node of an INSERT ... VALUES plan
// and returns the rowset expression bound for one input column at one row.
//
// Tables without a user-declared primary key carry MatrixOne's hidden
// AUTO_INCREMENT fake key, which forces the PRE_INSERT node through the
// hasAutoCol trigger on its own. The discriminating shapes for the computed
// NOT NULL trigger therefore carry an explicit constant-default single-column
// primary key: no auto-increment column, no composite key, no cluster-by, and
// no supplied NOT NULL cell - exactly the shape that otherwise skips the
// node.
func TestInsertIgnorePreInsertForComputedNotNull(t *testing.T) {
	drive := func(t *testing.T, ddl string, supplied map[string]*planpb.Expr, ignore bool) []*planpb.Node {
		tableDef := dependentBuildTable(t, ddl)
		return dependentDriveInsert(t, tableDef, supplied, ignore)
	}

	t.Run("computed not null generated forces the pre-insert node", func(t *testing.T) {
		ddl := "create table t(id int default 1 primary key, a int, g int not null generated always as (a+1) stored, check (g>0))"
		nodes := drive(t, ddl, map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, true)
		require.True(t, computedHasPreInsert(nodes),
			"INSERT IGNORE into a table whose NOT NULL column computes NULL must run the pre-insert adjustment")

		strict := drive(t, ddl, map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, false)
		require.False(t, computedHasPreInsert(strict),
			"a strict insert keeps the write-time NOT NULL rejection")
	})

	t.Run("pk-less tables keep the established node through the hidden fake key", func(t *testing.T) {
		nodes := drive(t,
			"create table t(a int, g int not null generated always as (a+1) stored, check (g>0))",
			map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, true)
		require.True(t, computedHasPreInsert(nodes),
			"the pk-less hidden auto-increment fake key keeps forcing the node")
	})

	t.Run("omitted dynamic not null default forces the pre-insert node", func(t *testing.T) {
		ddl := "create table t(id int default 1 primary key, a int, b int not null default (a+1))"
		nodes := drive(t, ddl, map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, true)
		require.True(t, computedHasPreInsert(nodes),
			"a NOT NULL column with an expression default can derive NULL and must be adjusted before the write")

		strict := drive(t, ddl, map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, false)
		require.False(t, computedHasPreInsert(strict))
	})

	t.Run("constant default and nullable computed columns keep the write-side path", func(t *testing.T) {
		constDefault := drive(t,
			"create table t(id int default 1 primary key, a int, b int not null default 5)",
			map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, true)
		require.False(t, computedHasPreInsert(constDefault),
			"a constant default never yields NULL; no adjustment node is added")

		nullableGenerated := drive(t,
			"create table t(id int default 1 primary key, a int, g int generated always as (a+1) stored)",
			map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, true)
		require.False(t, computedHasPreInsert(nullableGenerated),
			"a nullable generated column may compute NULL legally")
	})

	t.Run("supplied not null column keeps the established trigger", func(t *testing.T) {
		nodes := drive(t,
			"create table t(id int default 1 primary key, a int not null, z int)",
			map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, true)
		require.True(t, computedHasPreInsert(nodes),
			"the round-6 supplied-NULL trigger must stay in force")
	})
}

// computedDriveValueScan plans the DDL, registers the table in the mock
// optimizer context (the expression_default_test.go pattern) and runs the
// INSERT ... VALUES statement through the full planner, returning the
// VALUE_SCAN node with its rowset expressions. The statement text itself
// carries INSERT IGNORE (or its absence), so the wrap decision follows the
// same flag the production planner sets.
func computedDriveValueScan(t *testing.T, ddl string, insertSQL string) *planpb.Node {
	t.Helper()
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(mock.CurrentContext().GetContext(), dialect.MYSQL, ddl, 1)
	require.NoError(t, err)
	createPlan, err := BuildPlan(mock.CurrentContext(), stmt, false, nil)
	require.NoError(t, err)
	stmt.Free()
	tableDef := createPlan.GetDdl().GetCreateTable().GetTableDef()
	// The modern INSERT admission check admits targets by durable relkind.
	// Production resolution derives catalog.SystemOrdinaryRel from the
	// relation's catalog kind; a create-table plan-derived TableDef records
	// the kind only as a properties entry, so the test registration supplies
	// the field directly, as columnLimitFixture does. Without it a plain
	// INSERT would silently fall back to the legacy builder and an INSERT
	// IGNORE target cannot take that fallback at all (build.go gates it on
	// !stmt.IsIgnore()).
	tableDef.TableType = catalog.SystemOrdinaryRel
	// The same guard's downstream node bookkeeping walks the def columns the
	// way production resolved defs look: the engine catalog appends a hidden
	// rowid column after the (possible) fake primary key - the mock's static
	// schemas and columnLimitFixture register the same shape, and the dedup
	// UpdateCtx construction relies on it (it reserves len(Cols)-1 insert
	// slots and skips the trailing rowid).
	tableDef.Cols = append(tableDef.Cols, &planpb.ColDef{
		Name:   catalog.Row_ID,
		Hidden: true,
		Typ:    planpb.Type{Id: int32(types.T_Rowid)},
	})
	tableName := tableDef.Name
	tableID := uint64(29001 + len(mock.ctxt.tables))
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

	logicPlan, err := runOneStmt(mock, t, insertSQL)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	var valueScan *planpb.Node
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_VALUE_SCAN && node.RowsetData != nil {
			valueScan = node
			break
		}
	}
	require.NotNil(t, valueScan, "the plan must carry a VALUE_SCAN node with rowset data")
	require.Len(t, valueScan.RowsetData.Cols, len(valueScan.TableDef.Cols),
		"the rowset keeps one expression column per value-scan input column")
	return valueScan
}

// computedRowsetExprAt returns the rowset expression bound for one input
// column position at one row position.
func computedRowsetExprAt(node *planpb.Node, colPos, row int) *planpb.Expr {
	if colPos >= len(node.RowsetData.Cols) || row >= len(node.RowsetData.Cols[colPos].Data) {
		return nil
	}
	return node.RowsetData.Cols[colPos].Data[row].Expr
}

// computedOneCoalesceWithDefault asserts the expression holds exactly `want`
// coalesce nodes whose first argument is a local column reference and whose
// second argument is the integer implicit default zero.
func computedOneCoalesceWithDefault(t *testing.T, e *planpb.Expr, want int) {
	t.Helper()
	require.NotNil(t, e)
	total := 0
	var found *planpb.Expr
	var walk func(*planpb.Expr)
	walk = func(x *planpb.Expr) {
		if x == nil {
			return
		}
		if f := x.GetF(); f != nil {
			if f.Func != nil && f.Func.ObjName == "coalesce" {
				total++
				found = x
			}
			for _, arg := range f.Args {
				walk(arg)
			}
			return
		}
		if l := x.GetList(); l != nil {
			for _, item := range l.List {
				walk(item)
			}
		}
	}
	walk(e)
	require.Equal(t, want, total, "expression %s must hold %d coalesce fill(s)", e.String(), want)
	if want > 0 {
		f := found.GetF()
		require.Len(t, f.Args, 2)
		require.NotNil(t, f.Args[0].GetCol(), "the reference survives as the first coalesce argument")
		require.Equal(t, int32(0), f.Args[0].GetCol().RelPos, "row-local references keep RelPos 0")
		lit := f.Args[1].GetLit()
		require.NotNil(t, lit, "the implicit integer default zero is the second argument")
		require.EqualValues(t, 0, lit.GetI64Val())
	}
}

// TestInsertIgnoreNormalizesDefaultValueScanExpressions pins that a DEFAULT
// item inside a VALUES row binds the column default into the value-scan row
// image, and under INSERT IGNORE that expression reads its NOT NULL dependency
// through the same implicit-default coalesce the pre-insert projection
// applies: the raw NULL the pre-insert fill would adjust is never what a
// dependent expression observes inside VALUE_SCAN. Constant-default and
// strict shapes stay raw.
func TestInsertIgnoreNormalizesDefaultValueScanExpressions(t *testing.T) {
	t.Run("explicit default reads the adjusted supplied cell", func(t *testing.T) {
		ddl := "create table computed_d(a int not null, b int default (a+1))"
		node := computedDriveValueScan(t, ddl, "insert ignore into computed_d(a,b) values (null, default)")
		computedOneCoalesceWithDefault(t, computedRowsetExprAt(node, 1, 0), 1)

		strict := computedDriveValueScan(t, ddl, "insert into computed_d(a,b) values (null, default)")
		computedOneCoalesceWithDefault(t, computedRowsetExprAt(strict, 1, 0), 0)
	})

	t.Run("mixed rows adjust only the default row", func(t *testing.T) {
		ddl := "create table computed_d(a int not null, b int default (a+1))"
		node := computedDriveValueScan(t, ddl,
			"insert ignore into computed_d(a,b) values (null, default),(null, 0)")
		computedOneCoalesceWithDefault(t, computedRowsetExprAt(node, 1, 0), 1)
		row1 := computedRowsetExprAt(node, 1, 1)
		require.NotNil(t, row1)
		computedOneCoalesceWithDefault(t, row1, 0)
	})

	t.Run("dependency closure materializes adjusted defaults", func(t *testing.T) {
		ddl := "create table computed_c2(a int not null, c int not null default (a*2), b int default (c+1))"
		node := computedDriveValueScan(t, ddl, "insert ignore into computed_c2(a,b) values (null, default)")
		require.Len(t, node.TableDef.Cols, 3,
			"the dependency closure adds the omitted column to the row image")
		closureExpr := computedRowsetExprAt(node, 2, 0)
		require.NotNil(t, closureExpr, "the closure column carries a materialized default expression")
		computedOneCoalesceWithDefault(t, closureExpr, 1)
		bExpr := computedRowsetExprAt(node, 1, 0)
		require.NotNil(t, bExpr)
		require.True(t, dependentCoalesceUnderLit(bExpr, 1),
			"the dependent default (c+1) must read the adjusted dependency through the coalesce fill")
	})

	t.Run("all-default items adjust the dependent default", func(t *testing.T) {
		ddl := "create table computed_ad(a int not null default (nullif(7,9)), b int default (a+1))"
		node := computedDriveValueScan(t, ddl, "insert ignore into computed_ad(a,b) values (default, default)")
		computedOneCoalesceWithDefault(t, computedRowsetExprAt(node, 1, 0), 1)

		strict := computedDriveValueScan(t, ddl, "insert into computed_ad(a,b) values (default, default)")
		computedOneCoalesceWithDefault(t, computedRowsetExprAt(strict, 1, 0), 0)
	})

	t.Run("empty-values statement adjusts the dependent default once per column", func(t *testing.T) {
		ddl := "create table computed_ad(a int not null default (nullif(7,9)), b int default (a+1))"
		node := computedDriveValueScan(t, ddl, "insert ignore into computed_ad values ()")
		computedOneCoalesceWithDefault(t, computedRowsetExprAt(node, 1, 0), 1)
	})

	t.Run("constant-default dependency stays raw", func(t *testing.T) {
		ddl := "create table computed_ak(a int not null default 7, b int default (a+1))"
		// An omitted constant-default column never yields NULL: the
		// dependent default reads it raw under IGNORE.
		omitted := computedDriveValueScan(t, ddl, "insert ignore into computed_ak(b) values (default)")
		computedOneCoalesceWithDefault(t, computedRowsetExprAt(omitted, 1, 0), 0)

		emptyNode := computedDriveValueScan(t, ddl, "insert ignore into computed_ak values ()")
		computedOneCoalesceWithDefault(t, computedRowsetExprAt(emptyNode, 1, 0), 0)

		// A DEFAULT keyword records the column as supplied, and a supplied
		// cell is dynamic by definition (the generic key cannot depend on
		// the value the bound constant carries), so the dependent read keeps
		// one value-identical identity coalesce over the never-null cell.
		supplied := computedDriveValueScan(t, ddl, "insert ignore into computed_ak(a,b) values (default, default)")
		computedOneCoalesceWithDefault(t, computedRowsetExprAt(supplied, 1, 0), 1)
	})
}
