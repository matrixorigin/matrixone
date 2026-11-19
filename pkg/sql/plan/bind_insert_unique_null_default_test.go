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
	"github.com/stretchr/testify/require"
)

// uniqueNullDefaultDrive plans the insert row-image projection over the DDL's
// real catalog shape and returns the per-index unique-skip decision together
// with the built node set: the skip is the single plan-time input the base
// unique machinery consumes for dedup arbitration, lock keys and the
// index-table writes of this statement.
func uniqueNullDefaultDrive(
	t *testing.T,
	ddl string,
	supplied map[string]*planpb.Expr,
	ignore bool,
) ([]bool, []*planpb.Node) {
	t.Helper()
	tableDef := dependentBuildTable(t, ddl)
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	builder := NewQueryBuilder(planpb.Query_INSERT, ctx, false, true)
	bindCtx := NewBindContext(builder, nil)
	child := builder.appendNode(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}, bindCtx)
	objRef := &planpb.ObjectRef{SchemaName: "db", ObjName: tableDef.Name, Obj: 1}
	_, _, skipUniqueIdx, _, err := builder.appendNodesForInsertStmt(bindCtx, child, tableDef, objRef, supplied, ignore)
	require.NoError(t, err)
	require.NotEmpty(t, tableDef.Indexes, "the DDL must keep its unique index")
	require.Len(t, skipUniqueIdx, len(tableDef.Indexes))
	return skipUniqueIdx, builder.qry.Nodes
}

func uniqueNullDefaultHasPreInsert(nodes []*planpb.Node) bool {
	for _, node := range nodes {
		if node != nil && node.NodeType == planpb.Node_PRE_INSERT {
			return true
		}
	}
	return false
}

// TestInsertIgnoreUniqueMaintenanceSurvivesFilledNullDefault pins the unique
// skip decision for the review's counterexample family: an omitted NOT NULL
// column whose default folds to a NULL literal is rewritten to the column
// type's implicit default by the pre-insert IGNORE adjustment, ahead of every
// unique-key consumer, so it must not qualify its unique index as
// permanently NULL. Nullable parts keep their NULL, strict statements run no
// adjustment, and supplied cells keep their arbitration, so each of those
// keeps the historical decision.
func TestInsertIgnoreUniqueMaintenanceSurvivesFilledNullDefault(t *testing.T) {
	counterexample := "create table t(id int primary key, u bigint not null default (abs(null)), unique key uk(u))"

	t.Run("fillable null default keeps unique maintenance", func(t *testing.T) {
		skips, nodes := uniqueNullDefaultDrive(t, counterexample,
			map[string]*planpb.Expr{"id": makePlan2Int32ConstExprWithType(1)}, true)
		require.False(t, skips[0],
			"the pre-insert adjustment rewrites u to the implicit default before unique arbitration; keeping the skip drops both the arbitration and the uk index writes for entries the write produces")
		require.True(t, uniqueNullDefaultHasPreInsert(nodes),
			"the counterexample must keep the pre-insert adjustment node")
	})

	t.Run("nullable default keeps the historical skip", func(t *testing.T) {
		skips, _ := uniqueNullDefaultDrive(t,
			"create table t(id int primary key, u bigint default (abs(null)), unique key uk(u))",
			map[string]*planpb.Expr{"id": makePlan2Int32ConstExprWithType(1)}, true)
		require.True(t, skips[0],
			"a nullable part stores NULL under IGNORE and never reaches a fill; the historical skip stays")
	})

	t.Run("no default keeps the historical skip", func(t *testing.T) {
		// The base machinery also leaves a nullable acknowledged-NULL column
		// (a plain DEFAULT NULL) skipped; a NOT NULL column without any
		// default would reject the statement at bind time and is out of scope.
		skips, _ := uniqueNullDefaultDrive(t,
			"create table t(id int primary key, u bigint default null, unique key uk(u))",
			map[string]*planpb.Expr{"id": makePlan2Int32ConstExprWithType(1)}, true)
		require.True(t, skips[0])
	})

	t.Run("strict insert keeps the historical skip", func(t *testing.T) {
		skips, _ := uniqueNullDefaultDrive(t, counterexample,
			map[string]*planpb.Expr{"id": makePlan2Int32ConstExprWithType(1)}, false)
		require.True(t, skips[0],
			"without IGNORE no adjustment runs; the statement aborts at the runtime NOT NULL check and the historical decision stays")
	})

	t.Run("supplied cells keep their arbitration", func(t *testing.T) {
		skips, _ := uniqueNullDefaultDrive(t, counterexample,
			map[string]*planpb.Expr{
				"id": makePlan2Int32ConstExprWithType(1),
				"u":  makePlan2Int64ConstExprWithType(5),
			}, true)
		require.False(t, skips[0])
	})

	t.Run("string family default is filled too", func(t *testing.T) {
		skips, _ := uniqueNullDefaultDrive(t,
			"create table t(id int primary key, u varchar(32) not null default (nullif('a','a')), unique key uk(u))",
			map[string]*planpb.Expr{"id": makePlan2Int32ConstExprWithType(1)}, true)
		require.False(t, skips[0],
			"every fillable family - strings included - rewrites the omitted NULL-valued default")
	})

	t.Run("per-index independence", func(t *testing.T) {
		ddl := "create table t(id int primary key, u bigint not null default (abs(null)), w bigint default (abs(null)), unique key uk1(u), unique key uk2(w))"
		tableDef := dependentBuildTable(t, ddl)
		require.Len(t, tableDef.Indexes, 2)
		skips, _ := uniqueNullDefaultDrive(t, ddl,
			map[string]*planpb.Expr{"id": makePlan2Int32ConstExprWithType(1)}, true)
		require.Len(t, skips, 2)
		require.False(t, skips[0], "uk1's only part is filled to the implicit default")
		require.True(t, skips[1], "uk2's nullable part stays NULL and keeps the historical skip")
	})
}

// TestInsertIgnoreFilledNullDefaultKeepsUniqueArbitrationAndIndexWorks pins
// the counterexample end to end through the statement planner: the INSERT
// IGNORE plan must arbitrate uk(u) on the adjusted values and must emit the
// uk index-table write context, exactly like a regular unique key on a
// never-NULL column. A skip that trusts the raw NULL default silently drops
// both, storing duplicates with no index entries and no conflict error.
func TestInsertIgnoreFilledNullDefaultKeepsUniqueArbitrationAndIndexWorks(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	ddl := "create table uniq_null_default(id int primary key, u bigint not null default (abs(null)), unique key uk(u))"
	table := dependentBuildTable(t, ddl)
	// The modern INSERT admission check admits targets by durable relkind,
	// which a production resolution derives from the catalog; a create-plan
	// definition records the kind only as a property, so the registration
	// supplies the field directly (the pattern of the computed-notnull
	// value-scan test).
	table.TableType = catalog.SystemOrdinaryRel
	// Match the production catalog shape the dedup UpdateCtx construction
	// relies on: a hidden rowid after the (real) primary key, reserving
	// len(Cols)-1 insert slots and skipping the trailing rowid.
	table.Cols = append(table.Cols, &planpb.ColDef{
		Name:   catalog.Row_ID,
		Hidden: true,
		Typ:    planpb.Type{Id: int32(types.T_Rowid)},
	})

	// The unique key rides dept's registered single-column unique index
	// table, retyped to the counterexample (the mock-catalog pattern of the
	// irregular-maintenance tests).
	unique := *mock.ctxt.tables["dept"].Indexes[0]
	require.True(t, unique.Unique, "dept's first index must be the registered unique index")
	unique.IndexName = "uk"
	unique.Parts = []string{"u"}
	table.Indexes = []*planpb.IndexDef{&unique}
	idxTable := mock.ctxt.tables[unique.IndexTableName]
	require.NotNil(t, idxTable, "the mock must keep dept's unique index table")
	idxTable.Cols[idxTable.Name2ColIndex[catalog.IndexTableIndexColName]].Typ =
		planpb.Type{Id: int32(types.T_int64), Width: 64}
	idxTable.Cols[idxTable.Name2ColIndex[catalog.IndexTablePrimaryColName]].Typ =
		planpb.Type{Id: int32(types.T_int32), Width: 32}

	// Register the fresh table the way the mock catalog itself does, in the
	// schema that also owns the unique index table.
	tableID := uint64(29001 + len(mock.ctxt.tables))
	table.TblId = tableID
	qualifiedName := mockQualifiedTableName("constraint_test", table.Name)
	objRef := &planpb.ObjectRef{SchemaName: "constraint_test", ObjName: table.Name, Obj: int64(tableID)}
	mock.ctxt.tables[table.Name] = table
	mock.ctxt.objects[table.Name] = objRef
	mock.ctxt.tablesByQualifiedName[qualifiedName] = table
	mock.ctxt.objectsByQualifiedName[qualifiedName] = objRef
	mock.ctxt.legacyTableOwners[table.Name] = qualifiedName
	mock.ctxt.legacyObjectOwners[table.Name] = qualifiedName
	mock.ctxt.id2name[tableID] = qualifiedName

	p, err := runOneStmt(mock, t,
		"insert ignore into constraint_test.uniq_null_default(id) values (1),(2)")
	require.NoError(t, err)
	q := p.GetQuery()
	assertEverySinkStepHasConsumer(t, q)

	// For a real-PK table with one maintained unique key, INSERT IGNORE
	// arbitrates through the multi-dedup arbiter (each unique constraint
	// contributes its key names); a unique-only variant would use the legacy
	// IGNORE dedup join. The counterexample must participate in the first
	// shape after the fix, and no raw-NULL skip may silently drop either one.
	ukArbitration := false
	for _, n := range q.Nodes {
		if n == nil {
			continue
		}
		if n.NodeType == planpb.Node_JOIN && n.JoinType == planpb.Node_DEDUP &&
			n.OnDuplicateAction == planpb.Node_IGNORE && n.DedupColName == "u" {
			ukArbitration = true
		}
		if n.GetPreInsertUkCtx().GetInsertIgnoreMultiDedup() {
			for _, key := range n.PreInsertUkCtx.KeyNames {
				// formatDedupColumnName leaves a single-column key bare; a
				// key whose diagnostics are recorded is reported by the
				// index name instead.
				if key == "u" || key == "uk" {
					ukArbitration = true
				}
			}
		}
	}
	require.True(t, ukArbitration,
		"uk must arbitrate the two rows the adjustment fills to 0 inside the statement")

	idxWrites := 0
	for _, n := range q.Nodes {
		for _, uc := range n.UpdateCtxList {
			if uc != nil && uc.TableDef != nil && uc.TableDef.Name == unique.IndexTableName {
				idxWrites++
			}
		}
	}
	require.Greater(t, idxWrites, 0,
		"the uk index table must receive a write context so the adjusted entries are maintained")
}
