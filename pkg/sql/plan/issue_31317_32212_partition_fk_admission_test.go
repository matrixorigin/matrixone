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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// Issues #31317 / #32212 regression ledger.
//
// Invariant: a foreign key may never bind to or from a partitioned
// relation (MySQL 1506, moerr.ErrForeignKeyOnPartitioned). Every
// admission route has to reject at plan time, with no catalog side
// effect, and normal (non-partitioned) FK semantics must stay unchanged.
//
// Routes covered here:
//   - CREATE child referencing a resolved partitioned parent (HASH and
//     RANGE shapes) via the real buildCreateTable entry point;
//   - ALTER TABLE ... ADD FOREIGN KEY on a normal child referencing a
//     partitioned parent, through the real ALTER dispatcher;
//   - ALTER TABLE ... ADD FOREIGN KEY on a partitioned child referencing
//     a normal parent (previously silently accepted, which left
//     partitioned tables with bound FK definitions in the catalog);
//   - the existing CREATE guard for a partitioned child with inline FK
//     stays rejecting (control);
//   - forward references (foreign_key_checks=0, parent missing) keep
//     their existing admission for normal children, but a partitioned
//     child / rebinding to a partitioned parent is rejected;
//   - normal parent, self-referencing, missing-parent (checks on) and
//     temporary-parent FK semantics stay unchanged (controls).

func newFkIssue31317MockContext(t testing.TB, foreignKeyChecks interface{}) *MockCompilerContext {
	t.Helper()
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	ctx.SetContext(context.Background())
	// getForeignKeyData resolves the FK catalog layout from this relation.
	ctx.tables[catalog.MOForeignKeys] = &TableDef{
		Name: catalog.MOForeignKeys,
		Cols: []*ColDef{
			{Name: "referenced_index_name"},
			{Name: "on_delete_origin"},
			{Name: "on_update_origin"},
		},
	}
	ctx.ResolveVariableFunc = func(name string, _, _ bool) (interface{}, error) {
		if name == "foreign_key_checks" {
			return foreignKeyChecks, nil
		}
		return nil, moerr.NewInternalError(context.Background(), "unexpected variable")
	}
	return ctx
}

// registerFkIssue31317Table registers both the table definition and its
// ObjectRef the way the mock resolves relations for every admission route,
// so dispatcher-level objRef dereferences stay valid for ALTER statements.
func registerFkIssue31317Table(ctx *MockCompilerContext, def *TableDef) {
	ctx.tables[def.Name] = def
	ctx.objects[def.Name] = &ObjectRef{SchemaName: "db", ObjName: def.Name}
}

// newFkIssue31317TableDef builds a resolved relation definition. With
// partitioned set the definition carries the engine-resolved partition
// state that a bound FK must reject.
func newFkIssue31317TableDef(name string, partitioned bool, keyCols ...string) *TableDef {
	intType := plan.Type{Id: int32(types.T_int32)}
	def := &TableDef{
		Name: name,
		Cols: make([]*plan.ColDef, 0, len(keyCols)),
	}
	primary := len(keyCols) == 1
	for i, col := range keyCols {
		def.Cols = append(def.Cols, &plan.ColDef{
			ColId:   uint64(i + 1),
			Name:    col,
			Primary: primary && i == 0,
			NotNull: primary && i == 0,
			Typ:     intType,
			Default: &plan.Default{NullAbility: !(primary && i == 0)},
		})
	}
	if primary {
		def.Pkey = &plan.PrimaryKeyDef{Names: keyCols, PkeyColName: keyCols[0]}
	}
	if partitioned {
		def.Partition = &plan.Partition{
			PartitionDefs: []*plan.PartitionDef{{Def: &plan.Expr{}}},
		}
	}
	return def
}

// runFkIssue31317Stmt drives the real planner entry point: parse the SQL and
// run BuildPlan against the prepared mock catalog, exactly the path every
// CREATE / ALTER statement goes through in production planning.
func runFkIssue31317Stmt(t testing.TB, ctx *MockCompilerContext, sql string) (*Plan, error) {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	if err != nil {
		return nil, err
	}
	defer stmt.Free()
	return BuildPlan(ctx, stmt, false, nil)
}

func assertErrForeignKeyOnPartitioned(t testing.TB, err error, what string) {
	t.Helper()
	require.Error(t, err, "%s must be rejected", what)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrForeignKeyOnPartitioned),
		"%s must reject with 1506 foreign-keys-on-partitioned, got: %v", what, err)
	var me *moerr.Error
	require.ErrorAs(t, err, &me)
	require.Equal(t, uint16(1506), me.MySQLCode(), "%s: unexpected mysql error code", what)
}

// partitionedFkIssue31317Parent returns a primary-keyed parent carrying a
// synthetic resolved partition state: one or two placeholder partition defs.
// These pointer shapes exercise the resolved-Partition admission path only;
// real parsed HASH/RANGE semantics are asserted by the private-port SQL phase.
func partitionedFkIssue31317Parent(shape string) *TableDef {
	def := newFkIssue31317TableDef("pp", true, "a")
	if shape == "two-partition-defs" {
		def.Partition = &plan.Partition{
			PartitionDefs: []*plan.PartitionDef{
				{Def: &plan.Expr{}},
				{Def: &plan.Expr{}},
			},
		}
	}
	return def
}

func TestIssue31317CreateFkToPartitionedParentRejected(t *testing.T) {
	for _, tc := range []struct {
		shape string
	}{
		{shape: "single-partition-def"},
		{shape: "two-partition-defs"},
	} {
		ctx := newFkIssue31317MockContext(t, int64(1))
		registerFkIssue31317Table(ctx, partitionedFkIssue31317Parent(tc.shape))

		// CREATE child -> partitioned parent (issue 31317 path 1, issue 32212).
		_, err := runFkIssue31317Stmt(t, ctx,
			"create table c1(a int, foreign key(a) references pp(a))")
		assertErrForeignKeyOnPartitioned(t, err,
			"CREATE referencing partitioned parent ("+tc.shape+" synthetic state)")
	}
}

func TestIssue31317AlterFkToPartitionedParentRejected(t *testing.T) {
	for _, tc := range []struct {
		shape string
	}{
		{shape: "single-partition-def"},
		{shape: "two-partition-defs"},
	} {
		ctx := newFkIssue31317MockContext(t, int64(1))
		registerFkIssue31317Table(ctx, partitionedFkIssue31317Parent(tc.shape))
		registerFkIssue31317Table(ctx, newFkIssue31317TableDef("c3", false, "a"))

		// ALTER ADD FK on a normal child -> partitioned parent must persist
		// the same rejection through the real ALTER dispatcher.
		_, err := runFkIssue31317Stmt(t, ctx,
			"alter table c3 add constraint fk3 foreign key(a) references pp(a)")
		assertErrForeignKeyOnPartitioned(t, err,
			"ALTER ADD FK referencing partitioned parent ("+tc.shape+" synthetic state)")
	}
}

func TestIssue31317AlterFkOnPartitionedChildRejected(t *testing.T) {
	ctx := newFkIssue31317MockContext(t, int64(1))
	registerFkIssue31317Table(ctx, newFkIssue31317TableDef("par", false, "a"))
	// A partitioned child: the pre-fix bug bound an FK onto this table.
	registerFkIssue31317Table(ctx, newFkIssue31317TableDef("c4", true, "a"))

	_, err := runFkIssue31317Stmt(t, ctx,
		"alter table c4 add constraint fk4 foreign key(a) references par(a)")
	assertErrForeignKeyOnPartitioned(t, err,
		"ALTER ADD FK on partitioned child")
}

func TestIssue31317CreateControlPartitionedChildStillRejected(t *testing.T) {
	ctx := newFkIssue31317MockContext(t, int64(1))
	registerFkIssue31317Table(ctx, newFkIssue31317TableDef("par", false, "a"))

	// Existing clean-1506 contract: CREATE of a partitioned table with an
	// inline FK stays rejected (issue 32212 asymmetry control).
	_, err := runFkIssue31317Stmt(t, ctx,
		"create table part_fk(a int primary key, pid int, foreign key(pid) references par(a)) partition by hash(a) partitions 2")
	assertErrForeignKeyOnPartitioned(t, err,
		"CREATE partitioned child with inline FK")
}

func TestIssue31317ForwardFkOnPartitionedChildRejected(t *testing.T) {
	// foreign_key_checks=0 admits deferred (forward) references to a missing
	// parent; that admission must not extend to a partitioned child, whose
	// catalog row would otherwise bind a partitioned relation to an FK.
	ctx := newFkIssue31317MockContext(t, int64(0))

	_, err := runFkIssue31317Stmt(t, ctx,
		"create table cfwd(a int, foreign key(a) references missing_pp(a)) partition by hash(a) partitions 2")
	assertErrForeignKeyOnPartitioned(t, err,
		"CREATE partitioned child with forward FK")

	// Control: the same deferred reference on a normal child keeps its
	// existing admission (no partition clause, unresolved parent semantics).
	plan1, err := runFkIssue31317Stmt(t, ctx,
		"create table cfwd_normal(a int, foreign key(a) references missing_pp(a))")
	require.NoError(t, err, "forward FK on normal child must stay admissible")
	createTable := plan1.GetDdl().GetCreateTable()
	require.NotNil(t, createTable)
	require.NotEmpty(t, createTable.UpdateFkSqls,
		"normal forward FK must keep writing its deferred catalog row")
}

func TestIssue31317ForwardRebindToPartitionedParentRejected(t *testing.T) {
	ctx := newFkIssue31317MockContext(t, int64(1))
	registerFkIssue31317Table(ctx, newFkIssue31317TableDef("child", false, "parent_id"))
	fkDefs := []*FkReferDef{{
		Db: "db", Tbl: "child", Col: "parent_id", ReferCol: "a",
		OnDelete: "NO_ACTION", OnUpdate: "NO_ACTION",
	}}

	// Deferred reference rebinding to a partitioned parent cannot bind.
	partitionedParent := newFkIssue31317TableDef("pp", true, "a")
	_, err := buildFkDataOfForwardRefer(ctx, "fk_child_parent", fkDefs,
		&plan.CreateTable{Database: "db", TableDef: partitionedParent})
	assertErrForeignKeyOnPartitioned(t, err,
		"forward FK rebind onto partitioned parent")

	// Control: rebind to a normal parent keeps succeeding.
	normalParent := newFkIssue31317TableDef("parent", false, "a")
	data, err := buildFkDataOfForwardRefer(ctx, "fk_child_parent", fkDefs,
		&plan.CreateTable{Database: "db", TableDef: normalParent})
	require.NoError(t, err, "forward FK rebind onto normal parent must keep binding")
	require.NotNil(t, data)
	require.Equal(t, "PRIMARY", data.Def.ReferencedIndexName)
}

func TestIssue31317NormalFkControlsPreserved(t *testing.T) {
	ctx := newFkIssue31317MockContext(t, int64(1))
	registerFkIssue31317Table(ctx, newFkIssue31317TableDef("par", false, "a"))

	// Normal child -> normal parent keeps binding.
	p, err := runFkIssue31317Stmt(t, ctx,
		"create table c1n(a int, foreign key(a) references par(a))")
	require.NoError(t, err, "normal FK must keep binding")
	require.Len(t, p.GetDdl().GetCreateTable().GetTableDef().GetFkeys(), 1)
	require.NotEmpty(t, p.GetDdl().GetCreateTable().UpdateFkSqls)

	// Self-referencing normal FK keeps binding.
	_, err = runFkIssue31317Stmt(t, ctx,
		"create table s1(a int primary key, b int, foreign key(b) references s1(a))")
	require.NoError(t, err, "normal self-referencing FK must keep binding")

	// Missing parent with foreign_key_checks=1 keeps its NoSuchTable error.
	_, err = runFkIssue31317Stmt(t, ctx,
		"create table cmiss(a int, foreign key(a) references nopro(a))")
	require.Error(t, err, "missing parent with checks on must stay rejected")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable),
		"missing parent must keep the NoSuchTable error, got: %v", err)

	// The ALTER route still forwards temporary-parent parents to their
	// existing dedicated rejection.
	ctx2 := newFkIssue31317MockContext(t, int64(1))
	tempParent := newFkIssue31317TableDef("tp", false, "a")
	tempParent.IsTemporary = true
	registerFkIssue31317Table(ctx2, tempParent)
	registerFkIssue31317Table(ctx2, newFkIssue31317TableDef("c3", false, "a"))
	_, err = runFkIssue31317Stmt(t, ctx2,
		"alter table c3 add constraint fk3 foreign key(a) references tp(a)")
	require.Error(t, err, "temporary parent must stay rejected")
	require.False(t, moerr.IsMoErrCode(err, moerr.ErrForeignKeyOnPartitioned),
		"temporary parent must not be mislabeled as 1506, got: %v", err)
}

// TestIssue31317FlagOnlyPartitionIdentityRejected closes the canonical
// flag-only identity: the authored CREATE records the logical
// features.Partitioned flag on the table def before the FK pass, and resolved
// relations fill TableDef.Partition and FeatureFlag from independent engine
// state (the engine dispatches the combined partition relation on the flag,
// not the pointer). A partitioned def that carries only the flag must be
// rejected by every FK binding route; other FeatureFlag bits are not logical
// partitioning and must stay admissible.
func TestIssue31317FlagOnlyPartitionIdentityRejected(t *testing.T) {
	flagOnly := func(name string, keyCols ...string) *TableDef {
		def := newFkIssue31317TableDef(name, false, keyCols...)
		def.FeatureFlag = features.Partitioned
		return def
	}

	// Flag-only parent rejected by CREATE (issue 31317 path 1, issue 32212).
	ctx := newFkIssue31317MockContext(t, int64(1))
	registerFkIssue31317Table(ctx, flagOnly("pfo", "a"))
	_, err := runFkIssue31317Stmt(t, ctx,
		"create table cfo(a int, foreign key(a) references pfo(a))")
	assertErrForeignKeyOnPartitioned(t, err,
		"CREATE referencing flag-only partitioned parent")

	// Flag-only parent rejected through the direct (in-place) ALTER route.
	registerFkIssue31317Table(ctx, newFkIssue31317TableDef("c3", false, "a"))
	_, err = runFkIssue31317Stmt(t, ctx,
		"alter table c3 add constraint fk3 foreign key(a) references pfo(a)")
	assertErrForeignKeyOnPartitioned(t, err,
		"ALTER ADD FK referencing flag-only partitioned parent")

	// Flag-only child rejected through the COPY ALTER route: the FK option
	// precedes the copy-triggering add-column option, so the statement plans
	// through buildAlterTableCopy/addAlterCopyForeignKeys with the final
	// definition.
	ctx2 := newFkIssue31317MockContext(t, int64(1))
	registerFkIssue31317Table(ctx2, newFkIssue31317TableDef("par", false, "a"))
	registerFkIssue31317Table(ctx2, flagOnly("c4", "a"))
	_, err = runFkIssue31317Stmt(t, ctx2,
		"alter table c4 add constraint fk4 foreign key(a) references par(a), add column pad int")
	assertErrForeignKeyOnPartitioned(t, err,
		"COPY ALTER ADD FK on flag-only partitioned child")

	// Deferred rebinding must reject a flag-only side on either end.
	ctx3 := newFkIssue31317MockContext(t, int64(1))
	registerFkIssue31317Table(ctx3, newFkIssue31317TableDef("child", false, "parent_id"))
	fkDefs := []*FkReferDef{{
		Db: "db", Tbl: "child", Col: "parent_id", ReferCol: "a",
		OnDelete: "NO_ACTION", OnUpdate: "NO_ACTION",
	}}
	_, err = buildFkDataOfForwardRefer(ctx3, "fk_child_parent", fkDefs,
		&plan.CreateTable{Database: "db", TableDef: flagOnly("pfo", "a")})
	assertErrForeignKeyOnPartitioned(t, err,
		"forward FK rebind onto flag-only partitioned parent")

	normalParent := newFkIssue31317TableDef("parent", false, "a")
	_, err = BindForwardForeignKey(context.Background(), "fk_child_parent",
		fkDefs, flagOnly("child", "parent_id"), normalParent)
	assertErrForeignKeyOnPartitioned(t, err,
		"forward FK rebind from flag-only partitioned child")

	// Control: the physical partition bit and other feature flags are not
	// logical partitioning and must keep binding normally.
	ctx4 := newFkIssue31317MockContext(t, int64(1))
	nonLogical := newFkIssue31317TableDef("pphys", false, "a")
	nonLogical.FeatureFlag = features.Partition | features.IndexTable
	registerFkIssue31317Table(ctx4, nonLogical)
	_, err = runFkIssue31317Stmt(t, ctx4,
		"create table cphys(a int, foreign key(a) references pphys(a))")
	require.NoError(t, err,
		"physical partition and index-table feature bits must not be mislabeled as logical partitioning")
}

// TestIssue31317AlterAdmissionRouteControls pins both real ALTER admission
// routes with positive route assertions: a plain ADD FOREIGN KEY plans
// through the direct in-place builder (no COPY table definition), while the
// same foreign key ahead of a copy-triggering add-column option plans through
// the COPY builder (a COPY table definition is planned). Both routes carry
// the bound FK's catalog update SQL, and the partition guards above must
// reject through each route's own final definition.
func TestIssue31317AlterAdmissionRouteControls(t *testing.T) {
	ctx := newFkIssue31317MockContext(t, int64(1))
	registerFkIssue31317Table(ctx, newFkIssue31317TableDef("par", false, "a"))
	registerFkIssue31317Table(ctx, newFkIssue31317TableDef("nr", false, "a"))

	// Direct in-place route.
	p, err := runFkIssue31317Stmt(t, ctx,
		"alter table nr add constraint fkd foreign key(a) references par(a)")
	require.NoError(t, err, "in-place ADD FK on a normal child must keep binding")
	alter := p.GetDdl().GetAlterTable()
	require.NotNil(t, alter)
	require.Nil(t, alter.GetCopyTableDef(),
		"plain ADD FOREIGN KEY must plan through the direct in-place route")
	require.NotEmpty(t, alter.GetUpdateFkSqls(),
		"the direct route must keep the FK catalog update SQL")

	// COPY route: FK option first, copy-triggering add-column last.
	p2, err := runFkIssue31317Stmt(t, ctx,
		"alter table nr add constraint fkc foreign key(a) references par(a), add column pad int")
	require.NoError(t, err, "COPY ADD FK on a normal child must keep binding")
	alter2 := p2.GetDdl().GetAlterTable()
	require.NotNil(t, alter2)
	require.NotNil(t, alter2.GetCopyTableDef(),
		"FK ahead of a copy-triggering option must plan through the COPY route")
	require.NotEmpty(t, alter2.GetUpdateFkSqls(),
		"the COPY route must keep the FK catalog update SQL")
}
