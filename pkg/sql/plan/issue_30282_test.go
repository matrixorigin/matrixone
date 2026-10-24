// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestIssue30282SetNullOnNotNullColumnDDL pins MySQL error 1830: a foreign
// key declared with ON DELETE or ON UPDATE SET NULL must not name a NOT NULL
// child column. Before the fix the DDL was accepted and the DELETE action
// silently left an orphan child row while the UPDATE action failed at first
// parent update.
func TestIssue30282SetNullOnNotNullColumnDDL(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))

	shouldError := []struct {
		sql    string
		col    string
		fkName string
	}{
		// explicit NOT NULL + ON DELETE SET NULL (issue report)
		{
			"create table t30282_del (id int primary key, pid int not null, key idx_pid (pid), " +
				"constraint fk_pid_del foreign key (pid) references nation (n_nationkey) on delete set null)",
			"pid", "fk_pid_del",
		},
		// explicit NOT NULL + ON UPDATE SET NULL (issue comment)
		{
			"create table t30282_upd (id int primary key, pid int not null, key idx_pid (pid), " +
				"constraint fk_pid_upd foreign key (pid) references nation (n_nationkey) on update set null)",
			"pid", "fk_pid_upd",
		},
		// primary-key implied NOT NULL must also be rejected
		{
			"create table t30282_pk (pid int primary key, " +
				"constraint fk_pid_pk foreign key (pid) references nation (n_nationkey) on delete set null)",
			"pid", "fk_pid_pk",
		},
		// self-referencing foreign key over a NOT NULL column
		{
			"create table t30282_self (id int primary key, pid int not null, key idx_pid (pid), " +
				"constraint fk_pid_self foreign key (pid) references t30282_self (id) on delete set null)",
			"pid", "fk_pid_self",
		},
	}
	for _, tc := range shouldError {
		_, err := runOneStmt(mock, t, tc.sql)
		require.Error(t, err, tc.sql)
		var me *moerr.Error
		require.ErrorAs(t, err, &me, tc.sql)
		require.Equal(t, uint16(1830), me.MySQLCode(), tc.sql)
		require.Contains(t, me.Error(), "cannot be NOT NULL: needed in a foreign key constraint", tc.sql)
		require.Contains(t, me.Error(), "'"+tc.col+"'", tc.sql)
		require.Contains(t, me.Error(), "'"+tc.fkName+"'", tc.sql)
	}

	runTestShouldPass(mock, t, []string{
		// nullable child column with both SET NULL actions stays legal
		"create table t30282_ok (id int primary key, pid int, key idx_pid (pid), " +
			"constraint fk_pid_ok foreign key (pid) references nation (n_nationkey) " +
			"on delete set null on update set null)",
		// ALTER ... ADD FOREIGN KEY over a nullable column stays legal
		"alter table emp add constraint fk30282_emp foreign key (deptno) references dept (deptno) on delete set null",
	}, false, false)
}

// TestCheckFkSetNullColumnsRequireNullable covers the shared validation
// helper directly, including the three not-null markers (explicit flag,
// Default.NullAbility, Typ.NotNullable) that different construction paths
// populate.
func TestCheckFkSetNullColumnsRequireNullable(t *testing.T) {
	cols := []*plan.ColDef{
		{Name: "a", Default: &plan.Default{NullAbility: true}},                     // nullable
		{Name: "b", OriginName: "B_B", Default: &plan.Default{NullAbility: false}}, // explicit NOT NULL
		{Name: "c", NotNull: true},                                                 // flag-only NOT NULL
		{Name: "d", Typ: plan.Type{Id: 8, NotNullable: true}},                      // type-only NOT NULL
		{Name: "e"}, // nil default, nullable
	}
	gctx := context.Background()

	// RESTRICT/CASCADE actions must not trigger the check.
	require.NoError(t, checkFkSetNullColumnsRequireNullable(gctx, cols,
		&plan.ForeignKeyDef{Name: "fk_r", OnDelete: plan.ForeignKeyDef_RESTRICT, OnUpdate: plan.ForeignKeyDef_CASCADE},
		[]string{"b", "c", "d"}))
	// nil fkDef is tolerated defensively.
	require.NoError(t, checkFkSetNullColumnsRequireNullable(gctx, cols, nil, []string{"b"}))
	// nullable child columns pass, unknown names defer to the accurate
	// "column does not exist" error produced elsewhere.
	require.NoError(t, checkFkSetNullColumnsRequireNullable(gctx, cols,
		&plan.ForeignKeyDef{Name: "fk_a", OnDelete: plan.ForeignKeyDef_SET_NULL},
		[]string{"a", "e", "no_such_col"}))

	for _, action := range []plan.ForeignKeyDef_RefAction{
		plan.ForeignKeyDef_SET_NULL,
	} {
		fk := &plan.ForeignKeyDef{Name: "fk_d", OnDelete: action}
		err := checkFkSetNullColumnsRequireNullable(gctx, cols, fk, []string{"b"})
		require.Error(t, err)
		var me *moerr.Error
		require.ErrorAs(t, err, &me)
		require.Equal(t, uint16(1830), me.MySQLCode())
		// the error keeps the user's declared spelling of the column name
		require.Contains(t, me.Error(), "'B_B' cannot be NOT NULL: needed in a foreign key constraint 'fk_d' SET NULL")

		err = checkFkSetNullColumnsRequireNullable(gctx, cols, fk, []string{"c"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "'c' cannot be NOT NULL")

		err = checkFkSetNullColumnsRequireNullable(gctx, cols, fk, []string{"d"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "'d' cannot be NOT NULL")

		err = checkFkSetNullColumnsRequireNullable(gctx, cols,
			&plan.ForeignKeyDef{Name: "fk_u", OnUpdate: action}, []string{"c"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "'c' cannot be NOT NULL")
	}
}

// Sanity: a plain (non-replay) CREATE TABLE with the same definition shape
// reuses the same process construction as production DDL and must be caught.
func TestIssue30282NotSkippedForPlainDDL(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	_, err := runOneStmt(mock, t,
		"create table t30282_guard (id int primary key, pid int not null, key idx_pid (pid), "+
			"constraint fk_guard foreign key (pid) references nation (n_nationkey) on update set null)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot be NOT NULL")
}

// Review follow-up (#30282): unresolved declarations admitted under
// foreign_key_checks=0 must face the same nullability rule when they bind.
// BindForwardForeignKey is the shared binding point of the planner preview and
// the executor catalog path, so pinning it there covers both entry points.
func TestIssue30282ForwardBindingRejectsSetNullOnNotNullColumn(t *testing.T) {
	child := &plan.TableDef{
		Name: "c30282_fw",
		Cols: []*plan.ColDef{
			{
				ColId:   1,
				Name:    "pid",
				NotNull: true,
				Typ:     plan.Type{Id: int32(types.T_int32)},
				Default: &plan.Default{NullAbility: false},
			},
		},
	}
	parent := &plan.TableDef{
		Name: "p30282_fw",
		Cols: []*plan.ColDef{
			{
				ColId:   9,
				Name:    "id",
				Primary: true,
				Typ:     plan.Type{Id: int32(types.T_int32)},
				Default: &plan.Default{},
			},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
	}
	fkDefs := []*FkReferDef{{
		Db: "d", Tbl: "c30282_fw", Col: "pid", ReferCol: "id",
		OnDelete: "set null", OnUpdate: "restrict",
	}}

	_, err := BindForwardForeignKey(context.Background(), "fk_fw", fkDefs, child, parent)
	require.Error(t, err, "forward binding over a NOT NULL child column must be rejected")
	var me *moerr.Error
	require.ErrorAs(t, err, &me)
	require.Equal(t, uint16(1830), me.MySQLCode())

	// Control: a nullable child column binds cleanly.
	nullableChild := DeepCopyTableDef(child, true)
	nullableChild.Cols[0].NotNull = false
	nullableChild.Cols[0].Default.NullAbility = true
	_, err = BindForwardForeignKey(context.Background(), "fk_fw", fkDefs, nullableChild, parent)
	require.NoError(t, err)
}

// TestCheckAlterSurvivingFkSetNull drives the surviving-constraint guard
// directly: only a column whose NOT NULL status is introduced by the ALTER
// itself may raise 1830, and only when it actually backs a SET NULL action.
func TestCheckAlterSurvivingFkSetNull(t *testing.T) {
	mkCol := func(id uint64, name string, notNull bool) *plan.ColDef {
		return &plan.ColDef{
			ColId:   id,
			Name:    name,
			NotNull: notNull,
			Typ:     plan.Type{Id: int32(types.T_int32)},
			Default: &plan.Default{NullAbility: !notNull},
		}
	}
	oldDef := &plan.TableDef{Name: "c", Cols: []*plan.ColDef{mkCol(1, "pid", false), mkCol(2, "v", false)}}
	cascadeFk := &plan.ForeignKeyDef{Name: "fk_c", Cols: []uint64{1}, OnDelete: plan.ForeignKeyDef_CASCADE, OnUpdate: plan.ForeignKeyDef_RESTRICT}
	setNullFk := &plan.ForeignKeyDef{Name: "fk_s", Cols: []uint64{1}, OnDelete: plan.ForeignKeyDef_SET_NULL}
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	cctx := mock.CurrentContext()

	newTightened := &plan.TableDef{Name: "c", Cols: []*plan.ColDef{mkCol(1, "pid", true), mkCol(2, "v", false)}}
	// tightened column behind a SET NULL constraint -> 1830
	err := checkAlterSurvivingFkSetNull(cctx, "c", nil, oldDef, &plan.TableDef{
		Name: "c", Cols: newTightened.Cols,
		Fkeys: []*plan.ForeignKeyDef{setNullFk},
	})
	require.Error(t, err)
	var me *moerr.Error
	require.ErrorAs(t, err, &me)
	require.Equal(t, uint16(1830), me.MySQLCode())

	// ON DELETE CASCADE over the same tightened column is legal
	require.NoError(t, checkAlterSurvivingFkSetNull(cctx, "c", nil, oldDef, &plan.TableDef{
		Name: "c", Cols: newTightened.Cols, Fkeys: []*plan.ForeignKeyDef{cascadeFk},
	}))

	// column already NOT NULL before the ALTER keeps its (previously valid) constraint
	oldDef2 := &plan.TableDef{Name: "c", Cols: []*plan.ColDef{mkCol(1, "pid", true), mkCol(2, "v", false)}}
	require.NoError(t, checkAlterSurvivingFkSetNull(cctx, "c", nil, oldDef2, &plan.TableDef{
		Name: "c", Cols: newTightened.Cols, Fkeys: []*plan.ForeignKeyDef{setNullFk},
	}))

	// the constraint dropped by the same statement is ignored
	require.NoError(t, checkAlterSurvivingFkSetNull(cctx, "c", map[string]bool{"fk_s": true}, oldDef, &plan.TableDef{
		Name: "c", Cols: newTightened.Cols, Fkeys: []*plan.ForeignKeyDef{setNullFk},
	}))

	// no tightening at all -> nothing to complain about
	require.NoError(t, checkAlterSurvivingFkSetNull(cctx, "c", nil, oldDef, &plan.TableDef{
		Name: "c", Cols: oldDef.Cols, Fkeys: []*plan.ForeignKeyDef{setNullFk},
	}))
}

// End-to-end strategy coverage (#30282 review): the child carries a SURVIVING
// ON DELETE SET NULL constraint over a nullable column; every ALTER form that
// tightens that column to NOT NULL must be rejected with 1830, regardless of
// the constraint's clause position or the ALTER strategy finally chosen.
func TestIssue30282AlterSurvivingSetNullTightening(t *testing.T) {
	seed := func(t *testing.T, withPkey bool) *MockOptimizer {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		name := "t30282_alt"
		mock.ctxt.objects[name] = &ObjectRef{
			SchemaName: "constraint_test",
			ObjName:    name,
		}
		def := &TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Name:      name,
			Cols: []*plan.ColDef{
				{ColId: 0, Name: "id", Primary: withPkey,
					Typ: plan.Type{Id: int32(types.T_int32), Width: 32, Scale: -1}, Default: &plan.Default{NullAbility: !withPkey}},
				{ColId: 1, Name: "pid",
					Typ: plan.Type{Id: int32(types.T_int32), Width: 32, Scale: -1}, Default: &plan.Default{NullAbility: true}},
			},
			Fkeys: []*plan.ForeignKeyDef{{
				Name:        "fk_alt_setnull",
				Cols:        []uint64{1},
				ForeignTbl:  0, // durable self-reference marker
				ForeignCols: []uint64{0},
				OnDelete:    plan.ForeignKeyDef_SET_NULL,
				OnUpdate:    plan.ForeignKeyDef_RESTRICT,
			}},
		}
		if withPkey {
			def.Cols[0].NotNull = true
			def.Pkey = &plan.PrimaryKeyDef{PkeyColName: "id", Cols: []uint64{0}, Names: []string{"id"}}
		} else {
			// A table without an explicit primary key carries the fake-pk row
			// (stripped from SELECT plans by the mock resolver, like the real one).
			def.Cols = append(def.Cols, &plan.ColDef{
				ColId: 2, Name: catalog.FakePrimaryKeyColName, Primary: true,
				Typ: plan.Type{Id: int32(types.T_Rowid), Width: types.T_Rowid.ToType().Width},
			})
			def.Pkey = &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName}
		}
		mock.ctxt.tables[name] = def
		return mock
	}

	shouldError := []struct {
		sql      string
		withPkey bool
	}{
		{"alter table t30282_alt modify pid int not null", true},
		{"alter table t30282_alt change pid pid int not null", true},
		{"alter table t30282_alt add primary key (pid)", false},
	}
	for _, tc := range shouldError {
		mock := seed(t, tc.withPkey)
		_, err := runOneStmt(mock, t, tc.sql)
		require.Error(t, err, tc.sql)
		var me *moerr.Error
		require.ErrorAs(t, err, &me, tc.sql)
		require.Equal(t, uint16(1830), me.MySQLCode(), tc.sql)
		require.Contains(t, me.Error(), "'fk_alt_setnull'", tc.sql)
	}

	// CASCADE over the same tightened column stays legal
	cascade := seed(t, true)
	cascade.ctxt.tables["t30282_alt"].Fkeys[0].OnDelete = plan.ForeignKeyDef_CASCADE
	_, err := runOneStmt(cascade, t, "alter table t30282_alt modify pid int not null")
	require.NoError(t, err, "CASCADE over the tightened column is unaffected")

	// nullable-preserving and unrelated actions stay legal
	controls := seed(t, true)
	runTestShouldPass(controls, t, []string{
		"alter table t30282_alt modify pid int",
		"alter table t30282_alt add column w int",
	}, false, false)
}
