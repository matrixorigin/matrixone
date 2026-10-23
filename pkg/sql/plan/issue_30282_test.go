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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
