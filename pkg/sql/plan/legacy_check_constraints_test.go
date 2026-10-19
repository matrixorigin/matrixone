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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoverLegacyCheckConstraints(t *testing.T) {
	checks, err := RecoverLegacyCheckConstraints(t.Context(), "create table old(a int check(a>0), b int, constraint named check(b<10) not enforced)")
	require.NoError(t, err)
	require.Equal(t, []LegacyCheckConstraint{
		{Name: "__mo_chk_1", Clause: "`a` > 0", Enforced: "YES"},
		{Name: "named", Clause: "`b` < 10", Enforced: "NO"},
	}, checks)
	for _, sql := range []string{"", "create table old(a int)", "select 'CHECK'"} {
		checks, err := RecoverLegacyCheckConstraints(t.Context(), sql)
		require.NoError(t, err)
		require.Empty(t, checks)
	}
	_, err = RecoverLegacyCheckConstraints(t.Context(), "create table CHECK invalid syntax")
	require.Error(t, err)
	_, err = RecoverLegacyCheckConstraints(t.Context(), `create table old(a int, check("a">0))`)
	require.ErrorContains(t, err, "ambiguous SQL mode")
}

func TestRenameLegacyCheckConstraintNames(t *testing.T) {
	for _, tc := range []struct{ name, sql, old, next, want string }{
		{"generated", "create table t(x int, constraint t_chk_1 check(x>0))", "t", "u", "create table t(x int, constraint `u_chk_1` check(x>0))"},
		{"case insensitive", "create table t(x int, constraint T_CHK_2 check(x>0))", "t", "U", "create table t(x int, constraint `U_chk_2` check(x>0))"},
		{"column and table checks", "create table t(x int constraint t_chk_1 check(x>0),constraint t_chk_2 check(x<10),constraint keep check(x<>5) not enforced)", "t", "u", "create table t(x int constraint `u_chk_1` check(x>0),constraint `u_chk_2` check(x<10),constraint keep check(x<>5) not enforced)"},
		{"comments and quoted name", "create table t(x int, constraint /*keep*/ `t_chk_1` /*also keep*/ check(x>0))", "t", "u", "create table t(x int, constraint /*keep*/ `u_chk_1` /*also keep*/ check(x>0))"},
		{"executable comment", "create table t(x int, /*!80000 constraint t_chk_1 check(x>0) */)", "t", "u", "create table t(x int, /*!80000 constraint `u_chk_1` check(x>0) */)"},
		{"name-like text is not an identifier", "create table t(x int, s varchar(99) default 'constraint t_chk_1 check(x)', constraint t_chk_1 check(x>0)) /* t_chk_1 */", "t", "u", "create table t(x int, s varchar(99) default 'constraint t_chk_1 check(x)', constraint `u_chk_1` check(x>0)) /* t_chk_1 */"},
		{"mode-sensitive default stays byte identical", `create table t(x int, s varchar(99) default 'C:\temp', constraint t_chk_1 check(x>0))`, "t", "u", "create table t(x int, s varchar(99) default 'C:\\temp', constraint `u_chk_1` check(x>0))"},
		{"ANSI quotes", `create table t("x" int, constraint "t_chk_1" check("x">0))`, "t", "u", "create table t(\"x\" int, constraint `u_chk_1` check(\"x\">0))"},
		{"backticks are escaped", "create table `a``b`(x int, constraint `a``b_chk_1` check(x>0))", "a`b", "new`name", "create table `a``b`(x int, constraint `new``name_chk_1` check(x>0))"},
		{"historical unnamed identity", "create table t(x int, constraint __mo_chk_1 check(x>0))", "t", "u", "create table t(x int, constraint __mo_chk_1 check(x>0))"},
		{"no checks", "create table t(x int)", "t", "u", "create table t(x int)"},
		{"same name", "create table CHECK malformed", "t", "t", "create table CHECK malformed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RenameLegacyCheckConstraintNames(t.Context(), tc.sql, tc.old, tc.next)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			if tc.old == tc.next {
				return
			}
			before, err := RecoverLegacyCheckConstraints(t.Context(), tc.sql)
			require.NoError(t, err)
			after, err := RecoverLegacyCheckConstraints(t.Context(), got)
			require.NoError(t, err)
			require.Len(t, after, len(before))
			for i := range before {
				require.Equal(t, before[i].Clause, after[i].Clause)
				require.Equal(t, before[i].Enforced, after[i].Enforced)
			}
		})
	}
	for _, sql := range []string{"create table CHECK invalid", `create table t(x int, constraint t_chk_1 check("x">0))`} {
		_, err := RenameLegacyCheckConstraintNames(t.Context(), sql, "t", "u")
		require.Error(t, err, sql)
	}
}
