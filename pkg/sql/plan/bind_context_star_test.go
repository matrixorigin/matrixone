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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestUnqualifiedStarRightJoinUsingOrder(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "explicit using",
			sql: "select * from (select 1 as id, 2 as left_only) l " +
				"right join (select 1 as id, 3 as right_only) r using (id)",
			want: []string{"id", "right_only", "left_only"},
		},
		{
			name: "natural right",
			sql: "select * from (select 1 as id, 2 as left_only) l " +
				"natural right join (select 1 as id, 3 as right_only) r",
			want: []string{"id", "right_only", "left_only"},
		},
		{
			name: "right join on keeps existing order",
			sql: "select * from (select 1 as left_id, 2 as left_only) l " +
				"right join (select 1 as right_id, 3 as right_only) r " +
				"on l.left_id = r.right_id",
			want: []string{"left_id", "left_only", "right_id", "right_only"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			p, err := runOneStmt(optimizer, t, test.sql)
			require.NoError(t, err)
			require.Equal(t, test.want, p.GetQuery().Headings)
		})
	}
}

func TestNaturalJoinVisibleColumnOrder(t *testing.T) {
	const left = "(select 10 as c, 1 as b) l"
	const right = "(select 2 as a, 1 as b, 10 as c) r"
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{"inner", "select * from " + left + " natural join " + right, []string{"c", "b", "a"}},
		{"left", "select * from " + left + " natural left join " + right, []string{"c", "b", "a"}},
		{"full", "select * from " + left + " natural full join " + right, []string{"c", "b", "a"}},
		{
			"right follows preserved operand",
			"select * from (select 10 as c, 1 as b, 4 as x) l natural right join " + right,
			[]string{"b", "c", "a", "x"},
		},
		{
			"right nested",
			"select * from " + left + " natural join " +
				"((select 2 as a, 1 as b) m natural join (select 1 as b, 10 as c) r)",
			[]string{"c", "b", "a"},
		},
		{
			"left nested uses merged order",
			"select * from ((select 2 as a, 1 as b) l natural join (select 1 as b, 10 as c) m) " +
				"natural join (select 10 as c, 2 as a, 1 as b) r",
			[]string{"b", "a", "c"},
		},
		{
			"flat three table control",
			"select * from " + left + " natural join (select 2 as a, 1 as b) m " +
				"natural join (select 1 as b, 10 as c) r",
			[]string{"b", "c", "a"},
		},
		{
			"case insensitive common names",
			"select * from (select 10 as C, 1 as b) l natural join (select 2 as a, 1 as B, 10 as c) r",
			[]string{"c", "b", "a"},
		},
		{"no common columns", "select * from (select 10 as c) l natural join (select 2 as a) r", []string{"c", "a"}},
		{"explicit using order unchanged", "select * from " + left + " join " + right + " using (b, c)", []string{"b", "c", "a"}},
		{"qualified stars unchanged", "select l.*, r.* from " + left + " natural join " + right, []string{"c", "b", "a", "b", "c"}},
		{
			"correlated json table",
			"select * from (select 10 as c, 1 as b, '[{\"b\":1,\"c\":10}]' as j) l " +
				"natural join json_table(l.j, '$[*]' columns(b int path '$.b', c int path '$.c')) r",
			[]string{"c", "b", "j"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, test.sql)
			require.NoError(t, err)
			require.Equal(t, test.want, p.GetQuery().Headings)
		})
	}
}

func TestNaturalJoinRejectsAmbiguousCommonColumns(t *testing.T) {
	for _, sql := range []string{
		"select * from ((select 1 as c) l cross join (select 2 as c) m) natural join (select 1 as c) r",
		"select * from (select 1 as c) l natural join ((select 1 as c) m cross join (select 2 as c) r)",
	} {
		_, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
	}
}

func TestNaturalJoinColumnVisibility(t *testing.T) {
	optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
	builder := &QueryBuilder{compCtx: optimizer.CurrentContext()}
	left, right := NewBindContext(nil, nil), NewBindContext(nil, nil)
	left.bindingTree = &BindingTreeNode{binding: &Binding{
		table:          "l",
		cols:           []string{"account_id", "c", "left_hidden", "b", "right_hidden"},
		colIsHidden:    []bool{false, false, true, false, false},
		isClusterTable: true,
	}}
	right.bindingTree = &BindingTreeNode{binding: &Binding{
		table:          "r",
		cols:           []string{"right_hidden", "b", "account_id", "c", "left_hidden"},
		colIsHidden:    []bool{true, false, false, false, false},
		isClusterTable: true,
	}}
	for _, test := range []struct {
		name    string
		account uint32
		want    []string
	}{
		{"system account", 0, []string{"account_id", "c", "b"}},
		{"tenant account", 1, []string{"c", "b"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			optimizer.ctxt.GetAccountIdFunc = func() (uint32, error) { return test.account, nil }
			cols, err := builder.naturalJoinUsingColumns(left, right, planpb.Node_INNER)
			require.NoError(t, err)
			require.Equal(t, test.want, cols)
		})
	}
	lookupErr := moerr.NewInternalErrorNoCtx("account lookup failed")
	optimizer.ctxt.GetAccountIdFunc = func() (uint32, error) { return 0, lookupErr }
	_, err := builder.naturalJoinUsingColumns(left, right, planpb.Node_INNER)
	require.ErrorIs(t, err, lookupErr)
}
