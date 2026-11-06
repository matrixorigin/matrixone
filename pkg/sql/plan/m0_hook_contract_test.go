// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestM0PlanHookDispatchContract(t *testing.T) {
	previous := activeM0PlanHooks
	activeM0PlanHooks = m0PlanHooks{}
	t.Cleanup(func() { activeM0PlanHooks = previous })
	table := &TableDef{Name: "source"}
	create := &Plan{}
	options := []tree.AlterTableOption{nil}
	selected, err := m0SelectAddColumn(nil, options, table)
	require.NoError(t, err)
	require.False(t, selected)
	preserved, err := m0PreserveCloneLayout(table, create, "db", "child", false)
	require.NoError(t, err)
	require.False(t, preserved)
	require.Error(t, m0ValidateBoundAddColumn(nil))

	ctx := &MockCompilerContext{}
	col := &ColDef{Name: "added"}
	cause := moerr.NewInternalErrorNoCtx("hook failure")
	calls := [3]int{}
	activeM0PlanHooks = m0PlanHooks{
		selectAdd: func(gotCtx CompilerContext, gotOptions []tree.AlterTableOption, gotTable *TableDef) (bool, error) {
			calls[0]++
			require.Same(t, ctx, gotCtx)
			require.Equal(t, options, gotOptions)
			require.Same(t, table, gotTable)
			return true, cause
		},
		preserveClone: func(gotSource *TableDef, gotPlan *Plan, database, name string, temporary bool) (bool, error) {
			calls[1]++
			require.Same(t, table, gotSource)
			require.Same(t, create, gotPlan)
			require.Equal(t, "db", database)
			require.Equal(t, "child", name)
			require.True(t, temporary)
			return true, cause
		},
		validateAdd: func(got *ColDef) error {
			calls[2]++
			require.Same(t, col, got)
			return cause
		},
	}
	selected, err = m0SelectAddColumn(ctx, options, table)
	require.True(t, selected)
	require.Same(t, cause, err)
	preserved, err = m0PreserveCloneLayout(table, create, "db", "child", true)
	require.True(t, preserved)
	require.Same(t, cause, err)
	require.Same(t, cause, m0ValidateBoundAddColumn(col))
	require.Equal(t, [3]int{1, 1, 1}, calls)
}

func TestM0PlanDomainAndColumnResolutionContract(t *testing.T) {
	previous := activeM0PlanHooks
	activeM0PlanHooks = m0PlanHooks{}
	t.Cleanup(func() { activeM0PlanHooks = previous })
	newTable := func() *TableDef {
		key := &ColDef{Name: catalog.CPrimaryKeyColName, Hidden: true, Primary: true}
		return &TableDef{
			Cols:          []*ColDef{nil, key},
			Pkey:          &PrimaryKeyDef{PkeyColName: key.Name, CompPkeyCol: key},
			Name2ColIndex: map[string]int32{"added": 7},
		}
	}
	table := newTable()
	require.False(t, M0AppendDomain(table))
	require.Equal(t, 2, M0ResolveColumnPosition(table, "added", 2))
	activeM0PlanHooks.selectAdd = func(CompilerContext, []tree.AlterTableOption, *TableDef) (bool, error) {
		return false, nil
	}
	require.True(t, M0AppendDomain(table))
	require.False(t, M0AppendDomain(nil))
	require.Equal(t, 2, M0ResolveColumnPosition(nil, "added", 2))
	require.Equal(t, 2, M0ResolveColumnPosition(&TableDef{}, "added", 2))
	require.Equal(t, 7, M0ResolveColumnPosition(table, "DB.Customer.ADDED", 2))
	require.Equal(t, 7, M0ResolveColumnPosition(table, "added", 2))
	require.Equal(t, 2, M0ResolveColumnPosition(table, "missing", 2))
	for _, tc := range []struct {
		name   string
		mutate func(*TableDef)
		want   bool
	}{
		{"clustered", func(d *TableDef) { d.ClusterBy = &ClusterByDef{} }, false},
		{"no hidden key", func(d *TableDef) { d.Cols = nil }, false},
		{"visible key", func(d *TableDef) { d.Cols[1].Hidden = false }, false},
		{"wrong key name", func(d *TableDef) { d.Pkey.PkeyColName = "wrong" }, false},
		{"non primary anchor", func(d *TableDef) { d.Pkey.CompPkeyCol.Primary = false }, false},
		{"mismatched anchor", func(d *TableDef) { d.Pkey.CompPkeyCol = &ColDef{Name: "wrong", Hidden: true, Primary: true} }, false},
		{"no key descriptor", func(d *TableDef) { d.Pkey = nil }, true},
		{"no composite descriptor", func(d *TableDef) { d.Pkey.CompPkeyCol = nil }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := newTable()
			tc.mutate(table)
			require.Equal(t, tc.want, M0AppendDomain(table))
		})
	}
}
