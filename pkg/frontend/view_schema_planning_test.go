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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestViewSchemaPlanningOwnsChildNotParent(t *testing.T) {
	f := newViewSchemaOwnedFixture(t)
	f.parent.SetViews([]string{"parent_view"})
	statements, err := mysql.Parse(f.ctx, "select 1 as one", 1)
	require.NoError(t, err)
	defer statements[0].Free()
	generation, err := f.proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	before := generation.Used()
	authorize := func(context.Context, string, string, *plan.Snapshot) error {
		t.Fatal("a constant result must not authorize a nonexistent root")
		return nil
	}
	for _, prepared := range []bool{false, true} {
		p, err := f.parent.BuildPlanWithViewSchema(f.ctx, statements[0], prepared, authorize)
		require.NoError(t, err)
		require.Equal(t, prepared, p.IsPrepare)
		require.Equal(t, "one", plan.GetResultColumnsFromPlan(p)[0].Name)
		require.Equal(t, before, generation.Used())
		require.False(t, generation.Closed())
		require.Same(t, f.op, f.session.GetTxnHandler().GetTxn())
		require.Equal(t, []string{"parent_view"}, f.parent.GetViews())
	}
	p, err := f.parent.BuildPlanWithViewSchema(f.ctx, statements[0], false, nil)
	require.Error(t, err, "missing authorizer cannot silently opt in with allow-all")
	require.Nil(t, p)
	require.Equal(t, before, generation.Used())
}
