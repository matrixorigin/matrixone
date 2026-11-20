// Copyright 2024 Matrix Origin
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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/stretchr/testify/require"
)

func TestLockReadQueryContextUsesCurrentSelectOwner(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	root.EnableLockRead()
	ctx := resource.ContextWithRoot(context.Background(), root)
	selectPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT}}}
	selectCompile := &Compile{stmt: &tree.Select{}, pn: selectPlan, resourceAttemptOwnerEligible: true}
	query := selectCompile.lockReadQueryContext(ctx)
	require.Same(t, root, resource.LockReadRootFromContext(query))

	for _, c := range []*Compile{
		{stmt: &tree.Update{}, pn: selectPlan, resourceAttemptOwnerEligible: true},
		{stmt: &tree.Delete{}, pn: selectPlan, resourceAttemptOwnerEligible: true},
		{stmt: &tree.Select{}, pn: selectPlan, resourceAttemptOwnerEligible: true, isInternal: true},
		{stmt: &tree.Select{}, pn: selectPlan},
		{stmt: &tree.Select{}, resourceAttemptOwnerEligible: true},
		{stmt: &tree.Select{}, pn: &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_UPDATE}}}, resourceAttemptOwnerEligible: true},
	} {
		require.Nil(t, resource.LockReadRootFromContext(c.lockReadQueryContext(query)))
	}
	require.Nil(t, resource.LockReadRootFromContext(selectCompile.lockReadQueryContext(context.Background())))
	nextRoot := resource.NewRoot(resource.ConnExternal)
	nextRoot.EnableLockRead()
	next := resource.ContextWithRoot(query, nextRoot)
	require.Same(t, nextRoot, resource.LockReadRootFromContext(selectCompile.lockReadQueryContext(next)))
}
