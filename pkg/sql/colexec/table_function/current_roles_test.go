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

package table_function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCurrentRoleGrantQueryBoundsDistinctResults(t *testing.T) {
	require.Equal(t, "SELECT DISTINCT cast(granted_id AS bigint) FROM mo_catalog.mo_role_grant WHERE grantee_id IN (1,2) LIMIT 4097", buildCurrentRoleGrantQuery([]int64{1, 2}))
}

func TestCurrentRoleClosureMultipleRoots(t *testing.T) {
	edges := map[int64][]int64{1: {3}, 2: {3, 4}, 3: {1, 4}, 4: {}}
	calls := map[int64]int{}
	expand := func(_ *process.Process, frontier []int64, visit func(int64) error) error {
		for _, id := range frontier {
			calls[id]++
			for _, child := range edges[id] {
				if err := visit(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	roles, err := currentRoleClosure(nil, []int64{1, 2, 1}, expand)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4}, roles)
	require.Equal(t, map[int64]int{1: 1, 2: 1, 3: 1, 4: 1}, calls)
}

func TestCurrentRoleClosureLimitAndCancellation(t *testing.T) {
	roots := make([]int64, currentRoleClosureMaxRoles+1)
	for i := range roots {
		roots[i] = int64(i)
	}
	expand := func(_ *process.Process, _ []int64, _ func(int64) error) error { return nil }
	roles, err := currentRoleClosure(nil, roots[:currentRoleClosureMaxRoles], expand)
	require.NoError(t, err)
	require.Len(t, roles, currentRoleClosureMaxRoles)
	_, err = currentRoleClosure(nil, roots, expand)
	require.Error(t, err)
	_, err = currentRoleClosure(nil, roots[:currentRoleClosureMaxRoles], func(_ *process.Process, _ []int64, visit func(int64) error) error {
		return visit(int64(currentRoleClosureMaxRoles))
	})
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = currentRoleClosure(&process.Process{Ctx: ctx}, []int64{1}, expand)
	require.ErrorIs(t, err, context.Canceled)
}

func TestValidateCurrentRoleContext(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 0)
	for _, mode := range []uint32{1, 2} {
		require.NoError(t, validateCurrentRoleContext(ctx, process.CurrentRoleContext{Mode: mode}))
	}
	require.Error(t, validateCurrentRoleContext(ctx, process.CurrentRoleContext{}))
	require.Error(t, validateCurrentRoleContext(ctx, process.CurrentRoleContext{Mode: 3}))
	require.Error(t, validateCurrentRoleContext(context.Background(), process.CurrentRoleContext{Mode: 1}))
	require.Error(t, validateCurrentRoleContext(ctx, process.CurrentRoleContext{Mode: 1, AccountID: 1}))
	require.Error(t, validateCurrentRoleContext(nil, process.CurrentRoleContext{Mode: 1}))
}

func TestCurrentRoleLiteralEscaping(t *testing.T) {
	require.Equal(t, `'a\\\\b''c'`, currentRoleLiteral(`a\\b'c`))
}

func TestCurrentRoleSQLRequiresTransaction(t *testing.T) {
	_, err := runCurrentRolesSQLAsAccount(nil, 0, "select 1")
	require.Error(t, err)
}
