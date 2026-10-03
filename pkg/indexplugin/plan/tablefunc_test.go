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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestTableFuncRegistrationPlacement(t *testing.T) {
	builder := func(PlanBuilder, *tree.TableFunction, BindContext, []*plan.Expr, []int32) (int32, error) {
		return 42, nil
	}
	for _, coordinatorOnly := range []bool{false, true} {
		name := t.Name() + "/distributed"
		if coordinatorOnly {
			name = t.Name() + "/coordinator"
		}
		t.Cleanup(func() {
			tableFuncMu.Lock()
			defer tableFuncMu.Unlock()
			delete(tableFuncs, name)
		})
		if coordinatorOnly {
			RegisterCoordinatorTableFunc(name, builder)
		} else {
			RegisterTableFunc(name, builder)
		}
		registered, ok := TableFunc(name)
		require.True(t, ok)
		id, err := registered(nil, nil, nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, int32(42), id)
		require.Equal(t, coordinatorOnly, TableFuncRequiresCoordinator(name))
		require.Panics(t, func() { RegisterTableFunc(name, builder) })
		require.Panics(t, func() { RegisterCoordinatorTableFunc(name, builder) })
		require.Equal(t, coordinatorOnly, TableFuncRequiresCoordinator(name), "duplicate registration must not replace metadata")
	}
	require.False(t, TableFuncRequiresCoordinator(t.Name()+"/unknown"))
	missing, ok := TableFunc(t.Name() + "/unknown")
	require.False(t, ok)
	require.Nil(t, missing)
}
