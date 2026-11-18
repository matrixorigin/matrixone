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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/stretchr/testify/require"
)

func TestCurrentRolesExecutionCacheScopeAndGeneration(t *testing.T) {
	ctx := context.Background()
	c := &Compile{scopes: []*Scope{{PreScopes: []*Scope{{RootOp: &table_function.TableFunction{FuncName: "mo_current_roles"}}}}}}
	first := c.installCurrentRolesExecutionCache(ctx)
	require.True(t, table_function.HasCurrentRolesExecutionCache(first))
	old := c.currentRolesCache
	second := c.installCurrentRolesExecutionCache(first)
	require.True(t, table_function.HasCurrentRolesExecutionCache(second))
	require.NotSame(t, old, c.currentRolesCache)
	c.scopes = nil
	masked := c.installCurrentRolesExecutionCache(second)
	require.False(t, table_function.HasCurrentRolesExecutionCache(masked))
	require.Nil(t, c.currentRolesCache)
	plain := c.installCurrentRolesExecutionCache(ctx)
	require.Equal(t, ctx, plain)
	c.closeCurrentRolesExecutionCache()
}
