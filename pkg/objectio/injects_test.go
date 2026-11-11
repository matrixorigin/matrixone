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

package objectio

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInjectUpstreamSQLHelper(t *testing.T) {
	fault.Enable()
	defer fault.Disable()

	rmFault, err := InjectUpstreamSQLHelper("test-helper-msg")
	require.NoError(t, err)
	require.NotNil(t, rmFault)

	msg, injected := UpstreamSQLHelperInjected()
	assert.True(t, injected)
	assert.Equal(t, "test-helper-msg", msg)

	ok, err := rmFault()
	assert.True(t, ok)
	assert.NoError(t, err)

	_, injected = UpstreamSQLHelperInjected()
	assert.False(t, injected)
}

func TestUpstreamSQLHelperInjected_NotInjected(t *testing.T) {
	_, injected := UpstreamSQLHelperInjected()
	assert.False(t, injected)
}

func TestWorkspaceAllocationLimitIsDatabaseScoped(t *testing.T) {
	enabled := fault.Enable()
	const point = "fj/cn/workspace_allocation_limit"
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(t.Context(), point)
		if enabled {
			fault.Disable()
		}
	})
	require.NoError(t, fault.AddFaultPoint(t.Context(), point, ":::", "ECHO", 32, "workspace_test", false))
	require.Equal(t, uint64(32), CNWorkspaceAllocationLimit("workspace_test"))
	require.Zero(t, CNWorkspaceAllocationLimit("other_database"))
	_, err := fault.RemoveFaultPoint(t.Context(), point)
	require.NoError(t, err)
	require.NoError(t, fault.AddFaultPoint(t.Context(), point, ":::", "ECHO", 32, "", false))
	require.Zero(t, CNWorkspaceAllocationLimit("workspace_test"))
}
