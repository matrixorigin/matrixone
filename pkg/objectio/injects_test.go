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

func TestCommitWaitInjectedTargetsTenant(t *testing.T) {
	faultEnabledHere := fault.Enable()
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(t.Context(), FJ_CommitWait)
		_, _ = fault.RemoveFaultPoint(t.Context(), FJ_CommitWaitTargetTenant)
		if faultEnabledHere {
			fault.Disable()
		}
	})

	require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWaitTargetTenant, ":::", "ECHO", 7, "", false))
	require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWait, "1:1::", "ECHO", 0, "target", false))

	_, injected := CommitWaitInjected(8, "other")
	require.False(t, injected)
	message, injected := CommitWaitInjected(7, "target")
	require.True(t, injected)
	require.Equal(t, "target", message)
}

func TestCommitWaitInjectedTargetsTransaction(t *testing.T) {
	faultEnabledHere := fault.Enable()
	t.Cleanup(func() {
		for _, point := range []string{FJ_CommitWait, FJ_CommitWaitTargetTenant, FJ_CommitWaitTargetTxn} {
			_, _ = fault.RemoveFaultPoint(t.Context(), point)
		}
		if faultEnabledHere {
			fault.Disable()
		}
	})
	require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWaitTargetTenant, ":::", "ECHO", 7, "", false))
	require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWaitTargetTxn, ":::", "ECHO", 0, "accepted", false))
	require.NoError(t, fault.AddFaultPoint(t.Context(), FJ_CommitWait, "1:1::", "ECHO", 0, "barrier", false))

	// Neither an unrelated tenant nor an earlier transaction of the same tenant
	// may consume the single-shot barrier before the accepted commit arrives.
	_, injected := CommitWaitInjected(8, "accepted")
	require.False(t, injected)
	_, injected = CommitWaitInjected(7, "earlier")
	require.False(t, injected)
	message, injected := CommitWaitInjected(7, "accepted")
	require.True(t, injected)
	require.Equal(t, "barrier", message)
	_, injected = CommitWaitInjected(7, "accepted")
	require.False(t, injected)
}
