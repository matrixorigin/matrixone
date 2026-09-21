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

package process

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubmitBlockingEventPrefersDedicatedLane(t *testing.T) {
	proc := &Process{Base: &BaseProcess{}}
	eventCalls := 0
	blockingCalls := 0
	taskCalls := 0
	proc.SetEventSubmitter(func(_ string, task func()) error {
		eventCalls++
		task()
		return nil
	})
	proc.SetBlockingSubmitter(func(_ string, task func()) error {
		blockingCalls++
		task()
		return nil
	})

	require.True(t, proc.HasEventSubmitter())
	require.True(t, proc.HasBlockingSubmitter())
	require.NoError(t, proc.SubmitBlockingEvent("blocking", func() {
		taskCalls++
	}))
	require.Equal(t, 1, blockingCalls)
	require.Zero(t, eventCalls)
	require.Equal(t, 1, taskCalls)

	proc.SetBlockingSubmitter(nil)
	require.False(t, proc.HasBlockingSubmitter())
	require.NoError(t, proc.SubmitBlockingEvent("compat", func() {
		taskCalls++
	}))
	require.Equal(t, 1, eventCalls)
	require.Equal(t, 2, taskCalls)
}
