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

package iscp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnregisterConsumerDoesNotRemoveReplacement(t *testing.T) {
	exec := newRuntimeTestExecutor()
	key := NewJobRuntimeKey(1, 2, "job", 7)
	old, ok := exec.RegisterRunningConsumer(key, 7, 10, func() {}, nil)
	require.True(t, ok)
	replacement, ok := exec.RegisterRunningConsumer(key, 7, 10, func() {}, nil)
	require.True(t, ok)

	exec.UnregisterRunningConsumer(old)
	require.Same(t, replacement, exec.runningConsumers[key.consumerGroupKey()][7])
	select {
	case <-old.done:
	default:
		t.Fatal("old consumer was not signaled")
	}
	select {
	case <-replacement.done:
		t.Fatal("replacement consumer was signaled by stale unregister")
	default:
	}
	exec.UnregisterRunningConsumer(replacement)
	require.Empty(t, exec.runningConsumers)
}
