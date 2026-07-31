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

package compile

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineBatchFlowBoundsAndReleasesCredits(t *testing.T) {
	flow := newPipelineBatchFlow(1, 10)
	seq, err := flow.reserve(context.Background(), context.Background(), 8)
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = flow.reserve(canceled, context.Background(), 1)
	require.ErrorIs(t, err, context.Canceled)

	require.NoError(t, flow.acknowledge(seq))
	seq, err = flow.reserve(context.Background(), context.Background(), 20)
	require.NoError(t, err, "one oversized batch must make progress by itself")
	require.Equal(t, uint64(2), seq)
	require.NoError(t, flow.acknowledge(seq))
	require.NoError(t, flow.waitUntilDrained(context.Background(), context.Background(), nil))
}

func TestPipelineBatchFlowCumulativeAckAndRollback(t *testing.T) {
	flow := newPipelineBatchFlow(8, 1024)
	first, err := flow.reserve(context.Background(), context.Background(), 10)
	require.NoError(t, err)
	second, err := flow.reserve(context.Background(), context.Background(), 20)
	require.NoError(t, err)
	third, err := flow.reserve(context.Background(), context.Background(), 30)
	require.NoError(t, err)

	flow.rollback(second)
	require.NoError(t, flow.acknowledge(third))
	require.Equal(t, uint64(1), first)
	flow.mu.Lock()
	require.Empty(t, flow.pending)
	require.Zero(t, flow.bytes)
	flow.mu.Unlock()
}

func TestPipelineBatchFlowDrainWaitHonorsCancellation(t *testing.T) {
	flow := newPipelineBatchFlow(1, 1024)
	seq, err := flow.reserve(context.Background(), context.Background(), 10)
	require.NoError(t, err)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t,
		flow.waitUntilDrained(canceled, context.Background(), nil),
		context.Canceled,
		"terminal response must not bypass an outstanding batch")

	require.NoError(t, flow.acknowledge(seq))
	require.NoError(t,
		flow.waitUntilDrained(context.Background(), context.Background(), nil),
		"the ACK must release the terminal-response barrier")
}

func TestPipelineBatchFlowRejectsAckAheadOfSentData(t *testing.T) {
	flow := newPipelineBatchFlow(1, 1024)
	require.Error(t, flow.acknowledge(1))
}
