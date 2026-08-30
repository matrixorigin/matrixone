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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestDDLCommitGateLinearizesBlockAndCommit(t *testing.T) {
	gate := NewDDLCommitGate()
	releaseFirst, err := gate.Enter(context.Background())
	require.NoError(t, err)

	gate.mu.Lock()
	blockStarted := gate.changed
	gate.mu.Unlock()
	blockDone := make(chan error, 1)
	go func() { blockDone <- gate.Block(context.Background()) }()
	<-blockStarted // Block has published blocked=true.
	select {
	case err := <-blockDone:
		t.Fatalf("block returned before the admitted DDL left: %v", err)
	default:
	}
	releaseFirst()
	require.NoError(t, <-blockDone)

	enterDone := make(chan error, 1)
	go func() {
		release, enterErr := gate.Enter(context.Background())
		if enterErr == nil {
			release()
		}
		enterDone <- enterErr
	}()
	gate.mu.Lock()
	require.True(t, gate.blocked)
	require.Zero(t, gate.active)
	gate.mu.Unlock()
	select {
	case err := <-enterDone:
		t.Fatalf("DDL entered while activation held the gate: %v", err)
	default:
	}
	gate.Unblock()
	require.NoError(t, <-enterDone)
}

func TestDDLCommitGateTracksOnlyMonotonicDDLFrontier(t *testing.T) {
	gate := NewDDLCommitGate()
	older := timestamp.Timestamp{PhysicalTime: 100}
	newer := timestamp.Timestamp{PhysicalTime: 200}

	require.True(t, gate.LatestDDLFrontier().IsEmpty())
	gate.RecordDDLFrontier(newer)
	gate.RecordDDLFrontier(older)
	require.Equal(t, newer, gate.LatestDDLFrontier())
}

func TestDDLCommitGateCancellationAndClose(t *testing.T) {
	gate := NewDDLCommitGate()
	gate.EnablePublicDDL()
	release, err := gate.Enter(context.Background())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = gate.Block(ctx)
	require.ErrorIs(t, err, context.Canceled)
	release()
	require.NoError(t, gate.Block(context.Background()),
		"a canceled activation must retain the producer fence for a safe retry")
	_, err = gate.Enter(ctx)
	require.ErrorIs(t, err, context.Canceled)

	gate.Close()
	require.True(t, gate.PublicDDLEnabled(),
		"an admitted public background DDL retains shutdown fan-out eligibility")
	_, err = gate.Enter(context.Background())
	require.ErrorContains(t, err, "closed")
	err = gate.Block(context.Background())
	require.ErrorContains(t, err, "closed")
}
