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

package cnservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
)

func TestStartViewMetadataRecoveryOwnsTaskLifecycle(t *testing.T) {
	s := &service{
		stopper: stopper.NewStopper("test-view-metadata-recovery"),
		logger:  zap.NewNop(),
	}
	require.NoError(t, s.startViewMetadataRecovery())
	s.stopper.Stop()
	require.NoError(t, s.runViewMetadataRecoveryTick(context.Background()))
}

func TestRunViewMetadataRecoveryLoopContinuesAfterFailureAndCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	calls, failures := 0, 0
	runViewMetadataRecoveryLoop(ctx, ticks, func(context.Context) error {
		calls++
		if calls == 1 {
			return errors.New("retry next tick")
		}
		cancel()
		return nil
	}, func(error) { failures++ })
	require.Equal(t, 2, calls)
	require.Equal(t, 1, failures)
}

func TestRunViewMetadataRecoveryTickRequiresCurrentFencedReadyEpoch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state viewMetadataRecoveryTickState
	}{
		{name: "catalog not ready", state: viewMetadataRecoveryTickState{epoch: 1, catalogFenced: true, refreshReady: true}},
		{name: "zero epoch", state: viewMetadataRecoveryTickState{locallyReady: true, catalogFenced: true, refreshReady: true}},
		{name: "catalog not fenced", state: viewMetadataRecoveryTickState{locallyReady: true, epoch: 1, refreshReady: true}},
		{name: "cluster not ready", state: viewMetadataRecoveryTickState{locallyReady: true, epoch: 1, catalogFenced: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			require.NoError(t, runViewMetadataRecoveryTick(
				context.Background(), tc.state,
				func(context.Context) (*compile.ViewMetadataEpochLease, error) {
					called = true
					return nil, nil
				},
				func(context.Context) error { called = true; return nil },
				func(context.Context) error { called = true; return nil },
				func(context.Context) (bool, error) { called = true; return true, nil },
				func(uint64) bool { called = true; return true },
			))
			require.False(t, called)
		})
	}
}

func TestRunViewMetadataRecoveryTickPublishesDurableCompletion(t *testing.T) {
	fence := compile.NewViewMetadataEpochFence()
	require.NoError(t, fence.Advance(context.Background(), 4))
	require.True(t, fence.MarkCatalogFenced(4))

	started, recovered, checked, published := 0, 0, 0, uint64(0)
	err := runViewMetadataRecoveryTick(
		context.Background(),
		viewMetadataRecoveryTickState{
			locallyReady:         true,
			epoch:                4,
			catalogFenced:        true,
			refreshReady:         true,
			revalidationRequired: true,
		},
		fence.Acquire,
		func(context.Context) error { started++; return nil },
		func(context.Context) error { recovered++; return nil },
		func(context.Context) (bool, error) { checked++; return true, nil },
		func(epoch uint64) bool { published = epoch; return true },
	)
	require.NoError(t, err)
	require.Equal(t, 1, started)
	require.Equal(t, 1, recovered)
	require.Equal(t, 1, checked)
	require.Equal(t, uint64(4), published)
}

func TestRunViewMetadataRecoveryTickDoesNotPublishIncompleteOrStalePass(t *testing.T) {
	fence := compile.NewViewMetadataEpochFence()
	require.NoError(t, fence.Advance(context.Background(), 5))
	require.True(t, fence.MarkCatalogFenced(5))

	published := false
	require.NoError(t, runViewMetadataRecoveryTick(
		context.Background(),
		viewMetadataRecoveryTickState{
			locallyReady:         true,
			epoch:                5,
			catalogFenced:        true,
			refreshReady:         true,
			revalidationRequired: true,
		},
		fence.Acquire,
		func(context.Context) error { return nil },
		func(context.Context) error { return nil },
		func(context.Context) (bool, error) { return false, nil },
		func(uint64) bool { published = true; return true },
	))
	require.False(t, published)

	started := false
	require.NoError(t, runViewMetadataRecoveryTick(
		context.Background(),
		viewMetadataRecoveryTickState{
			locallyReady:         true,
			epoch:                4,
			catalogFenced:        true,
			refreshReady:         true,
			revalidationRequired: true,
		},
		fence.Acquire,
		func(context.Context) error { started = true; return nil },
		func(context.Context) error { return nil },
		func(context.Context) (bool, error) { return true, nil },
		func(uint64) bool { published = true; return true },
	))
	require.False(t, started)
	require.False(t, published)
}
