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

package substrait

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func replayedBrokerTestManager(t *testing.T) *LeaseManager {
	t.Helper()
	manager := NewPersistentLeaseManager(1, new(fakeProtector), new(fakeLeaseJournal))
	require.NoError(t, manager.Replay(context.Background()))
	require.True(t, manager.DurableReady())
	return manager
}

func TestLeaseManagerBrokerPreparePublishAbort(t *testing.T) {
	broker := NewLeaseManagerBroker()
	manager := replayedBrokerTestManager(t)

	publication, err := broker.Prepare("tae-tn-shard/1", manager)
	require.NoError(t, err)
	_, _, err = broker.Acquire()
	require.ErrorContains(t, err, "not published")

	require.NoError(t, publication.Abort())
	_, _, err = broker.Acquire()
	require.ErrorContains(t, err, "not published")

	publication, err = broker.Prepare("tae-tn-shard/1", manager)
	require.NoError(t, err)
	require.NoError(t, publication.Publish())
	acquired, identity, err := broker.Acquire()
	require.NoError(t, err)
	require.Same(t, manager, acquired)
	require.Equal(t, "tae-tn-shard/1", identity)
	require.NoError(t, publication.Abort(), "late abort must not clear a publication")
	acquired, _, err = broker.Acquire()
	require.NoError(t, err)
	require.Same(t, manager, acquired)
}

func TestLeaseManagerBrokerSecondGenerationSeals(t *testing.T) {
	broker := NewLeaseManagerBroker()
	first := replayedBrokerTestManager(t)
	publication, err := broker.Prepare("tae-tn-shard/1", first)
	require.NoError(t, err)
	require.NoError(t, publication.Publish())

	_, err = broker.Prepare("tae-tn-shard/1", replayedBrokerTestManager(t))
	require.ErrorContains(t, err, "second TAE lease manager generation")
	_, _, err = broker.Acquire()
	require.ErrorContains(t, err, "sealed")
	require.ErrorContains(t, publication.Publish(), "already terminal")
}

func TestLeaseManagerBrokerConcurrentPrepareHasNoReplacementWinner(t *testing.T) {
	broker := NewLeaseManagerBroker()
	managers := []*LeaseManager{
		replayedBrokerTestManager(t),
		replayedBrokerTestManager(t),
	}
	start := make(chan struct{})
	publications := make(chan *LeaseManagerPublication, len(managers))
	errs := make(chan error, len(managers))
	var workers sync.WaitGroup
	for _, manager := range managers {
		workers.Add(1)
		go func(manager *LeaseManager) {
			defer workers.Done()
			<-start
			publication, err := broker.Prepare("tae-tn-shard/1", manager)
			publications <- publication
			errs <- err
		}(manager)
	}
	close(start)
	workers.Wait()
	close(publications)
	close(errs)

	successes := 0
	for err := range errs {
		if err == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes)
	for publication := range publications {
		if publication != nil {
			require.ErrorContains(t, publication.Publish(), "superseded")
		}
	}
	_, _, err := broker.Acquire()
	require.ErrorContains(t, err, "sealed")
}

func TestLeaseManagerBrokerSealNeverReleasesProtection(t *testing.T) {
	broker := NewLeaseManagerBroker()
	manager := replayedBrokerTestManager(t)
	publication, err := broker.Prepare("tae-tn-shard/1", manager)
	require.NoError(t, err)
	require.NoError(t, publication.Publish())

	broker.Seal()
	_, _, err = broker.Acquire()
	require.ErrorContains(t, err, "sealed")
	require.True(t, manager.DurableReady(), "seal must not mutate or release the manager")
}
