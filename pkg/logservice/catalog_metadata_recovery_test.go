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

package logservice

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/stretchr/testify/require"
)

func TestCatalogExecutorDoesNotAdvertiseStaleIncarnation(t *testing.T) {
	s, permit := catalogTestSupervisor(t)
	host := &catalogTestReplica{ready: true}
	require.NoError(t, s.executeCatalogStartWith(permit, host))
	require.NotNil(t, s.catalogExecutorCapabilities())
	s.mu.Lock()
	s.mu.metadata.Incarnation = "replacement"
	s.mu.Unlock()
	require.Nil(t, s.catalogExecutorCapabilities())
	require.Nil(t, s.catalogStartHeartbeat())
}

func TestCatalogExecutorReplacementRetiresRestartMetadata(t *testing.T) {
	s, old := catalogTestSupervisor(t)
	host := &catalogTestReplica{ready: true}
	s.addMetadata(hakeeper.DefaultHAKeeperShardID, old.ReplicaID, old.NonVoting)
	require.NoError(t, s.executeCatalogStartWith(old, host))
	revoked := old
	revoked.Revoked = true
	require.NoError(t, s.executeCatalogStartWith(revoked, host))
	next := old
	next.Token++
	next.ReplicaID++
	host.ready = true
	require.NoError(t, s.executeCatalogStartWith(next, host))
	require.Empty(t, s.getShards(), "a retired ID must not remain an admitted restart candidate")
	require.Equal(t, next.ReplicaID, catalogReadRecord(t, s).Permit.ReplicaID)
	// The durable supervised identity, not leftover pre-cutover metadata,
	// decides which HAKeeper replica may restart. This must not touch a nil
	// NodeHost, nor attempt to restart the obsolete ID.
	require.NoError(t, s.recoverCatalogReplica(old.ReplicaID, old.NonVoting))
	require.Equal(t, 2, host.starts)
	require.Equal(t, 1, host.stops)
}
