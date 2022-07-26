// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

func TestAddMetadata(t *testing.T) {
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	cfg.Fill()
	s := store{cfg: cfg}
	require.NoError(t, mkdirAll(s.cfg.DataDir, cfg.FS))
	s.addMetadata(10, 1)
	ss := store{cfg: s.cfg}
	ss.mu.metadata = metadata.LogStore{}
	assert.NoError(t, ss.loadMetadata())
	require.Equal(t, 1, len(ss.mu.metadata.Shards))
	assert.Equal(t, uint64(10), ss.mu.metadata.Shards[0].ShardID)
	assert.Equal(t, uint64(1), ss.mu.metadata.Shards[0].ReplicaID)
}

func TestRemoveMetadata(t *testing.T) {
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	cfg.Fill()
	s := store{cfg: cfg}
	require.NoError(t, mkdirAll(s.cfg.DataDir, cfg.FS))
	s.addMetadata(10, 1)
	s.addMetadata(20, 2)
	s.removeMetadata(10, 1)
	ss := store{cfg: s.cfg}
	ss.mu.metadata = metadata.LogStore{}
	assert.NoError(t, ss.loadMetadata())
	require.Equal(t, 1, len(ss.mu.metadata.Shards))
	assert.Equal(t, uint64(20), ss.mu.metadata.Shards[0].ShardID)
	assert.Equal(t, uint64(2), ss.mu.metadata.Shards[0].ReplicaID)
}

func TestStartReplicas(t *testing.T) {
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	cfg.Fill()
	require.NoError(t, mkdirAll(cfg.DataDir, cfg.FS))
	func() {
		store, err := getTestStore(cfg, false)
		require.NoError(t, err)
		members := make(map[uint64]dragonboat.Target)
		members[1] = store.id()
		defer func() {
			require.NoError(t, store.close())
		}()
		require.NoError(t, store.startReplica(10, 1, members, false))
		require.NoError(t, store.startReplica(20, 1, members, false))
	}()

	store, err := getTestStore(cfg, false)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.close())
	}()
	require.NoError(t, store.loadMetadata())
	require.NoError(t, store.startReplicas())
	done := false
	for i := 0; i < 1000; i++ {
		hb := store.getHeartbeatMessage()
		if len(hb.Replicas) != 2 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		done = true
	}
	if !done {
		t.Fatalf("failed to start all replicas")
	}
}
