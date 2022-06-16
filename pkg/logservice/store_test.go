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
	"context"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	testIOTimeout = 5 * time.Second
)

func TestNodeHostConfig(t *testing.T) {
	cfg := Config{
		DeploymentID: 1234,
		DataDir:      "lalala",
	}
	nhConfig := getNodeHostConfig(cfg)
	assert.Equal(t, cfg.DeploymentID, nhConfig.DeploymentID)
	assert.Equal(t, cfg.DataDir, nhConfig.NodeHostDir)
	assert.True(t, nhConfig.AddressByNodeHostID)
}

func TestRaftConfig(t *testing.T) {
	cfg := getRaftConfig(1, 1)
	assert.True(t, cfg.CheckQuorum)
	assert.True(t, cfg.OrderedConfigChange)
}

func getStoreTestConfig() Config {
	cfg := Config{
		RTTMillisecond:      10,
		GossipSeedAddresses: []string{"127.0.0.1:9000"},
		DeploymentID:        1,
		FS:                  vfs.NewStrictMem(),
	}
	cfg.Fill()
	return cfg
}

func TestStoreCanBeCreatedAndClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	store, err := newLogStore(cfg)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, store.Close())
	}()
}

func getTestStore(cfg Config) (*logStore, error) {
	store, err := newLogStore(cfg)
	if err != nil {
		return nil, err
	}
	peers := make(map[uint64]dragonboat.Target)
	peers[2] = store.nh.ID()
	if err := store.StartReplica(1, 2, peers); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

func TestHAKeeperCanBeStarted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	store, err := newLogStore(cfg)
	assert.NoError(t, err)
	peers := make(map[uint64]dragonboat.Target)
	peers[2] = store.nh.ID()
	assert.NoError(t, store.StartHAKeeperReplica(2, peers))
	defer func() {
		assert.NoError(t, store.Close())
	}()
}

func TestStateMachineCanBeStarted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	store, err := getTestStore(cfg)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, store.Close())
	}()
}

func runStoreTest(t *testing.T, fn func(*testing.T, *logStore)) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	store, err := getTestStore(cfg)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, store.Close())
	}()
	fn(t, store)
}

func getTestUserEntry() []byte {
	cmd := make([]byte, 2+8+8)
	binaryEnc.PutUint16(cmd, userEntryTag)
	binaryEnc.PutUint64(cmd[headerSize:], 100)
	binaryEnc.PutUint64(cmd[headerSize+8:], 1234567890)
	return cmd
}

func TestGetOrExtendLease(t *testing.T) {
	fn := func(t *testing.T, store *logStore) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.GetOrExtendDNLease(ctx, 1, 100))
	}
	runStoreTest(t, fn)
}

func TestAppendLog(t *testing.T) {
	fn := func(t *testing.T, store *logStore) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.GetOrExtendDNLease(ctx, 1, 100))
		cmd := getTestUserEntry()
		lsn, err := store.Append(ctx, 1, cmd)
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)
	}
	runStoreTest(t, fn)
}

func TestAppendLogIsRejectedForMismatchedLeaseHolderID(t *testing.T) {
	fn := func(t *testing.T, store *logStore) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.GetOrExtendDNLease(ctx, 1, 100))
		cmd := make([]byte, 2+8+8)
		binaryEnc.PutUint16(cmd, userEntryTag)
		binaryEnc.PutUint64(cmd[headerSize:], 101)
		binaryEnc.PutUint64(cmd[headerSize+8:], 1234567890)
		_, err := store.Append(ctx, 1, cmd)
		assert.True(t, errors.Is(err, ErrNotLeaseHolder))
	}
	runStoreTest(t, fn)
}

func TestTruncateLog(t *testing.T) {
	fn := func(t *testing.T, store *logStore) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.GetOrExtendDNLease(ctx, 1, 100))
		cmd := getTestUserEntry()
		_, err := store.Append(ctx, 1, cmd)
		assert.NoError(t, err)
		assert.NoError(t, store.TruncateLog(ctx, 1, 4))
		err = store.TruncateLog(ctx, 1, 3)
		assert.True(t, errors.Is(err, ErrInvalidTruncateIndex))
	}
	runStoreTest(t, fn)
}

func TestGetTruncatedIndex(t *testing.T) {
	fn := func(t *testing.T, store *logStore) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		index, err := store.GetTruncatedIndex(ctx, 1)
		assert.Equal(t, uint64(0), index)
		assert.NoError(t, err)
		assert.NoError(t, store.GetOrExtendDNLease(ctx, 1, 100))
		cmd := getTestUserEntry()
		_, err = store.Append(ctx, 1, cmd)
		assert.NoError(t, err)
		assert.NoError(t, store.TruncateLog(ctx, 1, 4))
		index, err = store.GetTruncatedIndex(ctx, 1)
		assert.Equal(t, uint64(4), index)
		assert.NoError(t, err)
	}
	runStoreTest(t, fn)
}

func TestQueryLog(t *testing.T) {
	fn := func(t *testing.T, store *logStore) {
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		assert.NoError(t, store.GetOrExtendDNLease(ctx, 1, 100))
		cmd := getTestUserEntry()
		_, err := store.Append(ctx, 1, cmd)
		assert.NoError(t, err)
		entries, lsn, err := store.QueryLog(ctx, 1, 4, math.MaxUint64)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(entries))
		assert.Equal(t, uint64(4), lsn)
		assert.Equal(t, entries[0].Data, cmd)
		// lease holder ID update cmd at entry index 3
		entries, lsn, err = store.QueryLog(ctx, 1, 3, math.MaxUint64)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(entries))
		assert.Equal(t, uint64(3), lsn)
		assert.Equal(t, entries[0].Data, cmd)
		// size limited
		_, err = store.Append(ctx, 1, cmd)
		assert.NoError(t, err)
		entries, lsn, err = store.QueryLog(ctx, 1, 4, 1)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(entries))
		assert.Equal(t, uint64(5), lsn)
		assert.Equal(t, entries[0].Data, cmd)
		// more log available
		entries, lsn, err = store.QueryLog(ctx, 1, 5, 1)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(entries))
		assert.Equal(t, uint64(5), lsn)
		assert.Equal(t, entries[0].Data, cmd)
	}
	runStoreTest(t, fn)
}

func TestHAKeeperTick(t *testing.T) {
	fn := func(t *testing.T, store *logStore) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.ID()
		assert.NoError(t, store.StartHAKeeperReplica(1, peers))
		store.hakeeperTick()
	}
	runStoreTest(t, fn)
}

func TestGetHeartbeatMessage(t *testing.T) {
	fn := func(t *testing.T, store *logStore) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.ID()
		assert.NoError(t, store.StartReplica(10, 1, peers))
		assert.NoError(t, store.StartHAKeeperReplica(1, peers))

		m := store.getHeartbeatMessage()
		// hakeeper shard is included
		assert.Equal(t, 3, len(m.Shards))
	}
	runStoreTest(t, fn)
}

func TestAddHeartbeat(t *testing.T) {
	fn := func(t *testing.T, store *logStore) {
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = store.ID()
		assert.NoError(t, store.StartHAKeeperReplica(1, peers))

		m := store.getHeartbeatMessage()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		assert.NoError(t, store.AddLogStoreHeartbeat(ctx, m))

		dnMsg := pb.DNStoreHeartbeat{
			UUID:   store.ID(),
			Shards: make([]pb.DNShardInfo, 0),
		}
		dnMsg.Shards = append(dnMsg.Shards, pb.DNShardInfo{ShardID: 2, ReplicaID: 3})
		assert.NoError(t, store.AddDNStoreHeartbeat(ctx, dnMsg))
	}
	runStoreTest(t, fn)
}
