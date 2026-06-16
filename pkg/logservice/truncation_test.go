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
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

func TestTruncationExportSnapshot(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		tnID := uint64(100)
		req := pb.Request{
			Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    tnID,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		for i := 0; i < 10; i++ {
			data := make([]byte, 8)
			cmd := getTestAppendCmd(tnID, data)
			req = pb.Request{
				Method: pb.APPEND,
				LogRequest: pb.LogRequest{
					ShardID: 1,
				},
			}
			resp = s.handleAppend(ctx, req, cmd)
			assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
			assert.Equal(t, uint64(4+i), resp.LogResponse.Lsn) // applied index is 4+i
		}

		req = pb.Request{
			Method: pb.TRUNCATE,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Lsn:     4,
			},
		}
		resp = s.handleTruncate(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(0), resp.LogResponse.Lsn)

		req = pb.Request{
			Method: pb.GET_TRUNCATE,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleGetTruncatedIndex(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(4), resp.LogResponse.Lsn)

		err := s.store.processShardTruncateLog(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, 1, s.store.snapshotMgr.Count(1, 1))

		err = s.store.processShardTruncateLog(ctx, 1)
		// truncate lsn not advanced, no error is returned.
		assert.NoError(t, err)
	}
	runServiceTest(t, false, true, fn)
}

func TestTruncationSkipsStaleReplicaMetadata(t *testing.T) {
	const (
		shardID       = uint64(1)
		staleReplica  = uint64(99)
		activeReplica = uint64(1)
	)
	logShard := func(replicaID uint64) metadata.LogShard {
		return metadata.LogShard{
			LogShardRecord: metadata.LogShardRecord{ShardID: shardID},
			ReplicaID:      replicaID,
		}
	}

	tests := []struct {
		name   string
		shards []metadata.LogShard
	}{
		{
			name: "stale before active",
			shards: []metadata.LogShard{
				logShard(staleReplica),
				logShard(activeReplica),
			},
		},
		{
			name: "stale after active",
			shards: []metadata.LogShard{
				logShard(activeReplica),
				logShard(staleReplica),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fn := func(t *testing.T, s *Service) {
				nid := nodeID{shardID: shardID, replicaID: staleReplica}
				testPrepareSnapshot(t, s.store.snapshotMgr, nid, snapshotIndex(100))
				assert.NoError(t, s.store.snapshotMgr.Init(shardID, staleReplica))
				assert.Equal(t, 1, s.store.snapshotMgr.Count(shardID, staleReplica))

				s.store.mu.Lock()
				s.store.mu.metadata.Shards = append([]metadata.LogShard(nil), tc.shards...)
				s.store.mu.Unlock()

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				assert.NoError(t, s.store.processTruncateLog(ctx))

				assert.Equal(t, int64(activeReplica), s.store.getReplicaID(shardID))
				assert.Equal(t, 0, s.store.snapshotMgr.Count(shardID, staleReplica))
			}
			runServiceTest(t, false, true, fn)
		})
	}
}

func TestTruncationRealignsSnapshotIndexAfterStaleReplicaCleanup(t *testing.T) {
	const (
		shardID       = uint64(1)
		staleReplica  = uint64(99)
		activeReplica = uint64(1)
		staleIndex    = uint64(100)
	)
	logShard := func(replicaID uint64) metadata.LogShard {
		return metadata.LogShard{
			LogShardRecord: metadata.LogShardRecord{ShardID: shardID},
			ReplicaID:      replicaID,
		}
	}

	tests := []struct {
		name              string
		shards            []metadata.LogShard
		activeBeforeStale bool
	}{
		{
			name: "stale before active",
			shards: []metadata.LogShard{
				logShard(staleReplica),
				logShard(activeReplica),
			},
		},
		{
			name: "stale after active",
			shards: []metadata.LogShard{
				logShard(activeReplica),
				logShard(staleReplica),
			},
			activeBeforeStale: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fn := func(t *testing.T, s *Service) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()

				tnID := uint64(100)
				req := pb.Request{
					Method: pb.CONNECT_RO,
					LogRequest: pb.LogRequest{
						ShardID: shardID,
						TNID:    tnID,
					},
				}
				resp := s.handleConnect(ctx, req)
				assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

				for i := 0; i < 10; i++ {
					data := make([]byte, 8)
					cmd := getTestAppendCmd(tnID, data)
					req = pb.Request{
						Method: pb.APPEND,
						LogRequest: pb.LogRequest{
							ShardID: shardID,
						},
					}
					resp = s.handleAppend(ctx, req, cmd)
					assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
				}

				req = pb.Request{
					Method: pb.TRUNCATE,
					LogRequest: pb.LogRequest{
						ShardID: shardID,
						Lsn:     4,
					},
				}
				resp = s.handleTruncate(ctx, req)
				assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

				nid := nodeID{shardID: shardID, replicaID: staleReplica}
				testPrepareSnapshot(t, s.store.snapshotMgr, nid, snapshotIndex(staleIndex))
				assert.NoError(t, s.store.snapshotMgr.Init(shardID, staleReplica))
				assert.Equal(t, 1, s.store.snapshotMgr.Count(shardID, staleReplica))
				s.store.shardSnapshotInfo.setSnapshotIndex(shardID, staleIndex)

				s.store.mu.Lock()
				s.store.mu.metadata.Shards = append([]metadata.LogShard(nil), tc.shards...)
				s.store.mu.Unlock()

				assert.NoError(t, s.store.processTruncateLog(ctx))
				assert.Equal(t, 0, s.store.snapshotMgr.Count(shardID, staleReplica))
				if tc.activeBeforeStale {
					assert.Equal(t, 0, s.store.snapshotMgr.Count(shardID, activeReplica))
					assert.NoError(t, s.store.processTruncateLog(ctx))
				}

				assert.Equal(t, 1, s.store.snapshotMgr.Count(shardID, activeReplica))
				assert.Equal(t,
					s.store.snapshotMgr.NewestIndex(shardID, activeReplica),
					s.store.shardSnapshotInfo.getSnapshotIndex(shardID))
			}
			runServiceTest(t, false, true, fn)
		})
	}
}

func TestTruncationKeepsStaleReplicaMetadataOnSnapshotCleanupFailure(t *testing.T) {
	const (
		shardID      = uint64(1)
		staleReplica = uint64(99)
	)
	logShard := metadata.LogShard{
		LogShardRecord: metadata.LogShardRecord{ShardID: shardID},
		ReplicaID:      staleReplica,
	}

	fn := func(t *testing.T, s *Service) {
		nid := nodeID{shardID: shardID, replicaID: staleReplica}
		testPrepareSnapshot(t, s.store.snapshotMgr, nid, snapshotIndex(100))
		assert.NoError(t, s.store.snapshotMgr.Init(shardID, staleReplica))
		assert.Equal(t, 1, s.store.snapshotMgr.Count(shardID, staleReplica))

		s.store.mu.Lock()
		s.store.mu.metadata.Shards = []metadata.LogShard{logShard}
		s.store.mu.Unlock()

		originalSnapshotCfg := s.store.snapshotMgr.cfg
		failingSnapshotCfg := *originalSnapshotCfg
		failingSnapshotCfg.FS = vfs.Wrap(originalSnapshotCfg.FS, vfs.OnIndex(0, vfs.OpWrite))
		s.store.snapshotMgr.cfg = &failingSnapshotCfg

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		assert.NoError(t, s.store.processTruncateLog(ctx))
		assert.Equal(t, int64(staleReplica), s.store.getReplicaID(shardID))
		assert.Equal(t, 1, s.store.snapshotMgr.Count(shardID, staleReplica))

		s.store.snapshotMgr.cfg = originalSnapshotCfg
		assert.NoError(t, s.store.processTruncateLog(ctx))
		assert.Equal(t, int64(-1), s.store.getReplicaID(shardID))
		assert.Equal(t, 0, s.store.snapshotMgr.Count(shardID, staleReplica))
	}
	runServiceTest(t, false, true, fn)
}

func TestTruncationImportSnapshot(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		// start hakeeper
		peers := make(map[uint64]dragonboat.Target)
		peers[1] = s.ID()
		assert.NoError(t, s.store.startHAKeeperReplica(1, peers, false))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		tnID := uint64(100)
		req := pb.Request{
			Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    tnID,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		for i := 0; i < 10; i++ {
			data := make([]byte, 8)
			cmd := getTestAppendCmd(tnID, data)
			req = pb.Request{
				Method: pb.APPEND,
				LogRequest: pb.LogRequest{
					ShardID: 1,
				},
			}
			resp = s.handleAppend(ctx, req, cmd)
			assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
			assert.Equal(t, uint64(4+i), resp.LogResponse.Lsn) // applied index is 4+i
		}

		req = pb.Request{
			Method: pb.TRUNCATE,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Lsn:     4,
			},
		}
		resp = s.handleTruncate(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(0), resp.LogResponse.Lsn)

		req = pb.Request{
			Method: pb.GET_TRUNCATE,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleGetTruncatedIndex(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(4), resp.LogResponse.Lsn)

		// after this, snapshot index 14 is exported.
		err := s.store.processShardTruncateLog(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, 1, s.store.snapshotMgr.Count(1, 1))

		_, idx := s.store.snapshotMgr.EvalImportSnapshot(1, 1, 6)
		assert.Equal(t, uint64(0), idx)

		err = s.store.processShardTruncateLog(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, 1, s.store.snapshotMgr.Count(1, 1))

		req = pb.Request{
			Method: pb.TRUNCATE,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Lsn:     10,
			},
		}
		resp = s.handleTruncate(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(0), resp.LogResponse.Lsn)

		_, idx = s.store.snapshotMgr.EvalImportSnapshot(1, 1, 10)
		assert.Equal(t, uint64(0), idx)
		// index already advance to 15 because of truncate op.
		err = s.store.processShardTruncateLog(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, 2, s.store.snapshotMgr.Count(1, 1))

		for i := 0; i < 10; i++ {
			data := make([]byte, 8)
			cmd := getTestAppendCmd(tnID, data)
			req = pb.Request{
				Method: pb.APPEND,
				LogRequest: pb.LogRequest{
					ShardID: 1,
				},
			}
			resp = s.handleAppend(ctx, req, cmd)
			assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
			assert.Equal(t, uint64(16+i), resp.LogResponse.Lsn) // applied index is 16+i
		}

		req = pb.Request{
			Method: pb.TRUNCATE,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Lsn:     15,
			},
		}
		resp = s.handleTruncate(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(0), resp.LogResponse.Lsn)

		_, idx = s.store.snapshotMgr.EvalImportSnapshot(1, 1, 14)
		assert.Equal(t, uint64(14), idx)
		err = s.store.processShardTruncateLog(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, 0, s.store.snapshotMgr.Count(1, 1))
	}
	runServiceTest(t, false, true, fn)
}

func TestHAKeeperTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
		defer cancel()
		req := pb.Request{
			Method: pb.LOG_HEARTBEAT,
			LogHeartbeat: &pb.LogStoreHeartbeat{
				UUID: "uuid1",
			},
		}
		for i := 0; i < 10; i++ {
			resp := s.handleLogHeartbeat(ctx, req)
			assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		}
		v, err := s.store.read(ctx, hakeeper.DefaultHAKeeperShardID, &hakeeper.IndexQuery{})
		assert.NoError(t, err)
		assert.Equal(t, uint64(12), v.(uint64))

		err = s.store.processHAKeeperTruncation(ctx)
		assert.NoError(t, err)

		checkTick := time.NewTicker(time.Millisecond * 100)
		defer checkTick.Stop()
		for {
			select {
			case <-ctx.Done():
				panic("failed to truncate logs")
			case <-checkTick.C:
				rs, err := s.store.nh.QueryRaftLog(hakeeper.DefaultHAKeeperShardID,
					1, 100, 1024*100)
				assert.NoError(t, err)
				select {
				case v := <-rs.ResultC():
					// We cannot fetch the logs because they are truncated.
					if v.RequestOutOfRange() {
						return
					}
				case <-ctx.Done():
					panic("failed to truncate logs")
				}

			}
		}
	}
	runServiceTest(t, true, true, fn)
}

func TestTruncationImportSnapshot2(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		tnID := uint64(100)
		req := pb.Request{
			Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    tnID,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		for i := 0; i < 10; i++ {
			data := make([]byte, 8)
			cmd := getTestAppendCmd(tnID, data)
			req = pb.Request{
				Method: pb.APPEND,
				LogRequest: pb.LogRequest{
					ShardID: 1,
				},
			}
			resp = s.handleAppend(ctx, req, cmd)
			assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
			assert.Equal(t, uint64(4+i), resp.LogResponse.Lsn) // applied index is 4+i
		}

		req = pb.Request{
			Method: pb.TRUNCATE,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Lsn:     4,
			},
		}
		resp = s.handleTruncate(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(0), resp.LogResponse.Lsn)

		req = pb.Request{
			Method: pb.GET_TRUNCATE,
			LogRequest: pb.LogRequest{
				ShardID: 1,
			},
		}
		resp = s.handleGetTruncatedIndex(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.Equal(t, uint64(4), resp.LogResponse.Lsn)

		ctx2, cancel2 := context.WithTimeoutCause(context.Background(), time.Second, moerr.NewInternalErrorNoCtx("ut tester"))
		defer cancel2()

		// after this, snapshot index 14 is exported.
		err := s.store.processShardTruncateLog(ctx2, 1)
		assert.NoError(t, err)

	}
	runServiceTest(t, false, true, fn)
}

func TestProcessShardTruncateLogError(t *testing.T) {
	s := store{
		nh:      &dragonboat.NodeHost{},
		runtime: runtime.DefaultRuntime(),
	}
	assert.NoError(t, s.processShardTruncateLog(context.Background(), 100))
}

func TestHAKeeperTruncation2(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
		defer cancel()
		req := pb.Request{
			Method: pb.LOG_HEARTBEAT,
			LogHeartbeat: &pb.LogStoreHeartbeat{
				UUID: "uuid1",
			},
		}
		for i := 0; i < 10; i++ {
			resp := s.handleLogHeartbeat(ctx, req)
			assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		}
		v, err := s.store.read(ctx, hakeeper.DefaultHAKeeperShardID, &hakeeper.IndexQuery{})
		assert.NoError(t, err)
		assert.Equal(t, uint64(12), v.(uint64))

		//let ctx timeout
		ctx2, cancel2 := context.WithTimeoutCause(context.Background(), 0, moerr.NewInternalErrorNoCtx("ut tester"))
		defer cancel2()
		err = s.store.processHAKeeperTruncation(ctx2)
		assert.Error(t, err)

	}
	runServiceTest(t, true, true, fn)
}

// TestTruncationPurgeWhenFull covers the escape valve in
// processShardTruncateLog for issue #24315: once the exported-snapshot
// quota is full and the log-shard state machine's TruncatedLsn has
// advanced past the oldest items, those items must be purged from disk
// so that the loop can make progress on the next tick. Without this
// behavior a transient failure in the import path causes the WAL to
// grow without bound.

// TestTruncationDropNewestOnTimeout covers the escape valve that runs
// when getTruncatedLsn times out while the exported-snapshot quota is
// full. Without this path the loop wedges permanently: no quota ->
// no export -> no import -> no WAL compaction, while the replica keeps
// receiving AppendEntries. See issue #24315.
//
// Also asserts the "drop newest, preserve oldest" policy: the oldest
// tracked item is what EvalImportSnapshot will pick once SyncRead
// recovers, so we preferentially delete newer speculative exports.
func TestTruncationDropNewestOnTimeout(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		tnID := uint64(100)
		req := pb.Request{
			Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				TNID:    tnID,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		for i := 0; i < 10; i++ {
			data := make([]byte, 8)
			cmd := getTestAppendCmd(tnID, data)
			req = pb.Request{
				Method:     pb.APPEND,
				LogRequest: pb.LogRequest{ShardID: 1},
			}
			resp = s.handleAppend(ctx, req, cmd)
			assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		}

		// Shrink the quota so we can fill it with two exports.
		s.store.cfg.MaxExportedSnapshot = 2

		// Export twice to fill the quota at distinct indices.
		req = pb.Request{
			Method: pb.TRUNCATE,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				Lsn:     4,
			},
		}
		resp = s.handleTruncate(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NoError(t, s.store.processShardTruncateLog(ctx, 1))
		assert.Equal(t, 1, s.store.snapshotMgr.Count(1, 1))
		oldest := s.store.snapshotMgr.snapshots[nodeID{1, 1}].first().index

		req.LogRequest.Lsn = 6
		resp = s.handleTruncate(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		assert.NoError(t, s.store.processShardTruncateLog(ctx, 1))
		assert.Equal(t, 2, s.store.snapshotMgr.Count(1, 1))
		// shardSnapshotInfo.snapshotIndex should be tracking the newest
		// exported snapshot after the second tick.
		snapshotIndexBefore := s.store.shardSnapshotInfo.getSnapshotIndex(1)
		newestBefore := s.store.snapshotMgr.NewestIndex(1, 1)
		assert.Equal(t, newestBefore, snapshotIndexBefore)

		// Directly exercise the timeout-path helper. This is what the
		// production loop calls when getTruncatedLsn returns ErrTimeout.
		s.store.dropNewestOnTimeout(1, 1)
		assert.Equal(t, 1, s.store.snapshotMgr.Count(1, 1),
			"dropNewestOnTimeout must free exactly one quota slot")
		// The oldest item -- the one EvalImportSnapshot would have
		// returned -- must survive.
		assert.Equal(t, oldest,
			s.store.snapshotMgr.snapshots[nodeID{1, 1}].first().index,
			"dropNewestOnTimeout must preserve the oldest tracked item")
		// snapshotIndex must be realigned downward to the now-newest
		// remaining item so shouldDoExport does not suppress a needed
		// re-export on a quiescent shard. See issue #24315.
		assert.Equal(t, uint64(oldest),
			s.store.shardSnapshotInfo.getSnapshotIndex(1),
			"dropNewestOnTimeout must realign snapshotIndex to the "+
				"new newest item")

		// Second invocation must be a no-op: the sole remaining item
		// is the last importable candidate and must not be dropped.
		// Under MaxExportedSnapshot=1 this rule means the escape valve
		// is fully disabled; see issue #24315.
		s.store.dropNewestOnTimeout(1, 1)
		assert.Equal(t, 1, s.store.snapshotMgr.Count(1, 1),
			"dropNewestOnTimeout must preserve the sole importable candidate")

		// Safe when there is no (shard, replica) entry.
		s.store.dropNewestOnTimeout(999, 999)
	}
	runServiceTest(t, false, true, fn)
}

func TestShouldDoImport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("dir is null, do no import", func(t *testing.T) {
		st := &store{}
		st.runtime = runtime.DefaultRuntime()
		assert.False(t, st.shouldDoImport(ctx, 1, 0, 0, ""))
	})

	t.Run("no hakeeper, failed", func(t *testing.T) {
		fn := func(t *testing.T, s *Service) {
			assert.False(t, s.store.shouldDoImport(ctx, 1, 0, 0, "test-dir"))
		}
		runServiceTest(t, false, true, fn)
	})

	t.Run("lsn too small, failed", func(t *testing.T) {
		fn := func(t *testing.T, s *Service) {
			// start hakeeper
			peers := make(map[uint64]dragonboat.Target)
			peers[1] = s.ID()
			assert.NoError(t, s.store.startHAKeeperReplica(1, peers, false))

			hb := pb.TNStoreHeartbeat{
				UUID:        "tn1",
				ReplayedLsn: 90,
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*3)
			defer cancel()
			_, err := s.store.addTNStoreHeartbeat(ctx, hb)
			assert.NoError(t, err)

			assert.False(t, s.store.shouldDoImport(ctx, 1, 0, 0, "test-dir"))
		}
		runServiceTest(t, false, true, fn)
	})

	t.Run("replayed lsn is too small, failed", func(t *testing.T) {
		fn := func(t *testing.T, s *Service) {
			// start hakeeper
			peers := make(map[uint64]dragonboat.Target)
			peers[1] = s.ID()
			assert.NoError(t, s.store.startHAKeeperReplica(1, peers, false))

			hb := pb.TNStoreHeartbeat{
				UUID:        "tn1",
				ReplayedLsn: 90,
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*3)
			defer cancel()
			_, err := s.store.addTNStoreHeartbeat(ctx, hb)
			assert.NoError(t, err)

			assert.False(t, s.store.shouldDoImport(ctx, 1, 100, 100, "test-dir"))
		}
		runServiceTest(t, false, true, fn)
	})

	t.Run("not all replayed lsn are big enough, failed", func(t *testing.T) {
		fn := func(t *testing.T, s *Service) {
			// start hakeeper
			peers := make(map[uint64]dragonboat.Target)
			peers[1] = s.ID()
			assert.NoError(t, s.store.startHAKeeperReplica(1, peers, false))

			hb := pb.TNStoreHeartbeat{
				UUID:        "tn1",
				ReplayedLsn: 90,
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*3)
			defer cancel()
			_, err := s.store.addTNStoreHeartbeat(ctx, hb)
			assert.NoError(t, err)

			hb = pb.TNStoreHeartbeat{
				UUID:        "tn2",
				ReplayedLsn: 190,
			}
			_, err = s.store.addTNStoreHeartbeat(ctx, hb)
			assert.NoError(t, err)

			assert.False(t, s.store.shouldDoImport(ctx, 1, 100, 100, "test-dir"))
		}
		runServiceTest(t, false, true, fn)
	})

	t.Run("replayed lsn is big enough, ok", func(t *testing.T) {
		fn := func(t *testing.T, s *Service) {
			// start hakeeper
			peers := make(map[uint64]dragonboat.Target)
			peers[1] = s.ID()
			assert.NoError(t, s.store.startHAKeeperReplica(1, peers, false))

			hb := pb.TNStoreHeartbeat{
				UUID:        "tn1",
				ReplayedLsn: 90,
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*3)
			defer cancel()
			_, err := s.store.addTNStoreHeartbeat(ctx, hb)
			assert.NoError(t, err)

			assert.True(t, s.store.shouldDoImport(ctx, 1, 10, 10, "test-dir"))
		}
		runServiceTest(t, false, true, fn)
	})
}
