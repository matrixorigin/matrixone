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
	"fmt"
	"os"
	"strings"
	"sync"
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
	"github.com/stretchr/testify/require"
)

type pausedExportRenameFS struct {
	vfs.FS
	entered chan struct{}
	release chan struct{}
	pause   sync.Once
	unblock sync.Once
}

func (fs *pausedExportRenameFS) Rename(oldname, newname string) error {
	if strings.Contains(oldname, ".generating") &&
		strings.Contains(newname, "shard-1/replica-2/snapshot-") {
		fs.pause.Do(func() {
			close(fs.entered)
			<-fs.release
		})
	}
	return fs.FS.Rename(oldname, newname)
}

func (fs *pausedExportRenameFS) resume() {
	fs.unblock.Do(func() { close(fs.release) })
}

// controlledDeadlineContext lets the test expire an accepted request at a
// known phase without relying on a short wall-clock deadline.
type controlledDeadlineContext struct {
	context.Context
	deadline time.Time
	done     chan struct{}
	once     sync.Once
}

func (c *controlledDeadlineContext) expire() { c.once.Do(func() { close(c.done) }) }

func (c *controlledDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
func (c *controlledDeadlineContext) Done() <-chan struct{}       { return c.done }
func (c *controlledDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

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

func TestExportSnapshotTimeoutAfterAdmissionReconcilesLateCompletion(t *testing.T) {
	cfg := getStoreTestConfig()
	cfg.TruncateInterval.Duration = time.Hour
	cfg.HAKeeperTruncateInterval.Duration = time.Hour
	fs := &pausedExportRenameFS{
		FS: cfg.FS, entered: make(chan struct{}), release: make(chan struct{}),
	}
	cfg.FS = fs
	l, err := getTestStore(func() Config { return cfg }, true, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		fs.resume()
		require.NoError(t, l.close())
		vfs.ReportLeakedFD(fs, t)
	})
	require.NoError(t, l.startHAKeeperReplica(1,
		map[uint64]dragonboat.Target{1: l.nh.ID()}, false))

	const shardID, replicaID = uint64(1), uint64(2)
	ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
	defer cancel()
	require.NoError(t, l.getOrExtendTNLease(ctx, shardID, 100))
	_, err = l.append(ctx, shardID, getTestUserEntry())
	require.NoError(t, err)

	exportCtx := &controlledDeadlineContext{
		Context: context.Background(), deadline: time.Now().Add(30 * time.Second),
		done: make(chan struct{}),
	}
	t.Cleanup(exportCtx.expire)
	result := make(chan error, 1)
	go func() { result <- l.exportSnapshot(exportCtx, shardID, replicaID) }()
	select {
	case <-fs.entered:
	case err := <-result:
		t.Fatalf("export returned before finalization: %v", err)
	case <-ctx.Done():
		t.Fatal("export did not reach finalization")
	}
	exportCtx.expire()
	select {
	case err := <-result:
		require.ErrorIs(t, err, dragonboat.ErrTimeout)
	case <-ctx.Done():
		t.Fatal("timed-out request did not return")
	}

	// The first reconciliation precedes finalization, so it cannot yet
	// register the export. The uncertainty must survive this scan.
	require.NoError(t, l.processShardTruncateLogWithReplica(ctx, shardID, replicaID))
	require.Zero(t, l.snapshotMgr.Count(shardID, replicaID))
	fs.resume()

	nid := nodeID{shardID: shardID, replicaID: replicaID}
	var index uint64
	require.Eventually(t, func() bool {
		names, err := fs.List(l.snapshotMgr.exportPath(shardID, replicaID))
		if err != nil {
			return false
		}
		for _, name := range names {
			var candidate uint64
			count, err := fmt.Sscanf(name, snapshotPathPattern, &candidate)
			if err != nil || count != 1 || name != fmt.Sprintf(snapshotPathPattern, candidate) {
				continue
			}
			files, err := fs.List(l.snapshotMgr.snapshotPath(nid, snapshotIndex(candidate)))
			if err == nil && len(files) == 2 {
				index = candidate
				return true
			}
		}
		return false
	}, testIOTimeout, 10*time.Millisecond, "late export must become a complete final directory")
	require.NoError(t, l.processShardTruncateLogWithReplica(ctx, shardID, replicaID))
	require.Equal(t, 1, l.snapshotMgr.Count(shardID, replicaID))
	dir, importedIndex := l.snapshotMgr.EvalImportSnapshot(shardID, replicaID, index)
	require.Equal(t, l.snapshotMgr.snapshotPath(nid, snapshotIndex(index)), dir)
	require.Equal(t, index, importedIndex)

	require.NoError(t, l.truncateLog(ctx, shardID, Lsn(index)))
	require.NoError(t, l.processShardTruncateLogWithReplica(ctx, shardID, replicaID))
	require.Zero(t, l.snapshotMgr.Count(shardID, replicaID))
	_, err = fs.Stat(dir)
	require.True(t, os.IsNotExist(err), "later import must remove the timed-out export")
}

func TestUncertainExportReconcilesLateCompletion(t *testing.T) {
	runStoreTest(t, func(t *testing.T, l *store) {
		const shardID, replicaID = uint64(1), uint64(2)
		ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
		defer cancel()
		require.NoError(t, l.getOrExtendTNLease(ctx, shardID, 100))
		l.cfg.MaxExportedSnapshot = 2
		nid := nodeID{shardID: shardID, replicaID: replicaID}
		writeFinal := func(index snapshotIndex) {
			dir := l.snapshotMgr.snapshotPath(nid, index)
			require.NoError(t, l.cfg.FS.MkdirAll(dir, defaultExportedDirMode))
			for _, name := range []string{
				fmt.Sprintf(snapshotPathPattern+".gbsnap", index),
				"snapshot.metadata",
			} {
				f, err := l.cfg.FS.Create(l.cfg.FS.PathJoin(dir, name))
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}
		}

		// The completed directory can appear after the first scan. Keep
		// reconciling this replica until it is discovered on a later tick.
		l.snapshotMgr.MarkExportUncertain(shardID, replicaID)
		require.NoError(t, l.processShardTruncateLogWithReplica(ctx, shardID, replicaID))
		require.Zero(t, l.snapshotMgr.Count(shardID, replicaID))

		writeFinal(100)
		writeFinal(200)
		require.NoError(t, l.snapshotMgr.Add(shardID, replicaID, 200))
		require.NoError(t, l.processShardTruncateLogWithReplica(ctx, shardID, replicaID))
		require.Equal(t, 2, l.snapshotMgr.Count(shardID, replicaID))
		require.False(t, l.shouldDoExport(ctx, shardID, replicaID))
		dir, index := l.snapshotMgr.EvalImportSnapshot(shardID, replicaID, 150)
		require.Equal(t, l.snapshotMgr.snapshotPath(nid, 100), dir)
		require.Equal(t, uint64(100), index)
		require.NoError(t, l.snapshotMgr.Remove(shardID, replicaID, 150))
		_, err := l.cfg.FS.Stat(dir)
		require.True(t, os.IsNotExist(err))

		// The unknown-result flag remains active across scans: another late
		// final directory is registered before the next quota decision.
		writeFinal(300)
		writeFinal(400)
		l.shardSnapshotInfo.forwardSnapshot(shardID, 400)
		require.NoError(t, l.processShardTruncateLogWithReplica(ctx, shardID, replicaID))
		require.Equal(t, 2, l.snapshotMgr.Count(shardID, replicaID))
		require.Equal(t, uint64(300), l.shardSnapshotInfo.getSnapshotIndex(shardID))
		require.False(t, l.shouldDoExport(ctx, shardID, replicaID))
		_, err = l.cfg.FS.Stat(l.snapshotMgr.snapshotPath(nid, 400))
		require.True(t, os.IsNotExist(err), "excess newest export must be reclaimed")
	})
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

func TestTruncationCleansSkippedZombieReplicaMetadata(t *testing.T) {
	const (
		shardID      = uint64(1)
		staleReplica = uint64(99)
	)
	logShard := metadata.LogShard{
		LogShardRecord: metadata.LogShardRecord{ShardID: shardID},
		ReplicaID:      staleReplica,
	}

	tests := []struct {
		name    string
		skipped bool
	}{
		{
			name:    "skipped zombie without active replacement",
			skipped: true,
		},
		{
			name: "ordinary metadata without active replica",
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
				s.store.mu.metadata.Shards = []metadata.LogShard{logShard}
				if tc.skipped {
					s.store.mu.skippedZombies = map[zombieKey]struct{}{
						{shardID: shardID, replicaID: staleReplica}: {},
					}
				}
				s.store.mu.Unlock()

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				assert.NoError(t, s.store.processTruncateLog(ctx))

				if tc.skipped {
					assert.Equal(t, int64(-1), s.store.getReplicaID(shardID))
					assert.Equal(t, 0, s.store.snapshotMgr.Count(shardID, staleReplica))
					assert.False(t, s.store.isSkippedZombie(shardID, staleReplica))
					return
				}
				assert.Equal(t, int64(staleReplica), s.store.getReplicaID(shardID))
				assert.Equal(t, 1, s.store.snapshotMgr.Count(shardID, staleReplica))
			}
			runServiceTest(t, false, false, fn)
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
