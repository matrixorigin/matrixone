package logservice

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestKillZombieRemovesExportedSnapshot(t *testing.T) {
	runServiceTest(t, false, true, func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		resp := s.handleConnect(ctx, pb.Request{Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{ShardID: 1, TNID: 100}})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		for i := 0; i < 10; i++ {
			resp = s.handleAppend(ctx, pb.Request{Method: pb.APPEND,
				LogRequest: pb.LogRequest{ShardID: 1}}, getTestAppendCmd(100, make([]byte, 8)))
			require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		}
		resp = s.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE,
			LogRequest: pb.LogRequest{ShardID: 1, TNID: 100, Lsn: 4}})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
		require.Equal(t, 1, s.store.snapshotMgr.Count(1, 1))
		index := snapshotIndex(s.store.snapshotMgr.NewestIndex(1, 1))
		path := s.store.snapshotMgr.snapshotPath(nodeID{shardID: 1, replicaID: 1}, index)
		_, err := s.store.cfg.FS.Stat(path)
		require.NoError(t, err)

		cmd := pb.ScheduleCommand{ConfigChange: &pb.ConfigChange{
			ChangeType: pb.KillZombie,
			Replica:    pb.Replica{ShardID: 1, ReplicaID: 1},
		}}
		originalFS := s.store.snapshotMgr.cfg.FS
		s.store.snapshotMgr.cfg.FS = removeErrorFS{FS: originalFS}
		s.handleKillZombie(cmd)
		s.store.snapshotMgr.cfg.FS = originalFS
		has, err := hasMetadataRec(s.store.cfg.DataDir, logMetadataFilename, 1, 1, s.store.cfg.FS)
		require.NoError(t, err)
		require.True(t, has, "failed cleanup must retain the durable retry handle")
		var persisted metadata.LogStore
		require.NoError(t, readMetadataFile(s.store.cfg.DataDir, logMetadataFilename, &persisted, s.store.cfg.FS))
		retiring := false
		for _, rec := range persisted.Shards {
			if rec.ShardID == 1 && rec.ReplicaID == 1 {
				retiring = rec.Retiring
			}
		}
		require.True(t, retiring)
		_, err = s.store.cfg.FS.Stat(path)
		require.NoError(t, err)

		require.NoError(t, s.store.processTruncateLog(ctx))
		require.False(t, hasReplica(s.store, 1, 1))
		has, err = hasMetadataRec(s.store.cfg.DataDir, logMetadataFilename, 1, 1, s.store.cfg.FS)
		require.NoError(t, err)
		require.False(t, has)
		_, err = s.store.cfg.FS.Stat(path)
		require.ErrorIs(t, err, os.ErrNotExist)
		require.NoError(t, s.store.processTruncateLog(ctx))
		_, err = s.store.cfg.FS.Stat(path)
		require.ErrorIs(t, err, os.ErrNotExist)
	})
}
