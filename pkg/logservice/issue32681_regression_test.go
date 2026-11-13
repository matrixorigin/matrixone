package logservice

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestConnectDuringWALRecoveryFence(t *testing.T) {
	runServiceTest(t, false, true, func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		const shardID, tnID = uint64(1), uint64(100)
		connect := pb.Request{Method: pb.CONNECT, LogRequest: pb.LogRequest{ShardID: shardID, TNID: tnID}}
		resp := s.handleConnect(ctx, connect)
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		digest := strings.Repeat("a", walRecoveryDigestSize)
		result, err := s.store.propose(ctx, s.store.nh.GetNoOPSession(shardID), getBeginWALRecoveryCmd(digest, 0))
		require.NoError(t, err)
		require.Empty(t, result.Data)
		resp = s.handleConnect(ctx, connect)
		require.Equal(t, uint32(moerr.ErrNotLeaseHolder), resp.ErrorCode)
		resp = s.handleUpdateLeaseholderID(ctx, pb.Request{Method: pb.UPDATE_LEASEHOLDER_ID, LogRequest: connect.LogRequest})
		require.Equal(t, uint32(moerr.ErrNotLeaseHolder), resp.ErrorCode)
		resp = s.handleAppend(ctx, pb.Request{Method: pb.APPEND_WAL, LogRequest: pb.LogRequest{ShardID: shardID}}, getTestAppendCmd(tnID, []byte("fenced")), true)
		require.Equal(t, uint32(moerr.ErrNotLeaseHolder), resp.ErrorCode)
		t.Logf("CONNECT during recovery returned Ok; following APPEND_WAL returned %d (%s)", resp.ErrorCode, resp.ErrorMessage)
		result, err = s.store.propose(ctx, s.store.nh.GetNoOPSession(shardID), getEndWALRecoveryCmd(digest, 0))
		require.NoError(t, err)
		require.Empty(t, result.Data)
		resp = s.handleConnect(ctx, connect)
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		resp = s.handleAppend(ctx, pb.Request{Method: pb.APPEND_WAL, LogRequest: pb.LogRequest{ShardID: shardID}}, getTestAppendCmd(tnID, []byte("after")), true)
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
	})
}

func TestDiagnosticStopReplicaRetainsExportedSnapshot(t *testing.T) {
	runServiceTest(t, false, true, func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		resp := s.handleConnect(ctx, pb.Request{Method: pb.CONNECT_RO, LogRequest: pb.LogRequest{ShardID: 1, TNID: 100}})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		for i := 0; i < 10; i++ {
			resp = s.handleAppend(ctx, pb.Request{Method: pb.APPEND, LogRequest: pb.LogRequest{ShardID: 1}}, getTestAppendCmd(100, make([]byte, 8)))
			require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		}
		resp = s.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE, LogRequest: pb.LogRequest{ShardID: 1, TNID: 100, Lsn: 4}})
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		require.NoError(t, s.store.processShardTruncateLog(ctx, 1))
		require.Equal(t, 1, s.store.snapshotMgr.Count(1, 1))
		path := s.store.snapshotMgr.exportPath(1, 1)
		_, err := s.store.cfg.FS.Stat(path)
		require.NoError(t, err)
		s.handleStopReplica(pb.ScheduleCommand{ConfigChange: &pb.ConfigChange{ChangeType: pb.StopReplica, Replica: pb.Replica{ShardID: 1, ReplicaID: 1}}})
		require.NoError(t, s.store.processTruncateLog(ctx))
		_, err = s.store.cfg.FS.Stat(path)
		require.NoError(t, err, "stopped replica retains metadata and snapshot for restart")
		t.Logf("StopReplica then truncation retained %s", path)
	})
}
