package logservice

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestClientErrorDetails(t *testing.T) {
	for _, tc := range []struct {
		code uint16
		want string
	}{
		{moerr.ErrInvalidTruncateLsn, "invalid truncate lsn"},
		{moerr.ErrNotLeaseHolder, "not lease holder"},
	} {
		err := toError(context.Background(), pb.Response{ErrorCode: uint32(tc.code)})
		require.True(t, moerr.IsMoErrCode(err, tc.code))
		require.Equal(t, tc.want, err.Error())
	}
	RunClientTest(t, false, nil, func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rec := c.GetLogRecord(8)
		lsn, err := c.Append(ctx, rec)
		require.NoError(t, err)
		require.NoError(t, c.Truncate(ctx, lsn))
		serverTruncate := s.handleTruncate(ctx, pb.Request{Method: pb.TRUNCATE, LogRequest: pb.LogRequest{ShardID: 1, TNID: cfg.TNReplicaID, Lsn: lsn - 1}})
		require.Equal(t, uint32(moerr.ErrInvalidTruncateLsn), serverTruncate.ErrorCode)
		err = c.Truncate(ctx, lsn-1)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTruncateLsn))
		require.Equal(t, serverTruncate.ErrorMessage, err.Error())
		t.Logf("TRUNCATE server=%q client=%q", serverTruncate.ErrorMessage, err.Error())

		require.NoError(t, c.(*managedClient).prepareClient(ctx))
		connect := s.handleConnect(ctx, pb.Request{Method: pb.CONNECT, LogRequest: pb.LogRequest{ShardID: 1, TNID: 42}})
		require.Equal(t, uint32(moerr.Ok), connect.ErrorCode)
		serverAppend := s.handleAppend(ctx, pb.Request{Method: pb.APPEND, LogRequest: pb.LogRequest{ShardID: 1}}, rec.Data)
		require.Equal(t, uint32(moerr.ErrNotLeaseHolder), serverAppend.ErrorCode)
		_, err = c.Append(ctx, rec)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotLeaseHolder))
		require.Equal(t, serverAppend.ErrorMessage, err.Error())
		t.Logf("APPEND server=%q client=%q", serverAppend.ErrorMessage, err.Error())
	})
}
