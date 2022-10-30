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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
)

func TestTruncationExportSnapshot(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		dnID := uint64(100)
		req := pb.Request{
			Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				DNID:    dnID,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		for i := 0; i < 10; i++ {
			data := make([]byte, 8)
			cmd := getTestAppendCmd(dnID, data)
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

func TestTruncationImportSnapshot(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		dnID := uint64(100)
		req := pb.Request{
			Method: pb.CONNECT_RO,
			LogRequest: pb.LogRequest{
				ShardID: 1,
				DNID:    dnID,
			},
		}
		resp := s.handleConnect(ctx, req)
		assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

		for i := 0; i < 10; i++ {
			data := make([]byte, 8)
			cmd := getTestAppendCmd(dnID, data)
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
			cmd := getTestAppendCmd(dnID, data)
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
