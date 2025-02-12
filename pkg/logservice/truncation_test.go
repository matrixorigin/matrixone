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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
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

func TestShouldDoImport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("dir is null, do no import", func(t *testing.T) {
		st := &store{}
		assert.False(t, st.shouldDoImport(ctx, 1, 0, ""))
	})

	t.Run("no hakeeper, failed", func(t *testing.T) {
		fn := func(t *testing.T, s *Service) {
			assert.False(t, s.store.shouldDoImport(ctx, 1, 0, "test-dir"))
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

			assert.False(t, s.store.shouldDoImport(ctx, 1, 0, "test-dir"))
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

			assert.False(t, s.store.shouldDoImport(ctx, 1, 100, "test-dir"))
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

			assert.False(t, s.store.shouldDoImport(ctx, 1, 100, "test-dir"))
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

			assert.True(t, s.store.shouldDoImport(ctx, 1, 10, "test-dir"))
		}
		runServiceTest(t, false, true, fn)
	})
}
