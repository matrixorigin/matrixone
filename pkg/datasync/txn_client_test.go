// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/mohae/deepcopy"
	"github.com/stretchr/testify/assert"
)

type mockTxnClient struct {
	db  *testutil.TestEngine
	err error
}

func newMockTxnClient(db *testutil.TestEngine) TxnClient {
	return &mockTxnClient{
		db: db,
	}
}

func (c *mockTxnClient) getLatestCheckpoint(_ context.Context) (*api.CheckpointResp, error) {
	if c.err != nil {
		return nil, c.err
	}
	if c.db == nil {
		return &api.CheckpointResp{}, nil
	}
	var location string
	var lsn uint64
	data := c.db.BGCheckpointRunner.GetAllCheckpoints()
	for i := range data {
		location += data[i].GetLocation().String()
		location += ":"
		location += fmt.Sprintf("%d", data[i].GetVersion())
		location += ";"
		if lsn < data[i].GetTruncateLsn() {
			lsn = data[i].GetTruncateLsn()
		}
	}
	return &api.CheckpointResp{
		Location:    location,
		TruncateLsn: lsn,
	}, nil
}

func (c *mockTxnClient) close() {}

func (c *mockTxnClient) fakeError() {
	c.err = moerr.NewInternalErrorNoCtx("fake error")
}

func (c *mockTxnClient) clearFakeError() {
	c.err = nil
}

type mockHAKeeperClient struct {
	sync.RWMutex
	value logpb.ClusterDetails
	err   error
}

func (c *mockHAKeeperClient) Close() error                                 { return nil }
func (c *mockHAKeeperClient) AllocateID(_ context.Context) (uint64, error) { return 0, nil }
func (c *mockHAKeeperClient) AllocateIDByKey(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}
func (c *mockHAKeeperClient) AllocateIDByKeyWithBatch(_ context.Context, _ string, _ uint64) (uint64, error) {
	return 0, nil
}
func (c *mockHAKeeperClient) GetClusterDetails(_ context.Context) (logpb.ClusterDetails, error) {
	c.RLock()
	defer c.RUnlock()
	copied := deepcopy.Copy(c.value)
	return copied.(logpb.ClusterDetails), c.err
}
func (c *mockHAKeeperClient) GetClusterState(_ context.Context) (logpb.CheckerState, error) {
	return logpb.CheckerState{}, nil
}

func (c *mockHAKeeperClient) addTN(id string, addr string, replicaID uint64) {
	c.Lock()
	defer c.Unlock()
	c.value.TNStores = append(c.value.TNStores, logpb.TNStore{
		UUID:           id,
		ServiceAddress: addr,
		Shards: []logpb.TNShardInfo{
			{
				ShardID:   1,
				ReplicaID: replicaID,
			},
		},
	})
}

func newTestClock() clock.Clock {
	return clock.NewHLCClock(func() int64 { return 0 }, 0)
}

func newTestTxnServer(t *testing.T, addr string, opts ...morpc.CodecOption) morpc.RPCServer {
	assert.NoError(t, os.RemoveAll(addr[7:]))
	opts = append(opts,
		morpc.WithCodecIntegrationHLC(newTestClock()),
		morpc.WithCodecEnableChecksum())
	codec := morpc.NewMessageCodec(
		"",
		func() morpc.Message { return &txn.TxnRequest{} },
		opts...)
	s, err := morpc.NewRPCServer("test-txn-server", addr, codec)
	assert.NoError(t, err)
	assert.NoError(t, s.Start())
	return s
}

func runWithTxnClient(t *testing.T, fn func(tc *txnClient, ret any)) {
	temp := os.TempDir()
	addr := fmt.Sprintf("unix://%s/%d.sock", temp, time.Now().Nanosecond())
	assert.NoError(t, os.RemoveAll(addr))

	rt := runtime.DefaultRuntime()
	mockClient := &mockHAKeeperClient{}
	mockClient.addTN("", addr, 100)

	tc := newTxnClient(
		*newCommon().withRuntime(rt).withLog(rt.Logger()),
		func(ctx context.Context, sid string, cfg logservice.HAKeeperClientConfig) logservice.ClusterHAKeeperClient {
			return mockClient
		},
	)
	defer tc.close()

	ts := newTestTxnServer(t, addr)
	defer func() {
		_ = ts.Close()
	}()

	resp := &api.CheckpointResp{
		Location:    "aaaaa",
		TruncateLsn: 100,
	}
	payload, err := resp.Marshal()
	assert.NoError(t, err)
	ts.RegisterRequestHandler(func(
		ctx context.Context,
		request morpc.RPCMessage,
		sequence uint64,
		cs morpc.ClientSession,
	) error {
		return cs.Write(ctx, &txn.TxnResponse{
			RequestID: request.Message.GetID(),
			CNOpResponse: &txn.CNOpResponse{
				Payload: payload,
			},
		})
	})

	fn(tc, *resp)
}

func TestGetLatestCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runWithTxnClient(t, func(tc *txnClient, ret any) {
		ctx := context.Background()
		ckp, err := tc.getLatestCheckpoint(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, ckp)
		assert.Equal(t, ret, *ckp)
	})
}
