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
	"crypto/rand"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testLogger = logutil.GetGlobalLogger().Named("logservice-test")

func TestClientConfigIsValidated(t *testing.T) {
	cfg := ClientConfig{}
	cc, err := NewClient(context.TODO(), "", cfg)
	assert.Nil(t, cc)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
}

func TestClientCanBeReset(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		client := c.(*managedClient)
		client.resetClient()
		assert.Nil(t, client.client)
	}
	RunClientTest(t, false, nil, fn)
}

func TestPrepareClient(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		client := c.(*managedClient)
		assert.NoError(t, client.prepareClient(ctx))
		client.resetClient()
		assert.Nil(t, client.client)
		assert.NoError(t, client.prepareClient(ctx))
		assert.NotNil(t, client.client)
	}
	RunClientTest(t, false, nil, fn)
}

func TestLogShardNotFoundErrorIsConsideredAsTempError(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		require.NoError(t, s.store.stopReplica(1, 2))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.GetTSOTimestamp(ctx, 100)
		assert.True(t, isTempError(err))
		client := c.(*managedClient)
		assert.True(t, client.isRetryableError(err))
	}
	RunClientTest(t, false, nil, fn)
}

func TestClientCanBeCreated(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
	}
	RunClientTest(t, false, nil, fn)
	RunClientTest(t, true, nil, fn)
}

func TestClientCanBeConnectedByReverseProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewService(cfg,
		newFS(),
		nil,
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service.Close())
	}()

	init := make(map[uint64]string)
	init[2] = service.ID()
	assert.NoError(t, service.store.startReplica(1, 2, init, false))

	scfg := ClientConfig{
		LogShardID:       1,
		TNReplicaID:      2,
		ServiceAddresses: []string{"localhost:53032"}, // unreachable
		DiscoveryAddress: testServiceAddress,
	}

	done := false
	for i := 0; i < 1000; i++ {
		si, ok, err := GetShardInfo("", testServiceAddress, 1)
		if err != nil || !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		done = true
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, uint64(2), si.ReplicaID)
		addr, ok := si.Replicas[si.ReplicaID]
		assert.True(t, ok)
		assert.Equal(t, testServiceAddress, addr)
		break
	}
	if !done {
		t.Fatalf("failed to get shard info")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := NewClient(ctx, "", scfg)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, c.Close())
	}()
}

func TestClientGetTSOTimestamp(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		v, err := c.GetTSOTimestamp(ctx, 100)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), v)

		v, err = c.GetTSOTimestamp(ctx, 1000)
		require.NoError(t, err)
		assert.Equal(t, uint64(101), v)

		v, err = c.GetTSOTimestamp(ctx, 100)
		require.NoError(t, err)
		assert.Equal(t, uint64(1101), v)
	}
	RunClientTest(t, false, nil, fn)
}

func TestClientAppend(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(16)
		rand.Read(rec.Payload())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		lsn, err := c.Append(ctx, rec)
		require.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)

		lsn, err = c.Append(ctx, rec)
		require.NoError(t, err)
		assert.Equal(t, uint64(5), lsn)

		cmd := make([]byte, 16+headerSize+8)
		cmd = getAppendCmd(cmd, cfg.TNReplicaID+1)
		_, err = c.Append(ctx, pb.LogRecord{Data: cmd})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotLeaseHolder))
	}
	RunClientTest(t, false, nil, fn)
}

// FIXME: actually enforce allowed allocation
func TestClientAppendAlloc(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(16)
		rand.Read(rec.Payload())
		ac := testing.AllocsPerRun(1000, func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			_, err := c.Append(ctx, rec)
			require.NoError(t, err)
		})
		testLogger.Info(fmt.Sprintf("ac: %f", ac))
	}
	RunClientTest(t, false, nil, fn)
}

func TestClientRead(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(16)
		rand.Read(rec.Payload())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		lsn, err := c.Append(ctx, rec)
		require.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)

		rec2 := c.GetLogRecord(16)
		rand.Read(rec2.Payload())
		lsn, err = c.Append(ctx, rec2)
		require.NoError(t, err)
		assert.Equal(t, uint64(5), lsn)

		// FIXME: returned records should contain correct Index value
		recs, lsn, err := c.Read(ctx, 4, math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)
		require.Equal(t, 2, len(recs))
		assert.Equal(t, rec.Data, recs[0].Data)
		assert.Equal(t, rec2.Data, recs[1].Data)

		_, _, err = c.Read(ctx, 6, math.MaxUint64)
		assert.True(t, errors.Is(err, dragonboat.ErrInvalidRange))
	}
	RunClientTest(t, false, nil, fn)
}

func TestClientTruncate(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(16)
		rand.Read(rec.Payload())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		lsn, err := c.Append(ctx, rec)
		require.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)

		require.NoError(t, c.Truncate(ctx, 4))
		lsn, err = c.GetTruncatedLsn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, Lsn(4), lsn)

		err = c.Truncate(ctx, 3)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTruncateLsn))
	}
	RunClientTest(t, false, nil, fn)
}

func TestReadOnlyClientRejectWriteRequests(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(16)
		rand.Read(rec.Payload())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.Append(ctx, rec)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		err = c.Truncate(ctx, 4)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	}
	RunClientTest(t, true, nil, fn)
}

func TestClientSendWithMsgSize(t *testing.T) {
	cFn := func(readOnly bool) ClientConfig {
		return ClientConfig{
			ReadOnly:         readOnly,
			LogShardID:       1,
			TNReplicaID:      2,
			ServiceAddresses: []string{testServiceAddress},
			MaxMessageSize:   testServerMaxMsgSize,
		}
	}

	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(testServerMaxMsgSize + 80)
		rand.Read(rec.Payload())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// client only writes message whose size less than 190
		_, err := c.Append(ctx, rec)
		require.Error(t, err)
	}
	RunClientTest(t, false, cFn, fn)
}

func TestServerReceiveWithMsgSize(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(16)
		rand.Read(rec.Payload())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.Append(ctx, rec)
		require.NoError(t, err)

		rec = c.GetLogRecord(defaultMaxMessageSize + 20)
		rand.Read(rec.Payload())
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err = c.Append(ctx, rec)
		require.Error(t, err)
	}
	RunClientTest(t, false, nil, fn)
}

func TestClientGetLatestLsn(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(16)
		rand.Read(rec.Payload())
		appendFn := func(j int) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			lsn, err := c.Append(ctx, rec)
			require.NoError(t, err)
			assert.Equal(t, uint64(4+j), lsn)
		}
		for i := 0; i < 10; i++ {
			appendFn(i)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		lsn, err := c.GetLatestLsn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, Lsn(13), lsn)
	}
	RunClientTest(t, false, nil, fn)
}

func TestClientRequiredLsn(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(16)
		rand.Read(rec.Payload())
		appendFn := func(j int) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			lsn, err := c.Append(ctx, rec)
			require.NoError(t, err)
			assert.Equal(t, uint64(4+j), lsn)
		}
		for i := 0; i < 10; i++ {
			appendFn(i)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		require.NoError(t, c.SetRequiredLsn(ctx, 8))
		lsn, err := c.GetRequiredLsn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, Lsn(8), lsn)

		err = c.SetRequiredLsn(ctx, 100)
		assert.NoError(t, err)
		lsn, err = c.GetRequiredLsn(ctx) // return the max index
		assert.NoError(t, err)
		assert.Equal(t, Lsn(15), lsn)
	}
	RunClientTest(t, false, nil, fn)
}

func TestClientGetLeaderID(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		sc := c.(StandbyClient)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		leaderID, err := sc.GetLeaderID(ctx) // return the max index
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), leaderID)
	}
	RunClientTest(t, false, nil, fn)
}
