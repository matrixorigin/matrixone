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
	"math"
	"math/rand"
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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testLogger = logutil.GetGlobalLogger().Named("logservice-test")

var getClientConfig = func(readOnly bool) ClientConfig {
	return ClientConfig{
		ReadOnly:         readOnly,
		LogShardID:       1,
		DNReplicaID:      2,
		ServiceAddresses: []string{testServiceAddress},
		MaxMessageSize:   defaultMaxMessageSize,
	}
}

func runClientTest(
	t *testing.T,
	readOnly bool,
	cCfgFn func(bool) ClientConfig,
	fn func(*testing.T, *Service, ClientConfig, Client)) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewService(cfg,
		testutil.NewFS(),
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

	if cCfgFn == nil {
		cCfgFn = getClientConfig
	}
	scfg := cCfgFn(readOnly)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := NewClient(ctx, scfg)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	fn(t, service, scfg, c)
}

func TestClientConfigIsValidated(t *testing.T) {
	cfg := ClientConfig{}
	cc, err := NewClient(context.TODO(), cfg)
	assert.Nil(t, cc)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
}

func TestClientCanBeReset(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		client := c.(*managedClient)
		client.resetClient()
		assert.Nil(t, client.client)
	}
	runClientTest(t, false, nil, fn)
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
	runClientTest(t, false, nil, fn)
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
	runClientTest(t, false, nil, fn)
}

func TestClientCanBeCreated(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
	}
	runClientTest(t, false, nil, fn)
	runClientTest(t, true, nil, fn)
}

func TestClientCanBeConnectedByReverseProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := getServiceTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewService(cfg,
		testutil.NewFS(),
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
		DNReplicaID:      2,
		ServiceAddresses: []string{"localhost:53032"}, // unreachable
		DiscoveryAddress: testServiceAddress,
	}

	done := false
	for i := 0; i < 1000; i++ {
		si, ok, err := GetShardInfo(testServiceAddress, 1)
		if err != nil {
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClient(ctx, scfg)
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
	runClientTest(t, false, nil, fn)
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
		cmd = getAppendCmd(cmd, cfg.DNReplicaID+1)
		_, err = c.Append(ctx, pb.LogRecord{Data: cmd})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNotLeaseHolder))
	}
	runClientTest(t, false, nil, fn)
}

// FIXME: actually enforce allowed allocation
func TestClientAppendAlloc(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(16)
		rand.Read(rec.Payload())
		ac := testing.AllocsPerRun(1000, func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := c.Append(ctx, rec)
			require.NoError(t, err)
		})
		testLogger.Info(fmt.Sprintf("ac: %f", ac))
	}
	runClientTest(t, false, nil, fn)
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
	runClientTest(t, false, nil, fn)
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
	runClientTest(t, false, nil, fn)
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
	runClientTest(t, true, nil, fn)
}

func TestClientSendWithMsgSize(t *testing.T) {
	cFn := func(readOnly bool) ClientConfig {
		return ClientConfig{
			ReadOnly:         readOnly,
			LogShardID:       1,
			DNReplicaID:      2,
			ServiceAddresses: []string{testServiceAddress},
			MaxMessageSize:   testServerMaxMsgSize - 100,
		}
	}

	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(testServerMaxMsgSize - 80)
		rand.Read(rec.Payload())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.Append(ctx, rec)
		require.NoError(t, err)

		// client only accepts message whose size less than 190
		_, _, err = c.Read(ctx, 4, math.MaxUint64)
		require.Error(t, err)
	}
	runClientTest(t, false, cFn, fn)
}

func TestServerReceiveWithMsgSize(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(16)
		rand.Read(rec.Payload())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.Append(ctx, rec)
		require.NoError(t, err)

		rec = c.GetLogRecord(testServerMaxMsgSize + 20)
		rand.Read(rec.Payload())
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err = c.Append(ctx, rec)
		require.Error(t, err)
	}
	runClientTest(t, false, nil, fn)
}
