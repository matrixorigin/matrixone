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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	var cfg Config
	genCfg := func() Config {
		cfg = getServiceTestConfig()
		return cfg
	}
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewServiceWithRetry(
		genCfg,
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

	svcAddress := cfg.LogServiceServiceAddr()
	scfg := ClientConfig{
		LogShardID:       1,
		TNReplicaID:      2,
		ServiceAddresses: []string{"localhost:53032"}, // unreachable
		DiscoveryAddress: svcAddress,
	}

	done := false
	for i := 0; i < 1000; i++ {
		si, ok, err := GetShardInfo("", svcAddress, 1)
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
		assert.Equal(t, svcAddress, addr)
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

func TestClientCreationFallsBackFromUnreachableReplicaAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var cfg Config
	genCfg := func() Config {
		cfg = getServiceTestConfig()
		return cfg
	}
	defer vfs.ReportLeakedFD(cfg.FS, t)
	service, err := NewServiceWithRetry(
		genCfg,
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
	require.NoError(t, service.store.startReplica(1, 2, init, false))

	hangingAddress, cleanup := startHangingTCPServer(t)
	defer cleanup()

	scfg := ClientConfig{
		LogShardID:     1,
		TNReplicaID:    2,
		MaxMessageSize: defaultMaxMessageSize,
	}

	origFallbackDelay := logServiceConnectFallbackDelay
	logServiceConnectFallbackDelay = 50 * time.Millisecond
	defer func() { logServiceConnectFallbackDelay = origFallbackDelay }()

	timeout := 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	c, err := connectToLogServiceAddresses(ctx, "", []string{
		hangingAddress,
		cfg.LogServiceServiceAddr(),
	}, scfg)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotNil(t, c)
	defer func() {
		assert.NoError(t, c.close())
	}()
	assert.Less(t, elapsed, time.Second)
}

func TestShardInfoReplicaAddressesKeepsLeaderFirst(t *testing.T) {
	addresses := shardInfoReplicaAddresses(ShardInfo{
		ReplicaID: 2,
		Replicas: map[uint64]string{
			1: "replica-1",
			2: "leader",
			3: "replica-3",
		},
	})
	require.Len(t, addresses, 3)
	assert.Equal(t, "leader", addresses[0])
	assert.ElementsMatch(t, []string{"leader", "replica-1", "replica-3"}, addresses)
}

func TestConnectToLogServiceWithNoTargets(t *testing.T) {
	c, err := connectToLogService(context.Background(), "", nil, ClientConfig{})
	require.NoError(t, err)
	require.Nil(t, c)
}

func TestConnectToLogServiceAddressesReturnsLastError(t *testing.T) {
	origAttemptTimeout := logServiceConnectAttemptTimeout
	origFallbackDelay := logServiceConnectFallbackDelay
	logServiceConnectAttemptTimeout = 50 * time.Millisecond
	logServiceConnectFallbackDelay = time.Millisecond
	defer func() {
		logServiceConnectAttemptTimeout = origAttemptTimeout
		logServiceConnectFallbackDelay = origFallbackDelay
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := connectToLogServiceAddresses(ctx, "", []string{
		"127.0.0.1:1",
		"127.0.0.1:2",
	}, ClientConfig{MaxMessageSize: defaultMaxMessageSize})
	require.Error(t, err)
	require.Nil(t, c)
}

func TestConnectToLogServiceAddressesReturnsContextError(t *testing.T) {
	hangingAddress, cleanup := startHangingTCPServer(t)
	defer cleanup()

	origAttemptTimeout := logServiceConnectAttemptTimeout
	origFallbackDelay := logServiceConnectFallbackDelay
	logServiceConnectAttemptTimeout = time.Second
	logServiceConnectFallbackDelay = time.Second
	defer func() {
		logServiceConnectAttemptTimeout = origAttemptTimeout
		logServiceConnectFallbackDelay = origFallbackDelay
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	c, err := connectToLogServiceAddresses(
		ctx,
		"",
		[]string{hangingAddress},
		ClientConfig{MaxMessageSize: defaultMaxMessageSize},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, c)
}

func TestConnectToLogServiceAddressesReturnsContextErrorAfterPreviousFailure(t *testing.T) {
	origConnect := connectToLogServiceAddressFn
	origFallbackDelay := logServiceConnectFallbackDelay
	logServiceConnectFallbackDelay = time.Millisecond
	defer func() {
		connectToLogServiceAddressFn = origConnect
		logServiceConnectFallbackDelay = origFallbackDelay
	}()

	firstErr := errors.New("first failed")
	connectToLogServiceAddressFn = func(
		ctx context.Context,
		sid string,
		addr string,
		cfg ClientConfig,
	) (*client, error) {
		if addr == "first" {
			return nil, firstErr
		}
		<-ctx.Done()
		time.Sleep(10 * time.Millisecond)
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	c, err := connectToLogServiceAddresses(
		ctx,
		"",
		[]string{"first", "second"},
		ClientConfig{},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, firstErr.Error())
	require.Nil(t, c)
}

func TestConnectToLogServiceAddressesWithCanceledContextDoesNotDial(t *testing.T) {
	origConnect := connectToLogServiceAddressFn
	defer func() {
		connectToLogServiceAddressFn = origConnect
	}()

	called := make(chan struct{}, 1)
	connectToLogServiceAddressFn = func(
		ctx context.Context,
		sid string,
		addr string,
		cfg ClientConfig,
	) (*client, error) {
		called <- struct{}{}
		return nil, errors.New("unexpected dial")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c, err := connectToLogServiceAddresses(
		ctx,
		"",
		[]string{"first"},
		ClientConfig{},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, c)
	select {
	case <-called:
		t.Fatal("unexpected connection attempt")
	default:
	}
}

func TestConnectToLogServiceAddressesClosesSuccessfulLosers(t *testing.T) {
	origConnect := connectToLogServiceAddressFn
	origFallbackDelay := logServiceConnectFallbackDelay
	logServiceConnectFallbackDelay = time.Millisecond
	defer func() {
		connectToLogServiceAddressFn = origConnect
		logServiceConnectFallbackDelay = origFallbackDelay
	}()

	called := make(chan string, 2)
	release := make(chan struct{})
	closeCount := 0
	var closeMu sync.Mutex
	connectToLogServiceAddressFn = func(
		ctx context.Context,
		sid string,
		addr string,
		cfg ClientConfig,
	) (*client, error) {
		called <- addr
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &client{
			client: &closeTrackingRPCClient{
				closeMu:    &closeMu,
				closeCount: &closeCount,
			},
		}, nil
	}

	done := make(chan *client, 1)
	errs := make(chan error, 1)
	go func() {
		c, err := connectToLogServiceAddresses(
			context.Background(),
			"",
			[]string{"first", "second"},
			ClientConfig{},
		)
		if err != nil {
			errs <- err
			return
		}
		done <- c
	}()

	require.Equal(t, "first", readCalledAddress(t, called))
	require.Equal(t, "second", readCalledAddress(t, called))
	close(release)

	var c *client
	select {
	case c = <-done:
	case err := <-errs:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connection result")
	}
	require.NotNil(t, c)

	closeMu.Lock()
	assert.Equal(t, 1, closeCount)
	closeMu.Unlock()

	require.NoError(t, c.close())
	closeMu.Lock()
	assert.Equal(t, 2, closeCount)
	closeMu.Unlock()
}

func TestStopConnectFallbackTimer(t *testing.T) {
	stopConnectFallbackTimer(nil)

	timer := time.NewTimer(time.Hour)
	stopConnectFallbackTimer(timer)

	expired := time.NewTimer(time.Nanosecond)
	time.Sleep(time.Millisecond)
	stopConnectFallbackTimer(expired)
}

func readCalledAddress(t *testing.T, called <-chan string) string {
	t.Helper()
	select {
	case addr := <-called:
		return addr
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connection attempt")
	}
	return ""
}

type closeTrackingRPCClient struct {
	closeMu    *sync.Mutex
	closeCount *int
}

func (c *closeTrackingRPCClient) Send(context.Context, string, morpc.Message) (*morpc.Future, error) {
	return nil, errors.New("unexpected send")
}

func (c *closeTrackingRPCClient) NewStream(string, bool) (morpc.Stream, error) {
	return nil, errors.New("unexpected stream")
}

func (c *closeTrackingRPCClient) Ping(context.Context, string) error {
	return errors.New("unexpected ping")
}

func (c *closeTrackingRPCClient) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	(*c.closeCount)++
	return nil
}

func (c *closeTrackingRPCClient) CloseBackend() error {
	return nil
}

func startHangingTCPServer(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					assert.NoError(t, conn.Close())
				}()
				<-done
			}()
		}
	}()

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			assert.NoError(t, ln.Close())
			close(done)
			wg.Wait()
		})
	}
	return ln.Addr().String(), cleanup
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

func TestClientUpdateLeaseholderID(t *testing.T) {
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(16)
		rand.Read(rec.Payload())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()
		lsn, err := c.Append(ctx, rec)
		require.NoError(t, err)
		assert.Equal(t, uint64(4), lsn)

		var newLeaseholderID uint64 = 922
		err = c.UpdateLeaseholderID(ctx, newLeaseholderID)
		require.NoError(t, err)

		_, err = c.Append(ctx, rec)
		require.Error(t, err)

		c.(*managedClient).cfg.TNReplicaID = newLeaseholderID
		rec = c.GetLogRecord(16)
		lsn, err = c.Append(ctx, rec)
		require.NoError(t, err)
		assert.Equal(t, uint64(8), lsn)
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

func TestClientReadLsn(t *testing.T) {
	orig := defaultLogDBMaxLogFileSize
	defaultLogDBMaxLogFileSize = 500
	defaultArchiverEnabled = true
	defer func() {
		defaultArchiverEnabled = false
		defaultLogDBMaxLogFileSize = orig
	}()
	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		var lsn uint64
		for i := 0; i < 50; i++ {
			rec := c.GetLogRecord(16)
			n, err := rand.Read(rec.Payload())
			assert.Equal(t, n, 16)
			assert.NoError(t, err)
			lsn, err = c.Append(ctx, rec)
			require.NoError(t, err)
			assert.Equal(t, uint64(4+i), lsn)
		}
		searchTime := time.Now()
		for i := 0; i < 50; i++ {
			rec := c.GetLogRecord(16)
			n, err := rand.Read(rec.Payload())
			assert.Equal(t, n, 16)
			assert.NoError(t, err)
			lsn, err = c.Append(ctx, rec)
			require.NoError(t, err)
			assert.Equal(t, uint64(54+i), lsn)
		}

		// cannot read lsn by the ts
		readLsn, err := c.ReadLsn(ctx, time.Now())
		require.Error(t, err)
		assert.Equal(t, uint64(0), readLsn)

		opts := dragonboat.SnapshotOption{
			OverrideCompactionOverhead: true,
			CompactionIndex:            lsn - 1,
		}
		_, err = s.store.nh.SyncRequestSnapshot(ctx, 1, opts)
		assert.NoError(t, err)

		timeout := time.NewTimer(time.Second * 5)
		defer timeout.Stop()
		tick := time.NewTicker(time.Millisecond * 10)
		defer tick.Stop()
	FOR:
		for {
			select {
			case <-timeout.C:
				panic("the lsn is not valid")

			case <-tick.C:
				lsn, err = c.ReadLsn(ctx, searchTime)
				if err == nil && lsn != 0 {
					t.Logf("lsn is %d", lsn)
					break FOR
				}
			}
		}

		ents, lsn1, err := c.Read(ctx, lsn-10, math.MaxUint64)
		require.NoError(t, err)
		require.NotEqual(t, 0, len(ents))
		require.Equal(t, lsn-10, lsn1)
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
	cFn := func(readOnly bool, svcAddress ...string) ClientConfig {
		var addr string
		if len(svcAddress) > 0 {
			addr = svcAddress[0]
		}
		return ClientConfig{
			ReadOnly:         readOnly,
			LogShardID:       1,
			TNReplicaID:      2,
			ServiceAddresses: []string{addr},
			MaxMessageSize:   getTestServerMaxMsgSize(),
		}
	}

	fn := func(t *testing.T, s *Service, cfg ClientConfig, c Client) {
		rec := c.GetLogRecord(getTestServerMaxMsgSize() + 80)
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
		lsn, err = c.GetRequiredLsn(ctx)
		assert.NoError(t, err)
		assert.Equal(t, Lsn(100), lsn)
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
