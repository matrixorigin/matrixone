// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/lni/goutils/leaktest"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

type testProxyHandler struct {
	ctx        context.Context
	st         *stopper.Stopper
	logger     *log.MOLogger
	hc         *mockHAKeeperClient
	mc         clusterservice.MOCluster
	re         *rebalancer
	ru         Router
	closeFn    func()
	counterSet *counterSet
}

func newTestProxyHandler(t *testing.T) *testProxyHandler {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	ctx, cancel := context.WithCancel(context.TODO())
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	logger := rt.Logger()
	st := stopper.NewStopper("test-proxy", stopper.WithLogger(rt.Logger().RawLogger()))
	re := testRebalancer(t, st, logger, mc)
	return &testProxyHandler{
		ctx:    ctx,
		st:     st,
		logger: logger,
		hc:     hc,
		mc:     mc,
		re:     re,
		ru:     newRouter(mc, re, re.connManager, false),
		closeFn: func() {
			mc.Close()
			st.Stop()
			cancel()
		},
		counterSet: newCounterSet(),
	}
}

func certGen(basePath string) (*tlsConfig, error) {
	max := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, _ := rand.Int(rand.Reader, max)
	subject := pkix.Name{
		Country:            []string{"CN"},
		Province:           []string{"SH"},
		Organization:       []string{"MO"},
		OrganizationalUnit: []string{"Dev"},
	}

	// set up CA certificate
	ca := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      subject,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		IsCA:         true,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	// create our private and public key
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	// create the CA
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, err
	}

	// pem encode
	caFile := basePath + "/ca.pem"
	caOut, _ := os.Create(caFile)
	if err := pem.Encode(caOut, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	}); err != nil {
		return nil, err
	}
	defer func() {
		_ = caOut.Close()
	}()

	// set up server certificate
	cert := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      subject,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, err
	}

	certFile := basePath + "/server-cert.pem"
	certOut, _ := os.Create(certFile)
	if err := pem.Encode(certOut, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	}); err != nil {
		return nil, err
	}
	defer func() {
		_ = certOut.Close()
	}()

	keyFile := basePath + "/server-key.pem"
	keyOut, _ := os.Create(keyFile)
	if err := pem.Encode(keyOut, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
	}); err != nil {
		return nil, err
	}
	defer func() {
		_ = keyOut.Close()
	}()

	return &tlsConfig{
		caFile:   caFile,
		certFile: certFile,
		keyFile:  keyFile,
	}, nil
}

func TestHandler_Handle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	temp := os.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	listenAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(listenAddr))
	cfg := Config{
		ListenAddress:     "unix://" + listenAddr,
		RebalanceDisabled: true,
	}
	frontend.InitServerLevelVars("")
	frontend.SetSessionAlloc("", frontend.NewSessionAllocator(newTestPu()))
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	addr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr))
	cn1 := testMakeCNServer("cn11", addr, 0, "", labelInfo{})
	frontend.InitServerLevelVars(cn1.uuid)
	frontend.SetSessionAlloc(cn1.uuid, frontend.NewSessionAllocator(newTestPu()))
	hc.updateCN(cn1.uuid, cn1.addr, map[string]metadata.LabelList{})
	// start backend server.
	stopFn := startTestCNServer(t, ctx, addr, nil, withService(cn1.uuid))
	defer func() {
		require.NoError(t, stopFn())
	}()
	mc.ForceRefresh(true)

	// start proxy.
	s, err := NewServer(ctx, cfg, WithRuntime(runtime.DefaultRuntime()),
		WithHAKeeperClient(hc))
	defer func() {
		err := s.Close()
		require.NoError(t, err)
	}()
	require.NoError(t, err)
	require.NotNil(t, s)
	err = s.Start()
	require.NoError(t, err)

	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@unix(%s)/db1", listenAddr))
	// connect to server.
	require.NoError(t, err)
	require.NotNil(t, db)
	defer func() {
		_ = db.Close()
		timeout := time.NewTimer(time.Second * 15)
		tick := time.NewTicker(time.Millisecond * 100)
		var connTotal int64
		tt := false
		for {
			select {
			case <-tick.C:
				connTotal = s.counterSet.connTotal.Load()
			case <-timeout.C:
				tt = true
			}
			if connTotal == 0 || tt {
				break
			}
		}
		tick.Stop()
		timeout.Stop()
		require.Equal(t, int64(0), connTotal)
	}()
	_, err = db.Exec("anystmt")
	require.NoError(t, err)

	require.Equal(t, int64(1), s.counterSet.connAccepted.Load())
	require.Equal(t, int64(1), s.counterSet.connTotal.Load())
}

func TestHandlerCurrentConnections(t *testing.T) {
	testWithServer(t, func(t *testing.T, addr string, s *Server) {
		const connectionCount = 3
		before := testutil.ToFloat64(metricv2.ProxyConnectionsCurrentGauge)
		closedBefore := testutil.ToFloat64(metricv2.ProxyConnectClosedCounter)
		dbs := make([]*sql.DB, 0, connectionCount)
		defer func() {
			for _, db := range dbs {
				_ = db.Close()
			}
		}()

		for i := 0; i < connectionCount; i++ {
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@unix(%s)/db1", addr))
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			dbs = append(dbs, db)
			_, err = db.Exec("select 1")
			require.NoError(t, err)
		}

		require.Eventually(t, func() bool {
			return s.counterSet.connTotal.Load() == connectionCount &&
				testutil.ToFloat64(metricv2.ProxyConnectionsCurrentGauge) == before+connectionCount &&
				testutil.ToFloat64(metricv2.ProxyConnectClosedCounter) == closedBefore
		}, 5*time.Second, 10*time.Millisecond)

		for i, db := range dbs {
			require.NoError(t, db.Close())
			expected := connectionCount - i - 1
			require.Eventually(t, func() bool {
				return s.counterSet.connTotal.Load() == int64(expected) &&
					testutil.ToFloat64(metricv2.ProxyConnectionsCurrentGauge) == before+float64(expected) &&
					testutil.ToFloat64(metricv2.ProxyConnectClosedCounter) == closedBefore+float64(i+1)
			}, 5*time.Second, 10*time.Millisecond)
		}
	})
}

func TestHandler_HandleErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	currentBefore := testutil.ToFloat64(metricv2.ProxyConnectionsCurrentGauge)
	closedBefore := testutil.ToFloat64(metricv2.ProxyConnectClosedCounter)

	temp := os.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	listenAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(listenAddr))
	cfg := Config{
		ListenAddress:     "unix://" + listenAddr,
		RebalanceDisabled: true,
	}
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	addr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr))

	// start proxy.
	s, err := NewServer(ctx, cfg, WithRuntime(runtime.DefaultRuntime()),
		WithHAKeeperClient(hc))
	defer func() {
		err := s.Close()
		require.NoError(t, err)
	}()
	require.NoError(t, err)
	require.NotNil(t, s)
	err = s.Start()
	require.NoError(t, err)

	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@unix(%s)/db1", listenAddr))
	// connect to server.
	require.NoError(t, err)
	require.NotNil(t, db)
	defer func() {
		_ = db.Close()
		timeout := time.NewTimer(time.Second * 15)
		tick := time.NewTicker(time.Millisecond * 100)
		var connTotal int64
		tt := false
		for {
			select {
			case <-tick.C:
				connTotal = s.counterSet.connTotal.Load()
			case <-timeout.C:
				tt = true
			}
			if connTotal == 0 || tt {
				break
			}
		}
		tick.Stop()
		timeout.Stop()
		require.Equal(t, int64(0), connTotal)
	}()
	_, err = db.Exec("anystmt")
	require.Error(t, err)
	require.Eventually(t, func() bool {
		return s.counterSet.connTotal.Load() == 0 &&
			testutil.ToFloat64(metricv2.ProxyConnectionsCurrentGauge) == currentBefore &&
			testutil.ToFloat64(metricv2.ProxyConnectClosedCounter) == closedBefore+1
	}, 5*time.Second, 10*time.Millisecond)

	require.Equal(t, int64(1), s.counterSet.connAccepted.Load())
}

func TestHandler_cleanupBackendOnClientDisconnect(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	h := &handler{logger: rt.Logger()}

	killCalled := 0
	closeCalled := 0
	expectedSC := &killCurrentServerConn{
		cn: &CNServer{connID: 11, uuid: "cn-new"},
		closeFn: func() error {
			closeCalled++
			return nil
		},
	}
	tun := &tunnel{}
	tun.mu.sc = expectedSC
	markTunnelInFlight(tun)
	cc := &mockClientConn{
		killFn: func(sc ServerConn) error {
			require.Same(t, expectedSC, sc)
			killCalled++
			return nil
		},
	}

	h.cleanupBackendOnClientDisconnect(withCode(io.EOF, codeClientDisconnect), cc, tun)
	require.Equal(t, 1, killCalled)
	require.Equal(t, 1, closeCalled)

	h.cleanupBackendOnClientDisconnect(withCode(io.EOF, codeServerDisconnect), cc, tun)
	h.cleanupBackendOnClientDisconnect(io.EOF, cc, tun)
	require.Equal(t, 1, killCalled)
	require.Equal(t, 1, closeCalled)

	h.cleanupBackendOnClientDisconnect(withCode(moerr.NewInternalErrorNoCtx("send message error: connection reset by peer"), codeClientDisconnect), cc, tun)
	require.Equal(t, 2, killCalled)
	require.Equal(t, 2, closeCalled)
}

func TestHandler_handleTunnelErrCleansUpWrappedClientDisconnect(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	h := &handler{
		logger:     rt.Logger(),
		counterSet: newCounterSet(),
	}

	called := 0
	closeCalled := 0
	expectedSC := &killCurrentServerConn{
		cn: &CNServer{connID: 11, uuid: "cn-new"},
		closeFn: func() error {
			closeCalled++
			return nil
		},
	}
	tun := &tunnel{}
	tun.mu.sc = expectedSC
	cc := &mockClientConn{
		killFn: func(sc ServerConn) error {
			require.Same(t, expectedSC, sc)
			called++
			return nil
		},
	}

	err := withCode(moerr.NewInternalErrorNoCtx("send message error: connection reset by peer"), codeClientDisconnect)
	ret := h.handleTunnelErr(err, cc, tun, 733923, 100)
	require.Same(t, err, ret)
	require.Equal(t, 1, called)
	require.Equal(t, 1, closeCalled)
	require.Equal(t, int64(1), h.counterSet.clientDisconnect.Load())
}

func TestHandler_handleTunnelErrClosesBackendForIdleEOFClientDisconnect(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	h := &handler{
		logger:     rt.Logger(),
		counterSet: newCounterSet(),
	}

	killCalled := 0
	closeCalled := 0
	tun := &tunnel{}
	tun.mu.sc = &killCurrentServerConn{
		cn: &CNServer{connID: 11, uuid: "cn-new"},
		closeFn: func() error {
			closeCalled++
			return nil
		},
	}
	cc := &mockClientConn{
		killFn: func(sc ServerConn) error {
			killCalled++
			return nil
		},
	}

	ret := h.handleTunnelErr(withCode(io.EOF, codeClientDisconnect), cc, tun, 733923, 100)
	require.NoError(t, ret)
	require.Equal(t, 0, killCalled)
	require.Equal(t, 1, closeCalled)
	require.Equal(t, int64(0), h.counterSet.clientDisconnect.Load())
}

func TestHandler_handleTunnelErrKillsBackendForInFlightEOFClientDisconnect(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	h := &handler{
		logger:     rt.Logger(),
		counterSet: newCounterSet(),
	}

	killCalled := 0
	closeCalled := 0
	tun := &tunnel{}
	markTunnelInFlight(tun)
	tun.mu.sc = &killCurrentServerConn{
		cn: &CNServer{connID: 11, uuid: "cn-new"},
		closeFn: func() error {
			closeCalled++
			return nil
		},
	}
	cc := &mockClientConn{
		killFn: func(sc ServerConn) error {
			killCalled++
			return nil
		},
	}

	ret := h.handleTunnelErr(withCode(io.EOF, codeClientDisconnect), cc, tun, 733923, 100)
	require.NoError(t, ret)
	require.Equal(t, 1, killCalled)
	require.Equal(t, 1, closeCalled)
	require.Equal(t, int64(0), h.counterSet.clientDisconnect.Load())
}

func TestHandler_handleTunnelErrKillsBackendForTrackedInFlightClientRequest(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	h := &handler{
		logger:     rt.Logger(),
		counterSet: newCounterSet(),
	}

	killCalled := 0
	closeCalled := 0
	tun := &tunnel{}
	// Exercise the explicit request-in-flight marker without relying on the
	// c2s/s2c lastCmdTime heuristic.
	tun.markClientRequestInFlight()
	tun.mu.sc = &killCurrentServerConn{
		cn: &CNServer{connID: 11, uuid: "cn-new"},
		closeFn: func() error {
			closeCalled++
			return nil
		},
	}
	cc := &mockClientConn{
		killFn: func(sc ServerConn) error {
			killCalled++
			return nil
		},
	}

	ret := h.handleTunnelErr(withCode(io.EOF, codeClientDisconnect), cc, tun, 733923, 100)
	require.NoError(t, ret)
	require.Equal(t, 1, killCalled)
	require.Equal(t, 1, closeCalled)
	require.Equal(t, int64(0), h.counterSet.clientDisconnect.Load())
}

func TestHandler_handleTunnelErrClosesBackendForWrappedConnEndClientDisconnectWithoutInFlight(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	h := &handler{
		logger:     rt.Logger(),
		counterSet: newCounterSet(),
	}

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "net.ErrClosed",
			err:  withCode(net.ErrClosed, codeClientDisconnect),
		},
		{
			name: "syscall.ECONNRESET",
			err:  withCode(syscall.ECONNRESET, codeClientDisconnect),
		},
		{
			name: "joined send net.ErrClosed",
			err: withCode(
				errors.Join(
					moerr.NewInternalErrorNoCtxf("send message error: %v", net.ErrClosed),
					net.ErrClosed,
				),
				codeClientDisconnect,
			),
		},
		{
			name: "joined send syscall.ECONNRESET",
			err: withCode(
				errors.Join(
					moerr.NewInternalErrorNoCtxf("send message error: %v", syscall.ECONNRESET),
					syscall.ECONNRESET,
				),
				codeClientDisconnect,
			),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			killCalled := 0
			closeCalled := 0
			tun := &tunnel{}
			tun.mu.sc = &killCurrentServerConn{
				cn: &CNServer{connID: 11, uuid: "cn-new"},
				closeFn: func() error {
					closeCalled++
					return nil
				},
			}
			cc := &mockClientConn{
				killFn: func(sc ServerConn) error {
					killCalled++
					return nil
				},
			}

			ret := h.handleTunnelErr(tc.err, cc, tun, 733923, 100)
			require.NoError(t, ret)
			require.Equal(t, 0, killCalled)
			require.Equal(t, 1, closeCalled)
			require.Equal(t, int64(0), h.counterSet.clientDisconnect.Load())
		})
	}
}

func TestHandler_handleTunnelErrSkipsBackendCleanupForExpectedCacheQuit(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	h := &handler{
		logger:     rt.Logger(),
		counterSet: newCounterSet(),
	}

	killCalled := 0
	closeCalled := 0
	tun := &tunnel{}
	tun.markExpectedCacheQuit()
	tun.mu.sc = &killCurrentServerConn{
		cn: &CNServer{connID: 11, uuid: "cn-new"},
		closeFn: func() error {
			closeCalled++
			return nil
		},
	}
	cc := &mockClientConn{
		killFn: func(sc ServerConn) error {
			killCalled++
			return nil
		},
	}

	ret := h.handleTunnelErr(withCode(io.EOF, codeClientDisconnect), cc, tun, 733923, 100)
	require.NoError(t, ret)
	require.Equal(t, 0, killCalled)
	require.Equal(t, 0, closeCalled)
	require.Equal(t, int64(0), h.counterSet.clientDisconnect.Load())
}

func TestHandler_handleTunnelErrSkipsKillForForwardedClientQuit(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	h := &handler{
		logger:     rt.Logger(),
		counterSet: newCounterSet(),
	}

	killCalled := 0
	closeCalled := 0
	tun := &tunnel{}
	tun.markExpectedClientQuit()
	markTunnelInFlight(tun)
	tun.mu.sc = &killCurrentServerConn{
		cn: &CNServer{connID: 11, uuid: "cn-new"},
		closeFn: func() error {
			closeCalled++
			return nil
		},
	}
	cc := &mockClientConn{
		killFn: func(sc ServerConn) error {
			killCalled++
			return nil
		},
	}

	ret := h.handleTunnelErr(withCode(io.EOF, codeClientDisconnect), cc, tun, 733923, 100)
	require.NoError(t, ret)
	require.Equal(t, 0, killCalled)
	require.Equal(t, 1, closeCalled)
	require.Equal(t, int64(0), h.counterSet.clientDisconnect.Load())
}

func TestHandler_handleTunnelErrDiscardsBackendWhenKillFails(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	h := &handler{
		logger:     rt.Logger(),
		counterSet: newCounterSet(),
	}

	killCalled := 0
	closeCalled := 0
	tun := &tunnel{}
	markTunnelInFlight(tun)
	tun.mu.sc = &killCurrentServerConn{
		cn: &CNServer{connID: 11, uuid: "cn-new"},
		closeFn: func() error {
			closeCalled++
			return nil
		},
	}
	cc := &mockClientConn{
		killFn: func(sc ServerConn) error {
			killCalled++
			return moerr.NewInternalErrorNoCtx("kill failed")
		},
	}

	ret := h.handleTunnelErr(withCode(io.EOF, codeClientDisconnect), cc, tun, 733923, 100)
	require.NoError(t, ret)
	require.Equal(t, 1, killCalled)
	require.Equal(t, 1, closeCalled)
	require.Equal(t, int64(0), h.counterSet.clientDisconnect.Load())
}

func TestHandler_handleTunnelErrDoesNotKillConcurrentIdleEOFs(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	h := &handler{
		logger:     rt.Logger(),
		counterSet: newCounterSet(),
	}

	const n = 64
	var wg sync.WaitGroup
	var killCalled atomic.Int32
	var closeCalled atomic.Int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tun := &tunnel{}
			tun.mu.sc = &killCurrentServerConn{
				cn: &CNServer{connID: 11, uuid: "cn-new"},
				closeFn: func() error {
					closeCalled.Add(1)
					return nil
				},
			}
			cc := &mockClientConn{
				killFn: func(sc ServerConn) error {
					killCalled.Add(1)
					return nil
				},
			}

			ret := h.handleTunnelErr(withCode(io.EOF, codeClientDisconnect), cc, tun, 733923, 100)
			require.NoError(t, ret)
		}()
	}
	wg.Wait()
	require.Equal(t, int32(0), killCalled.Load())
	require.Equal(t, int32(n), closeCalled.Load())
}

func TestHandler_handleTunnelErrSkipsConnCacheCleanupAfterAbortKill(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	h := &handler{
		logger:     rt.Logger(),
		counterSet: newCounterSet(),
		connCache:  &mockConnCache{},
	}

	closeCalled := 0
	currentCN := &CNServer{
		connID: 10,
		uuid:   "cn1",
		addr:   "127.0.0.1:6001",
		salt:   testSlat,
	}
	currentSC := &killCurrentServerConn{
		cn: currentCN,
		closeFn: func() error {
			closeCalled++
			return nil
		},
	}
	execSC := &killExecServerConn{}
	tun := &tunnel{}
	markTunnelInFlight(tun)
	tun.mu.sc = currentSC
	tun.mu.scp = &pipe{}
	tun.mu.scp.mu.cond = sync.NewCond(&tun.mu.scp.mu)
	cc := &clientConn{
		connID: 42,
		log:    rt.Logger(),
		router: &killTestRouter{
			connectFn: func(c *CNServer, handshakeResp *frontend.Packet, tun *tunnel) (ServerConn, []byte, error) {
				require.Equal(t, currentCN.uuid, c.uuid)
				require.Equal(t, currentCN.addr, c.addr)
				return execSC, makeOKPacket(8), nil
			},
		},
		tun: tun,
		sc:  currentSC,
		connCache: &mockConnCache{
			pushFn: func(key cacheKey, sc ServerConn) bool {
				t.Fatalf("killed backend connection must not be returned to conn cache")
				return false
			},
		},
	}

	ret := h.handleTunnelErr(withCode(io.EOF, codeClientDisconnect), cc, tun, 733923, 100)
	require.NoError(t, ret)
	require.Equal(t, cmdQuery, execSC.stmt.cmdType)
	require.Equal(t, "kill connection 42", execSC.stmt.s)
	require.Equal(t, 1, closeCalled)
	require.Equal(t, int64(0), h.counterSet.clientDisconnect.Load())
}

func TestHandler_handleTunnelErrSkipsConnCacheCleanupAfterAbortKillFailure(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	h := &handler{
		logger:     rt.Logger(),
		counterSet: newCounterSet(),
		connCache:  &mockConnCache{},
	}

	closeCalled := 0
	currentSC := &killCurrentServerConn{
		cn: &CNServer{
			connID: 10,
			uuid:   "cn1",
			addr:   "127.0.0.1:6001",
			salt:   testSlat,
		},
		closeFn: func() error {
			closeCalled++
			return nil
		},
	}
	tun := &tunnel{}
	markTunnelInFlight(tun)
	tun.mu.sc = currentSC
	tun.mu.scp = &pipe{}
	tun.mu.scp.mu.cond = sync.NewCond(&tun.mu.scp.mu)
	cc := &clientConn{
		connID: 42,
		log:    rt.Logger(),
		router: &killTestRouter{
			connectFn: func(c *CNServer, handshakeResp *frontend.Packet, tun *tunnel) (ServerConn, []byte, error) {
				return nil, nil, moerr.NewInternalErrorNoCtx("kill connect failed")
			},
		},
		tun: tun,
		sc:  currentSC,
		connCache: &mockConnCache{
			pushFn: func(key cacheKey, sc ServerConn) bool {
				t.Fatalf("backend connection must not be returned to conn cache after kill failure")
				return false
			},
		},
	}

	ret := h.handleTunnelErr(withCode(io.EOF, codeClientDisconnect), cc, tun, 733923, 100)
	require.NoError(t, ret)
	require.Equal(t, 1, closeCalled)
	require.Equal(t, int64(0), h.counterSet.clientDisconnect.Load())
}

func markTunnelInFlight(tun *tunnel) {
	now := time.Now()
	tun.mu.csp = &pipe{}
	tun.mu.scp = &pipe{}
	tun.mu.csp.mu.lastCmdTime = now
	tun.mu.scp.mu.lastCmdTime = now.Add(-time.Second)
}

func TestHandler_HandleWithSSL(t *testing.T) {
	defer leaktest.AfterTest(t)()

	temp := os.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	listenAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(listenAddr))
	cfg := Config{
		ListenAddress:     "unix://" + listenAddr,
		RebalanceDisabled: true,
	}
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	addr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr))
	cn1 := testMakeCNServer("cn11", addr, 0, "", labelInfo{})
	hc.updateCN(cn1.uuid, cn1.addr, map[string]metadata.LabelList{})

	tlsC, err := certGen(temp)
	require.NoError(t, err)
	tlsC.enabled = true
	// start backend server.
	stopFn := startTestCNServer(t, ctx, addr, tlsC)
	defer func() {
		require.NoError(t, stopFn())
	}()
	mc.ForceRefresh(true)

	// start proxy.
	s, err := NewServer(ctx, cfg, WithRuntime(runtime.DefaultRuntime()),
		WithHAKeeperClient(hc),
		WithTLSEnabled(),
		WithTLSCAFile(tlsC.caFile),
		WithTLSCertFile(tlsC.certFile),
		WithTLSKeyFile(tlsC.keyFile))
	defer func() {
		err := s.Close()
		require.NoError(t, err)
	}()
	require.NoError(t, err)
	require.NotNil(t, s)
	err = s.Start()
	require.NoError(t, err)

	rootCertPool := x509.NewCertPool()

	pem1, err := os.ReadFile(tlsC.caFile)
	require.NoError(t, err)

	ok := rootCertPool.AppendCertsFromPEM(pem1)
	require.True(t, ok)

	err = mysql.RegisterTLSConfig("custom", &tls.Config{
		RootCAs:            rootCertPool,
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)

	db, err := sql.Open("mysql",
		fmt.Sprintf("dump:111@unix(%s)/db1?tls=custom", listenAddr))
	// connect to server.
	require.NoError(t, err)
	require.NotNil(t, db)
	defer func() {
		_ = db.Close()
	}()
	_, _ = db.Exec("any stmt")
	_, err = db.Exec("any stmt")
	require.NoError(t, err)
	require.Equal(t, int64(1), s.counterSet.connAccepted.Load())
	require.Equal(t, int64(1), s.counterSet.connTotal.Load())
}

func testWithServer(t *testing.T, fn func(*testing.T, string, *Server)) {
	defer leaktest.AfterTest(t)()

	temp := os.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	listenAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(listenAddr))
	cfg := Config{
		ListenAddress:     "unix://" + listenAddr,
		RebalanceDisabled: true,
	}
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster("", hc, 3*time.Second)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)
	addr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr))
	cn1 := testMakeCNServer("cn11", addr, 0, "", labelInfo{})
	hc.updateCN(cn1.uuid, cn1.addr, map[string]metadata.LabelList{})
	// start backend server.
	stopFn := startTestCNServer(t, ctx, addr, nil)
	defer func() {
		require.NoError(t, stopFn())
	}()
	mc.ForceRefresh(true)

	// start proxy.
	s, err := NewServer(ctx, cfg, WithRuntime(runtime.DefaultRuntime()),
		WithHAKeeperClient(hc), WithTest())
	defer func() {
		err := s.Close()
		require.NoError(t, err)
	}()
	require.NoError(t, err)
	require.NotNil(t, s)
	err = s.Start()
	require.NoError(t, err)

	fn(t, listenAddr, s)
}

func TestHandler_HandleEventKill(t *testing.T) {
	testWithServer(t, func(t *testing.T, addr string, s *Server) {
		db1, err := sql.Open("mysql", fmt.Sprintf("dump:111@unix(%s)/db1", addr))
		// connect to server.
		require.NoError(t, err)
		require.NotNil(t, db1)
		defer func() {
			_ = db1.Close()
		}()
		res, err := db1.Exec("select 1")
		require.NoError(t, err)
		connID, _ := res.LastInsertId() // fake connection id

		db2, err := sql.Open("mysql", fmt.Sprintf("dump:111@unix(%s)/db1", addr))
		// connect to server.
		require.NoError(t, err)
		require.NotNil(t, db2)
		defer func() {
			_ = db2.Close()
		}()

		_, err = db2.Exec(fmt.Sprintf("kill query %d", connID))
		require.NoError(t, err)

		require.Equal(t, int64(2), s.counterSet.connAccepted.Load())
	})
}

func TestHandler_HandleEventSetVar(t *testing.T) {
	testWithServer(t, func(t *testing.T, addr string, s *Server) {
		db1, err := sql.Open("mysql", fmt.Sprintf("dump:111@unix(%s)/db1", addr))
		// connect to server.
		require.NoError(t, err)
		require.NotNil(t, db1)
		defer func() {
			_ = db1.Close()
		}()
		_, err = db1.Exec("set session cn_label='acc1'")
		require.NoError(t, err)

		res, err := db1.Query("show session variables")
		require.NoError(t, err)
		defer res.Close()
		var varName, varValue string
		for res.Next() {
			err := res.Scan(&varName, &varValue)
			require.NoError(t, err)
			require.Equal(t, "cn_label", varName)
			require.Equal(t, "acc1", varValue)
		}
		err = res.Err()
		require.NoError(t, err)

		require.Equal(t, int64(1), s.counterSet.connAccepted.Load())
	})
}

func TestHandler_HandleTxn(t *testing.T) {
	testWithServer(t, func(t *testing.T, addr string, s *Server) {
		db1, err := sql.Open("mysql", fmt.Sprintf("a1#root:111@unix(%s)/db1", addr))
		// connect to server.
		require.NoError(t, err)
		require.NotNil(t, db1)
		defer func() {
			_ = db1.Close()
		}()
		_, err = db1.Exec("select 1")
		require.NoError(t, err)
	})
}

func TestHandler_HandleEventUpgrade(t *testing.T) {
	testWithServer(t, func(t *testing.T, addr string, s *Server) {
		db1, err := sql.Open("mysql", fmt.Sprintf("dump:111@unix(%s)/db1", addr))
		// connect to server.
		require.NoError(t, err)
		require.NotNil(t, db1)
		defer func() {
			_ = db1.Close()
		}()
		_, err = db1.Exec("upgrade account all")
		require.NoError(t, err)

		require.Equal(t, int64(1), s.counterSet.connAccepted.Load())
	})
}
