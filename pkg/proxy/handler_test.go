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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
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
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	ctx, cancel := context.WithCancel(context.TODO())
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
	rt := runtime.DefaultRuntime()
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
		ru:     newRouter(mc, re, false),
		closeFn: func() {
			mc.Close()
			st.Stop()
			cancel()
		},
		counterSet: newCounterSet(),
	}
}

func TestHandler_Handle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	cfg := Config{
		ListenAddress:     "127.0.0.1:40019",
		RebalanceDisabled: true,
	}
	cfg.HAKeeper.ClientConfig.ServiceAddresses = []string{"127.0.0.1:8000"}
	hc := &mockHAKeeperClient{}
	addr := "127.0.0.1:48090"
	cn1 := testMakeCNServer("cn11", addr, 0, "", labelInfo{})
	hc.updateCN(cn1.uuid, cn1.addr, map[string]metadata.LabelList{})
	// start backend server.
	stopFn := startTestCNServer(t, ctx, addr)
	defer func() {
		require.NoError(t, stopFn())
	}()

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

	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(%s)/db1", cfg.ListenAddress))
	// connect to server.
	require.NoError(t, err)
	require.NotNil(t, db)
	defer func() {
		_ = db.Close()
		timeout := time.NewTimer(time.Second * 3)
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

func TestHandler_HandleWithSSL(t *testing.T) {
	t.Skip("ssl is not supported")
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	cfg := Config{
		ListenAddress:     "127.0.0.1:40029",
		RebalanceDisabled: true,
	}
	cfg.HAKeeper.ClientConfig.ServiceAddresses = []string{"127.0.0.1:8020"}
	hc := &mockHAKeeperClient{}
	addr := "127.0.0.1:48091"
	cn1 := testMakeCNServer("cn11", addr, 0, "", labelInfo{})
	hc.updateCN(cn1.uuid, cn1.addr, map[string]metadata.LabelList{})
	// start backend server.
	stopFn := startTestCNServer(t, ctx, addr)
	defer func() {
		require.NoError(t, stopFn())
	}()

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

	// generate a test certificate to use
	priv, _ := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)

	duration2Hours, _ := time.ParseDuration("-2h")
	notBefore := time.Now().Add(duration2Hours)
	duration2Hours, _ = time.ParseDuration("2h")
	notAfter := notBefore.Add(duration2Hours)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, _ := rand.Int(rand.Reader, serialNumberLimit)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"gIRC-Go Co"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	template.IPAddresses = append(template.IPAddresses, net.ParseIP("127.0.0.1"))
	template.IPAddresses = append(template.IPAddresses, net.ParseIP("::"))
	template.DNSNames = append(template.DNSNames, "localhost")

	derBytes, _ := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)

	c := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	rootCertPool := x509.NewCertPool()
	require.NoError(t, err)

	ok := rootCertPool.AppendCertsFromPEM(c)
	require.True(t, ok)
	err = mysql.RegisterTLSConfig("custom", &tls.Config{
		RootCAs: rootCertPool,
	})
	require.NoError(t, err)

	db, err := sql.Open("mysql",
		fmt.Sprintf("dump:111@tcp(%s)/db1?tls=custom", cfg.ListenAddress))
	// connect to server.
	require.NoError(t, err)
	require.NotNil(t, db)
	defer func() {
		_ = db.Close()
	}()
	_, err = db.Exec("anystmt")
	require.NoError(t, err)

	require.Equal(t, int64(1), s.counterSet.connAccepted.Load())
	require.Equal(t, int64(1), s.counterSet.connTotal.Load())
}

func TestHandler_HandleEventKillQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	cfg := Config{
		ListenAddress:     "127.0.0.1:40019",
		RebalanceDisabled: true,
	}
	cfg.HAKeeper.ClientConfig.ServiceAddresses = []string{"127.0.0.1:8000"}
	hc := &mockHAKeeperClient{}
	addr := "127.0.0.1:48190"
	cn1 := testMakeCNServer("cn11", addr, 0, "", labelInfo{})
	hc.updateCN(cn1.uuid, cn1.addr, map[string]metadata.LabelList{})
	// start backend server.
	stopFn := startTestCNServer(t, ctx, addr)
	defer func() {
		require.NoError(t, stopFn())
	}()

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

	db1, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(%s)/db1", cfg.ListenAddress))
	// connect to server.
	require.NoError(t, err)
	require.NotNil(t, db1)
	defer func() {
		_ = db1.Close()
	}()
	res, err := db1.Exec("select 1")
	require.NoError(t, err)
	connID, _ := res.LastInsertId() // fake connection id

	db2, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(%s)/db1", cfg.ListenAddress))
	// connect to server.
	require.NoError(t, err)
	require.NotNil(t, db2)
	defer func() {
		_ = db2.Close()
	}()

	_, err = db2.Exec("kill query 9999")
	require.Error(t, err)

	_, err = db2.Exec(fmt.Sprintf("kill query %d", connID))
	require.NoError(t, err)

	require.Equal(t, int64(2), s.counterSet.connAccepted.Load())
}

func TestHandler_HandleEventSetVar(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	cfg := Config{
		ListenAddress:     "127.0.0.1:40020",
		RebalanceDisabled: true,
	}
	cfg.HAKeeper.ClientConfig.ServiceAddresses = []string{"127.0.0.1:8000"}
	hc := &mockHAKeeperClient{}
	addr := "127.0.0.1:48190"
	cn1 := testMakeCNServer("cn11", addr, 0, "", labelInfo{})
	hc.updateCN(cn1.uuid, cn1.addr, map[string]metadata.LabelList{})
	// start backend server.
	stopFn := startTestCNServer(t, ctx, addr)
	defer func() {
		require.NoError(t, stopFn())
	}()

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

	db1, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(%s)/db1", cfg.ListenAddress))
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

	require.Equal(t, int64(1), s.counterSet.connAccepted.Load())
}
