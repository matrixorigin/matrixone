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
	"fmt"
	"math/big"
	"os"
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
	rt := runtime.DefaultRuntime()
	runtime.SetupProcessLevelRuntime(rt)
	ctx, cancel := context.WithCancel(context.TODO())
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
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
		ru:     newRouter(mc, re, false),
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
	runtime.SetupProcessLevelRuntime(rt)
	listenAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(listenAddr))
	cfg := Config{
		ListenAddress:     "unix://" + listenAddr,
		RebalanceDisabled: true,
	}
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
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
	mc.ForceRefresh()
	time.Sleep(time.Millisecond * 200)

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

func TestHandler_HandleErr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	temp := os.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupProcessLevelRuntime(rt)
	listenAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(listenAddr))
	cfg := Config{
		ListenAddress:     "unix://" + listenAddr,
		RebalanceDisabled: true,
	}
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
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

	require.Equal(t, int64(1), s.counterSet.connAccepted.Load())
}

func TestHandler_HandleWithSSL(t *testing.T) {
	defer leaktest.AfterTest(t)()

	temp := os.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := runtime.DefaultRuntime()
	runtime.SetupProcessLevelRuntime(rt)
	listenAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(listenAddr))
	cfg := Config{
		ListenAddress:     "unix://" + listenAddr,
		RebalanceDisabled: true,
	}
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
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
	mc.ForceRefresh()
	time.Sleep(time.Millisecond * 200)

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
	runtime.SetupProcessLevelRuntime(rt)
	listenAddr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(listenAddr))
	cfg := Config{
		ListenAddress:     "unix://" + listenAddr,
		RebalanceDisabled: true,
	}
	hc := &mockHAKeeperClient{}
	mc := clusterservice.NewMOCluster(hc, 3*time.Second)
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
	mc.ForceRefresh()
	time.Sleep(time.Millisecond * 200)

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

	fn(t, listenAddr, s)
}

func TestHandler_HandleEventKillQuery(t *testing.T) {
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

		_, err = db2.Exec("kill query 9999")
		require.Error(t, err)

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

func TestHandler_HandleEventSuspendAccount(t *testing.T) {
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

		db2, err := sql.Open("mysql", fmt.Sprintf("dump:111@unix(%s)/db1", addr))
		// connect to server.
		require.NoError(t, err)
		require.NotNil(t, db2)
		defer func() {
			_ = db2.Close()
		}()

		_, err = db2.Exec("alter account a1 suspend")
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 200)
		res, err := db1.Query("show global variables")
		require.NoError(t, err)
		defer res.Close()
		var rows int
		var varName, varValue string
		for res.Next() {
			rows += 1
			err := res.Scan(&varName, &varValue)
			require.NoError(t, err)
			require.Equal(t, "killed", varName)
			require.Equal(t, "yes", varValue)
		}
		require.Equal(t, 1, rows)
		err = res.Err()
		require.NoError(t, err)

		require.Equal(t, int64(2), s.counterSet.connAccepted.Load())
	})
}

func TestHandler_HandleEventDropAccount(t *testing.T) {
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

		db2, err := sql.Open("mysql", fmt.Sprintf("dump:111@unix(%s)/db1", addr))
		// connect to server.
		require.NoError(t, err)
		require.NotNil(t, db2)
		defer func() {
			_ = db2.Close()
		}()

		_, err = db2.Exec("drop account a1")
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 200)
		res, err := db1.Query("show global variables")
		require.NoError(t, err)
		defer res.Close()
		var rows int
		var varName, varValue string
		for res.Next() {
			rows += 1
			err := res.Scan(&varName, &varValue)
			require.NoError(t, err)
			require.Equal(t, "killed", varName)
			require.Equal(t, "yes", varValue)
		}
		require.Equal(t, 1, rows)
		err = res.Err()
		require.NoError(t, err)

		require.Equal(t, int64(2), s.counterSet.connAccepted.Load())
	})
}
