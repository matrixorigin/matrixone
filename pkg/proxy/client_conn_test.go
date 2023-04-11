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
	"net"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/require"
)

type mockClientConn struct {
	conn      net.Conn
	tenant    Tenant
	labelInfo labelInfo // need to set it explicitly
	router    Router
	tun       *tunnel
}

var _ ClientConn = (*mockClientConn)(nil)

func newMockClientConn(
	conn net.Conn, tenant Tenant, li labelInfo, router Router, tun *tunnel,
) ClientConn {
	c := &mockClientConn{
		conn:      conn,
		tenant:    tenant,
		labelInfo: li,
		router:    router,
		tun:       tun,
	}
	return c
}

func (c *mockClientConn) ConnID() uint32         { return 0 }
func (c *mockClientConn) GetSalt() []byte        { return nil }
func (c *mockClientConn) RawConn() net.Conn      { return c.conn }
func (c *mockClientConn) GetTenant() Tenant      { return c.tenant }
func (c *mockClientConn) SendErrToClient(string) {}
func (c *mockClientConn) BuildConnWithServer(_ bool) (ServerConn, error) {
	cn, err := c.router.Select(c.labelInfo)
	if err != nil {
		return nil, err
	}
	sc, _, err := c.router.Connect(cn, nil, c.tun)
	if err != nil {
		return nil, err
	}
	return sc, nil
}
func (c *mockClientConn) Close() error { return nil }

func testStartClient(t *testing.T, tp *testProxyHandler, li labelInfo, cn *CNServer) func() {
	clientProxy, client := net.Pipe()
	go func(ctx context.Context) {
		b := make([]byte, 10)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_, _ = client.Read(b)
		}
	}(tp.ctx)
	tu := newTunnel(tp.ctx, tp.logger)
	sc, _, err := tp.ru.Connect(cn, nil, tu)
	require.NoError(t, err)
	cc := newMockClientConn(clientProxy, "t1", li, tp.ru, tu)
	err = tu.run(cc, sc)
	require.NoError(t, err)
	select {
	case err := <-tu.errC:
		t.Fatalf("tunnel error: %v", err)
	default:
	}
	return func() {
		_ = tu.Close()
	}
}

func testStartNClients(t *testing.T, tp *testProxyHandler, li labelInfo, cn *CNServer, n int) func() {
	var cleanFns []func()
	for i := 0; i < n; i++ {
		c := testStartClient(t, tp, li, cn)
		cleanFns = append(cleanFns, c)
	}
	return func() {
		for _, f := range cleanFns {
			f()
		}
	}
}

func TestAccountParser(t *testing.T) {
	a := accountInfo{}
	err := a.parse("t1:u1")
	require.NoError(t, err)
	require.Equal(t, string(a.tenant), "t1")
	require.Equal(t, a.username, "u1")

	a = accountInfo{}
	err = a.parse("t1#u1")
	require.NoError(t, err)
	require.Equal(t, string(a.tenant), "t1")
	require.Equal(t, a.username, "u1")

	a = accountInfo{}
	err = a.parse(":u1")
	require.NoError(t, err)
	require.Equal(t, string(a.tenant), "")
	require.Equal(t, a.username, "u1")

	a = accountInfo{}
	err = a.parse("a1:")
	require.Error(t, err)
	require.Equal(t, string(a.tenant), "")
	require.Equal(t, a.username, "")

	a = accountInfo{}
	err = a.parse("u1")
	require.NoError(t, err)
	require.Equal(t, string(a.tenant), "")
	require.Equal(t, a.username, "u1")
}

func TestClientConn_ConnectToBackend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()

	t.Run("cannot connect", func(t *testing.T) {
		cc := &clientConn{
			log: logger,
		}
		cc.testHelper.connectToBackend = func() (ServerConn, error) {
			return nil, moerr.NewInternalErrorNoCtx("123 456")
		}

		sc, err := cc.BuildConnWithServer(false)
		require.ErrorContains(t, err, "123 456")
		require.Nil(t, sc)
	})
}
