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

package proxy

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	query "github.com/matrixorigin/matrixone/pkg/pb/query"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockGoodAuthenticator struct{}

func newMockGoodAuthenticator() Authenticator {
	return &mockGoodAuthenticator{}
}

func (a *mockGoodAuthenticator) Authenticate(_, _ []byte) bool {
	return true
}

func TestEntryOperation(t *testing.T) {
	var nilStore *cacheStore
	assert.Equal(t, 0, nilStore.count())

	store := newCacheStore()
	assert.NotNil(t, store)

	for _, co := range connOperator {
		sc1 := newMockServerConn(nil)
		co.push(nilStore, newServerConnAuth(sc1, newMockGoodAuthenticator()), func() {})
		assert.Nil(t, co.peek(nilStore))
		assert.Nil(t, co.pop(nilStore, func() {}))

		assert.Nil(t, co.peek(store))
		assert.Nil(t, co.pop(store, func() {}))

		total := 10
		for i := 0; i < total; i++ {
			sc := newMockServerConn(nil)
			co.push(store, newServerConnAuth(sc, newMockGoodAuthenticator()), func() {})
		}
		assert.Equal(t, 10, store.count())

		for i := 0; i < total; i++ {
			assert.NotNil(t, co.peek(store))
			assert.Equal(t, total, store.count())
		}

		for i := 0; i < total; i++ {
			assert.NotNil(t, co.pop(store, func() {}))
			assert.Equal(t, total-1-i, store.count())
		}
	}
}

func TestAuthentication(t *testing.T) {
	pw := "mypassword"
	// the saved password
	authString, _ := frontend.GetPassWord(frontend.HashPassWord(pw))
	au := newPwdAuthenticator(authString)

	// begin to authenticate
	salt := mockGenSalt(20)

	authResp := simulateScramble(pw, salt)
	assert.True(t, au.Authenticate(salt, authResp))

	authRespWrong := simulateScramble(pw+"wrong", salt)
	assert.False(t, au.Authenticate(salt, authRespWrong))
}

func mockGenSalt(n int) []byte {
	buf := make([]byte, n)
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	r.Read(buf)
	for i := 0; i < n; i++ {
		buf[i] &= 0x7f
		if buf[i] == 0 || buf[i] == '$' {
			buf[i]++
		}
	}
	return buf
}

func simulateScramble(password string, salt []byte) []byte {
	stage1 := frontend.HashSha1([]byte(password))
	stage2 := frontend.HashSha1(stage1)
	h := sha1.New()
	h.Write(salt)
	h.Write(stage2)
	hash1 := h.Sum(nil)
	scrambled := make([]byte, len(stage1))
	for i := 0; i < len(stage1); i++ {
		scrambled[i] = stage1[i] ^ hash1[i]
	}
	return scrambled
}

type blockingContextServerConn struct {
	*mockServerConn
	enteredOnce sync.Once
	closeOnce   sync.Once
	entered     chan struct{}
	closed      chan struct{}
}

type cacheReuseBarrierServerConn struct {
	*mockServerConn
	waitEntered chan struct{}
	ready       chan struct{}
	closed      chan struct{}
	enteredOnce sync.Once
	readyOnce   sync.Once
	closeOnce   sync.Once
}

func newCacheReuseBarrierServerConn() *cacheReuseBarrierServerConn {
	return &cacheReuseBarrierServerConn{
		mockServerConn: newMockServerConn(nil),
		waitEntered:    make(chan struct{}),
		ready:          make(chan struct{}),
		closed:         make(chan struct{}),
	}
}

func (s *cacheReuseBarrierServerConn) waitCacheReuseReady(ctx context.Context) error {
	s.enteredOnce.Do(func() { close(s.waitEntered) })
	select {
	case <-s.ready:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (s *cacheReuseBarrierServerConn) rebindTunnel(*tunnel) bool {
	return true
}

func (s *cacheReuseBarrierServerConn) releaseOrigin() {
	s.readyOnce.Do(func() { close(s.ready) })
}

func (s *cacheReuseBarrierServerConn) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return s.mockServerConn.Close()
}

func newBlockingContextServerConn(conn net.Conn) *blockingContextServerConn {
	return &blockingContextServerConn{
		mockServerConn: newMockServerConn(conn),
		entered:        make(chan struct{}),
		closed:         make(chan struct{}),
	}
}

func (s *blockingContextServerConn) ExecStmt(internalStmt, chan<- []byte) (bool, error) {
	s.enteredOnce.Do(func() { close(s.entered) })
	<-s.closed
	return false, net.ErrClosed
}

func (s *blockingContextServerConn) ExecStmtContext(
	ctx context.Context,
	_ internalStmt,
	_ chan<- []byte,
) (bool, error) {
	s.enteredOnce.Do(func() { close(s.entered) })
	select {
	case <-ctx.Done():
		return false, context.Cause(ctx)
	case <-s.closed:
		return false, net.ErrClosed
	}
}

func (s *blockingContextServerConn) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		_ = s.mockServerConn.Close()
	})
	return nil
}

func runTestWithNewConnCache(
	t *testing.T,
	maxNumTotal int,
	maxNumPerTenant int,
	connTimeout time.Duration,
	ac authenticatorConstructor,
	qt qclient.QueryClient,
	fn func(cc ConnCache),
) {
	ctx := context.Background()
	rt := runtime.ServiceRuntime("")
	if rt == nil {
		rt = runtime.DefaultRuntime()
	}
	logger := rt.Logger()

	cc := newConnCache(ctx, "", logger,
		withQueryClient(qt),
		withResetSessionFunc(func(conn ServerConn) ([]byte, error) {
			return nil, nil
		}),
		withMaxNumTotal(maxNumTotal),
		withMaxNumPerTenant(maxNumPerTenant),
		withAuthConstructor(ac),
		withConnTimeout(connTimeout),
	)
	assert.NotNil(t, cc)
	defer cc.Close()

	fn(cc)
}
func runTestWithNewConnCacheWithMaxNum(t *testing.T, maxNumTotal int, maxNumPerTenant int, fn func(cc ConnCache)) {
	runTestWithNewConnCache(t, maxNumTotal, maxNumPerTenant, defaultConnTimeout, nil, nil, fn)
}

func runTestWithNewConnCacheWithAuthConstructor(t *testing.T, ac authenticatorConstructor, fn func(cc ConnCache)) {
	runTestWithNewConnCache(t, 100, 50, defaultConnTimeout, ac, nil, fn)
}

func runTestWithNewConnCacheWithConnTimeout(t *testing.T, timeout time.Duration, fn func(cc ConnCache)) {
	runTestWithNewConnCache(t, 100, 50, timeout, nil, nil, fn)
}

func runTestWithNewConnCacheWithQueryClient(t *testing.T, qt qclient.QueryClient, fn func(cc ConnCache)) {
	runTestWithNewConnCache(t, 100, 50, defaultConnTimeout, nil, qt, fn)
}

func TestConnCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("push", func(t *testing.T) {
		runTestWithNewConnCacheWithMaxNum(t, 10, 3, func(cc ConnCache) {
			c1, _ := net.Pipe()
			mockConn1 := newMockServerConn(c1)
			assert.True(t, cc.Push("k100", mockConn1))
			assert.False(t, cc.Push("k100", mockConn1))
		})
	})

	t.Run("push - max num total", func(t *testing.T) {
		runTestWithNewConnCacheWithMaxNum(t, 10, 3, func(cc ConnCache) {
			for i := 0; i < 15; i++ {
				tempC, _ := net.Pipe()
				tempMockConn := newMockServerConn(tempC)
				if i < 10 {
					assert.True(t, cc.Push(cacheKey(fmt.Sprintf("k%d", i)), tempMockConn))
				} else {
					assert.False(t, cc.Push(cacheKey(fmt.Sprintf("k%d", i)), tempMockConn))
				}
			}
		})
	})

	t.Run("push - max num per tenant", func(t *testing.T) {
		runTestWithNewConnCacheWithMaxNum(t, 10, 3, func(cc ConnCache) {
			for i := 0; i < 15; i++ {
				tempC, _ := net.Pipe()
				tempMockConn := newMockServerConn(tempC)
				if i < 3 {
					assert.True(t, cc.Push("k1", tempMockConn))
				} else {
					assert.False(t, cc.Push("k1", tempMockConn))
				}
			}
		})
	})

	t.Run("pop - nil auth", func(t *testing.T) {
		runTestWithNewConnCacheWithAuthConstructor(t, nil, func(cc ConnCache) {
			c1, _ := net.Pipe()
			mockConn1 := newMockServerConn(c1)
			assert.True(t, cc.Push("k100", mockConn1))
			assert.Equal(t, 1, cc.Count())

			sc := cc.Pop("k100", 1, nil, nil, clientInfo{})
			assert.NotNil(t, sc)
			assert.Equal(t, 0, cc.Count())
		})
	})

	t.Run("pop - skip route-ineligible cn before reuse", func(t *testing.T) {
		runTestWithNewConnCacheWithAuthConstructor(t, nil, func(cc ConnCache) {
			ccc := cc.(*connCache)
			var checkedClient clientInfo
			ccc.canReuseCN = func(cn *CNServer, client clientInfo) bool {
				checkedClient = client
				return cn == nil || cn.uuid != "bad-cn" || client.username == "root"
			}

			c1, _ := net.Pipe()
			mockConn1 := newMockServerConn(c1)
			mockConn1.setCN(&CNServer{uuid: "bad-cn"})
			assert.True(t, cc.Push("k100", mockConn1))
			assert.Equal(t, 1, cc.Count())

			login := clientInfo{username: "ordinary"}
			sc := cc.Pop("k100", 1, nil, nil, login)
			assert.Nil(t, sc)
			assert.Equal(t, 0, cc.Count())
			assert.Equal(t, login.username, checkedClient.username)
		})
	})

	t.Run("pop - nil auth, return err", func(t *testing.T) {
		runTestWithNewConnCacheWithAuthConstructor(t, nil, func(cc ConnCache) {
			c1, _ := net.Pipe()
			mockConn1 := newMockServerConn(c1)
			// mock server error
			mockConn1.setReturnErr(context.DeadlineExceeded)
			assert.True(t, cc.Push("k100", mockConn1))
			assert.Equal(t, 1, cc.Count())

			sc := cc.Pop("k100", 1, nil, nil, clientInfo{})
			assert.Nil(t, sc)
			assert.Equal(t, 0, cc.Count())
		})
	})

	t.Run("pop - pwd auth", func(t *testing.T) {
		runTestWithNewConnCacheWithAuthConstructor(t, newPwdAuthenticator, func(cc ConnCache) {
			c1, _ := net.Pipe()
			mockConn1 := newMockServerConn(c1)
			assert.True(t, cc.Push("k100", mockConn1))
			assert.Equal(t, 1, cc.Count())

			sc := cc.Pop("k100", 1, nil, nil, clientInfo{})
			assert.Nil(t, sc)
			assert.Equal(t, 1, cc.Count())
		})
	})

	t.Run("pop - timeout", func(t *testing.T) {
		runTestWithNewConnCacheWithConnTimeout(t, 0, func(cc ConnCache) {
			c1, _ := net.Pipe()
			mockConn1 := newMockServerConn(c1)
			assert.True(t, cc.Push("k100", mockConn1))
			assert.Equal(t, 1, cc.Count())

			sc := cc.Pop("k100", 1, nil, nil, clientInfo{})
			// cannot get conn as timeout.
			assert.Nil(t, sc)
			// count is 0 because the connection has been removed.
			assert.Equal(t, 0, cc.Count())
		})
	})

	t.Run("close - disables push and pop", func(t *testing.T) {
		runTestWithNewConnCacheWithAuthConstructor(t, nil, func(cc ConnCache) {
			c1, _ := net.Pipe()
			mockConn1 := newMockServerConn(c1)
			assert.True(t, cc.Push("k100", mockConn1))
			assert.Equal(t, 1, cc.Count())

			assert.NoError(t, cc.Close())
			assert.Equal(t, 0, cc.Count())

			sc := cc.Pop("k100", 1, nil, nil, clientInfo{})
			assert.Nil(t, sc)

			c2, _ := net.Pipe()
			mockConn2 := newMockServerConn(c2)
			assert.False(t, cc.Push("k100", mockConn2))
		})
	})
}

func TestConnCacheRejectsDifferentPrincipal(t *testing.T) {
	runTestWithNewConnCacheWithAuthConstructor(t, nil, func(cc ConnCache) {
		c1, _ := net.Pipe()
		backend := newMockServerConn(c1)
		backend.setCN(&CNServer{uuid: "cn-1"})
		identity := cacheReuseIdentity{
			tenant:      "tenant-a",
			username:    "cached-user",
			role:        "role-a",
			originIP:    "127.0.0.1",
			capability:  frontend.CLIENT_PROTOCOL_41,
			collationID: 45,
		}
		identityCache := cc.(identityConnCache)
		require.True(t, identityCache.PushWithIdentity("tenant-a", backend, identity))

		client := clientInfo{labelInfo: labelInfo{Tenant: "tenant-a"}, username: "other-user"}
		require.Nil(t, identityCache.PopWithIdentity(
			"tenant-a", 1, nil, nil, client,
			cacheReuseIdentity{
				tenant:      "tenant-a",
				username:    client.username,
				role:        "role-a",
				originIP:    "127.0.0.1",
				capability:  frontend.CLIENT_PROTOCOL_41,
				collationID: 45,
			},
		))
		require.Equal(t, 1, cc.Count())
	})
}

func TestConnCacheSelectsCompatibleGenerationWithinBucket(t *testing.T) {
	runTestWithNewConnCacheWithAuthConstructor(t, nil, func(cc ConnCache) {
		identityCache := cc.(identityConnCache)
		firstLocal, firstPeer := net.Pipe()
		defer firstPeer.Close()
		secondLocal, secondPeer := net.Pipe()
		defer secondPeer.Close()
		first := newMockServerConn(firstLocal)
		second := newMockServerConn(secondLocal)
		firstIdentity := cacheReuseIdentity{
			tenant:      "tenant-a",
			username:    "first",
			role:        "role-a",
			originIP:    "127.0.0.1",
			capability:  frontend.CLIENT_PROTOCOL_41,
			collationID: 45,
		}
		secondIdentity := firstIdentity
		secondIdentity.username = "second"
		require.True(t, identityCache.PushWithIdentity("tenant-a", first, firstIdentity))
		require.True(t, identityCache.PushWithIdentity("tenant-a", second, secondIdentity))

		reused := identityCache.PopWithIdentity(
			"tenant-a", 2, nil, nil,
			clientInfo{labelInfo: labelInfo{Tenant: "tenant-a"}, username: "second"},
			secondIdentity,
		)
		require.Same(t, second, reused)
		require.Equal(t, 1, cc.Count())

		incompatible := firstIdentity
		incompatible.capability++
		require.Nil(t, identityCache.PopWithIdentity(
			"tenant-a", 3, nil, nil,
			clientInfo{labelInfo: labelInfo{Tenant: "tenant-a"}, username: "first"},
			incompatible,
		))
		require.Equal(t, 1, cc.Count())
		incompatible = firstIdentity
		incompatible.collationID++
		require.Nil(t, identityCache.PopWithIdentity(
			"tenant-a", 3, nil, nil,
			clientInfo{labelInfo: labelInfo{Tenant: "tenant-a"}, username: "first"},
			incompatible,
		))
		require.Equal(t, 1, cc.Count())

		require.Same(t, first, identityCache.PopWithIdentity(
			"tenant-a", 4, nil, nil,
			clientInfo{labelInfo: labelInfo{Tenant: "tenant-a"}, username: "first"},
			firstIdentity,
		))
	})
}

func TestConnCacheRefreshesAuthenticationBeforeReuse(t *testing.T) {
	var (
		seenClient clientInfo
		seenSalt   []byte
		seenAuth   []byte
		calls      atomic.Int64
		gotAuth    string
	)
	cache := newConnCache(
		context.Background(), "", runtime.DefaultRuntime().Logger(),
		withResetSessionFunc(func(ServerConn) ([]byte, error) { return nil, nil }),
		withAuthConstructor(func(auth []byte) Authenticator {
			gotAuth = string(auth)
			return newMockGoodAuthenticator()
		}),
		withRefreshSessionAuthFunc(func(_ context.Context, _ ServerConn, client clientInfo, salt, auth []byte) ([]byte, error) {
			seenClient = client
			seenSalt = append([]byte(nil), salt...)
			seenAuth = append([]byte(nil), auth...)
			calls.Add(1)
			return []byte("fresh-auth"), nil
		}),
	)
	defer cache.Close()

	local, peer := net.Pipe()
	defer peer.Close()
	backend := newMockServerConn(local)
	identity := cacheReuseIdentity{
		tenant:      "tenant-a",
		username:    "dump",
		originIP:    "127.0.0.1",
		capability:  frontend.CLIENT_PROTOCOL_41,
		collationID: 45,
	}
	require.True(t, cache.(identityConnCache).PushWithIdentity("tenant-a", backend, identity))

	client := clientInfo{
		labelInfo:  labelInfo{Tenant: "tenant-a"},
		username:   "dump",
		userInput:  "tenant-a:dump",
		database:   "db_a",
		originIP:   net.ParseIP("127.0.0.1"),
		originPort: 3307,
	}
	reused := cache.(identityConnCache).PopWithIdentity(
		"tenant-a", 7, []byte("salt"), []byte("response"), client, identity)
	require.Same(t, backend, reused)
	require.Equal(t, int64(1), calls.Load())
	require.Equal(t, client.userInput, seenClient.userInput)
	require.Equal(t, client.database, seenClient.database)
	require.Equal(t, "127.0.0.1:3307", seenClient.clientAddress())
	require.Equal(t, []byte("salt"), seenSalt)
	require.Equal(t, []byte("response"), seenAuth)
	require.Equal(t, "fresh-auth", gotAuth)
	require.NoError(t, reused.Close())
}

func TestConnCacheRefreshAuthenticationFailureDiscardsGeneration(t *testing.T) {
	cache := newConnCache(
		context.Background(), "", runtime.DefaultRuntime().Logger(),
		withResetSessionFunc(func(ServerConn) ([]byte, error) { return nil, nil }),
		withAuthConstructor(nil),
		withRefreshSessionAuthFunc(func(context.Context, ServerConn, clientInfo, []byte, []byte) ([]byte, error) {
			return nil, fmt.Errorf("credentials are no longer valid")
		}),
	)
	defer cache.Close()

	local, peer := net.Pipe()
	defer peer.Close()
	backend := newMockServerConn(local)
	identity := cacheReuseIdentity{tenant: "tenant-a", username: "dump"}
	require.True(t, cache.(identityConnCache).PushWithIdentity("tenant-a", backend, identity))

	client := clientInfo{labelInfo: labelInfo{Tenant: "tenant-a"}, username: "dump"}
	require.Nil(t, cache.(identityConnCache).PopWithIdentity(
		"tenant-a", 7, nil, nil, client, identity))
	require.Zero(t, cache.Count())
}

func TestConnCachePopClearsReadDeadlineAfterConnectionID(t *testing.T) {
	runTestWithNewConnCacheWithAuthConstructor(t, nil, func(cc ConnCache) {
		local, remote := net.Pipe()
		defer remote.Close()
		raw := &phaseDeadlineConn{Conn: local}
		sc := &deadlineRearmingServerConn{
			ServerConn: newMockServerConn(raw),
			raw:        raw,
		}
		assert.True(t, cc.Push("tenant-a", sc))

		popped := cc.Pop("tenant-a", 1, nil, nil, clientInfo{})
		require.Same(t, sc, popped)
		require.True(t, raw.readDeadline().IsZero(),
			"cache Pop must clear the deadline armed by SET CONNECTION ID")
		assert.NoError(t, popped.Close())
	})
}

func TestConnCachePopDiscardsReadDeadlineClearFailure(t *testing.T) {
	runTestWithNewConnCacheWithAuthConstructor(t, nil, func(cc ConnCache) {
		local, remote := net.Pipe()
		defer remote.Close()
		raw := &phaseDeadlineConn{Conn: local, failClear: true}
		sc := &deadlineRearmingServerConn{
			ServerConn: newMockServerConn(raw),
			raw:        raw,
		}
		assert.True(t, cc.Push("tenant-a", sc))

		assert.Nil(t, cc.Pop("tenant-a", 1, nil, nil, clientInfo{}))
		assert.Zero(t, cc.Count(),
			"a backend whose handoff deadline cannot be cleared must be discarded")
		assert.False(t, raw.readDeadline().IsZero())
	})
}

func TestConnCacheBlockedPopDoesNotBlockOtherTenantsOrClose(t *testing.T) {
	ctx := context.Background()
	logger := runtime.DefaultRuntime().Logger()
	cache := newConnCache(ctx, "", logger,
		withResetSessionFunc(func(ServerConn) ([]byte, error) { return nil, nil }),
		withAuthConstructor(nil),
	)

	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	blocked := newBlockingContextServerConn(serverSide)
	require.True(t, cache.Push("tenant-a", blocked))

	popDone := make(chan ServerConn, 1)
	go func() {
		popDone <- cache.Pop("tenant-a", 1, nil, nil, clientInfo{})
	}()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		require.FailNow(t, "Pop did not enter backend I/O")
	}

	otherClient, otherServer := net.Pipe()
	defer otherClient.Close()
	other := newMockServerConn(otherServer)
	pushDone := make(chan bool, 1)
	go func() {
		pushDone <- cache.Push("tenant-b", other)
	}()
	select {
	case pushed := <-pushDone:
		require.True(t, pushed)
	case <-time.After(time.Second):
		require.FailNow(t, "blocked Pop held the global cache mutex")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- cache.Close()
	}()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "cache Close waited for blocked backend I/O")
	}
	select {
	case sc := <-popDone:
		require.Nil(t, sc)
	case <-time.After(time.Second):
		require.FailNow(t, "cache Close did not terminate in-flight Pop")
	}
}

func TestConnCachePopContextCancelsBackendValidation(t *testing.T) {
	cache := newConnCache(context.Background(), "", runtime.DefaultRuntime().Logger(),
		withResetSessionFunc(func(ServerConn) ([]byte, error) { return nil, nil }),
		withAuthConstructor(nil),
	)
	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	blocked := newBlockingContextServerConn(serverSide)
	require.True(t, cache.Push("tenant-a", &protocolMemoryServerConn{ServerConn: blocked}))

	ctx, cancel := context.WithCancel(context.Background())
	popDone := make(chan ServerConn, 1)
	go func() {
		popDone <- cache.(*connCache).PopContext(ctx, "tenant-a", 1, nil, nil, clientInfo{})
	}()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("PopContext did not enter backend validation")
	}
	cancel()
	select {
	case sc := <-popDone:
		require.Nil(t, sc)
	case <-time.After(time.Second):
		t.Fatal("PopContext ignored lifecycle cancellation")
	}
	require.Zero(t, cache.Count())
	require.NoError(t, cache.Close())
}

func TestConnCacheCloseDoesNotWaitForPushReset(t *testing.T) {
	ctx := context.Background()
	logger := runtime.DefaultRuntime().Logger()
	resetEntered := make(chan struct{})
	releaseReset := make(chan struct{})
	cache := newConnCache(ctx, "", logger,
		withResetSessionFunc(func(ServerConn) ([]byte, error) {
			close(resetEntered)
			<-releaseReset
			return nil, nil
		}),
		withAuthConstructor(nil),
	)

	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	conn := newMockServerConn(serverSide)
	pushDone := make(chan bool, 1)
	go func() {
		pushDone <- cache.Push("tenant-a", conn)
	}()
	select {
	case <-resetEntered:
	case <-time.After(time.Second):
		require.FailNow(t, "Push did not enter reset-session I/O")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- cache.Close()
	}()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "cache Close waited for reset-session I/O")
	}

	close(releaseReset)
	select {
	case pushed := <-pushDone:
		require.False(t, pushed)
	case <-time.After(time.Second):
		require.FailNow(t, "Push did not recheck the closed cache after reset")
	}
	require.NoError(t, conn.Close())
}

func TestConnCachePopWaitsForOriginGenerationCleanup(t *testing.T) {
	cache := newConnCache(
		context.Background(),
		"",
		runtime.DefaultRuntime().Logger(),
		withResetSessionFunc(func(ServerConn) ([]byte, error) { return nil, nil }),
		withAuthConstructor(nil),
	)
	defer cache.Close()

	backend := newCacheReuseBarrierServerConn()
	t.Cleanup(backend.releaseOrigin)
	require.True(t, cache.Push("tenant-a", backend))

	result := make(chan ServerConn, 1)
	go func() {
		result <- cache.(*connCache).PopContext(
			context.Background(), "tenant-a", 1, nil, nil, clientInfo{},
		)
	}()
	select {
	case <-backend.waitEntered:
	case <-time.After(time.Second):
		require.FailNow(t, "PopContext did not wait for the originating tunnel")
	}
	select {
	case <-result:
		require.FailNow(t, "cached backend was reused before origin cleanup")
	default:
	}

	backend.releaseOrigin()
	select {
	case popped := <-result:
		require.Same(t, backend, popped)
		require.NoError(t, popped.Close())
	case <-time.After(time.Second):
		require.FailNow(t, "cached backend did not become reusable after cleanup")
	}

	blocked := newCacheReuseBarrierServerConn()
	t.Cleanup(blocked.releaseOrigin)
	require.True(t, cache.Push("tenant-b", blocked))
	cancelCtx, cancel := context.WithCancel(context.Background())
	canceledResult := make(chan ServerConn, 1)
	go func() {
		canceledResult <- cache.(*connCache).PopContext(
			cancelCtx, "tenant-b", 2, nil, nil, clientInfo{},
		)
	}()
	select {
	case <-blocked.waitEntered:
	case <-time.After(time.Second):
		require.FailNow(t, "PopContext did not enter the cancelable origin wait")
	}
	cancel()
	select {
	case popped := <-canceledResult:
		require.Nil(t, popped)
	case <-time.After(time.Second):
		require.FailNow(t, "cancellation did not terminate cache reuse wait")
	}
	select {
	case <-blocked.closed:
	case <-time.After(time.Second):
		require.FailNow(t, "canceled cache reuse did not close the backend")
	}
	require.Zero(t, cache.Count())
}

func TestResetSession(t *testing.T) {
	c1, _ := net.Pipe()
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: c1.RemoteAddr().String()}
	runTestWithQueryService(t, cn, func(cc *clientConn, addr string) {
		runTestWithNewConnCacheWithQueryClient(t, cc.queryClient, func(cc ConnCache) {
			mockConn1 := newMockServerConn(c1)
			_, err := cc.(*connCache).resetSession(mockConn1)
			assert.NoError(t, err)
		})
	})
}

func TestPreparedShortConnectionQuitPublishesReusableBackend(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	resetEntered := make(chan struct{})
	runTestWithQueryServiceResetHandler(t, cn, func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
		if req.ResetSessionRequest == nil {
			return fmt.Errorf("missing ResetSession request")
		}
		if _, ok := ctx.Deadline(); !ok {
			return fmt.Errorf("ResetSession request is missing its production deadline")
		}
		close(resetEntered)
		resp.ResetSessionResponse = &query.ResetSessionResponse{Success: true, AuthString: []byte("auth")}
		return nil
	}, func(cc *clientConn, _ string) {
		tun := &tunnel{}
		tun.mu.csp = &pipe{}
		tun.mu.csp.mu.cond = sync.NewCond(&tun.mu.csp.mu)
		tun.mu.scp = &pipe{}
		tun.mu.scp.mu.cond = sync.NewCond(&tun.mu.scp.mu)

		// A completed prepared-statement request has a response fence. QUIT must
		// see that clean boundary and may publish the backend after ResetSession
		// completes.
		prepare := makeSimplePacket("select 1")
		prepare[4] = byte(frontend.COM_STMT_PREPARE)
		tun.trackClientRequest(prepare)
		tun.trackServerResponse(makePrepareOKPacket(0, 0))
		require.False(t, tun.hasInFlightClientRequest())
		closeStmt := makeStmtCommandPacket(frontend.COM_STMT_CLOSE, 0)
		closeCommit := tun.trackClientRequest(closeStmt)
		tun.commitClientRequest(closeCommit)
		require.True(t, tun.hasUnsafeClientState())

		clientSide, backendSide := net.Pipe()
		defer clientSide.Close()
		defer backendSide.Close()
		backend := newMockServerConn(backendSide)
		backend.setCN(&CNServer{connID: 27, uuid: "cn1"})

		cache := newConnCache(
			context.Background(),
			"",
			runtime.DefaultRuntime().Logger(),
			withMOCluster(cc.moCluster),
			withQueryClient(cc.queryClient),
			withAuthConstructor(nil),
		)
		defer cache.Close()

		client := &clientConn{
			log:        runtime.DefaultRuntime().Logger(),
			tun:        tun,
			sc:         backend,
			connCache:  cache,
			clientInfo: clientInfo{hash: "tenant-a"},
		}
		require.NoError(t, client.handleQuitCommand(context.Background()))
		select {
		case <-resetEntered:
		case <-time.After(time.Second):
			t.Fatal("connCache.Push did not reach QueryService ResetSession")
		}
		require.True(t, client.isConnCached())
		require.Equal(t, 1, cache.Count())

		// Pop is the next client's login/SET CONNECTION ID boundary. The same
		// backend must remain usable for a prepared statement and a query.
		reused := cache.(identityConnCache).PopWithIdentity(
			"tenant-a", 99, nil, nil, clientInfo{}, client.cacheReuseIdentity())
		require.Same(t, backend, reused)
		ok, err := reused.ExecStmt(internalStmt{cmdType: cmdQuery, s: "prepare p from 'select 1'"}, nil)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = reused.ExecStmt(internalStmt{cmdType: cmdQuery, s: "select 1"}, nil)
		require.NoError(t, err)
		require.True(t, ok)
	})
}

func readProxyTestPacket(r io.Reader) ([]byte, error) {
	header := make([]byte, mysqlHeadLen)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return append(header, payload...), nil
}

func newPipeServerConnForCacheTest(t *testing.T) (*serverConn, net.Conn, func()) {
	local, remote := net.Pipe()
	frontend.InitServerLevelVars("cn1")
	fp := config.FrontendParameters{}
	fp.SetDefaultValues()
	pu := config.NewParameterUnit(&fp, nil, nil, nil)
	allocator := frontend.NewLeakCheckAllocator()
	ios, err := frontend.NewIOSessionWithOptions(
		local,
		pu,
		"cn1",
		frontend.WithIOSessionBufferSize(proxyIOSessionBufferSize),
		frontend.WithIOSessionAllowedPacketSize(proxyBackendPacketLimit),
		frontend.WithIOSessionAllocator(allocator),
	)
	require.NoError(t, err)
	sc := &serverConn{
		cnServer:   &CNServer{connID: 27, uuid: "cn1"},
		conn:       local,
		connID:     27,
		createTime: time.Now(),
		mysqlProto: frontend.NewMysqlClientProtocol(
			"cn1", 27, ios, 0, &fp),
	}
	return sc, remote, func() {
		_ = sc.Close()
		_ = remote.Close()
		require.True(t, allocator.CheckBalance())
	}
}

type preparedCacheTestRouter struct {
	sc           ServerConn
	onConnect    func()
	connectCount int
}

func (r *preparedCacheTestRouter) Route(
	context.Context, string, clientInfo, func(string) bool,
) (*CNServer, error) {
	return r.sc.GetCNServer(), nil
}

func (r *preparedCacheTestRouter) SelectByConnID(uint32) (*CNServer, error) {
	return nil, nil
}

func (r *preparedCacheTestRouter) AllServers(string) ([]*CNServer, error) {
	return nil, nil
}

func (r *preparedCacheTestRouter) Connect(
	_ *CNServer, _ *frontend.Packet, tun *tunnel,
) (ServerConn, []byte, error) {
	r.connectCount++
	if r.connectCount != 1 {
		return nil, nil, fmt.Errorf("cache miss created backend generation %d", r.connectCount)
	}
	if r.onConnect != nil {
		r.onConnect()
	}
	if !rebindServerConnTunnel(r.sc, tun) {
		return nil, nil, errPipeClosed
	}
	return r.sc, makeOKPacket(8), nil
}

func makeLegalStmtExecutePacket(statementID, value uint32) []byte {
	// This matches go-sql-driver/mysql's first execution of a single LONG
	// parameter: no cursor, iteration-count 1, non-NULL, new type binding, then
	// the little-endian value.
	tail := []byte{
		0,          // CURSOR_TYPE_NO_CURSOR
		1, 0, 0, 0, // iteration-count
		0, // NULL bitmap
		1, // new-params-bound flag
		byte(defines.MYSQL_TYPE_LONG), 0,
		0, 0, 0, 0,
	}
	binary.LittleEndian.PutUint32(tail[len(tail)-4:], value)
	return makeStmtCommandPacket(frontend.COM_STMT_EXECUTE, statementID, tail...)
}

func makePayloadPacket(sequence byte, payload ...byte) []byte {
	packet := make([]byte, mysqlHeadLen+len(payload))
	packet[0] = byte(len(payload))
	packet[1] = byte(len(payload) >> 8)
	packet[2] = byte(len(payload) >> 16)
	packet[3] = sequence
	copy(packet[mysqlHeadLen:], payload)
	return packet
}

func TestPreparedShortConnectionQuitProductionPath(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1", SQLAddress: "pipe"}
	type resetSnapshot struct {
		database string
		prepared bool
	}
	resetEvents := make(chan resetSnapshot, 128)
	var backendStateMu sync.Mutex
	var backendDatabase string
	var backendPrepared *frontend.PrepareStmt
	var backendPrepareSQL string
	var refreshChecks atomic.Int64
	refreshRequests := make(chan struct {
		userInput string
		database  string
		address   string
	}, 128)
	runTestWithQueryServiceHandlersAndRefresh(t, cn, nil, func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
		if req.ResetSessionRequest == nil {
			return fmt.Errorf("missing ResetSession request")
		}
		if _, ok := ctx.Deadline(); !ok {
			return fmt.Errorf("ResetSession request is missing its production deadline")
		}
		backendStateMu.Lock()
		if backendPrepared != nil {
			backendPrepared.Close()
			backendPrepared = nil
		}
		backendPrepareSQL = ""
		backendDatabase = ""
		snapshot := resetSnapshot{database: backendDatabase, prepared: backendPrepared != nil}
		backendStateMu.Unlock()
		resetEvents <- snapshot
		resp.ResetSessionResponse = &query.ResetSessionResponse{Success: true}
		return nil
	}, func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
		if req.RefreshSessionAuthRequest == nil || req.RefreshSessionAuthRequest.UserInput == "" {
			return fmt.Errorf("refresh request did not carry the handshake principal")
		}
		refreshChecks.Add(1)
		refreshRequests <- struct {
			userInput string
			database  string
			address   string
		}{
			userInput: req.RefreshSessionAuthRequest.UserInput,
			database:  req.RefreshSessionAuthRequest.Database,
			address:   req.RefreshSessionAuthRequest.ClientAddress,
		}
		resp.RefreshSessionAuthResponse = &query.RefreshSessionAuthResponse{
			AuthString: []byte("auth"),
			Success:    true,
		}
		return nil
	}, func(cc *clientConn, _ string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cache := newConnCache(
			ctx,
			"",
			runtime.DefaultRuntime().Logger(),
			withMOCluster(cc.moCluster),
			withQueryClient(cc.queryClient),
		)
		defer cache.Close()

		sc, backend, backendCleanup := newPipeServerConnForCacheTest(t)
		defer backendCleanup()

		type backendEvent struct {
			command   byte
			query     string
			database  string
			parameter uint32
		}
		backendEvents := make(chan backendEvent, 1024)
		backendDone := make(chan error, 1)
		preparePlan := &planpb.Plan{
			Plan: &planpb.Plan_Dcl{Dcl: &planpb.DataControl{
				DclType: planpb.DataControl_PREPARE,
				Control: &planpb.DataControl_Prepare{Prepare: &planpb.Prepare{
					ParamTypes: []int32{0},
				}},
			}},
		}
		proc := process.NewTopProcess(
			context.Background(), mpool.MustNewZeroNoFixed(),
			nil, nil, nil, nil, nil, nil, nil, nil, nil,
		)
		defer proc.Free()
		newPrepareStmt := func() *frontend.PrepareStmt {
			return &frontend.PrepareStmt{PreparePlan: preparePlan}
		}
		defer func() {
			backendStateMu.Lock()
			defer backendStateMu.Unlock()
			if backendPrepared != nil {
				backendPrepared.Close()
				backendPrepared = nil
			}
		}()
		go func() {
			receiver := newMySQLConn("cache-test-backend", backend, 0, nil, nil, false, 0)
			for {
				packet, err := receiver.receive()
				if err != nil {
					backendDone <- err
					return
				}
				if len(packet) <= mysqlHeadLen {
					backendDone <- fmt.Errorf("backend received an empty command")
					return
				}
				cmd := packet[4]
				event := backendEvent{command: cmd}
				if (cmd == byte(cmdQuery) || cmd == byte(frontend.COM_STMT_PREPARE)) &&
					len(packet) > mysqlHeadLen+1 {
					event.query = string(packet[5:])
				}
				switch frontend.CommandType(cmd) {
				case frontend.COM_STMT_PREPARE:
					backendStateMu.Lock()
					if backendPrepared != nil {
						backendStateMu.Unlock()
						backendDone <- fmt.Errorf("prepare reached backend before the prior statement was cleared")
						return
					}
					backendPrepared = newPrepareStmt()
					backendPrepareSQL = strings.ToLower(event.query)
					event.database = backendDatabase
					backendStateMu.Unlock()
					backendEvents <- event
					if err := writeAll(backend, makePrepareOKPacket(0, 1)); err != nil {
						backendDone <- err
						return
					}
					parameter := makePayloadPacket(2, 'p')
					if err := writeAll(backend, parameter); err != nil {
						backendDone <- err
						return
					}
				case frontend.COM_STMT_EXECUTE:
					const executePacketLength = mysqlHeadLen + 1 + 4 + 13
					if len(packet) != executePacketLength ||
						binary.LittleEndian.Uint32(packet[5:9]) != 1 ||
						packet[9] != 0 ||
						binary.LittleEndian.Uint32(packet[10:14]) != 1 ||
						packet[14] != 0 || packet[15] != 1 ||
						packet[16] != byte(defines.MYSQL_TYPE_LONG) || packet[17] != 0 {
						backendDone <- fmt.Errorf("malformed binary COM_STMT_EXECUTE packet")
						return
					}
					event.parameter = binary.LittleEndian.Uint32(packet[18:22])
					backendStateMu.Lock()
					if backendPrepared == nil {
						backendStateMu.Unlock()
						backendDone <- fmt.Errorf("execute reached backend without a prepare")
						return
					}
					event.query = backendPrepareSQL
					if err := sc.mysqlProto.ParseExecuteData(
						context.Background(), proc, backendPrepared, packet[5:], 4); err != nil {
						backendStateMu.Unlock()
						backendDone <- fmt.Errorf("CN execute parser rejected packet: %w", err)
						return
					}
					event.database = backendDatabase
					backendStateMu.Unlock()
					if event.database == "" && !strings.EqualFold(event.query, "select ?") {
						backendDone <- fmt.Errorf("execute reached backend without a selected database")
						return
					}
					backendEvents <- event
					if err := writeAll(backend, makeOKPacket(8)); err != nil {
						backendDone <- err
						return
					}
				case frontend.COM_STMT_CLOSE:
					backendStateMu.Lock()
					if backendPrepared != nil {
						backendPrepared.Close()
						backendPrepared = nil
					}
					backendPrepareSQL = ""
					event.database = backendDatabase
					backendStateMu.Unlock()
					backendEvents <- event
				case frontend.CommandType(cmdPing):
					backendStateMu.Lock()
					event.database = backendDatabase
					backendStateMu.Unlock()
					backendEvents <- event
					if err := writeAll(backend, makeOKPacket(8)); err != nil {
						backendDone <- err
						return
					}
				case frontend.CommandType(cmdQuery):
					queryText := strings.ToLower(event.query)
					backendStateMu.Lock()
					if strings.Contains(queryText, "set connection id") {
						// Production SET CONNECTION ID changes only the connection id.
					} else if strings.HasPrefix(queryText, "use ") {
						backendDatabase = strings.Trim(strings.TrimSpace(event.query[4:]), "`")
					}
					event.database = backendDatabase
					backendStateMu.Unlock()
					backendEvents <- event
					if queryText == "select database()" {
						terminal := makeDeprecatedEOFPacket(0)
						terminal[3] = 4
						packets := [][]byte{
							makePayloadPacket(1, 1),
							makePayloadPacket(2, 'd'),
							makePayloadPacket(3, 0xfb),
							terminal,
						}
						for _, response := range packets {
							if err := writeAll(backend, response); err != nil {
								backendDone <- err
								return
							}
						}
					} else if err := writeAll(backend, makeOKPacket(8)); err != nil {
						backendDone <- err
						return
					}
				default:
					backendEvents <- event
					if err := writeAll(backend, makeOKPacket(8)); err != nil {
						backendDone <- err
						return
					}
				}
			}
		}()

		waitBackendCommand := func(expected byte) backendEvent {
			t.Helper()
			select {
			case event := <-backendEvents:
				require.Equal(t, expected, event.command)
				return event
			case err := <-backendDone:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatalf("timed out waiting for backend command 0x%x", expected)
			}
			return backendEvent{}
		}

		var initialDatabase string
		router := &preparedCacheTestRouter{
			sc: sc,
			onConnect: func() {
				backendStateMu.Lock()
				backendDatabase = initialDatabase
				backendStateMu.Unlock()
			},
		}
		const generations = 100
		for generation := 0; generation < generations; generation++ {
			clientConnValue, clientCleanup := createNewClientConn(t)
			client := clientConnValue.(*clientConn)
			client.queryClient = cc.queryClient
			client.moCluster = cc.moCluster
			client.connCache = cache
			client.router = router
			client.clientInfo.hash = LabelHash("tenant-a")
			client.clientInfo.Tenant = "tenant-a"
			client.clientInfo.username = "dump"
			client.clientInfo.userInput = "tenant-a:dump"
			client.clientInfo.originIP = net.ParseIP("127.0.0.1")
			client.clientInfo.originPort = 3307
			client.mysqlProto.SetUserName(client.clientInfo.userInput)

			clientProxy, clientRemote := net.Pipe()
			client.conn.UseConn(clientProxy)
			client.mysqlProto.UseConn(clientProxy)
			responses := make(chan []byte, 8)
			clientReaderDone := make(chan error, 1)
			go func() {
				for {
					packet, err := readProxyTestPacket(clientRemote)
					if err != nil {
						clientReaderDone <- err
						return
					}
					responses <- packet
				}
			}()
			writeClient := func(packet []byte) {
				t.Helper()
				_, err := clientRemote.Write(packet)
				require.NoError(t, err)
			}
			waitResponse := func() []byte {
				t.Helper()
				select {
				case packet := <-responses:
					return packet
				case <-time.After(time.Second):
					t.Fatal("timed out waiting for backend response")
					return nil
				}
			}

			tun := newTunnel(
				ctx,
				runtime.DefaultRuntime().Logger(),
				newCounterSet(),
				withConnCacheEnabled(true),
				withCacheReuseBarrier(),
			)
			resourcesClosed := false
			closeGenerationResources := func() {
				if resourcesClosed {
					return
				}
				resourcesClosed = true
				_ = clientRemote.Close()
				_ = tun.Close()
				clientCleanup()
			}
			defer closeGenerationResources()
			client.tun = tun
			require.True(t, tun.connCacheEnabled)

			database := "db_a"
			if generation&1 == 1 {
				database = "db_b"
			}
			if generation == generations-1 {
				database = ""
			}
			initialDatabase = database
			client.mysqlProto.SetDatabaseName(database)
			backendConn, err := client.connectToBackendContext(ctx, "")
			require.NoError(t, err)
			require.Same(t, sc, backendConn)
			client.sc = backendConn
			require.True(t, isOKPacket(waitResponse()))
			if generation > 0 {
				setConnID := waitBackendCommand(byte(cmdQuery))
				require.Contains(t, strings.ToLower(setConnID.query), "set connection id")
				require.Empty(t, setConnID.database,
					"ResetSession must clear the prior database before SET CONNECTION ID")
				if database != "" {
					useEvent := waitBackendCommand(byte(cmdQuery))
					require.Equal(t, "use `"+database+"`", strings.ToLower(useEvent.query))
					require.Equal(t, database, useEvent.database)
				}
			}
			backendStateMu.Lock()
			require.Equal(t, database, backendDatabase)
			require.Nil(t, backendPrepared)
			backendStateMu.Unlock()
			require.NoError(t, tun.run(client, backendConn))

			eventDone := make(chan error, 1)
			quitHandled := make(chan struct{})
			go func() {
				for {
					select {
					case event, ok := <-tun.reqC:
						if !ok {
							eventDone <- nil
							return
						}
						if err := client.HandleEvent(ctx, event, tun.respC); err != nil {
							eventDone <- err
							return
						}
						if _, ok := event.(*quitEvent); ok {
							close(quitHandled)
						}
					case <-ctx.Done():
						eventDone <- ctx.Err()
						return
					}
				}
			}()

			cleaned := false
			cleanupGeneration := func() {
				if cleaned {
					return
				}
				cleaned = true
				closeGenerationResources()
				select {
				case <-eventDone:
				case <-time.After(time.Second):
					t.Error("quit event loop did not terminate")
				}
				select {
				case <-clientReaderDone:
				case <-time.After(time.Second):
					t.Error("client reader did not terminate")
				}
				// Production handler cleanup marks this barrier only after tunnel,
				// client, and event-handler cleanup has completed.  Pop must wait for
				// the same terminal point before issuing SET CONNECTION ID.
				tun.markCacheReuseReady()
			}
			defer cleanupGeneration()

			if database == "" {
				writeClient(makeSimplePacket("select database()"))
				databaseEvent := waitBackendCommand(byte(cmdQuery))
				require.Equal(t, "select database()", strings.ToLower(databaseEvent.query))
				require.Empty(t, databaseEvent.database)
				require.Equal(t, byte(1), waitResponse()[4])
				_ = waitResponse() // column definition
				row := waitResponse()
				require.Equal(t, []byte{0xfb}, row[mysqlHeadLen:], "DATABASE() must return NULL")
				_ = waitResponse() // result terminator
			}

			queryText := "select v from t where id=?"
			if database == "" {
				queryText = "select ?"
			}
			prepare := makeSimplePacket(queryText)
			prepare[4] = byte(frontend.COM_STMT_PREPARE)
			writeClient(prepare)
			prepareEvent := waitBackendCommand(byte(frontend.COM_STMT_PREPARE))
			require.Equal(t, database, prepareEvent.database)
			require.Equal(t, byte(1), waitResponse()[3])
			_ = waitResponse() // parameter definition
			require.False(t, tun.hasUnsafeClientState(), "prepare response must close its tracked request")

			parameter := uint32(generation + 1)
			writeClient(makeLegalStmtExecutePacket(1, parameter))
			executeEvent := waitBackendCommand(byte(frontend.COM_STMT_EXECUTE))
			require.Equal(t, parameter, executeEvent.parameter)
			require.Equal(t, database, executeEvent.database)
			require.True(t, isOKPacket(waitResponse()))
			require.False(t, tun.hasUnsafeClientState(), "execute response must close its tracked request")

			writeClient(makeStmtCommandPacket(frontend.COM_STMT_CLOSE, 1))
			waitBackendCommand(byte(frontend.COM_STMT_CLOSE))
			require.Eventually(t, tun.hasFenceableClosedStatementState,
				time.Second, time.Millisecond,
				"COM_STMT_CLOSE must commit its tombstone before QUIT is published")
			require.False(t, tun.hasInFlightClientRequest())

			writeClient(makePayloadPacket(0, byte(cmdQuit)))
			waitBackendCommand(byte(cmdPing))
			select {
			case snapshot := <-resetEvents:
				require.Empty(t, snapshot.database)
				require.False(t, snapshot.prepared)
			case <-time.After(time.Second):
				t.Fatal("cache publication did not call ResetSession")
			}
			select {
			case <-quitHandled:
			case <-time.After(time.Second):
				t.Fatal("quit event handler did not finish")
			}
			require.True(t, client.isConnCached())
			require.Equal(t, 1, cache.Count())

			cleanupGeneration()
		}
		require.Equal(t, 1, router.connectCount,
			"all later client generations must reuse the single backend connection")
		require.Equal(t, int64(generations-1), refreshChecks.Load(),
			"every cached login must revalidate against the CN catalog")
		for generation := 1; generation < generations; generation++ {
			request := <-refreshRequests
			require.Equal(t, "tenant-a:dump", request.userInput)
			database := "db_a"
			if generation&1 == 1 {
				database = "db_b"
			}
			if generation == generations-1 {
				database = ""
			}
			require.Equal(t, database, request.database)
			require.Equal(t, "127.0.0.1:3307", request.address)
		}
		require.Empty(t, resetEvents, "all reset events must be consumed by their originating generation")

		require.NoError(t, cache.Close())
		select {
		case err := <-backendDone:
			require.Error(t, err)
		case <-time.After(time.Second):
			t.Fatal("backend responder did not terminate")
		}
	})
}
