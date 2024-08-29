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
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/assert"
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

			sc := cc.Pop("k100", 1, nil, nil)
			assert.NotNil(t, sc)
			assert.Equal(t, 0, cc.Count())
		})
	})

	t.Run("pop - pwd auth", func(t *testing.T) {
		runTestWithNewConnCacheWithAuthConstructor(t, newPwdAuthenticator, func(cc ConnCache) {
			c1, _ := net.Pipe()
			mockConn1 := newMockServerConn(c1)
			assert.True(t, cc.Push("k100", mockConn1))
			assert.Equal(t, 1, cc.Count())

			sc := cc.Pop("k100", 1, nil, nil)
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

			sc := cc.Pop("k100", 1, nil, nil)
			// cannot get conn as timeout.
			assert.Nil(t, sc)
			// count is 0 because the connection has been removed.
			assert.Equal(t, 0, cc.Count())
		})
	})
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
