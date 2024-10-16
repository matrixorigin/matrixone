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
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"go.uber.org/zap"
)

const (
	defaultMaxNumTotal     = 1024
	defaultMaxNumPerTenant = 128
	defaultConnTimeout     = time.Hour * 12
	// The SQL used to reset connection ID of the reused connection.
	// TODO(volgariver6): how to make this SQL could only be
	// executed by internal request, but not external user?
	setConnectionIDSQL = "/* cloud_nonuser */ SET CONNECTION ID TO %d"
)

// OpStrategy is the strategy to the operation of the connection on the store.
type OpStrategy uint8

const (
	// OpFIFO means that pop the connection entry which is pushed firstly.
	OpFIFO OpStrategy = 0
)

// EntryOperation contains the operations of the entry in the store.
// Locked is required before the operations are called.
type EntryOperation interface {
	// doPush is the operator to push the connection entry into the store.
	push(store *cacheStore, conn *serverConnAuth, postPush func())
	// doPop is the operator to pop the connection entry from the store.
	pop(store *cacheStore, postPop func()) *serverConnAuth
	// doPeek is the operator to peek the connection entry in the store.
	peek(store *cacheStore) *serverConnAuth
}

// entryOpFIFO is the first-in-first-out operator of the connection entry.
type entryOpFIFO struct{}

// push implements the EntryOperation interface.
func (o *entryOpFIFO) push(store *cacheStore, conn *serverConnAuth, postPush func()) {
	if store == nil {
		return
	}
	store.connections = append(store.connections, conn)
	if postPush != nil {
		postPush()
	}
}

// pop implements the EntryOperation interface.
func (o *entryOpFIFO) pop(store *cacheStore, postPop func()) *serverConnAuth {
	if store == nil || len(store.connections) == 0 {
		return nil
	}
	sc := store.connections[0]
	store.connections = store.connections[1:]
	if postPop != nil {
		postPop()
	}
	return sc
}

// peek implements the EntryOperation interface.
func (o *entryOpFIFO) peek(store *cacheStore) *serverConnAuth {
	if store == nil || len(store.connections) == 0 {
		return nil
	}
	return store.connections[0]
}

// ConnOperator is the type of connection operator. It is a map
// which key is operation strategy and value is the entry operation.
type ConnOperator map[OpStrategy]EntryOperation

// connOperator contains all strategies of operators.
var connOperator ConnOperator = map[OpStrategy]EntryOperation{
	OpFIFO: &entryOpFIFO{},
}

// Authenticator is used to check if the connection is ok to use,
// which is mainly password-check for now.
type Authenticator interface {
	// Authenticate does the authentication.
	Authenticate(salt, authResp []byte) bool
}

type authenticatorConstructor func([]byte) Authenticator

// pwdAuthenticator implements the Authenticator with password checking.
type pwdAuthenticator struct {
	authString []byte
}

func newPwdAuthenticator(authString []byte) Authenticator {
	return &pwdAuthenticator{
		authString: authString,
	}
}

// Authenticate implements the Authenticator interface.
func (a *pwdAuthenticator) Authenticate(salt, authResp []byte) bool {
	return frontend.CheckPassword(a.authString, salt, authResp)
}

// serverConnAuth wraps the ServerConn and Authenticator.
type serverConnAuth struct {
	ServerConn
	Authenticator
}

// newServerConnAuth creates a new server connection entry with authenticator.
func newServerConnAuth(sc ServerConn, auth Authenticator) *serverConnAuth {
	return &serverConnAuth{
		ServerConn:    sc,
		Authenticator: auth,
	}
}

// cacheStore is the storage which stores the cached connections.
type cacheStore struct {
	connections []*serverConnAuth
}

// newCacheStore creates a new cacheStore.
func newCacheStore() *cacheStore {
	return &cacheStore{}
}

// count returns the connection number in the store.
func (s *cacheStore) count() int {
	if s == nil {
		return 0
	}
	return len(s.connections)
}

// cacheKey is the key used to distinguish connections. Connections with
// same cacheKey is stored in the same bucket.
type cacheKey = LabelHash

// ConnCache is the interface which managed the server connections.
type ConnCache interface {
	// Push pushes a server connection by the key to the cache.
	// Returns if the connection is pushed into the cache.
	Push(cacheKey, ServerConn) bool
	// Pop pops a server connection from the cache.
	Pop(cacheKey, uint32, []byte, []byte) ServerConn
	// Count returns the total number of cached connections. It is
	// mainly for testing.
	Count() int
	// Close closes connection cache instance.
	Close() error
}

// the main cache struct.
type connCache struct {
	ctx    context.Context
	logger *log.MOLogger

	// maxNum is the max number in the cache totally.
	maxNumTotal int
	// maxNum is the max number in the cache for per tenant.
	maxNumPerTenant int

	// the cache store.
	mu struct {
		sync.Mutex
		cache    map[cacheKey]*cacheStore
		allConns map[ServerConn]struct{}
	}
	// resetSessionFunc is the function used to reset session.
	resetSessionFunc func(ServerConn) ([]byte, error)
	// connTimeout is the timeout for all connections in cache.
	connTimeout time.Duration
	// OpStrategy is the strategy to operate a connection on the store.
	// There is only one strategy for now, which is OpFirst.
	opStrategy OpStrategy
	// moCluster is the cluster service instance.
	moCluster clusterservice.MOCluster
	// queryClient is the client which could send RPC request to
	// query service in cn node.
	queryClient client.QueryClient
	// authConstructor constructs the authenticator.
	authConstructor authenticatorConstructor
}

// connCacheOption is the option for connCache.
type connCacheOption func(c *connCache)

// withConnTimeout is the option to set connection timeout of connCache.
func withConnTimeout(t time.Duration) connCacheOption {
	return func(c *connCache) {
		c.connTimeout = t
	}
}

// withAuthConstructor is the option to set authentication constructor of connCache.
func withAuthConstructor(ac authenticatorConstructor) connCacheOption {
	return func(c *connCache) {
		c.authConstructor = ac
	}
}

// withResetSessionFunc is the option to set resetSessionFunc of connCache.
func withResetSessionFunc(f func(ServerConn) ([]byte, error)) connCacheOption {
	return func(c *connCache) {
		c.resetSessionFunc = f
	}
}

// withMaxNumTotal is the option to set maxNumTotal of connCache.
func withMaxNumTotal(n int) connCacheOption {
	return func(c *connCache) {
		c.maxNumTotal = n
	}
}

// withMaxNumPerTenant is the option to set maxNumPerTenant of connCache.
func withMaxNumPerTenant(n int) connCacheOption {
	return func(c *connCache) {
		c.maxNumPerTenant = n
	}
}

// withQueryClient is the option to set query client of connCache.
func withQueryClient(qc client.QueryClient) connCacheOption {
	return func(c *connCache) {
		c.queryClient = qc
	}
}

func newConnCache(
	ctx context.Context, sid string, logger *log.MOLogger, opts ...connCacheOption,
) ConnCache {
	var mc clusterservice.MOCluster
	v, ok := runtime.ServiceRuntime(sid).GetGlobalVariables(runtime.ClusterService)
	if ok {
		mc = v.(clusterservice.MOCluster)
	}
	cc := &connCache{
		ctx:             ctx,
		logger:          logger,
		maxNumTotal:     defaultMaxNumTotal,
		maxNumPerTenant: defaultMaxNumPerTenant,
		connTimeout:     defaultConnTimeout,
		opStrategy:      OpFIFO,
		moCluster:       mc,
		authConstructor: newPwdAuthenticator,
	}
	// Set the default resetSession function.
	cc.resetSessionFunc = cc.resetSession
	cc.mu.cache = make(map[cacheKey]*cacheStore)
	cc.mu.allConns = make(map[ServerConn]struct{})
	for _, opt := range opts {
		opt(cc)
	}
	return cc
}

func (c *connCache) resetSession(sc ServerConn) ([]byte, error) {
	// Clear the session in frontend.
	req := c.queryClient.NewRequest(query.CmdMethod_ResetSession)
	req.ResetSessionRequest = &query.ResetSessionRequest{
		ConnID: sc.ConnID(),
	}
	ctx, cancel := context.WithTimeout(c.ctx, time.Second*3)
	defer cancel()
	addr := getQueryAddress(c.moCluster, sc.RawConn().RemoteAddr().String())
	if addr == "" {
		return nil, moerr.NewInternalErrorf(ctx,
			"failed to get query service address, conn ID: %d", sc.ConnID())
	}
	resp, err := c.queryClient.SendMessage(ctx, addr, req)
	if err != nil {
		c.logger.Error("failed to send clear session request",
			zap.Uint32("conn ID", sc.ConnID()), zap.Error(err))
		return nil, err
	}
	if resp != nil {
		defer c.queryClient.Release(resp)
	}
	if resp == nil || resp.ResetSessionResponse == nil || !resp.ResetSessionResponse.Success {
		return nil, moerr.NewInternalErrorf(ctx,
			"failed to clear session, conn ID: %d", sc.ConnID())
	}
	return resp.ResetSessionResponse.AuthString, nil
}

// Push implements the ConnCache interface.
func (c *connCache) Push(key cacheKey, sc ServerConn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.mu.allConns) >= c.maxNumTotal {
		return false
	}
	// The connection already exists in the cache.
	if _, ok := c.mu.allConns[sc]; ok {
		return false
	}
	store, ok := c.mu.cache[key]
	if !ok {
		store = newCacheStore()
		c.mu.cache[key] = store
	}
	if len(store.connections) >= c.maxNumPerTenant {
		return false
	}

	var err error
	var authString []byte
	if c.resetSessionFunc != nil {
		if authString, err = c.resetSessionFunc(sc); err != nil {
			return false
		}
	}

	var scWithAuth *serverConnAuth
	if c.authConstructor != nil {
		scWithAuth = newServerConnAuth(sc, c.authConstructor(authString))
	} else {
		scWithAuth = newServerConnAuth(sc, nil)
	}

	connOperator[c.opStrategy].push(
		store,
		scWithAuth,
		func() { c.mu.allConns[sc] = struct{}{} },
	)
	return true
}

// Pop implements the ConnCache interface.
func (c *connCache) Pop(key cacheKey, connID uint32, salt []byte, authResp []byte) ServerConn {
	c.mu.Lock()
	defer c.mu.Unlock()
	store, ok := c.mu.cache[key]
	if !ok {
		return nil
	}
	if store == nil || len(store.connections) == 0 {
		return nil
	}
	// If it has expired, trash it and pop the second one.
	for len(store.connections) != 0 {
		sc := connOperator[c.opStrategy].peek(c.mu.cache[key])

		postPop := func() {
			delete(c.mu.allConns, sc.ServerConn)
		}

		// Check if the connection is expired.
		if time.Since(sc.CreateTime()) < c.connTimeout {
			ok, err := sc.ExecStmt(internalStmt{
				cmdType: cmdQuery,
				s:       fmt.Sprintf(setConnectionIDSQL, connID),
			}, nil)
			if err != nil || !ok {
				// Failed to set connection ID, try to send quit command to the server.
				if err := sc.Quit(); err != nil {
					c.logger.Error("failed to send quit cmd to server",
						zap.Uint32("conn ID", sc.ConnID()),
						zap.Error(err),
					)
				} else {
					// If send quit successfully, pop the connection from the store.
					connOperator[c.opStrategy].pop(c.mu.cache[key], postPop)
				}
				continue
			}

			// Before use the connection, we have to check the authentication.
			if sc.Authenticator != nil && !sc.Authenticate(salt, authResp) {
				c.logger.Error("authenticate failed",
					zap.String("hash key", string(key)),
					zap.Uint32("conn ID", connID),
				)
				return nil
			}

			// The peeked connection is ok to use, pop it from the store.
			connOperator[c.opStrategy].pop(c.mu.cache[key], postPop)

			return sc.ServerConn
		} else {
			if err := sc.Quit(); err != nil {
				c.logger.Error("failed to send quit cmd to server",
					zap.Uint32("conn ID", sc.ConnID()),
					zap.Error(err),
				)
			}
			// The connection is expired, pop it from cache.
			connOperator[c.opStrategy].pop(c.mu.cache[key], postPop)
		}
	}
	// There is no connection to use.
	return nil
}

// Count implements the ConnCache interface.
func (c *connCache) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.mu.allConns)
}

// Close implements the ConnCache interface.
func (c *connCache) Close() error {
	c.mu.Lock()
	conns := c.mu.allConns
	c.mu.allConns = nil
	c.mu.Unlock()
	for conn := range conns {
		_ = conn.Quit()
	}
	return nil
}
