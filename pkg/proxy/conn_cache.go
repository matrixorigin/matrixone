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

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
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
	// push is the operator to push the connection entry into the store.
	push(store *cacheStore, conn *serverConnAuth, postPush func())
	// pop is the operator to pop the connection entry from the store.
	pop(store *cacheStore, postPop func()) *serverConnAuth
	// peek is the operator to peek the connection entry in the store.
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
	closeOnce sync.Once
}

// newServerConnAuth creates a new server connection entry with authenticator.
func newServerConnAuth(sc ServerConn, auth Authenticator) *serverConnAuth {
	return &serverConnAuth{
		ServerConn:    sc,
		Authenticator: auth,
	}
}

func (s *serverConnAuth) close() error {
	var err error
	s.closeOnce.Do(func() {
		err = s.ServerConn.Close()
	})
	return err
}

func (s *serverConnAuth) ExecStmtContext(
	ctx context.Context,
	stmt internalStmt,
	resp chan<- []byte,
) (bool, error) {
	return execStmtWithContext(ctx, s.ServerConn, stmt, resp)
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
	Pop(cacheKey, uint32, []byte, []byte, clientInfo) ServerConn
	// Count returns the total number of cached connections. It is
	// mainly for testing.
	Count() int
	// Close closes connection cache instance.
	Close() error
}

type contextConnCache interface {
	PopContext(context.Context, cacheKey, uint32, []byte, []byte, clientInfo) ServerConn
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
		allConns map[ServerConn]*serverConnAuth
		closed   bool
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
	// canReuseCN decides whether a cached connection to the CN may be reused
	// for a fresh client login. If it returns false, the cached connection is
	// discarded before any SET CONNECTION ID or auth work is attempted.
	canReuseCN func(*CNServer, clientInfo) bool
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

func withCanReuseCN(f func(*CNServer, clientInfo) bool) connCacheOption {
	return func(c *connCache) {
		c.canReuseCN = f
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
	cc.mu.allConns = make(map[ServerConn]*serverConnAuth)
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
	ctx, cancel := context.WithTimeoutCause(c.ctx, time.Second*3, moerr.CauseResetSession)
	defer cancel()
	addr := getQueryAddress(c.moCluster, sc.RawConn().RemoteAddr().String())
	if addr == "" {
		return nil, moerr.NewInternalErrorf(ctx,
			"failed to get query service address, conn ID: %d", sc.ConnID())
	}
	resp, err := c.queryClient.SendMessage(ctx, addr, req)
	if err != nil {
		err = moerr.AttachCause(ctx, err)
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

// canPushLocked reports whether sc can be added to key. c.mu must be held.
// Push checks this both before and after resetSession because the network call
// deliberately runs without the cache mutex.
func (c *connCache) canPushLocked(key cacheKey, sc ServerConn) bool {
	if c.mu.closed || len(c.mu.allConns) >= c.maxNumTotal {
		return false
	}
	if _, ok := c.mu.allConns[sc]; ok {
		return false
	}
	store := c.mu.cache[key]
	return store == nil || len(store.connections) < c.maxNumPerTenant
}

// Push implements the ConnCache interface.
func (c *connCache) Push(key cacheKey, sc ServerConn) bool {
	c.mu.Lock()
	if !c.canPushLocked(key, sc) {
		c.mu.Unlock()
		return false
	}
	c.mu.Unlock()

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

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.canPushLocked(key, sc) {
		return false
	}
	store := c.mu.cache[key]
	if store == nil {
		store = newCacheStore()
		c.mu.cache[key] = store
	}
	connOperator[c.opStrategy].push(
		store,
		scWithAuth,
		func() { c.mu.allConns[sc] = scWithAuth },
	)
	return true
}

// closeCachedConnection terminates a connection without protocol I/O. The
// wrapper's closeOnce lets this race safely with Close, which also owns every
// connection that has been removed from a store but not handed to a caller.
func (c *connCache) closeCachedConnection(sc *serverConnAuth) {
	if err := sc.close(); err != nil {
		c.logger.Error("failed to close cached server connection",
			zap.Uint32("conn ID", sc.ConnID()),
			zap.Error(err),
		)
	}
	c.mu.Lock()
	delete(c.mu.allConns, sc.ServerConn)
	c.mu.Unlock()
}

// Pop implements the ConnCache interface.
func (c *connCache) Pop(
	key cacheKey, connID uint32, salt []byte, authResp []byte, client clientInfo,
) ServerConn {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTransferTimeout)
	defer cancel()
	return c.PopContext(ctx, key, connID, salt, authResp, client)
}

func (c *connCache) PopContext(
	ctx context.Context,
	key cacheKey,
	connID uint32,
	salt []byte,
	authResp []byte,
	client clientInfo,
) ServerConn {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if operationContextCause(ctx) != nil {
			return nil
		}
		// Reserve one available connection by removing it from the store. Keep
		// it in allConns until it is handed to the caller so Close can terminate
		// an in-flight SET CONNECTION ID that is blocked in backend I/O.
		c.mu.Lock()
		if c.mu.closed {
			c.mu.Unlock()
			return nil
		}
		store := c.mu.cache[key]
		sc := connOperator[c.opStrategy].pop(store, nil)
		c.mu.Unlock()
		if sc == nil {
			return nil
		}

		// Push is initiated by the originating tunnel's event handler. The cache
		// entry can therefore become visible before that old handler and its pipes
		// have finished cleanup. Never issue SET CONNECTION ID or authentication
		// against the backend until the old generation has released it.
		if err := waitServerConnCacheReuseReady(ctx, sc.ServerConn); err != nil {
			c.closeCachedConnection(sc)
			return nil
		}

		// Before using a cached connection for a fresh login, ensure its CN is
		// still eligible under the current login's route and health policy. This
		// avoids executing SET CONNECTION ID against a CN that a fresh Route()
		// would reject.
		if c.canReuseCN != nil && !c.canReuseCN(sc.GetCNServer(), client) {
			cnUUID := ""
			if cn := sc.GetCNServer(); cn != nil {
				cnUUID = cn.uuid
			}
			c.logger.Warn("skip cached connection on ineligible cn",
				zap.Uint32("conn ID", sc.ConnID()),
				zap.String("cn", cnUUID),
			)
			c.closeCachedConnection(sc)
			continue
		}

		// Check if the connection is expired.
		if time.Since(sc.CreateTime()) < c.connTimeout {
			ok, err := execStmtWithContext(ctx, sc, internalStmt{
				cmdType: cmdQuery,
				s:       fmt.Sprintf(setConnectionIDSQL, connID),
			}, nil)
			if err != nil || !ok {
				if operationContextCause(ctx) != nil {
					c.logger.Debug("set conn id canceled",
						zap.Uint32("conn ID", sc.ConnID()),
						zap.Error(err),
					)
				} else {
					c.logger.Error("failed to set conn id",
						zap.Uint32("conn ID", sc.ConnID()),
						zap.Error(err),
					)
				}
				c.closeCachedConnection(sc)
				continue
			}

			// Before use the connection, we have to check the authentication.
			if sc.Authenticator != nil && !sc.Authenticate(salt, authResp) {
				c.logger.Error("authenticate failed",
					zap.String("hash key", string(key)),
					zap.Uint32("conn ID", connID),
				)
				// Preserve the existing behavior of keeping the connection reusable
				// after an authentication mismatch. A concurrent Push may have filled
				// the freed tenant slot; in that case discard this connection instead
				// of exceeding the configured bound.
				c.mu.Lock()
				if c.mu.closed {
					c.mu.Unlock()
					return nil
				}
				store = c.mu.cache[key]
				if store == nil {
					store = newCacheStore()
					c.mu.cache[key] = store
				}
				if len(store.connections) < c.maxNumPerTenant {
					connOperator[c.opStrategy].push(store, sc, nil)
					c.mu.Unlock()
					return nil
				}
				c.mu.Unlock()
				c.closeCachedConnection(sc)
				return nil
			}

			// Transfer ownership to the caller. If Close linearized first, it
			// already owns and closes this in-flight connection, so do not return it.
			c.mu.Lock()
			if c.mu.closed {
				c.mu.Unlock()
				return nil
			}
			delete(c.mu.allConns, sc.ServerConn)
			c.mu.Unlock()
			return sc.ServerConn
		} else {
			c.closeCachedConnection(sc)
		}
	}
}

// Count implements the ConnCache interface.
func (c *connCache) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.closed {
		return 0
	}
	return len(c.mu.allConns)
}

// Close implements the ConnCache interface.
func (c *connCache) Close() error {
	c.mu.Lock()
	if c.mu.closed {
		c.mu.Unlock()
		return nil
	}
	c.mu.closed = true
	conns := c.mu.allConns
	c.mu.cache = nil
	c.mu.allConns = nil
	c.mu.Unlock()
	for _, conn := range conns {
		_ = conn.close()
	}
	return nil
}
