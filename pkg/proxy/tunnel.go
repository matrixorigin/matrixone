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
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/petermattis/goid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	// The default transfer timeout is 10s.
	defaultTransferTimeout = time.Second * 10

	connClientName = "client"
	connServerName = "server"

	pipeClientToServer = "c2s"
	pipeServerToClient = "s2c"

	minSequenceID = 0
	maxSequenceID = 255
)

var (
	// errPipeClosed indicates that the pipe has been closed.
	errPipeClosed = moerr.NewInternalErrorNoCtx("pipe has been closed")
)

type tunnelOption func(*tunnel)

func withRebalancer(r *rebalancer) tunnelOption {
	return func(t *tunnel) {
		t.rebalancer = r
	}
}

func withRebalancePolicy(policy RebalancePolicy) tunnelOption {
	return func(t *tunnel) {
		t.rebalancePolicy = policy
	}
}

func withRealConn() tunnelOption {
	return func(t *tunnel) {
		t.realConn = true
	}
}

func withConnCacheEnabled(v bool) tunnelOption {
	return func(t *tunnel) {
		t.connCacheEnabled = v
	}
}

// withCacheReuseBarrier prevents a cached backend from being reused until the
// originating handler has finished all of its tunnel cleanup. Closing the
// tunnel is only the terminal linearization point; pipe and handler defers may
// still hold references to that generation afterwards.
func withCacheReuseBarrier() tunnelOption {
	return func(t *tunnel) {
		t.cacheReuseReady = make(chan struct{})
	}
}

type transferType int

const (
	transferByRebalance transferType = 0
	transferByScaling   transferType = 1
)

// tunnel is used to forward client message to CN server.
type tunnel struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
	logger    *log.MOLogger
	// errC is a channel indicates the tunnel error.
	errC chan error
	// cc is the client connection which this tunnel holds.
	cc ClientConn
	// reqC is the event request channel. Events may be happened in tunnel data flow,
	// and need to be handled in client connection.
	reqC chan IEvent
	// respC is the event response channel.
	respC chan []byte
	// closeOnce controls the close function to close tunnel only once.
	closeOnce sync.Once
	// cacheReuseReady is closed by the owning handler after all work from this
	// tunnel generation has stopped. A cached backend must not execute commands
	// for its next client before this barrier, or stale pipes can access the
	// reused transport concurrently.
	cacheReuseReady     chan struct{}
	cacheReuseReadyOnce sync.Once
	// counterSet counts the events in proxy.
	counterSet *counterSet
	// the global rebalancer.
	rebalancer *rebalancer
	// transferProactive means that the connection transfer is more proactive.
	rebalancePolicy RebalancePolicy
	// connCacheEnabled indicates if the connection cache is enabled.
	connCacheEnabled bool
	// transferType is the type for transferring: rebalancing and scaling.
	transferType transferType
	// realConn indicates the connection in the tunnel is a real network
	// connection but not a net.Pipe. It is used for testing. If it does NOt
	// run in testing, the Close() method does not to be called, as it is
	// closed in goetty module.
	realConn bool

	// transferIntent indicates that this tunnel was tried to transfer to
	// other servers, but not safe to. Set it to true to do the transfer
	// more proactive.
	// It only works if RebalancePolicy is "active".
	transferIntent atomic.Bool
	// expectedCacheQuit indicates this tunnel already intercepted a client QUIT
	// for connection caching, so the follow-up client EOF should not tear down
	// the backend session that is being cached.
	expectedCacheQuit atomic.Bool
	// expectedClientQuit indicates the client sent COM_QUIT. It covers both
	// the conn-cache path above and the non-cache path where COM_QUIT is
	// forwarded to CN.
	expectedClientQuit atomic.Bool
	// cacheIdentityChanged permanently disables cache publication for this
	// tunnel generation after a command changes the authenticated principal.
	// COM_CHANGE_USER and SET ROLE alter CN-side identity that ResetSession does
	// not reconstruct from the original handshake.
	cacheIdentityChanged atomic.Bool
	// requestBoundary is the authoritative request/response ownership state.
	// It deliberately becomes permanently unsafe for this tunnel generation if
	// a client pipelines commands: the MySQL command protocol is sequential and
	// retaining a queue here would let an unauthenticated peer grow proxy memory.
	requestBoundary struct {
		sync.Mutex
		inFlight                 bool
		ambiguous                bool
		command                  frontend.CommandType
		statementID              uint32
		statementIDValid         bool
		requestContinuation      bool
		localInfileUpload        bool
		requestNextSequence      byte
		phase                    responsePhase
		legacyResultEOFSeen      bool
		prepareMetadataRemaining uint32
		// pendingLongData records whether a later backend response has fenced
		// each statement's most recent COM_STMT_SEND_LONG_DATA. An unfenced
		// entry cannot be reconciled through the query service because the
		// no-response command may still be waiting on the SQL socket.
		pendingLongData  map[uint32]bool
		closedStatements map[string]struct{}
		// The maps above stay bounded. Overflow is recoverable because a later
		// terminal response fences every earlier no-response command. Unknown
		// long-data state additionally requires an authoritative CN check before
		// migration, since the response proves delivery but not consumption.
		closedStatementsOverflow      bool
		pendingLongDataOverflow       bool
		pendingLongDataOverflowFenced bool
	}
	clientDeprecatesEOF bool

	mu struct {
		sync.Mutex
		// closed is the terminal generation state. It shares this lock with
		// backend publication so a replacement can never become reachable after
		// Close has selected the resources it owns.
		closed bool
		// started indicates that the tunnel has started.
		started bool
		// inTransfer means a transfer of server connection is in progress.
		inTransfer bool

		// sc is the server connection which this tunnel holds. when the connection transfer,
		// close the old one.
		sc ServerConn
		// clientConn is the connection between client and proxy.
		clientConn *MySQLConn
		// serverConn is the connection between server and proxy.
		serverConn *MySQLConn
		// There are two pipes in a tunnel: client to server and server to client,
		// which controls the data flow.
		// csp is a pipe from client to server.
		csp *pipe
		// scp is a pipe from server to client.
		scp *pipe
	}

	//id of the goroutine that runs tunnel
	goId int64
}

// newTunnel creates a tunnel.
func newTunnel(ctx context.Context, logger *log.MOLogger, cs *counterSet, opts ...tunnelOption) *tunnel {
	ctx, cancel := context.WithCancel(ctx)
	t := &tunnel{
		ctx:       ctx,
		ctxCancel: cancel,
		logger:    logger,
		errC:      make(chan error, 1),
		// We need to handle events synchronously, so this channel has no buffer.
		reqC: make(chan IEvent),
		// response channel should have buffer, because it is handled in the same
		// for-select with reqC.
		respC: make(chan []byte, 10),
		// set the counter set.
		counterSet: cs,
		goId:       goid.Get(),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// run starts the tunnel, make the data between client and server flow in it.
func (t *tunnel) run(cc ClientConn, sc ServerConn) error {
	if provider, ok := cc.(interface{ GetCapability() uint32 }); ok {
		t.clientDeprecatesEOF = provider.GetCapability()&frontend.CLIENT_DEPRECATE_EOF != 0
	}
	digThrough := func() error {
		t.mu.Lock()
		defer t.mu.Unlock()

		if t.ctx.Err() != nil {
			return t.ctx.Err()
		}
		t.cc = cc
		t.mu.sc = sc
		t.logger = t.logger.With(zap.Uint32("conn ID", cc.ConnID()), zap.Int64("tunnel goId", t.goId))
		t.mu.clientConn = newMySQLConn(
			connClientName,
			cc.RawConn(),
			0,
			t.reqC,
			t.respC,
			t.connCacheEnabled,
			cc.ConnID(),
		)
		t.mu.serverConn = newMySQLConn(
			connServerName,
			sc.RawConn(),
			0,
			t.reqC,
			t.respC,
			t.connCacheEnabled,
			sc.ConnID(),
		)

		// Create the pipes from client to server and server to client.
		t.mu.csp = t.newPipe(pipeClientToServer, t.mu.clientConn, t.mu.serverConn)
		t.mu.scp = t.newPipe(pipeServerToClient, t.mu.serverConn, t.mu.clientConn)

		return nil
	}

	if err := digThrough(); err != nil {
		return moerr.NewInternalErrorNoCtxf("set up tunnel failed: %v", err)
	}
	if err := t.kickoff(); err != nil {
		return moerr.NewInternalErrorNoCtxf("kickoff pipe failed: %v", err)
	}

	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.mu.started = true
	}()

	return nil
}

// getPipes returns the pipes.
func (t *tunnel) getPipes() (*pipe, *pipe) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.csp, t.mu.scp
}

// getConns returns the client connection and server connection.
func (t *tunnel) getConns() (*MySQLConn, *MySQLConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.clientConn, t.mu.serverConn
}

// getServerConn returns the ServerConn in the tunnel.
func (t *tunnel) getServerConn() ServerConn {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.sc
}

func (t *tunnel) markExpectedCacheQuit() {
	if t != nil {
		t.expectedCacheQuit.Store(true)
		t.markExpectedClientQuit()
	}
}

func (t *tunnel) hasExpectedCacheQuit() bool {
	return t != nil && t.expectedCacheQuit.Load()
}

func (t *tunnel) markExpectedClientQuit() {
	if t != nil {
		t.expectedClientQuit.Store(true)
	}
}

func (t *tunnel) hasExpectedClientQuit() bool {
	return t != nil && t.expectedClientQuit.Load()
}

func (t *tunnel) waitCacheReuseReady(ctx context.Context) error {
	if t == nil || t.cacheReuseReady == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-t.cacheReuseReady:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (t *tunnel) markCacheReuseReady() {
	if t == nil || t.cacheReuseReady == nil {
		return
	}
	t.cacheReuseReadyOnce.Do(func() {
		close(t.cacheReuseReady)
	})
}

func (t *tunnel) markCacheIdentityChanged() {
	if t != nil {
		t.cacheIdentityChanged.Store(true)
	}
}

func (t *tunnel) hasCacheIdentityChanged() bool {
	return t != nil && t.cacheIdentityChanged.Load()
}

type responsePhase uint8

const (
	responsePhaseFirst responsePhase = iota
	responsePhaseResult
	responsePhaseLocalInfile
	responsePhasePrepareMetadata
)

const maxTrackedStatementIDs = 1024

type clientRequestCommit struct {
	closedStatementID uint32
	closesStatement   bool
}

func (t *tunnel) trackClientRequest(msg []byte) clientRequestCommit {
	var commit clientRequestCommit
	if t == nil {
		return commit
	}
	msg = firstMySQLPacketPrefix(msg)
	if len(msg) < mysqlHeadLen {
		return commit
	}

	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	s := &t.requestBoundary
	if s.localInfileUpload {
		if msg[3] != s.requestNextSequence {
			s.ambiguous = true
			return commit
		}
		s.requestNextSequence++
		if mysqlPacketPayloadLength(msg) == 0 {
			s.localInfileUpload = false
		}
		return commit
	}
	if s.requestContinuation {
		if msg[3] != s.requestNextSequence {
			s.ambiguous = true
			return commit
		}
		s.requestNextSequence++
		if mysqlPacketPayloadLength(msg) < int(frontend.MaxPayloadSize) {
			s.requestContinuation = false
		}
		return commit
	}
	if msg[3] != 0 {
		// A non-zero sequence is safe only in one of the explicitly tracked
		// multi-packet request phases above. The CN packet reader does not reject
		// an otherwise unexpected sequence, so ignoring it here could let a
		// second command execute without owning a tracked response.
		s.ambiguous = true
		return commit
	}
	if len(msg) < preRecvLen || mysqlPacketPayloadLength(msg) < 1 {
		s.ambiguous = true
		return commit
	}
	if s.inFlight {
		// MySQL commands are sequential. Once a peer pipelines two commands we
		// keep this generation non-cacheable instead of retaining an unbounded
		// command queue or guessing which response belongs to which request.
		s.ambiguous = true
		return commit
	}
	if mysqlPacketPayloadLength(msg) == int(frontend.MaxPayloadSize) {
		s.requestContinuation = true
		s.requestNextSequence = 1
	}

	cmd := frontend.CommandType(msg[4])
	switch cmd {
	case frontend.COM_QUIT:
		return commit
	case frontend.COM_CHANGE_USER:
		// COM_CHANGE_USER carries a new authenticated principal and database.
		// The backend generation cannot be safely reconstructed by ResetSession,
		// so never publish it to the cache after this command is observed.
		t.cacheIdentityChanged.Store(true)
	case frontend.COM_STMT_SEND_LONG_DATA:
		if mysqlPacketPayloadLength(msg) < 7 || len(msg) < mysqlHeadLen+7 {
			s.ambiguous = true
			return commit
		}
		statementID := binary.LittleEndian.Uint32(msg[5:9])
		if s.pendingLongData == nil {
			s.pendingLongData = make(map[uint32]bool)
		}
		if _, ok := s.pendingLongData[statementID]; !ok {
			if len(s.pendingLongData) >= maxTrackedStatementIDs {
				s.pendingLongDataOverflow = true
				s.pendingLongDataOverflowFenced = false
				return commit
			}
		}
		// Repeated chunks start a new unfenced generation too. A response that
		// preceded this chunk says nothing about whether the backend received it.
		s.pendingLongData[statementID] = false
		return commit
	case frontend.COM_STMT_CLOSE:
		if mysqlPacketPayloadLength(msg) < 5 || len(msg) < mysqlHeadLen+5 {
			s.ambiguous = true
			return commit
		}
		statementID := binary.LittleEndian.Uint32(msg[5:9])
		s.inFlight = true
		s.command = cmd
		s.statementID = statementID
		s.statementIDValid = true
		commit.closedStatementID = statementID
		commit.closesStatement = true
		return commit
	}

	s.inFlight = true
	s.command = cmd
	s.statementIDValid = false
	if (cmd == frontend.COM_STMT_EXECUTE || cmd == frontend.COM_STMT_RESET) &&
		mysqlPacketPayloadLength(msg) >= 5 && len(msg) >= mysqlHeadLen+5 {
		s.statementID = binary.LittleEndian.Uint32(msg[5:9])
		s.statementIDValid = true
	}
	s.phase = responsePhaseFirst
	s.legacyResultEOFSeen = false
	s.prepareMetadataRemaining = 0
	return commit
}

func (t *tunnel) commitClientRequest(commit clientRequestCommit) {
	if t == nil || !commit.closesStatement {
		return
	}
	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	if t.requestBoundary.ambiguous ||
		!t.requestBoundary.inFlight ||
		t.requestBoundary.command != frontend.COM_STMT_CLOSE ||
		!t.requestBoundary.statementIDValid ||
		t.requestBoundary.statementID != commit.closedStatementID {
		return
	}
	if t.requestBoundary.closedStatements == nil {
		t.requestBoundary.closedStatements = make(map[string]struct{})
	}
	statementName := frontend.GetPrepareStmtName(commit.closedStatementID)
	if _, ok := t.requestBoundary.closedStatements[statementName]; !ok {
		if len(t.requestBoundary.closedStatements) >= maxTrackedStatementIDs {
			t.requestBoundary.closedStatementsOverflow = true
		} else {
			t.requestBoundary.closedStatements[statementName] = struct{}{}
		}
	}
	delete(t.requestBoundary.pendingLongData, commit.closedStatementID)
	t.resetTrackedRequestLocked()
}

func (t *tunnel) hasInFlightClientRequest() bool {
	if t == nil {
		return false
	}
	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	return t.requestBoundary.inFlight
}

func (t *tunnel) hasUnsafeClientState() bool {
	if t == nil {
		return false
	}
	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	return t.requestBoundary.inFlight ||
		t.requestBoundary.requestContinuation ||
		t.requestBoundary.localInfileUpload ||
		t.requestBoundary.ambiguous ||
		t.requestBoundary.closedStatementsOverflow ||
		t.requestBoundary.pendingLongDataOverflow ||
		len(t.requestBoundary.pendingLongData) > 0 ||
		len(t.requestBoundary.closedStatements) > 0
}

// hasFenceableClosedStatementState reports whether the only remaining unsafe
// client state is a completed COM_STMT_CLOSE. A successful COM_PING on the same
// backend socket fences every packet sent before it, including an overflowed
// close tombstone set. Other protocol states remain non-cacheable.
func (t *tunnel) hasFenceableClosedStatementState() bool {
	if t == nil {
		return false
	}
	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	s := &t.requestBoundary
	if s.inFlight || s.requestContinuation || s.localInfileUpload || s.ambiguous ||
		s.pendingLongDataOverflow || len(s.pendingLongData) > 0 {
		return false
	}
	return s.closedStatementsOverflow || len(s.closedStatements) > 0
}

// completeClosedStatementFence clears only state proven delivered by the
// same-backend PING. It deliberately leaves every other protocol state intact.
func (t *tunnel) completeClosedStatementFence() {
	if t == nil {
		return
	}
	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	clear(t.requestBoundary.closedStatements)
	t.requestBoundary.closedStatementsOverflow = false
}

func (t *tunnel) hasUntransferableClientState() bool {
	if t == nil {
		return false
	}
	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	if t.requestBoundary.inFlight || t.requestBoundary.requestContinuation ||
		t.requestBoundary.localInfileUpload ||
		t.requestBoundary.ambiguous ||
		t.requestBoundary.closedStatementsOverflow ||
		(t.requestBoundary.pendingLongDataOverflow &&
			!t.requestBoundary.pendingLongDataOverflowFenced) {
		return true
	}
	for _, fenced := range t.requestBoundary.pendingLongData {
		if !fenced {
			return true
		}
	}
	return false
}

// rejectPendingLongDataReconciliation makes the current staged-data
// generation non-transferable again after the old CN reports that it still
// owns binary parameter data. A later backend response may fence a subsequent
// SQL EXECUTE, RESET, DEALLOCATE, or PREPARE transition and permit another
// authoritative reconciliation attempt.
func (t *tunnel) rejectPendingLongDataReconciliation() {
	if t == nil {
		return
	}
	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	for statementID := range t.requestBoundary.pendingLongData {
		t.requestBoundary.pendingLongData[statementID] = false
	}
	if t.requestBoundary.pendingLongDataOverflow {
		t.requestBoundary.pendingLongDataOverflowFenced = false
	}
}

// acceptPendingLongDataSnapshot verifies that every staged-data generation is
// fenced by a later SQL-socket response and that the exporting CN understands
// the authoritative pending-data check. The capability check keeps rolling
// upgrades fail-closed when a new proxy is paired with an older CN.
func (t *tunnel) acceptPendingLongDataSnapshot(checked bool) bool {
	if t == nil {
		return true
	}
	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	if len(t.requestBoundary.pendingLongData) == 0 &&
		!t.requestBoundary.pendingLongDataOverflow {
		return true
	}
	if !checked {
		return false
	}
	for _, fenced := range t.requestBoundary.pendingLongData {
		if !fenced {
			return false
		}
	}
	return true
}

func (t *tunnel) filterClosedStatementsForMigration(stmts []*query.PrepareStmt) []*query.PrepareStmt {
	if t == nil {
		return stmts
	}
	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	if len(t.requestBoundary.closedStatements) == 0 {
		return stmts
	}
	filtered := stmts[:0]
	for _, stmt := range stmts {
		if stmt == nil {
			filtered = append(filtered, stmt)
			continue
		}
		if _, closed := t.requestBoundary.closedStatements[stmt.Name]; !closed {
			filtered = append(filtered, stmt)
		}
	}
	return filtered
}

func (t *tunnel) clearMigratedStatementState() {
	if t == nil {
		return
	}
	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	clear(t.requestBoundary.closedStatements)
	clear(t.requestBoundary.pendingLongData)
	t.requestBoundary.closedStatementsOverflow = false
	t.requestBoundary.pendingLongDataOverflow = false
	t.requestBoundary.pendingLongDataOverflowFenced = false
}

func (t *tunnel) resetTrackedRequestLocked() {
	// Do not clear the framing substates here. A no-response command can commit
	// after its first MaxPayload packet; its continuation must keep migration
	// gated until the final packet is forwarded. A valid backend response only
	// arrives after both substates have already closed.
	t.requestBoundary.inFlight = false
	t.requestBoundary.command = 0
	t.requestBoundary.statementID = 0
	t.requestBoundary.statementIDValid = false
	t.requestBoundary.phase = responsePhaseFirst
	t.requestBoundary.legacyResultEOFSeen = false
	t.requestBoundary.prepareMetadataRemaining = 0
}

// finishLocallyConsumedRequest closes only the proxy-owned request boundary.
// Unlike a backend response, a local KILL or UPGRADE completion does not prove
// that the CN has processed earlier no-response statement commands.
func (t *tunnel) finishLocallyConsumedRequest() {
	if t == nil {
		return
	}
	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	if t.requestBoundary.inFlight && !t.requestBoundary.ambiguous {
		t.resetTrackedRequestLocked()
	}
}

func (t *tunnel) finishTrackedResponseLocked(status uint16, successful bool) {
	if status&frontend.SERVER_MORE_RESULTS_EXISTS != 0 {
		t.requestBoundary.phase = responsePhaseFirst
		t.requestBoundary.legacyResultEOFSeen = false
		return
	}
	if t.requestBoundary.statementIDValid {
		switch t.requestBoundary.command {
		case frontend.COM_STMT_EXECUTE:
			delete(t.requestBoundary.pendingLongData, t.requestBoundary.statementID)
		case frontend.COM_STMT_RESET:
			if successful {
				delete(t.requestBoundary.pendingLongData, t.requestBoundary.statementID)
			}
		}
	}
	// A terminal response is a causal fence for every earlier no-response
	// command on this MySQL connection. CLOSE tombstones are no longer needed:
	// the old CN snapshot now reflects the close and any same-name SQL PREPARE
	// that followed it. Staged long data becomes eligible for authoritative
	// reconciliation by MigrateConnFrom, but remains unsafe for connection cache.
	clear(t.requestBoundary.closedStatements)
	t.requestBoundary.closedStatementsOverflow = false
	for statementID := range t.requestBoundary.pendingLongData {
		t.requestBoundary.pendingLongData[statementID] = true
	}
	if t.requestBoundary.pendingLongDataOverflow {
		t.requestBoundary.pendingLongDataOverflowFenced = true
	}
	t.resetTrackedRequestLocked()
}

func (t *tunnel) trackServerResponse(msg []byte) {
	if t == nil {
		return
	}
	msg = firstMySQLPacketPrefix(msg)
	if len(msg) < preRecvLen {
		return
	}
	t.requestBoundary.Lock()
	defer t.requestBoundary.Unlock()
	s := &t.requestBoundary
	if !s.inFlight || s.ambiguous {
		return
	}
	if s.requestContinuation || s.localInfileUpload {
		// A backend response cannot complete while the corresponding client
		// request is still being framed or uploaded. Keep this generation
		// permanently non-transferable rather than guessing packet ownership.
		s.ambiguous = true
		return
	}
	if isErrPacket(msg) {
		t.finishTrackedResponseLocked(0, false)
		return
	}

	switch s.phase {
	case responsePhasePrepareMetadata:
		if s.prepareMetadataRemaining > 0 {
			s.prepareMetadataRemaining--
		}
		if s.prepareMetadataRemaining == 0 {
			t.finishTrackedResponseLocked(0, true)
		}
		return
	case responsePhaseLocalInfile:
		if status, ok := okPacketStatus(msg); ok {
			t.finishTrackedResponseLocked(status, true)
		}
		return
	case responsePhaseResult:
		if t.clientDeprecatesEOF {
			if status, ok := eofOKPacketStatus(msg); ok {
				t.finishTrackedResponseLocked(status, true)
			}
			return
		}
		status, ok := legacyEOFPacketStatus(msg)
		if !ok {
			return
		}
		if !s.legacyResultEOFSeen {
			s.legacyResultEOFSeen = true
			return
		}
		t.finishTrackedResponseLocked(status, true)
		return
	}

	if s.command == frontend.COM_STMT_PREPARE && len(msg) > 4 && msg[4] == 0 {
		remaining, ok := prepareMetadataPacketCount(msg, t.clientDeprecatesEOF)
		if !ok {
			return
		}
		statementID := binary.LittleEndian.Uint32(msg[5:9])
		delete(s.closedStatements, frontend.GetPrepareStmtName(statementID))
		if remaining == 0 {
			t.finishTrackedResponseLocked(0, true)
		} else {
			s.phase = responsePhasePrepareMetadata
			s.prepareMetadataRemaining = remaining
		}
		return
	}
	if status, ok := okPacketStatus(msg); ok {
		t.finishTrackedResponseLocked(status, true)
		return
	}
	if s.command == frontend.COM_STATISTICS {
		t.finishTrackedResponseLocked(0, true)
		return
	}
	if s.command == frontend.COM_FIELD_LIST || s.command == frontend.COM_STMT_FETCH {
		if status, ok := legacyEOFPacketStatus(msg); ok {
			t.finishTrackedResponseLocked(status, true)
		} else if status, ok := eofOKPacketStatus(msg); ok {
			t.finishTrackedResponseLocked(status, true)
		}
		return
	}
	if isLoadDataLocalInfileRespPacket(msg) {
		s.phase = responsePhaseLocalInfile
		s.localInfileUpload = true
		s.requestNextSequence = msg[3] + 1
		return
	}
	// All other first packets begin a result set. Its terminal packet depends
	// on CLIENT_DEPRECATE_EOF; row packets cannot release request ownership.
	s.phase = responsePhaseResult
}

func wrapPipeSendError(name string, err error) error {
	wrapped := errors.Join(
		moerr.NewInternalErrorNoCtxf("send message error: %v", err),
		err,
	)
	if name == pipeServerToClient && isConnEndErr(err) {
		return withCode(wrapped, codeClientDisconnect)
	}
	return wrapped
}

func (t *tunnel) reportPipeError(err error, defaultCode errorCode) {
	code := getErrorCode(err)
	if code == codeNone {
		code = defaultCode
		err = withCode(err, code)
	}
	switch code {
	case codeClientDisconnect:
		v2.ProxyClientDisconnectCounter.Inc()
	case codeServerDisconnect:
		v2.ProxyServerDisconnectCounter.Inc()
	}
	t.setError(err)
}

// setError tries to set the tunnel error if there is no error.
func (t *tunnel) setError(err error) {
	select {
	case t.errC <- err:
		_ = t.Close()
	default:
	}
}

// kickoff starts up the tunnel
func (t *tunnel) kickoff() error {
	csp, scp := t.getPipes()
	go func() {
		if err := csp.kickoff(t.ctx, scp); err != nil {
			t.reportPipeError(err, codeClientDisconnect)
		}
	}()
	go func() {
		if err := scp.kickoff(t.ctx, csp); err != nil {
			t.reportPipeError(err, codeServerDisconnect)
		}
	}()
	if err := csp.waitReady(t.ctx); err != nil {
		return err
	}
	if err := scp.waitReady(t.ctx); err != nil {
		return err
	}
	return nil
}

// replaceServerConn replaces the CN server.
func (t *tunnel) replaceServerConn(newServerConn *MySQLConn, newSC ServerConn, sync bool) error {
	t.mu.Lock()
	if t.mu.closed {
		t.mu.Unlock()
		// newSC owns the raw backend transport, connManager registration and
		// transient protocol-memory lease. The unpublished MySQL wrapper owns
		// only Go-heap buffers and becomes unreachable on return.
		if newSC != nil {
			_ = newSC.Close()
		} else if newServerConn != nil {
			_ = newServerConn.Close()
		}
		return errPipeClosed
	}
	defer t.mu.Unlock()

	oldServerConn := t.mu.serverConn

	// Preserve bufDst before closing the old connection. It targets the client
	// connection, which is unchanged, and may already contain ordered response
	// bytes from the old backend. Moving the writer preserves those bytes and
	// avoids a blocking network flush while t.mu is held; otherwise Close could
	// queue behind the data path it is supposed to terminate.
	var savedBufDst *bufio.Writer
	if oldServerConn != nil && oldServerConn.msgBuf != nil && oldServerConn.msgBuf.bufDst != nil {
		savedBufDst = oldServerConn.msgBuf.bufDst
		oldServerConn.msgBuf.bufDst = nil // detach before close
	}

	// close the old ones.
	_ = oldServerConn.Close()
	_ = t.mu.sc.Close()

	// set the new ones.
	t.mu.serverConn = newServerConn
	t.mu.sc = newSC
	// The writer targets the unchanged client connection, so it is safe to move
	// for both transfer modes. Reusing it avoids a 64 KiB allocation and leaves
	// no detached writer for the GC after every non-sync migration.
	if savedBufDst != nil {
		t.mu.serverConn.msgBuf.bufDst = savedBufDst
	}

	if sync {
		t.mu.csp.dst = t.mu.serverConn
		t.mu.scp.src = t.mu.serverConn
	} else {
		t.mu.csp = t.newPipe(pipeClientToServer, t.mu.clientConn, t.mu.serverConn)
		t.mu.scp = t.newPipe(pipeServerToClient, t.mu.serverConn, t.mu.clientConn)
	}
	// The new backend is now the one steady backend reserved for this client.
	// Release transient overlap only after the old backend has been closed and
	// every tunnel pointer has been switched.
	if leased, ok := newSC.(interface{ promoteProtocolMemory() }); ok {
		leased.promoteProtocolMemory()
	}
	return nil
}

// admitTransfer atomically checks transfer eligibility and closes the client
// publication gate. For synchronous migration, the returned c2s pipe owns the
// gate until finishSyncTransfer is called. Asynchronous migration uses the
// existing paused-pipe lifecycle instead.
func (t *tunnel) admitTransfer(sync bool) (*pipe, bool) {
	return t.admitTransferWithGate(sync, nil)
}

func (t *tunnel) admitTransferWithGate(sync bool, prearmed *pipe) (*pipe, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// The tunnel has not started.
	if t.mu.closed || !t.mu.started {
		return nil, false
	}

	csp, scp := t.mu.csp, t.mu.scp
	csp.mu.Lock()
	scp.mu.Lock()
	defer csp.mu.Unlock()
	defer scp.mu.Unlock()

	gateOwned := sync && prearmed == csp && csp.mu.syncTransferDone != nil
	if prearmed != nil && !gateOwned {
		return nil, false
	}
	if csp.mu.paused || scp.mu.paused {
		return nil, false
	}
	if csp.clientMessageActive.Load() ||
		(csp.mu.syncTransferDone != nil && !gateOwned) {
		t.logger.Info("reason: client message publication is active")
		return nil, false
	}
	if t.hasUntransferableClientState() {
		t.logger.Info("reason: client protocol state is not transferable")
		return nil, false
	}
	if t.hasExpectedClientQuit() {
		t.logger.Info("reason: client connection is terminating")
		return nil, false
	}

	// We are now in a transaction.
	if !scp.safeToTransferLocked() {
		t.logger.Info("reason: txn status is true")
		return nil, false
	}

	if sync && !gateOwned {
		csp.mu.syncTransferDone = make(chan struct{})
	} else if !sync {
		csp.mu.paused = true
		scp.mu.paused = true
	}

	return csp, true
}

func (t *tunnel) setTransferIntent(i bool) {
	t.setTransferIntentForType(i, t.getTransferType())
}

func (t *tunnel) setTransferIntentForType(i bool, typ transferType) {
	if i &&
		t.rebalancePolicy == RebalancePolicyPassive &&
		typ == transferByRebalance {
		return
	}
	if t.transferIntent.Swap(i) == i {
		return
	}
	t.logger.Info("set tunnel transfer intent", zap.Bool("value", i))
	if i {
		v2.ProxyConnectionsTransferIntentGauge.Inc()
	} else {
		v2.ProxyConnectionsTransferIntentGauge.Dec()
	}
}

func (t *tunnel) tryStartTransferAttempt() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.inTransfer {
		return false
	}
	t.mu.inTransfer = true
	return true
}

// finishTransferAttempt clears the queue latch for attempts that exit before pipes are paused.
func (t *tunnel) finishTransferAttempt() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.inTransfer = false
}

func (t *tunnel) finishTransfer(start time.Time) {
	t.setTransferIntent(false)
	t.setTransferType(transferByRebalance)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.inTransfer = false
	resume := func(p *pipe) {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.mu.paused = false
	}
	resume(t.mu.scp)
	resume(t.mu.csp)

	t.logger.Info("transfer end")
	duration := time.Since(start)
	if duration > time.Second {
		t.logger.Info("slow transfer for tunnel",
			zap.Duration("transfer duration", duration),
		)
	}
	v2.ProxyTransferDurationHistogram.Observe(time.Since(start).Seconds())
}

func (t *tunnel) doReplaceConnection(ctx context.Context, sync bool) error {
	newSC, newConn, err := t.getNewServerConn(ctx)
	if err != nil {
		t.logger.Error("failed to get a new connection", zap.Error(err))
		return err
	}
	if err := t.replaceServerConn(newConn, newSC, sync); err != nil {
		return err
	}
	t.counterSet.connMigrationSuccess.Add(1)
	t.logger.Info("transfer to a new CN server",
		zap.String("addr", newConn.RemoteAddr().String()))
	return nil
}

// transfer transfers the serverConn of tunnel to a new one.
func (t *tunnel) transfer(ctx context.Context) error {
	t.counterSet.connMigrationRequested.Add(1)
	// Must check if it is safe to start the transfer.
	if _, ok := t.admitTransfer(false); !ok {
		t.logger.Info("cannot start transfer safely")
		typ := t.getTransferType()
		t.finishTransferAttempt()
		t.setTransferIntentForType(true, typ)
		t.counterSet.connMigrationCannotStart.Add(1)
		return moerr.GetOkExpectedNotSafeToStartTransfer()
	}

	start := time.Now()
	defer t.finishTransfer(start)
	t.logger.Info("transfer begin")

	ctx, cancel := context.WithTimeoutCause(ctx, defaultTransferTimeout, moerr.CauseTransfer)
	defer cancel()

	csp, scp := t.getPipes()
	// Pause pipes before the transfer.
	if err := csp.pause(ctx); err != nil {
		v2.ProxyTransferFailCounter.Inc()
		return moerr.AttachCause(ctx, err)
	}
	if err := scp.pause(ctx); err != nil {
		v2.ProxyTransferFailCounter.Inc()
		return moerr.AttachCause(ctx, err)
	}
	if err := t.doReplaceConnection(ctx, false); err != nil {
		v2.ProxyTransferFailCounter.Inc()
		err = moerr.AttachCause(ctx, err)
		t.logger.Error("failed to replace connection", zap.Error(err))
	}
	// Restart pipes even if the error happened in last step.
	if err := t.kickoff(); err != nil {
		t.logger.Error("failed to kickoff tunnel", zap.Error(err))
		_ = t.Close()
	} else {
		v2.ProxyTransferSuccessCounter.Inc()
	}
	return nil
}

func (t *tunnel) transferSync(ctx context.Context) error {
	return t.transferSyncWithGate(ctx, nil)
}

func (t *tunnel) transferSyncWithGate(ctx context.Context, gate *pipe) error {
	if gate != nil {
		defer gate.finishSyncTransfer()
	}
	if ok := t.tryStartTransferAttempt(); !ok {
		t.logger.Info("tunnel is already in transfer, skip sync transfer")
		return nil
	}
	// Must check if it is safe to start the transfer.
	csp, ok := t.admitTransferWithGate(true, gate)
	if !ok {
		t.finishTransferAttempt()
		return moerr.GetOkExpectedNotSafeToStartTransfer()
	}
	if gate == nil {
		defer csp.finishSyncTransfer()
	}
	start := time.Now()
	defer t.finishTransfer(start)
	t.logger.Info("transfer begin")
	ctx, cancel := context.WithTimeoutCause(ctx, defaultTransferTimeout, moerr.CauseTransferSync)
	defer cancel()
	if err := t.doReplaceConnection(ctx, true); err != nil {
		v2.ProxyTransferFailCounter.Inc()
		return moerr.AttachCause(ctx, err)
	}
	v2.ProxyTransferSuccessCounter.Inc()
	return nil
}

// getNewServerConn selects a new CN server and connects to it then
// returns the new connection.
func (t *tunnel) getNewServerConn(ctx context.Context) (ServerConn, *MySQLConn, error) {
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}
	prevAddr := t.mu.serverConn.RemoteAddr().String()
	t.logger.Info("build connection with new server", zap.String("prev addr", prevAddr))
	newConn, err := t.cc.BuildConnWithServer(ctx, prevAddr)
	if err != nil {
		t.logger.Error("failed to build connection with new server",
			zap.String("prev addr", prevAddr),
			zap.Error(err),
		)
		return nil, nil, err
	}
	return newConn, newMySQLConn(
		connServerName,
		newConn.RawConn(),
		0,
		t.reqC,
		t.respC,
		t.connCacheEnabled,
		newConn.ConnID(),
	), nil
}

func (t *tunnel) getTransferType() transferType {
	return t.transferType
}

func (t *tunnel) setTransferType(typ transferType) {
	t.transferType = typ
}

// Close closes the tunnel.
func (t *tunnel) Close() error {
	t.closeOnce.Do(func() {
		// Select the terminal generation and its cleanup resources before any
		// cancellation can race a replacement into publishing new state.
		t.mu.Lock()
		t.mu.closed = true
		cc, sc := t.mu.clientConn, t.mu.serverConn
		serverC := t.mu.sc
		t.mu.Unlock()

		if t.ctxCancel != nil {
			t.ctxCancel()
		}
		// Close the event channels.
		close(t.reqC)
		// close(t.respC)

		// cc.Close() just only close the raw net connection, and it
		// is closed in goetty module, so do NOT need to close it here:
		// cc, sc := t.getConns()
		if cc != nil && !t.realConn {
			_ = cc.Close()
		}
		if !t.connCacheEnabled {
			// close the server connection
			if serverC != nil {
				_ = serverC.Close()
			} else if sc != nil {
				_ = sc.Close()
			}
		}
	})
	return nil
}

// pipe must be created through newPipe.
type pipe struct {
	name   string
	logger *log.MOLogger

	// source connection and destination connection wrapped
	// by a message buffer.
	src *MySQLConn
	dst *MySQLConn

	// syncTransferArmed is owned by the s2c goroutine. It records that c2s has
	// closed its publication gate for the next synchronous transfer attempt.
	syncTransferArmed bool
	// clientMessageActive covers the interval after c2s claims a buffered
	// message and before forwarding/local consumption commits. Admission and
	// the false-to-true transition are serialized by mu; completion only needs
	// a release store.
	clientMessageActive atomic.Bool

	mu struct {
		sync.Mutex
		// cond is used to control the pause of the pipe.
		cond *sync.Cond
		// closed indicates that the pipe is closed.
		closed bool
		// started indicates that the pipe has started.
		started bool
		// inPreRecv indicates that the pipe in the preRecv phase.
		inPreRecv bool
		// paused indicates that the pipe is paused to do transfer.
		paused bool
		// inTxn indicates that if the session is in a txn. It only
		// matters for server end.
		inTxn bool
		// syncTransferDone is non-nil while synchronous migration owns the
		// client publication gate. Closing it releases a waiting c2s pipe.
		syncTransferDone chan struct{}
	}

	// tun is the tunnel that the pipe belongs to.
	tun *tunnel

	testHelper struct {
		beforeSend         func()
		onSyncTransferWait func()
	}
	//id of goroutine that runs the pipe
	goId int64
}

// newPipe creates a pipe.
func (t *tunnel) newPipe(name string, src, dst *MySQLConn) *pipe {
	p := &pipe{
		name:   name,
		logger: t.logger.With(zap.String("pipe-direction", name)),
		src:    src,
		dst:    dst,
		tun:    t,
	}
	p.mu.cond = sync.NewCond(&p.mu)
	// Enable write batching for the server-to-client direction.
	// Result sets flow s2c and generate many small write syscalls;
	// bufDst accumulates them and flushes when the read buffer drains.
	if name == pipeServerToClient && src.msgBuf.bufDst == nil {
		src.msgBuf.bufDst = bufio.NewWriterSize(dst.Conn, writeBufLen)
	}
	return p
}

// kickoff starts up the pipe and the data would flow in it.
func (p *pipe) kickoff(ctx context.Context, peer *pipe) (e error) {
	start := func() (bool, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.goId = goid.Get()
		if p.logger != nil {
			p.logger = p.logger.With(zap.Int64("pipe goId", p.goId))
		}
		if p.mu.closed {
			return false, errPipeClosed
		}
		if p.mu.started {
			return true, nil
		}
		p.mu.started = true
		p.mu.cond.Broadcast()
		return false, nil
	}
	finish := func() {
		if p.name == pipeServerToClient && p.syncTransferArmed {
			p.syncTransferArmed = false
		}
		// Best-effort flush of buffered writes before shutting down.
		if p.src != nil && p.src.msgBuf != nil {
			_ = p.src.flushBufDst()
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if e != nil {
			p.mu.closed = true
		}
		p.mu.started = false
		p.mu.cond.Broadcast()
	}

	var firstCond bool
	var currSeq int16
	var lastSeq int16 = -1
	var rotated bool
	var stopAfterSend bool
	var armSyncTransfer bool
	var clientCommit clientRequestCommit
	var clientRequest []byte
	prepareNextMessage := func() (terminate bool, err error) {
		stopAfterSend = false
		armSyncTransfer = false
		clientCommit = clientRequestCommit{}
		clientRequest = nil
		if terminate := func() bool {
			p.mu.Lock()
			defer p.mu.Unlock()
			// pipe is paused to begin a connection transfer.
			if p.mu.paused {
				return true
			}
			p.mu.inPreRecv = true
			return false
		}(); terminate {
			return true, nil
		}
		packetSize, re := p.src.preRecv()
		// A fragmented packet may leave only its four-byte header in the buffer.
		// c2s needs the command and prepared-statement identifiers for request
		// tracking. s2c needs a bounded prefix containing both length-encoded OK
		// fields and its status flags;
		// otherwise a fragmented terminal response would never release request
		// ownership and every later clean QUIT would unnecessarily miss cache.
		if re == nil && packetSize >= preRecvLen {
			var prefixLen int
			if p.name == pipeServerToClient {
				const responseTrackingPrefixLen = mysqlHeadLen + 1 + 9 + 9 + 2
				prefixLen = min(packetSize, responseTrackingPrefixLen)
			} else {
				const requestTrackingPrefixLen = mysqlHeadLen + 1 + 4 + 2
				prefixLen = min(packetSize, requestTrackingPrefixLen)
			}
			re = p.src.receiveAtLeast(prefixLen)
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		p.mu.inPreRecv = false

		var netErr net.Error
		if p.mu.paused && re == nil {
			return true, nil
		} else if p.mu.paused && errors.As(re, &netErr) && netErr.Timeout() {
			// The preRecv is cut off by set the connection deadline to a pastime.
			return true, nil
		} else if re != nil {
			if errors.Is(re, io.EOF) {
				return false, re
			}
			return false, moerr.NewInternalErrorf(errutil.ContextWithNoReport(ctx, true),
				"preRecv message: %s, name %s", re.Error(), p.name)
		}
		tempBuf := p.src.readAvailBuf()
		// set txn status and cmd time within the mutex together.
		// only server->client pipe need to set the txn status.
		if p.name == pipeServerToClient {
			// issue#16042
			if len(tempBuf) > 3 {
				currSeq = int16(tempBuf[3])
			}

			// last sequence id is 255 and current sequence id is 0, the
			// sequence ID is rotated, in which case, we do NOT allow to
			// do the migration.
			if currSeq == minSequenceID && lastSeq == maxSequenceID {
				rotated = true
			}

			// the server starts a new response, reset the rotated.
			if rotated && currSeq != minSequenceID && currSeq < lastSeq {
				rotated = false
			}

			// seqID is mainly used for server side. It records the sequence ID of
			// each packet.
			// In the case of "load data local infile" statement, client sends the
			// first packet, then server sends response, which is "0xFB + filename",
			// after that, client sends content of filename and an empty packet, at
			// last, server sends OK packet. The sequence ID of this OK packet is not
			// 1, and will cause the session cannot be transferred after this stmt
			// finished.
			// So, the solution is: when server sends 0xFB and the sequence ID of
			// next packet is 3 bigger than last one, the next packet MUST be an
			// OK packet, and the transfer is allowed.
			// Related issue: https://github.com/matrixorigin/mo-cloud/issues/4088
			var mustOK bool
			if !firstCond {
				firstCond = isLoadDataLocalInfileRespPacket(tempBuf)
			} else {
				mustOK = currSeq-lastSeq == 3
				firstCond = false
			}

			inTxn, ok := checkTxnStatus(tempBuf, mustOK)
			if ok {
				p.mu.inTxn = inTxn
			}
			p.tun.trackServerResponse(tempBuf)
			if !p.mu.inTxn && p.tun.transferIntent.Load() && !rotated {
				armSyncTransfer = true
			}
			if len(tempBuf) > 3 {
				lastSeq = int16(tempBuf[3])
			}
		} else {
			if isEmptyPacket(tempBuf) {
				p.logger.Warn("there comes an empty packet from client")
			}
			if !isEmptyPacket(tempBuf) {
				clientRequest = tempBuf
				stopAfterSend = isCmdQuit(tempBuf)
			}
		}
		return false, nil
	}

	started, err := start()
	// If this pipe is started already, return nil directly.
	if started {
		return nil
	}
	if err != nil {
		return err
	}
	defer finish()

	for ctx.Err() == nil {
		if p.name == pipeServerToClient && p.syncTransferArmed {
			if err := p.handleTransferIntent(ctx, peer); err != nil {
				p.logger.Error("failed to transfer connection", zap.Error(err))
			}
		}
		if terminate, err := prepareNextMessage(); err != nil || terminate {
			return err
		}
		if p.name == pipeServerToClient && armSyncTransfer {
			p.syncTransferArmed = peer.tryArmSyncTransfer()
		}
		if p.testHelper.beforeSend != nil {
			p.testHelper.beforeSend()
		}
		if p.name == pipeClientToServer {
			var publish bool
			clientCommit, publish, err = p.claimClientMessage(
				ctx, clientRequest, stopAfterSend)
			if err != nil {
				return err
			}
			if !publish {
				// Asynchronous migration won the publication boundary. Keep the
				// packet in the shared client buffer; the replacement c2s pipe will
				// publish and forward it after migration.
				return nil
			}
		}

		handled, err := p.src.sendTo(p.dst)
		if err != nil {
			if p.name == pipeClientToServer {
				p.finishClientMessageClaim()
			}
			return wrapPipeSendError(p.name, err)
		}
		if p.name == pipeClientToServer {
			if handled {
				if !stopAfterSend {
					p.tun.finishLocallyConsumedRequest()
				}
			} else {
				p.tun.commitClientRequest(clientCommit)
			}
			p.finishClientMessageClaim()
		}
		if stopAfterSend {
			// COM_QUIT is terminal even when another complete packet is already
			// buffered. Reporting a client disconnect lets the owning handler run
			// its normal cleanup after cache publication has completed.
			return withCode(io.EOF, codeClientDisconnect)
		}
	}
	return ctx.Err()
}

// claimClientMessage is the common ownership boundary between c2s and both
// transfer modes. If c2s wins, clientMessageActive blocks transfer admission
// until forwarding/local consumption commits. If synchronous transfer wins,
// c2s waits for the replacement backend. If asynchronous transfer wins, the old
// pipe leaves the message in the shared buffer for its replacement.
func (p *pipe) claimClientMessage(
	ctx context.Context,
	request []byte,
	quit bool,
) (clientRequestCommit, bool, error) {
	for {
		p.mu.Lock()
		if p.mu.paused {
			p.mu.Unlock()
			return clientRequestCommit{}, false, nil
		}
		if done := p.mu.syncTransferDone; done != nil {
			onWait := p.testHelper.onSyncTransferWait
			p.mu.Unlock()
			if onWait != nil {
				onWait()
			}
			select {
			case <-done:
				if err := context.Cause(ctx); err != nil {
					return clientRequestCommit{}, false, err
				}
				continue
			case <-ctx.Done():
				return clientRequestCommit{}, false, context.Cause(ctx)
			}
		}
		p.clientMessageActive.Store(true)
		var commit clientRequestCommit
		if len(request) > 0 {
			if quit {
				p.tun.markExpectedClientQuit()
			} else {
				commit = p.tun.trackClientRequest(request)
			}
		}
		p.mu.Unlock()
		return commit, true, nil
	}
}

func (p *pipe) finishClientMessageClaim() {
	p.clientMessageActive.Store(false)
}

func (p *pipe) finishSyncTransfer() {
	p.mu.Lock()
	if done := p.mu.syncTransferDone; done != nil {
		p.mu.syncTransferDone = nil
		close(done)
	}
	p.mu.Unlock()
}

func (p *pipe) tryArmSyncTransfer() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.closed || p.mu.paused || p.mu.syncTransferDone != nil ||
		p.clientMessageActive.Load() {
		return false
	}
	p.mu.syncTransferDone = make(chan struct{})
	return true
}

func (p *pipe) handleTransferIntent(ctx context.Context, peer *pipe) error {
	if p.tun == nil {
		peer.finishSyncTransfer()
		p.syncTransferArmed = false
		return nil
	}
	err := p.tun.transferSyncWithGate(ctx, peer)
	p.syncTransferArmed = false
	return err
}

// waitReady waits the pip starts up.
func (p *pipe) waitReady(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.mu.started {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if p.mu.closed {
			return errPipeClosed
		}
		p.mu.cond.Wait()
	}
	return nil
}

// seal makes a pipe generation terminal without waiting for its goroutine.
// This is used by COM_QUIT while c2s is synchronously blocked on its event: the
// caller can publish the reset backend knowing the old pipe cannot restart or
// advance to another packet after notification.
func (p *pipe) seal() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.closed = true
	p.mu.paused = true
	if p.mu.inPreRecv && p.src != nil {
		if err := p.src.SetReadDeadline(time.Unix(1, 0)); err != nil {
			return err
		}
	}
	if p.mu.cond != nil {
		p.mu.cond.Broadcast()
	}
	return nil
}

// waitStopped joins a pipe after seal. Unlike pause, a closed pipe is a valid
// terminal state here and can never be restarted by a later generation.
func (p *pipe) waitStopped(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.mu.started {
		return nil
	}
	if p.mu.cond == nil {
		return errPipeClosed
	}
	stopCtxWatcher := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			p.mu.Lock()
			p.mu.cond.Broadcast()
			p.mu.Unlock()
		case <-stopCtxWatcher:
		}
	}()
	defer close(stopCtxWatcher)
	for p.mu.started {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		p.mu.cond.Wait()
	}
	return nil
}

// pause sets paused to true and make the pipe finished, then
// sets paused to false again. When paused, the pipe should stop
// and transfer server connection to a new one then start pipe again.
func (p *pipe) pause(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.closed {
		return errPipeClosed
	}
	defer func() {
		if p.mu.paused {
			p.mu.paused = false
			// Recover the deadline time.
			_ = p.src.SetReadDeadline(time.Time{})
		}
	}()
	// If the context is canceled while waiting on cond.Wait, wake the waiter
	// to re-check ctx.Err and return promptly.
	stopCtxWatcher := make(chan struct{})
	if p.mu.started && p.mu.cond != nil {
		go func() {
			select {
			case <-ctx.Done():
				p.mu.Lock()
				if p.mu.cond != nil {
					p.mu.cond.Broadcast()
				}
				p.mu.Unlock()
			case <-stopCtxWatcher:
			}
		}()
		defer close(stopCtxWatcher)
	}

	for p.mu.started {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		p.mu.paused = true
		// If the net connection is waiting for messages, we force it terminated by
		// set the deadline.
		if p.mu.inPreRecv {
			if err := p.src.SetReadDeadline(time.Unix(1, 0)); err != nil {
				return err
			}
		}
		p.mu.cond.Wait()
	}
	return nil
}

func (p *pipe) safeToTransferLocked() bool {
	return !p.mu.inTxn
}

// txnStatus return if the session is within a transaction.
// first, we consider it as true and check the three conditions:
// 1. SERVER_STATUS_IN_TRANS is not set
// 2. SERVER_QUERY_WAS_SLOW and SERVER_STATUS_NO_GOOD_INDEX_USED is set
func txnStatus(status uint16) bool {
	// assume it is in txn by priority.
	v := true
	if status&frontend.SERVER_QUERY_WAS_SLOW != 0 &&
		status&frontend.SERVER_STATUS_NO_GOOD_INDEX_USED != 0 &&
		status&frontend.SERVER_STATUS_IN_TRANS == 0 {
		v = false
	}
	return v
}

// handleOKPacket handles the OK packet from server to update the txn state.
func handleOKPacket(msg []byte, mustOK bool) bool {
	// if the mustOK is false, then the sequence ID should be 1 for OK packet.
	if !mustOK && msg[3] != 1 {
		return txnStatus(0)
	}
	status, ok := okPacketStatus(msg)
	if !ok {
		return txnStatus(0)
	}
	return txnStatus(status)
}

func okPacketStatus(msg []byte) (uint16, bool) {
	msg = firstMySQLPacketPrefix(msg)
	if !isOKPacket(msg) {
		return 0, false
	}
	var mp *frontend.MysqlProtocolImpl
	pos := 5
	_, pos, ok := mp.ReadIntLenEnc(msg, pos)
	if !ok {
		return 0, false
	}
	_, pos, ok = mp.ReadIntLenEnc(msg, pos)
	if !ok {
		return 0, false
	}
	if len(msg[pos:]) < 2 {
		return 0, false
	}
	return binary.LittleEndian.Uint16(msg[pos:]), true
}

func eofOKPacketStatus(msg []byte) (uint16, bool) {
	msg = firstMySQLPacketPrefix(msg)
	payloadLen := mysqlPacketPayloadLength(msg)
	if len(msg) < 5 || msg[4] != 0xfe || payloadLen < 7 || payloadLen >= 9 {
		return 0, false
	}
	var mp *frontend.MysqlProtocolImpl
	pos := 5
	_, pos, ok := mp.ReadIntLenEnc(msg, pos)
	if !ok {
		return 0, false
	}
	_, pos, ok = mp.ReadIntLenEnc(msg, pos)
	if !ok || len(msg[pos:]) < 2 {
		return 0, false
	}
	return binary.LittleEndian.Uint16(msg[pos:]), true
}

func legacyEOFPacketStatus(msg []byte) (uint16, bool) {
	msg = firstMySQLPacketPrefix(msg)
	if len(msg) < 9 || mysqlPacketPayloadLength(msg) != 5 || msg[4] != 0xfe {
		return 0, false
	}
	return binary.LittleEndian.Uint16(msg[7:9]), true
}

func mysqlPacketPayloadLength(msg []byte) int {
	if len(msg) < mysqlHeadLen {
		return -1
	}
	return int(uint32(msg[0]) | uint32(msg[1])<<8 | uint32(msg[2])<<16)
}

func firstMySQLPacketPrefix(msg []byte) []byte {
	payloadLen := mysqlPacketPayloadLength(msg)
	if payloadLen < 0 {
		return nil
	}
	packetLen := mysqlHeadLen + payloadLen
	if packetLen < len(msg) {
		return msg[:packetLen]
	}
	return msg
}

func prepareMetadataPacketCount(msg []byte, deprecateEOF bool) (uint32, bool) {
	msg = firstMySQLPacketPrefix(msg)
	if len(msg) < 16 || mysqlPacketPayloadLength(msg) < 12 || msg[4] != 0 {
		return 0, false
	}
	columns := uint32(binary.LittleEndian.Uint16(msg[9:11]))
	params := uint32(binary.LittleEndian.Uint16(msg[11:13]))
	remaining := columns + params
	if !deprecateEOF {
		if params > 0 {
			remaining++
		}
		if columns > 0 {
			remaining++
		}
	}
	return remaining, true
}

// handleEOFPacket handles the EOF packet from server to update the txn state.
func handleEOFPacket(msg []byte) bool {
	if len(msg) < 9 {
		return txnStatus(0)
	}
	return txnStatus(binary.LittleEndian.Uint16(msg[7:]))
}

// the first return value is the txn status, and the second return value
// indicates if we can get the txn status from the packet. If it is a ERROR
// packet, the second return value is false.
func checkTxnStatus(msg []byte, mustOK bool) (bool, bool) {
	ok := true
	inTxn := true
	// For the server->client pipe, we get the transaction status from the
	// OK and EOF packet, which is used in connection transfer. If the session
	// is in a transaction, a transfer should not start.
	if isOKPacket(msg) {
		inTxn = handleOKPacket(msg, mustOK)
	} else if isEOFPacket(msg) {
		inTxn = handleEOFPacket(msg)
	} else if isErrPacket(msg) {
		ok = false
	}
	return inTxn, ok
}
