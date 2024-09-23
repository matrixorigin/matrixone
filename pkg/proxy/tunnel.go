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
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
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

	mu struct {
		sync.Mutex
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
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// run starts the tunnel, make the data between client and server flow in it.
func (t *tunnel) run(cc ClientConn, sc ServerConn) error {
	digThrough := func() error {
		t.mu.Lock()
		defer t.mu.Unlock()

		if t.ctx.Err() != nil {
			return t.ctx.Err()
		}
		t.cc = cc
		t.mu.sc = sc
		t.logger = t.logger.With(zap.Uint32("conn ID", cc.ConnID()))
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
			v2.ProxyClientDisconnectCounter.Inc()
			t.setError(withCode(err, codeClientDisconnect))
		}
	}()
	go func() {
		if err := scp.kickoff(t.ctx, csp); err != nil {
			v2.ProxyServerDisconnectCounter.Inc()
			t.setError(withCode(err, codeServerDisconnect))
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
func (t *tunnel) replaceServerConn(newServerConn *MySQLConn, newSC ServerConn, sync bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// close the old ones.
	_ = t.mu.serverConn.Close()
	_ = t.mu.sc.Close()

	// set the new ones.
	t.mu.serverConn = newServerConn
	t.mu.sc = newSC

	if sync {
		t.mu.csp.dst = t.mu.serverConn
		t.mu.scp.src = t.mu.serverConn
	} else {
		t.mu.csp = t.newPipe(pipeClientToServer, t.mu.clientConn, t.mu.serverConn)
		t.mu.scp = t.newPipe(pipeServerToClient, t.mu.serverConn, t.mu.clientConn)
	}
}

// canStartTransfer checks whether the transfer can be started.
func (t *tunnel) canStartTransfer(sync bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	// The tunnel has not started.
	if !t.mu.started {
		return false
	}

	csp, scp := t.mu.csp, t.mu.scp
	csp.mu.Lock()
	scp.mu.Lock()
	defer csp.mu.Unlock()
	defer scp.mu.Unlock()

	// The last message must be from server to client.
	if scp.mu.lastCmdTime.Before(csp.mu.lastCmdTime) {
		t.logger.Info("reason: client packet is after server packet")
		return false
	}

	// We are now in a transaction.
	if !scp.safeToTransferLocked() {
		t.logger.Info("reason: txn status is true")
		return false
	}

	if !sync {
		csp.mu.paused = true
		scp.mu.paused = true
	}

	return true
}

func (t *tunnel) setTransferIntent(i bool) {
	if t.rebalancePolicy == RebalancePolicyPassive &&
		t.getTransferType() == transferByRebalance {
		return
	}
	t.logger.Info("set tunnel transfer intent", zap.Bool("value", i))
	t.transferIntent.Store(i)
	if i {
		v2.ProxyConnectionsTransferIntentGauge.Inc()
	} else {
		v2.ProxyConnectionsTransferIntentGauge.Dec()
	}
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
	t.replaceServerConn(newConn, newSC, sync)
	t.counterSet.connMigrationSuccess.Add(1)
	t.logger.Info("transfer to a new CN server",
		zap.String("addr", newConn.RemoteAddr().String()))
	return nil
}

// transfer transfers the serverConn of tunnel to a new one.
func (t *tunnel) transfer(ctx context.Context) error {
	t.counterSet.connMigrationRequested.Add(1)
	// Must check if it is safe to start the transfer.
	if ok := t.canStartTransfer(false); !ok {
		t.logger.Info("cannot start transfer safely")
		t.setTransferIntent(true)
		t.counterSet.connMigrationCannotStart.Add(1)
		return moerr.GetOkExpectedNotSafeToStartTransfer()
	}

	start := time.Now()
	defer t.finishTransfer(start)
	t.logger.Info("transfer begin")

	ctx, cancel := context.WithTimeout(ctx, defaultTransferTimeout)
	defer cancel()

	csp, scp := t.getPipes()
	// Pause pipes before the transfer.
	if err := csp.pause(ctx); err != nil {
		v2.ProxyTransferFailCounter.Inc()
		return err
	}
	if err := scp.pause(ctx); err != nil {
		v2.ProxyTransferFailCounter.Inc()
		return err
	}
	if err := t.doReplaceConnection(ctx, false); err != nil {
		v2.ProxyTransferFailCounter.Inc()
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
	// Must check if it is safe to start the transfer.
	if ok := t.canStartTransfer(true); !ok {
		return moerr.GetOkExpectedNotSafeToStartTransfer()
	}
	start := time.Now()
	defer t.finishTransfer(start)
	t.logger.Info("transfer begin")
	ctx, cancel := context.WithTimeout(ctx, defaultTransferTimeout)
	defer cancel()
	if err := t.doReplaceConnection(ctx, true); err != nil {
		v2.ProxyTransferFailCounter.Inc()
		return err
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
	newConn, err := t.cc.BuildConnWithServer(prevAddr)
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
		if t.ctxCancel != nil {
			t.ctxCancel()
		}
		// Close the event channels.
		close(t.reqC)
		// close(t.respC)

		cc, sc := t.getConns()
		// cc.Close() just only close the raw net connection, and it
		// is closed in goetty module, so do NOT need to close it here:
		// cc, sc := t.getConns()
		if cc != nil && !t.realConn {
			_ = cc.Close()
		}
		if !t.connCacheEnabled && sc != nil {
			_ = sc.Close()
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

	// this value do not need in mutex as it is read and write in
	// a single goroutine.
	transferred bool

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
		// Track last cmd time and whether we are in a transaction.
		lastCmdTime time.Time
	}

	// tun is the tunnel that the pipe belongs to.
	tun *tunnel

	wg sync.WaitGroup

	testHelper struct {
		beforeSend func()
	}
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
	return p
}

// kickoff starts up the pipe and the data would flow in it.
func (p *pipe) kickoff(ctx context.Context, peer *pipe) (e error) {
	start := func() (bool, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
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
	prepareNextMessage := func() (terminate bool, err error) {
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
		_, re := p.src.preRecv()
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
			if !p.mu.inTxn && p.tun.transferIntent.Load() && !rotated {
				peer.wg.Add(1)
				p.transferred = true
			}
			if len(tempBuf) > 3 {
				lastSeq = int16(tempBuf[3])
			}
			p.mu.lastCmdTime = time.Now()
		} else {
			if isEmptyPacket(tempBuf) {
				p.logger.Warn("there comes an empty packet from client")
			}
			if !isEmptyPacket(tempBuf) && !isDeallocatePacket(tempBuf) {
				p.mu.lastCmdTime = time.Now()
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
		if p.name == pipeServerToClient && p.transferred {
			if err := p.handleTransferIntent(ctx, &peer.wg); err != nil {
				p.logger.Error("failed to transfer connection", zap.Error(err))
			}
		}
		if terminate, err := prepareNextMessage(); err != nil || terminate {
			return err
		}
		if p.testHelper.beforeSend != nil {
			p.testHelper.beforeSend()
		}
		// If the server is in transfer, we wait here until the transfer is finished.
		p.wg.Wait()

		if err = p.src.sendTo(p.dst); err != nil {
			return moerr.NewInternalErrorNoCtxf("send message error: %v", err)
		}
	}
	return ctx.Err()
}

func (p *pipe) handleTransferIntent(ctx context.Context, wg *sync.WaitGroup) error {
	// If it is not in a txn and transfer intent is true, transfer it sync.
	if p.tun != nil && p.safeToTransfer() {
		err := p.tun.transferSync(ctx)
		// we have set transferred back to false, with "wg.Done()" together.
		p.transferred = false
		wg.Done()
		return err
	}
	return nil
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

// pause sets paused to true and make the pipe finished, then
// sets paused to false again. When paused, the pipe should stop
// and transfer server connection to a new one then start pipe again.
func (p *pipe) pause(ctx context.Context) error {
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

// safeToTransfer indicates whether it is safe to transfer the session.
// NB: the pipe MUST be server-to-client pipe.
func (p *pipe) safeToTransfer() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.mu.inTxn
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
	var mp *frontend.MysqlProtocolImpl
	// if the mustOK is false, then the sequence ID should be 1 for OK packet.
	if !mustOK && msg[3] != 1 {
		return txnStatus(0)
	}
	pos := 5
	_, pos, ok := mp.ReadIntLenEnc(msg, pos)
	if !ok {
		return txnStatus(0)
	}
	_, pos, ok = mp.ReadIntLenEnc(msg, pos)
	if !ok {
		return txnStatus(0)
	}
	if len(msg[pos:]) < 2 {
		return txnStatus(0)
	}
	status := binary.LittleEndian.Uint16(msg[pos:])
	return txnStatus(status)
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
