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
	"errors"
	"net"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"go.uber.org/zap"
)

const (
	// The default transfer timeout is 10s.
	defaultTransferTimeout = time.Second * 10
)

var (
	// errPipeClosed indicates that the pipe has been closed.
	errPipeClosed = moerr.NewInternalErrorNoCtx("pipe has been closed")
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

	mu struct {
		sync.Mutex
		// started indicates that the tunnel has started.
		started bool
		// inTransfer means a transfer of server connection is in progress.
		inTransfer bool
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
func newTunnel(ctx context.Context, logger *log.MOLogger) *tunnel {
	ctx, cancel := context.WithCancel(ctx)
	t := &tunnel{
		ctx:       ctx,
		ctxCancel: cancel,
		logger:    logger,
		errC:      make(chan error, 1),
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
		t.mu.clientConn = newMySQLConn("client", cc.RawConn(), 0)
		t.mu.serverConn = newMySQLConn("server", sc.RawConn(), 0)

		// Create the pipes from client to server and server to client.
		t.mu.csp = newPipe("client->server", t.mu.clientConn, t.mu.serverConn)
		t.mu.scp = newPipe("server->client", t.mu.serverConn, t.mu.clientConn)

		return nil
	}

	if err := digThrough(); err != nil {
		return moerr.NewInternalErrorNoCtx("set up tunnel failed: %v", err)
	}
	if err := t.kickoff(); err != nil {
		return moerr.NewInternalErrorNoCtx("kickoff pipe failed: %v", err)
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
		if err := csp.kickoff(t.ctx); err != nil {
			t.setError(err)
		}
	}()
	go func() {
		if err := scp.kickoff(t.ctx); err != nil {
			t.setError(err)
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
func (t *tunnel) replaceServerConn(newServerConn *MySQLConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	_ = t.mu.serverConn.Close()
	t.mu.serverConn = newServerConn
	t.mu.csp = newPipe("client->server", t.mu.clientConn, t.mu.serverConn)
	t.mu.scp = newPipe("server->client", t.mu.serverConn, t.mu.clientConn)
}

// canStartTransfer checks whether the transfer can be started.
func (t *tunnel) canStartTransfer() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	// The tunnel has not started.
	if !t.mu.started {
		return false
	}

	// Another transfer is already in progress.
	if t.mu.inTransfer {
		return false
	}

	csp, scp := t.mu.csp, t.mu.scp
	csp.mu.Lock()
	scp.mu.Lock()
	defer csp.mu.Unlock()
	defer scp.mu.Unlock()

	// The last message must be from server to client.
	if scp.mu.lastCmdTime.Before(csp.mu.lastCmdTime) {
		return false
	}

	// We are now in a transaction.
	if csp.mu.inTxn {
		return false
	}

	// Set the tunnel in transfer and the pipes paused directly.
	t.mu.inTransfer = true
	csp.mu.paused = true
	scp.mu.paused = true
	return true
}

// transfer transfers the serverConn of tunnel to a new one.
func (t *tunnel) transfer(ctx context.Context) error {
	// Must check if it is safe to start the transfer.
	if ok := t.canStartTransfer(); !ok {
		return moerr.NewInternalError(ctx, "not safe to start transfer")
	}
	defer func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.mu.inTransfer = false
	}()

	ctx, cancel := context.WithTimeout(ctx, defaultTransferTimeout)
	defer cancel()

	csp, scp := t.getPipes()
	// Pause pipes before the transfer.
	if err := csp.pause(ctx); err != nil {
		return err
	}
	if err := scp.pause(ctx); err != nil {
		return err
	}
	newConn, err := t.getNewServerConn(ctx)
	if err != nil {
		t.logger.Error("failed to get a new connection", zap.Error(err))
		return err
	}
	t.replaceServerConn(newConn)
	t.logger.Info("transfer to a new CN server",
		zap.String("addr", newConn.RemoteAddr().String()))

	// After replace connections, restart pipes.
	if err := t.kickoff(); err != nil {
		t.logger.Error("failed to kickoff tunnel", zap.Error(err))
		_ = t.Close()
	}
	return nil
}

// getNewServerConn selects a new CN server and connects to it then
// returns the new connection.
func (t *tunnel) getNewServerConn(ctx context.Context) (*MySQLConn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	newConn, err := t.cc.BuildConnWithServer(false)
	if err != nil {
		return nil, err
	}
	return newMySQLConn("server", newConn.RawConn(), 0), nil
}

// Close closes the tunnel.
func (t *tunnel) Close() error {
	if t.ctxCancel != nil {
		t.ctxCancel()
	}

	select {
	case t.errC <- moerr.NewInternalErrorNoCtx("tunnel closed"):
	default:
	}
	cc, sc := t.getConns()
	if cc != nil {
		_ = cc.Close()
	}
	if sc != nil {
		_ = sc.Close()
	}
	return nil
}

// pipe must be created through newPipe.
type pipe struct {
	// name is for debug
	name string

	// source connection and destination connection wrapped
	// by a message buffer.
	src *MySQLConn
	dst *MySQLConn

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

		// Track last cmd time and whether we are in a transaction.
		lastCmdTime time.Time
		inTxn       bool
	}

	testHelper struct {
		beforeSend func()
	}
}

// newPipe creates a pipe.
func newPipe(name string, src, dst *MySQLConn) *pipe {
	p := &pipe{
		name: name,
		src:  src,
		dst:  dst,
	}
	p.mu.cond = sync.NewCond(&p.mu)
	return p
}

// kickoff starts up the pipe and the data would flow in it.
func (p *pipe) kickoff(ctx context.Context) (e error) {
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
		_, txn, re := p.src.preRecv()
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
			return false, moerr.NewInternalErrorNoCtx("preRecv message: %s", re.Error())
		}
		p.mu.lastCmdTime = time.Now()
		if txn == txnBegin {
			p.mu.inTxn = true
		} else if txn == txnEnd {
			p.mu.inTxn = false
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
		if terminate, err := prepareNextMessage(); err != nil || terminate {
			return err
		}
		if p.testHelper.beforeSend != nil {
			p.testHelper.beforeSend()
		}
		if _, err := p.src.sendTo(p.dst); err != nil {
			return moerr.NewInternalErrorNoCtx("send message error:: %v", err)
		}
	}
	return ctx.Err()
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
