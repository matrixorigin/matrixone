package morpc

import (
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type localMessage struct {
	id uint64
	v  any
}

var (
	local  = newLocalRegister()
	outBuf = buf.NewByteBuf(0)
)

type localRegister struct {
	sync.RWMutex
	id               atomic.Uint64
	sessionRegisters map[string]func(goetty.IOSession) (chan localMessage, func(uint64))
}

func newLocalRegister() *localRegister {
	l := &localRegister{
		sessionRegisters: make(map[string]func(goetty.IOSession) (chan localMessage, func(uint64))),
	}
	l.id.Store(math.MaxUint64)
	return l
}

func (l *localRegister) isLocal(address string) bool {
	l.RLock()
	defer l.RUnlock()
	_, ok := l.sessionRegisters[address]
	return ok
}

func (l *localRegister) registerServer(
	address string,
	register func(goetty.IOSession) (chan localMessage, func(uint64))) {
	l.Lock()
	defer l.Unlock()
	l.sessionRegisters[address] = register
}

func (l *localRegister) registerLocal(
	address string,
	conn goetty.IOSession) (chan localMessage, func(uint64)) {
	l.Lock()
	defer l.Unlock()
	return l.sessionRegisters[address](conn)
}

func (l *localRegister) nextID() uint64 {
	return l.id.Add(^uint64(0))
}

type channelBasedIOSession struct {
	sync.RWMutex
	id         uint64
	address    string
	connected  bool
	input      chan any
	output     chan localMessage
	unregister func(uint64)
}

func newChannelBasedIOSession(address string) goetty.IOSession {
	return &channelBasedIOSession{
		address: address,
	}
}

func (cs *channelBasedIOSession) ID() uint64 {
	return cs.id
}

func (cs *channelBasedIOSession) Connect(
	addr string,
	timeout time.Duration) error {
	cs.Lock()
	defer cs.Unlock()

	if cs.connected {
		return nil
	}

	cs.id = local.nextID()
	cs.input = make(chan any, 16)
	cs.output, cs.unregister = local.registerLocal(cs.address, newChannelBasedIOSessionPeer(cs.id, cs))
	cs.connected = true
	return nil
}

func (cs *channelBasedIOSession) Connected() bool {
	cs.RLock()
	defer cs.RUnlock()

	return cs.connected
}

func (cs *channelBasedIOSession) Disconnect() error {
	return cs.Close()
}

func (cs *channelBasedIOSession) Close() error {
	cs.Lock()
	defer cs.Unlock()

	if cs.connected {
		cs.id = 0
		cs.connected = false
		cs.unregister(cs.id)
		close(cs.input)
	}
	return nil
}

func (cs *channelBasedIOSession) Read(option goetty.ReadOptions) (any, error) {
	cs.RLock()
	c := cs.input
	cs.RUnlock()

	if c == nil {
		return nil, moerr.NewBackendClosedNoCtx()
	}

	v, ok := <-c
	if !ok {
		return nil, moerr.NewBackendClosedNoCtx()
	}
	return v, nil
}

func (cs *channelBasedIOSession) Write(
	msg any,
	options goetty.WriteOptions) error {
	cs.RLock()
	defer cs.RUnlock()

	c := cs.output
	if c == nil {
		return moerr.NewBackendClosedNoCtx()
	}

	c <- localMessage{
		id: cs.id,
		v:  msg,
	}
	return nil
}

func (cs *channelBasedIOSession) Flush(timeout time.Duration) error {
	return nil
}

func (cs *channelBasedIOSession) RemoteAddress() string {
	return "local-io-session"
}

func (cs *channelBasedIOSession) RawConn() net.Conn {
	panic("invalid operation")
}

func (cs *channelBasedIOSession) UseConn(net.Conn) {
	panic("invalid operation")
}

func (cs *channelBasedIOSession) OutBuf() *buf.ByteBuf {
	return outBuf
}

func (cs *channelBasedIOSession) Ref() {
	panic("invalid operation")
}

type channelBasedIOSessionPeer struct {
	id   uint64
	peer *channelBasedIOSession
}

func newChannelBasedIOSessionPeer(id uint64, peer *channelBasedIOSession) goetty.IOSession {
	return &channelBasedIOSessionPeer{
		id:   id,
		peer: peer,
	}
}

func (cs *channelBasedIOSessionPeer) ID() uint64 {
	return cs.id
}

func (cs *channelBasedIOSessionPeer) Close() error {
	return nil
}

func (cs *channelBasedIOSessionPeer) Write(
	msg any,
	options goetty.WriteOptions) error {
	cs.peer.RLock()
	defer cs.peer.RUnlock()
	if cs.peer.id != cs.id {
		return moerr.NewBackendClosedNoCtx()
	}
	cs.peer.input <- msg
	return nil
}

func (cs *channelBasedIOSessionPeer) Flush(timeout time.Duration) error {
	return nil
}

func (cs *channelBasedIOSessionPeer) RemoteAddress() string {
	return "local-io-session"
}

func (cs *channelBasedIOSessionPeer) Connect(
	addr string,
	timeout time.Duration) error {
	panic("invalid operation")
}

func (cs *channelBasedIOSessionPeer) RawConn() net.Conn {
	panic("invalid operation")
}

func (cs *channelBasedIOSessionPeer) UseConn(net.Conn) {
	panic("invalid operation")
}

func (cs *channelBasedIOSessionPeer) OutBuf() *buf.ByteBuf {
	return outBuf
}

func (cs *channelBasedIOSessionPeer) Ref() {

}

func (cs *channelBasedIOSessionPeer) Read(option goetty.ReadOptions) (any, error) {
	panic("invalid operation")
}

func (cs *channelBasedIOSessionPeer) Connected() bool {
	panic("invalid operation")
}

func (cs *channelBasedIOSessionPeer) Disconnect() error {
	panic("invalid operation")
}
