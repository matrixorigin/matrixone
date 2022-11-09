// Copyright 2021 - 2022 Matrix Origin
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

package connpool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	// ErrClosed indicates the connection pool is closed.
	ErrClosed = moerr.NewInternalError("connection pool is closed")
	// ErrTimeout indicates the waiting timed out.
	ErrTimeout = moerr.NewInternalError("connection pool timeout")
	// default value for MaxSize.
	defaultMaxSize = 1000
	// default value for WaitTimeout.
	defaultTimeout = 3 * time.Second
)

// Factory is the function which creates underlying connection.
type Factory func() (IConn, error)

// Config is the configuration for connection pool.
type Config struct {
	// MaxSize is the max size of the pool. The pool can only hold
	// MaxSize connections. If the AllowOverUse is false, you have
	// to wait until some other one release a connection; and if
	// AllowOverUse is true, a new connection would be created but
	// keep it outside the pool, which means it is just a temp one,
	// and when Release() is called, it is not put back to the pool.
	MaxSize int
	// InitSize is the connection size to add to the pool when create
	// a new pool.
	InitSize int
	// WaitTimeout sets the time when the connection pool is empty and
	// have to wait. ErrTimeout is returned if there are still no connections
	// after this time. It must be non-zero if the AllowOverUse is false.
	WaitTimeout time.Duration
	// AllowOverUse if true means that if the connection pool is empty,
	// we can still get a connection which is created temporarily and
	// closed directly when Release() is called.
	AllowOverUse bool
	// MaxLifetime is the max lifetime of a connection. If it is non-zero,
	// a connection whose lifetime is larger than this time in the pool
	// will be removed when try to acquire it. And if it is zero, connections
	// have unlimited lifetime.
	MaxLifetime time.Duration
	// MaxIdleTime sets the max idle time of a connection. If it is non-zero,
	// a connection which has not been used for this time in the pool will
	// be removed when try to acquire it. And if it is zero, connections
	// have unlimited idle time.
	MaxIdleTime time.Duration
	// CheckConn is a function when check the availability of a connection.
	CheckConn func(*Conn) bool
	// InitSync inits the pool synchronized. This is for test.
	InitSync bool
}

// ConnPool is connection pool which manage connections. It enables
// connection reuse. After the connection is used, it is not released
// directly, but is returned to the connection pool for the next request.
// It has the following functions:
//
//  1. Initialize and create a certain number of idle connections in advance.
//  2. Acquire method is provides to get a connection.
//  3. If there is an idle connection in the connection pool, directly return it.
//  4. And if there is none, then judge whether the number of connections in the
//     current connection pool exceeds the maximum threshold, if so, check if
//     AllowOverUse is true. If it is true, return a temp connection, otherwise, wait.
//  5. If the maximum threshold is not exceeded, create a new connection and
//     store it in the connection pool.
//  6. Release method is provided to release the connection. If the connection is temp,
//     close it directly. Otherwise, put it back into the pool.
type ConnPool struct {
	cfg     Config
	name    string
	factory Factory
	cond    chan struct{}
	stats   Stats
	// Following fields need mu.
	mu sync.Mutex
	// All connections store in this pool.
	pool   []*Conn
	closed bool
}

// Stats is the statistics of the connection pool.
type Stats struct {
	NumHit     atomic.Int64
	NumMiss    atomic.Int64
	NumTimeout atomic.Int64
}

var _ IConnPool = (*ConnPool)(nil)

// NewConnPool creates a connection pool and initialize the idle pool.
func NewConnPool(cfg Config, name string, f Factory) *ConnPool {
	if f == nil {
		panic("connection pool factory is nil")
	}
	p := &ConnPool{
		cfg:     cfg,
		name:    name,
		factory: f,
		cond:    make(chan struct{}, cfg.MaxSize),
		pool:    make([]*Conn, 0, cfg.MaxSize),
	}
	p.validateConfig()
	if p.cfg.InitSync {
		p.initPoolSync()
		return p
	}
	p.mu.Lock()
	p.initPool()
	p.mu.Unlock()
	return p
}

// validateConfig validates config and amends it.
func (p *ConnPool) validateConfig() {
	if p.cfg.MaxSize <= 0 {
		p.cfg.MaxSize = defaultMaxSize
	}
	if p.cfg.InitSize < 0 {
		p.cfg.InitSize = 0
	}
	if p.cfg.InitSize > p.cfg.MaxSize {
		p.cfg.InitSize = p.cfg.MaxSize
	}
	if !p.cfg.AllowOverUse && p.cfg.WaitTimeout == 0 {
		p.cfg.WaitTimeout = defaultTimeout
	}
}

func (p *ConnPool) initPoolSync() {
	if p.cfg.InitSize == 0 {
		return
	}
	for i := 0; i < p.cfg.InitSize; i++ {
		c, e := p.createConn(true)
		if e == nil && c != nil {
			_ = p.push(c)
		}
	}
}

func (p *ConnPool) initPool() {
	if p.cfg.InitSize == 0 {
		return
	}
	for i := 0; i < p.cfg.InitSize; i++ {
		go func() {
			c, e := p.createConn(true)
			if e == nil && c != nil {
				_ = p.push(c)
			}
		}()
	}
}

// createConn creates a new connection. If pooled is true,
// put it into the pool, otherwise do not.
func (p *ConnPool) createConn(pooled bool) (*Conn, error) {
	c, err := p.factory()
	if err != nil {
		return nil, err
	}
	conn := NewConn(c, pooled)
	return conn, nil
}

// wait until there are connection in the pool. The first
// return value indicates that whether keep the connection in the
// pool.
func (p *ConnPool) wait(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case p.cond <- struct{}{}: // the pool has
		return true, nil
	default:
	}

	// There are no connections in the pool, if p.cfg.AllowOverUse is true,
	// do NOT wait and return false.
	if p.cfg.AllowOverUse {
		return false, nil
	}

	timer := time.NewTimer(p.cfg.WaitTimeout)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		return false, ctx.Err()
	case p.cond <- struct{}{}:
		if !timer.Stop() {
			<-timer.C
		}
		return true, nil
	case <-timer.C:
		p.stats.NumTimeout.Add(1)
		return false, ErrTimeout
	}
}

func (p *ConnPool) signal() {
	<-p.cond
}

// push a connection to the pool.
func (p *ConnPool) push(conn *Conn) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = conn.Close()
		return ErrClosed
	}
	p.pool = append(p.pool, conn)
	return nil
}

// pop a connection from the pool.
func (p *ConnPool) pop() (*Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	l := len(p.pool)
	if l == 0 {
		return nil, nil
	}

	var conn *Conn
	idx := l - 1
	conn = p.pool[idx]
	p.pool = p.pool[:idx]
	return conn, nil
}

// removes a connection from the pool and close the connection.
func (p *ConnPool) removeConnFromPool(conn *Conn) {
	p.mu.Lock()
	p.removeConnLocked(conn)
	p.signal()
	p.mu.Unlock()
	_ = p.closeConn(conn)
}

func (p *ConnPool) removeConnLocked(conn *Conn) {
	for i, c := range p.pool {
		if c == conn {
			p.pool = append(p.pool[:i], p.pool[i+1:]...)
			break
		}
	}
}

// check the connection is active by the config.
func (p *ConnPool) connActive(conn *Conn) bool {
	now := time.Now()
	if p.cfg.MaxLifetime > 0 &&
		now.Sub(conn.createdAt) >= p.cfg.MaxLifetime {
		return false
	}
	if p.cfg.MaxIdleTime > 0 &&
		now.Sub(conn.UsedAt()) >= p.cfg.MaxIdleTime {
		return false
	}
	if p.cfg.CheckConn != nil && !p.cfg.CheckConn(conn) {
		return false
	}
	return true
}

func (p *ConnPool) closeConn(conn *Conn) error {
	return conn.Close()
}

// Acquire implements the IConnPool interface.
func (p *ConnPool) Acquire(ctx context.Context) (*Conn, error) {
	if p.closed {
		return nil, ErrClosed
	}
	pooled, err := p.wait(ctx)
	if err != nil {
		return nil, err
	}
	for {
		conn, err := p.pop()
		if err != nil {
			return nil, err
		}
		// No available conn in pool.
		if conn == nil {
			break
		}

		if !p.connActive(conn) {
			p.removeConnFromPool(conn)
			continue
		}
		conn.Use(time.Now())
		p.stats.NumHit.Add(1)
		return conn, nil
	}

	p.stats.NumMiss.Add(1)
	// Cannot get connection from pool, so create a new one.
	// If pooled is true, put it into the pool, otherwise, do not.
	newConn, err := p.createConn(pooled)
	if err != nil {
		if pooled {
			p.signal()
		}
		return nil, err
	}
	newConn.Use(time.Now())
	return newConn, nil
}

// Release implements the IConnPool interface.
func (p *ConnPool) Release(conn *Conn) {
	if !conn.pooled {
		_ = p.closeConn(conn)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.pool = append(p.pool, conn)
		p.signal()
	}
}

// Len implements the IConnPool interface.
func (p *ConnPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pool)
}

// Close implements the IConnPool interface.
func (p *ConnPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var firstErr error
	for _, c := range p.pool {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if p.cond != nil {
		close(p.cond)
	}
	p.pool = nil
	p.cond = nil
	return firstErr
}

// Stats implements the IConnPool interface.
func (p *ConnPool) Stats() *Stats {
	return &p.stats
}

func (p *ConnPool) String() string {
	return fmt.Sprintf("name: %s, max size: %d, current size: %d, stats: %s",
		p.name, p.cfg.MaxSize, p.Len(), p.stats.String())
}

func (s *Stats) String() string {
	return fmt.Sprintf("NumHit: %d, NumMiss: %d, NumTimeout: %d",
		s.NumHit.Load(), s.NumMiss.Load(), s.NumTimeout.Load())
}
