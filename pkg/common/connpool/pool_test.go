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
	"sync"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
)

type MockConn struct {
	closed bool
}

func (c *MockConn) Close() error {
	c.closed = true
	return nil
}

var mf Factory = func() (IConn, error) {
	return &MockConn{}, nil
}

func TestInitConnPool(t *testing.T) {
	cfg := Config{
		MaxSize:  20,
		InitSize: 20,
		InitSync: true,
	}
	p := NewConnPool(cfg, "test", mf)
	assert.NotNil(t, p)
	assert.Equal(t, 20, p.Len())
}

func TestInvalidCfg(t *testing.T) {
	cfg := Config{
		MaxSize:  5,
		InitSize: 10,
		InitSync: true,
	}
	p := NewConnPool(cfg, "test", mf)
	assert.NotNil(t, p)
	assert.Equal(t, 5, p.Len())
}

func TestCanAcquireConn(t *testing.T) {
	cfg := Config{
		MaxSize:  10,
		InitSize: 5,
		InitSync: true,
	}
	p := NewConnPool(cfg, "test", mf)
	assert.NotNil(t, p)
	ctx := context.TODO()
	for i := 0; i < 5; i++ {
		c, err := p.Acquire(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, c)
	}
	assert.Equal(t, 0, p.Len())
	c1, err := p.Acquire(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, c1)
	assert.Equal(t, 0, p.Len())
	p.Release(c1)
	assert.Equal(t, 1, p.Len())
}

func TestAcquireConnTimeout(t *testing.T) {
	cfg := Config{
		MaxSize:     5,
		InitSize:    5,
		WaitTimeout: time.Millisecond * 100,
		InitSync:    true,
	}
	p := NewConnPool(cfg, "test", mf)
	assert.NotNil(t, p)
	ctx := context.TODO()
	for i := 0; i < 5; i++ {
		c, err := p.Acquire(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, c)
	}
	_, err := p.Acquire(ctx)
	assert.ErrorIs(t, err, ErrTimeout)
}

func TestAcquireConnOverUse(t *testing.T) {
	cfg := Config{
		MaxSize:      2,
		InitSize:     2,
		WaitTimeout:  time.Millisecond * 100,
		AllowOverUse: true,
		InitSync:     true,
	}
	p := NewConnPool(cfg, "test", mf)
	assert.NotNil(t, p)
	ctx := context.TODO()
	c1, err := p.Acquire(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, c1)
	c2, err := p.Acquire(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, c2)
	c3, err := p.Acquire(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, c3)
	p.Release(c3)
	assert.Equal(t, 0, p.Len())
	mc3 := c3.conn.(*MockConn)
	assert.Equal(t, true, mc3.closed)
	p.Release(c2)
	assert.Equal(t, 1, p.Len())
	mc2 := c2.conn.(*MockConn)
	assert.Equal(t, false, mc2.closed)
	p.Release(c1)
	assert.Equal(t, 2, p.Len())
	mc1 := c1.conn.(*MockConn)
	assert.Equal(t, false, mc1.closed)
}

func TestConnPoolClosed(t *testing.T) {
	cfg := Config{
		MaxSize:      2,
		InitSize:     2,
		WaitTimeout:  time.Millisecond * 100,
		AllowOverUse: true,
		InitSync:     true,
	}
	p := NewConnPool(cfg, "test", mf)
	assert.NotNil(t, p)
	ctx := context.TODO()
	c1, err := p.Acquire(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, c1)
	err = p.Close()
	assert.NoError(t, err)
	p.Release(c1)
	c2, err := p.Acquire(ctx)
	assert.ErrorIs(t, err, ErrClosed)
	assert.Nil(t, c2)
}

func TestAcquireConcurrently(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := Config{
		MaxSize:     10,
		InitSize:    5,
		WaitTimeout: time.Millisecond * 100,
		InitSync:    true,
	}
	p := NewConnPool(cfg, "test", mf)
	assert.NotNil(t, p)
	assert.Equal(t, 5, p.Len())
	ctx := context.TODO()
	var e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15 error
	var c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15 *Conn
	conns := []*Conn{c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15}
	errs := []error{e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15}
	var wg sync.WaitGroup
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func(idx int) {
			conns[idx], errs[idx] = p.Acquire(ctx)
			assert.NoError(t, errs[idx])
			assert.NotNil(t, conns[idx])
			p.Release(conns[idx])
			wg.Done()
		}(i)
	}
	wg.Wait()
}
