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

package morpc

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type testStream struct {
	id uint64
	c  chan Message
}

func NewTestStream(id uint64) Stream {
	return &testStream{
		id: id,
		c:  make(chan Message),
	}
}

func (ts *testStream) ID() uint64 { return ts.id }

func (ts *testStream) Send(ctx context.Context, request Message, opts WriteOptions) error {
	ts.c <- request
	return nil
}

func (ts *testStream) Resume(ctx context.Context) error {
	return nil
}

func (ts *testStream) Receive() (chan Message, error) {
	return ts.c, nil
}

func (ts *testStream) Close() error {
	return nil
}

type testClientSession struct {
	sync.RWMutex
	closed bool
	c      chan Message
	caches map[uint64]cacheWithContext
}

func NewTestClientSession() (ClientSession, chan Message) {
	cs := &testClientSession{
		c:      make(chan Message, 1024),
		caches: make(map[uint64]cacheWithContext),
	}
	return cs, cs.c
}

func (cs *testClientSession) Close() error {
	cs.Lock()
	defer cs.Unlock()
	cs.closed = true
	return nil
}

func (cs *testClientSession) Write(
	ctx context.Context,
	response Message,
	opts WriteOptions,
) error {
	cs.RLock()
	defer cs.RUnlock()
	if cs.closed {
		return moerr.NewClientClosedNoCtx()
	}
	cs.c <- response
	return nil
}

func (cs *testClientSession) CreateCache(
	ctx context.Context,
	cacheID uint64,
) (MessageCache, error) {
	cs.Lock()
	defer cs.Unlock()

	if cs.closed {
		return nil, moerr.NewClientClosedNoCtx()
	}

	v, ok := cs.caches[cacheID]
	if !ok {
		v = cacheWithContext{cache: newCache(), ctx: ctx}
		cs.caches[cacheID] = v
	}
	return v.cache, nil
}

func (cs *testClientSession) DeleteCache(cacheID uint64) {
	cs.Lock()
	defer cs.Unlock()

	if cs.closed {
		return
	}
	if c, ok := cs.caches[cacheID]; ok {
		c.cache.Close()
		delete(cs.caches, cacheID)
	}
}

func (cs *testClientSession) GetCache(cacheID uint64) (MessageCache, error) {
	cs.RLock()
	defer cs.RUnlock()

	if cs.closed {
		return nil, moerr.NewClientClosedNoCtx()
	}

	if c, ok := cs.caches[cacheID]; ok {
		return c.cache, nil
	}
	return nil, nil
}

func (cs *testClientSession) RemoteAddress() string {
	return ""
}
