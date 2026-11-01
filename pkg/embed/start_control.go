// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embed

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// startControl lets shutdown cancel a start before taking its lifecycle lock.
// A new generation cannot start until every concurrent shutdown has finished.
type startControl struct {
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	stopping int
}

func (c *startControl) begin(parent context.Context) (context.Context, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil || c.stopping != 0 {
		return nil, moerr.NewInvalidStateNoCtx("startup or shutdown already in progress")
	}
	c.ctx, c.cancel = context.WithCancel(parent)
	return c.ctx, nil
}

func (c *startControl) finish(ctx context.Context, err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx == ctx {
		c.cancel()
		c.ctx, c.cancel = nil, nil
	}
}

func (c *startControl) context() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping != 0 && c.ctx == nil {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

func (c *startControl) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopping++
	if c.cancel != nil {
		c.cancel()
	}
}

func (c *startControl) stopped() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopping--
	if c.stopping == 0 {
		c.ctx, c.cancel = nil, nil
	}
}
