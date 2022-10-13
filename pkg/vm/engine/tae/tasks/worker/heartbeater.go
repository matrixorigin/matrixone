// Copyright 2021 Matrix Origin
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

package ops

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker/base"
)

type lamdaHandle struct {
	onExec func()
	onStop func()
}

func (h *lamdaHandle) OnExec() {
	if h.onExec != nil {
		h.onExec()
	}
}

func (h *lamdaHandle) OnStopped() {
	if h.onStop != nil {
		h.onStop()
	}
}

type heartbeater struct {
	handle   base.IHBHandle
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	wg       *sync.WaitGroup
}

func NewHeartBeaterWithFunc(interval time.Duration, onExec, onStop func()) *heartbeater {
	h := &lamdaHandle{onExec: onExec, onStop: onStop}
	return NewHeartBeater(interval, h)
}

func NewHeartBeater(interval time.Duration, handle base.IHBHandle) *heartbeater {
	c := &heartbeater{
		interval: interval,
		handle:   handle,
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

func (c *heartbeater) Start() {
	c.wg = &sync.WaitGroup{}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.interval)
		for {
			select {
			case <-c.ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				c.handle.OnExec()
			}
		}
	}()
}

func (c *heartbeater) Stop() {
	c.cancel()
	c.wg.Wait()
	c.handle.OnStopped()
}
