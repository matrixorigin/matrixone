// Copyright 2023 Matrix Origin
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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type cache struct {
	sync.RWMutex
	closed bool
	queue  []Message
}

func newCache() MessageCache {
	return &cache{}
}

func (c *cache) Add(value Message) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return moerr.NewInvalidStateNoCtx("cache is closed")
	}
	c.queue = append(c.queue, value)
	return nil
}

func (c *cache) Len() (int, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed {
		return 0, moerr.NewInvalidStateNoCtx("cache is closed")
	}
	return len(c.queue), nil
}

func (c *cache) Pop() (Message, bool, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, false, moerr.NewInvalidStateNoCtx("cache is closed")
	}

	if len(c.queue) == 0 {
		return nil, false, nil
	}
	v := c.queue[0]
	c.queue[0] = nil
	c.queue = c.queue[1:]
	return v, true, nil
}

func (c *cache) Close() {
	c.Lock()
	defer c.Unlock()
	for idx := range c.queue {
		c.queue[idx] = nil
	}
	c.closed = true
}
