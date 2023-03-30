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

package stats

import "sync/atomic"

// Counter represents a combination of global & current_window counter.
type Counter struct {
	window atomic.Int64
	global atomic.Int64
}

// Add adds to global and window counter
func (c *Counter) Add(delta int64) (new int64) {
	c.window.Add(delta)
	return c.global.Add(delta)
}

// Load return the global counter value
func (c *Counter) Load() int64 {
	return c.global.Load()
}

// SwapW swaps current window counter with new
func (c *Counter) SwapW(new int64) int64 {
	return c.window.Swap(new)
}

// LoadW returns current window value
func (c *Counter) LoadW() int64 {
	return c.window.Load()
}
