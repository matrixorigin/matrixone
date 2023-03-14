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

// Counter represents a combination of global & local counter.
type Counter struct {
	local  atomic.Int64
	global atomic.Int64
}

// Add adds to local counter
func (c *Counter) Add(delta int64) (new int64) {
	return c.local.Add(delta)
}

// Load return the sum of local and global
func (c *Counter) Load() int64 {
	return c.global.Load() + c.local.Load()
}

// LoadC returns current local counter value.
// TODO: Right now, only user of LoadC is LogExporter. Hence, MergeAndReset is also embedded here. It can be seperated later if required.
func (c *Counter) LoadC() int64 {
	val := c.local.Load()

	{
		// Merge and Reset
		c.global.Add(val)
		c.local.Store(0)
	}

	return val
}
