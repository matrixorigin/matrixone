// Copyright 2024 Matrix Origin
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

package spool

type Cursor[T Element] struct {
	spool  *Spool[T]
	last   *node[T]
	next   *node[T]
	closed bool
}

// Next reads the next value
// if no value is available, Next blocks until value is queued
// if cursor is closed or no more values can be queued, returns zero value and false
func (c *Cursor[T]) Next() (ret T, ok bool) {
	if c.closed {
		return ret, false
	}

	for {

		// decrease reference of last node
		if c.last != nil {
			c.last.numCursors.Add(-1)
		}

		// check free
		c.spool.checkFree()

		// wait value ok
		c.next.mu.Lock()
		for !c.next.valueOK {
			c.next.cond.Wait()
		}
		c.next.mu.Unlock()

		// check readability
		skip := false
		if n := c.next.maxConsumer.Add(-1); n < 0 {
			skip = true
		}
		if c.next.target != nil && c.next.target != c {
			skip = true
		}

		// read
		if !skip {
			if c.next.stop {
				ok = false
				c.closed = true
			} else {
				ret = c.next.value
				ok = true
			}
		}

		c.last = c.next
		c.next = c.next.next
		c.next.numCursors.Add(1)

		if !skip {
			return
		}

	}
}

// Peek returns the next value if available
// Peek does not block if no value is available
func (c *Cursor[T]) Peek() (ret T, ok bool) {
	if c.closed {
		return ret, false
	}
	c.next.mu.Lock()
	defer c.next.mu.Unlock()
	if !c.next.valueOK {
		return ret, false
	}
	if c.next.target != nil && c.next.target != c {
		return ret, false
	}
	if c.next.stop {
		return ret, false
	}
	return c.next.value, true
}

// Close closes the cursor
func (c *Cursor[T]) Close() {
	if c.closed {
		return
	}
	c.closed = true

	if c.last != nil {
		c.last.numCursors.Add(-1)
	}

	c.next.numCursors.Add(-1)

	c.spool.checkFree()
}
