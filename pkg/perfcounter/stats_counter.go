package perfcounter

import "sync/atomic"

// StatsCounter represents a combination of global & curr counter.
type StatsCounter struct {
	curr   atomic.Int64
	global atomic.Int64
}

// Add adds to curr counter
func (c *StatsCounter) Add(delta int64) {
	c.curr.Add(delta)
}

// Load return the sum of curr and global
func (c *StatsCounter) Load() int64 {
	return c.global.Load() + c.curr.Load()
}

// LoadC loads current counter value
func (c *StatsCounter) LoadC() int64 {
	return c.curr.Load()
}

// MergeAndReset merge the curr to global and reset curr
func (c *StatsCounter) MergeAndReset() {
	c.global.Add(c.curr.Load())
	c.curr.Store(0)
}
