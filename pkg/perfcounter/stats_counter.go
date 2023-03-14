package perfcounter

import "sync/atomic"

// StatsCounter represents a combination of global & local counter.
type StatsCounter struct {
	local  atomic.Int64
	global atomic.Int64
}

// Add adds to local counter
func (c *StatsCounter) Add(delta int64) (new int64) {
	return c.local.Add(delta)
}

// Load return the sum of local and global
func (c *StatsCounter) Load() int64 {
	return c.global.Load() + c.local.Load()
}

// LoadC loads local counter value
func (c *StatsCounter) LoadC() int64 {
	return c.local.Load()
}

// MergeAndReset merge the local to global and reset local
func (c *StatsCounter) MergeAndReset() (new int64) {
	new = c.global.Add(c.local.Load())
	c.local.Store(0)
	return
}
